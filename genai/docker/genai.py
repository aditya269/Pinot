#!/usr/bin/env python3

import json
import os
import re
from pathlib import Path
from urllib.request import urlopen

from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_community.document_loaders import DataFrameLoader
import matplotlib.pyplot as plt
import pandas as pd
from pinotdb import connect
from openai import OpenAI

PROMPT_TEMPLATE = """
Answer the question based only on the following context:

{context}

---

Answer the question based on the above context: {question}
"""

PINOT_SQL_PROMPT = """
You write Apache Pinot SQL for these tables.

{table_catalog}

Join rule:
{join_guidance}

Rules:
- Only generate a single SELECT query.
- Never use INSERT, UPDATE, DELETE, DROP, or ALTER.
- Prefer simple Pinot-compatible SQL.
- If the user asks about "class", treat that as the `grade` column when the `student` table exists.
- Return only SQL, with no explanation and no markdown fences.

Question: {question}
"""

PINOT_ANSWER_PROMPT = """
You are answering an end user.
Use only the SQL result below.
Do not write SQL.
Do not mention tables, joins, or query generation.
Write a short, clear natural-language answer.
If the result is a single value or single row, state it directly.

SQL result:

{context}

Question: {question}
"""

PINOT_CHART_ANSWER_PROMPT = """
You already have the SQL result and a chart has been generated from it.
Do not say that you cannot create or show a chart.
Do not write SQL.
Do not mention tables, joins, or query generation.
Briefly summarize the chart insight in plain English using only this SQL result.

SQL result:
{context}

Question: {question}
"""

CHART_KEYWORDS = ("chart", "plot", "graph", "visualize", "visualization")
OUTPUT_DIR = Path(os.environ.get("OUTPUT_DIR", "/tmp/genai-output"))
PIE_KEYWORDS = ("pie", "donut")
LINE_KEYWORDS = ("line", "trend")
SCATTER_KEYWORDS = ("scatter", "relationship", "correlation", "vs", "versus")
HISTOGRAM_KEYWORDS = ("histogram", "distribution")


def build_openai_client():
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError(
            "OPENAI_API_KEY is not set. Export it before running `make question`."
        )

    return OpenAI(api_key=api_key)

class PinotVector():
    def __init__(self, host, port=8099, path='/query/sql', scheme='http', model='text-embedding-ada-002') -> None:
        self.conn = connect(host=host, port=port, path=path, scheme=scheme)
        self.client = build_openai_client()
        self.model = model

    def get_embedding(self, text):
        text = text.replace("\n", " ")
        return self.client.embeddings.create(input = [text], model=self.model).data[0].embedding
    
    def similarity_search(self, query_text:str, dist:int=.3, limit:int=5):

        search_embedding = self.get_embedding(query_text)
        
        curs = self.conn.cursor()
        sql = f"""
            with DIST as (
                SELECT 
                    source, 
                    content, 
                    metadata,
                    cosine_distance(embedding, ARRAY{search_embedding}) AS cosine
                from documentation
                where VECTOR_SIMILARITY(embedding, ARRAY{search_embedding}, 10)
            )
            select * from DIST
            where cosine < {dist}
            order by cosine asc
            limit {limit}
            """
        
        curs.execute(sql, queryOptions="useMultistageEngine=true")
        df = pd.DataFrame(curs, columns=[item[0] for item in curs.description])
        loader = DataFrameLoader(df, page_content_column="content")
        return loader.load()


class PinotStudent:
    def __init__(
        self,
        host,
        port=8099,
        path="/query/sql",
        scheme="http",
        controller_host="pinot-controller",
        controller_port=9000,
    ) -> None:
        self.conn = connect(host=host, port=port, path=path, scheme=scheme)
        self.model = ChatOpenAI()
        self.controller_host = controller_host
        self.controller_port = controller_port
        self.allowed_tables, self.table_catalog = self._load_table_catalog()

    def _fetch_json(self, path: str) -> dict:
        url = f"http://{self.controller_host}:{self.controller_port}{path}"
        with urlopen(url) as response:
            return json.load(response)

    def _extract_schema_name(self, table_config: dict, table_name: str) -> str:
        if "OFFLINE" in table_config and table_config["OFFLINE"]:
            return table_config["OFFLINE"]["segmentsConfig"]["schemaName"]
        if "REALTIME" in table_config and table_config["REALTIME"]:
            return table_config["REALTIME"]["segmentsConfig"]["schemaName"]
        if "segmentsConfig" in table_config:
            return table_config["segmentsConfig"]["schemaName"]
        raise RuntimeError(f"Unable to find schema name for table {table_name}")

    def _build_join_guidance(self, tables: set[str]) -> str:
        guidance = []
        if {"student", "student_exam"}.issubset(tables):
            guidance.append(
                "- When a question needs both student details and exam details, join `student s` with `student_exam se` on `s.student_id = se.student_id`."
            )
        if {"student", "student_fees"}.issubset(tables):
            guidance.append(
                "- When a question needs both student details and fee details, join `student s` with `student_fees sf` on `s.student_id = sf.student_id`."
            )
        if {"student", "student_exam", "student_fees"}.issubset(tables):
            guidance.append(
                "- When a question needs exam details and fee details together, join through `student` using `student_exam se`, `student s`, and `student_fees sf`."
            )
        if "cdr_data" in tables and "student" in tables:
            guidance.append(
                "- Do not join `cdr_data c` to student tables unless the user explicitly asks and the join key is clear from the question."
            )
        guidance.append("- When a question can be answered from a single table, do not join.")
        guidance.append("- Use short aliases when helpful, for example `s`, `se`, `sf`, or `c`.")
        return "\n".join(guidance)

    def _load_table_catalog(self) -> tuple[set[str], str]:
        excluded = {
            name.strip().lower()
            for name in os.environ.get("PINOT_EXCLUDED_TABLES", "documentation").split(",")
            if name.strip()
        }
        table_names = self._fetch_json("/tables").get("tables", [])
        allowed_tables = []
        table_sections = []
        for table_name in sorted(table_names):
            if table_name.lower() in excluded:
                continue
            table_config = self._fetch_json(f"/tables/{table_name}")
            schema_name = self._extract_schema_name(table_config, table_name)
            schema = self._fetch_json(f"/schemas/{schema_name}")
            field_specs = []
            for group_name in ("dimensionFieldSpecs", "metricFieldSpecs", "dateTimeFieldSpecs"):
                for field in schema.get(group_name, []):
                    field_specs.append(f"- {field['name']} {field['dataType']}")
            if not field_specs:
                continue
            allowed_tables.append(table_name)
            table_sections.append(f"Table `{table_name}`:\n" + "\n".join(field_specs))

        allowed_table_set = set(allowed_tables)
        if not allowed_table_set:
            raise RuntimeError("No Pinot tables were discovered for query mode.")
        table_catalog = "\n\n".join(table_sections)
        return allowed_table_set, table_catalog

    def _clean_sql(self, response_text: str) -> str:
        sql = response_text.strip()
        sql = re.sub(r"^```sql\s*", "", sql, flags=re.IGNORECASE)
        sql = re.sub(r"^```\s*", "", sql)
        sql = re.sub(r"\s*```$", "", sql)
        return sql.strip().rstrip(";")

    def generate_sql(self, question: str) -> str:
        prompt_template = ChatPromptTemplate.from_template(PINOT_SQL_PROMPT)
        join_guidance = self._build_join_guidance(self.allowed_tables)
        prompt = prompt_template.format(
            table_catalog=self.table_catalog,
            join_guidance=join_guidance,
            question=question,
        )
        response = self.model.invoke(prompt)
        sql = self._clean_sql(response.content)
        if not sql.lower().startswith("select"):
            raise RuntimeError(f"Refusing to run non-SELECT SQL: {sql}")
        if not references_allowed_tables(sql, self.allowed_tables):
            raise RuntimeError(f"Refusing to run SQL against unexpected tables: {sql}")
        return sql

    def run_query(self, sql: str) -> pd.DataFrame:
        curs = self.conn.cursor()
        curs.execute(sql, queryOptions="useMultistageEngine=true")
        return pd.DataFrame(curs, columns=[item[0] for item in curs.description])

    def answer_question(self, question: str):
        sql = self.generate_sql(question)
        df = self.run_query(sql)
        if df.empty:
            return sql, "No matching rows were found.", df, None

        context_text = df.to_string(index=False)
        chart_path = None
        if wants_chart(question):
            chart_path = render_chart(df, question)
            prompt_template = ChatPromptTemplate.from_template(PINOT_CHART_ANSWER_PROMPT)
        else:
            prompt_template = ChatPromptTemplate.from_template(PINOT_ANSWER_PROMPT)

        prompt = prompt_template.format(context=context_text, question=question)
        response = self.model.invoke(prompt)
        return sql, response.content, df, chart_path


def wants_chart(question: str) -> bool:
    lowered = question.lower()
    return any(keyword in lowered for keyword in CHART_KEYWORDS)


def references_allowed_tables(sql: str, allowed_tables: set[str]) -> bool:
    lowered = sql.lower()
    referenced = re.findall(r"\b(?:from|join)\s+([a-zA-Z_][a-zA-Z0-9_]*)", lowered)
    return all(table in allowed_tables for table in referenced)


def pick_chart_columns(df: pd.DataFrame):
    if df.shape[1] < 2:
        raise RuntimeError("Need at least two columns in the SQL result to plot a chart.")

    x_col = df.columns[0]
    numeric_columns = [col for col in df.columns[1:] if pd.api.types.is_numeric_dtype(df[col])]
    if not numeric_columns:
        raise RuntimeError("Need at least one numeric column in the SQL result to plot a chart.")

    return x_col, numeric_columns[0]


def pick_scatter_columns(df: pd.DataFrame):
    numeric_columns = [col for col in df.columns if pd.api.types.is_numeric_dtype(df[col])]
    if len(numeric_columns) < 2:
        raise RuntimeError("Need at least two numeric columns in the SQL result for a scatter plot.")
    return numeric_columns[0], numeric_columns[1]


def pick_histogram_column(df: pd.DataFrame):
    numeric_columns = [col for col in df.columns if pd.api.types.is_numeric_dtype(df[col])]
    if not numeric_columns:
        raise RuntimeError("Need at least one numeric column in the SQL result for a histogram.")
    return numeric_columns[0]


def detect_chart_type(question: str) -> str:
    lowered = question.lower()
    if any(keyword in lowered for keyword in SCATTER_KEYWORDS):
        return "scatter"
    if any(keyword in lowered for keyword in HISTOGRAM_KEYWORDS):
        return "histogram"
    if any(keyword in lowered for keyword in PIE_KEYWORDS):
        return "pie"
    if any(keyword in lowered for keyword in LINE_KEYWORDS):
        return "line"
    return "bar"


def sanitize_filename(text: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", "_", text.strip().lower()).strip("_")
    return slug or "student_chart"


def render_chart(df: pd.DataFrame, question: str) -> str:
    chart_type = detect_chart_type(question)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_DIR / f"{sanitize_filename(question)}.png"

    plt.figure(figsize=(10, 6))
    if chart_type == "scatter":
        x_col, y_col = pick_scatter_columns(df)
        plt.scatter(df[x_col], df[y_col], color="#2f6db3")
        plt.xlabel(x_col)
        plt.ylabel(y_col)
    elif chart_type == "histogram":
        value_col = pick_histogram_column(df)
        plt.hist(df[value_col], bins=min(10, max(3, len(df))), color="#2f6db3", edgecolor="white")
        plt.xlabel(value_col)
        plt.ylabel("count")
    else:
        x_col, y_col = pick_chart_columns(df)
        if chart_type == "pie":
            plt.pie(df[y_col], labels=df[x_col].astype(str), autopct="%1.1f%%", startangle=90)
        elif chart_type == "line":
            plt.plot(df[x_col].astype(str), df[y_col], marker="o", color="#2f6db3")
            plt.xlabel(x_col)
            plt.ylabel(y_col)
            plt.xticks(rotation=30, ha="right")
        else:
            plt.bar(df[x_col].astype(str), df[y_col], color="#2f6db3")
            plt.xlabel(x_col)
            plt.ylabel(y_col)
            plt.xticks(rotation=30, ha="right")

    plt.title(question)
    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close()

    return str(output_path)


def run_documentation_mode():
    db = PinotVector(host="pinot-broker")
    while True:
        query_text = input("\nAsk a question: ")
        if query_text == "stop":
            break

        results = db.similarity_search(query_text, dist=.5)
        if len(results) == 0:
            print("Unable to find matching results.")
            continue

        context_text = "\n\n---\n\n".join([doc.page_content for doc in results])
        prompt_template = ChatPromptTemplate.from_template(PROMPT_TEMPLATE)
        prompt = prompt_template.format(context=context_text, question=query_text)

        model = ChatOpenAI()
        response_text = model.invoke(prompt)

        sources = [doc.metadata.get("source", None) for doc in results]
        print("response:                                                                     ")
        print(f"{response_text.content} \n")
        [print(f" - {source}") for source in sources]


def run_student_mode():
    db = PinotStudent(host="pinot-broker")
    while True:
        query_text = input("\nAsk a question: ")
        if query_text == "stop":
            break

        try:
            sql, answer, df, chart_path = db.answer_question(query_text)
        except Exception as exc:
            print(f"Unable to answer question: {exc}")
            continue

        print("sql:")
        print(f"{sql}\n")
        print("response:")
        print(f"{answer}\n")
        print(df.to_string(index=False))
        if chart_path:
            print(f"\nchart:\nSaved chart to {chart_path}")


if __name__ == "__main__":
    query_mode = os.environ.get("QUERY_MODE", "pinot").strip().lower()
    if query_mode in ("student", "pinot"):
        run_student_mode()
    else:
        run_documentation_mode()
