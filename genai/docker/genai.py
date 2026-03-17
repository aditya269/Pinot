#!/usr/bin/env python3

import os
import re
from pathlib import Path

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

STUDENT_SQL_PROMPT = """
You write Apache Pinot SQL for these tables.

Table `student`:
- student_id INT
- name STRING
- age INT
- grade STRING
- department STRING
- city STRING
- gpa FLOAT
- attendance_pct FLOAT

Table `student_exam`:
- exam_record_id INT
- student_id INT
- subject STRING
- exam_name STRING
- semester STRING
- marks FLOAT
- max_marks INT
- exam_date STRING

Table `student_fees`:
- fee_record_id INT
- student_id INT
- fee_type STRING
- semester STRING
- payment_status STRING
- total_fee FLOAT
- paid_amount FLOAT
- due_amount FLOAT
- payment_date STRING

Join rule:
- When a question needs both student details and exam details, join `student s` with `student_exam se` on `s.student_id = se.student_id`.
- When a question needs both student details and fee details, join `student s` with `student_fees sf` on `s.student_id = sf.student_id`.
- When a question needs exam details and fee details together, join through `student` using `student_exam se`, `student s`, and `student_fees sf`.
- When the question can be answered from a single table, do not join.

Rules:
- Only generate a single SELECT query.
- Never use INSERT, UPDATE, DELETE, DROP, or ALTER.
- Prefer simple Pinot-compatible SQL.
- If the user asks about "class", treat that as the `grade` column.
- Use table aliases `s` for `student`, `se` for `student_exam`, and `sf` for `student_fees` when joining.
- Return only SQL, with no explanation and no markdown fences.

Question: {question}
"""

STUDENT_ANSWER_PROMPT = """
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

STUDENT_CHART_ANSWER_PROMPT = """
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
    def __init__(self, host, port=8099, path="/query/sql", scheme="http") -> None:
        self.conn = connect(host=host, port=port, path=path, scheme=scheme)
        self.model = ChatOpenAI()

    def _clean_sql(self, response_text: str) -> str:
        sql = response_text.strip()
        sql = re.sub(r"^```sql\s*", "", sql, flags=re.IGNORECASE)
        sql = re.sub(r"^```\s*", "", sql)
        sql = re.sub(r"\s*```$", "", sql)
        return sql.strip().rstrip(";")

    def generate_sql(self, question: str) -> str:
        prompt_template = ChatPromptTemplate.from_template(STUDENT_SQL_PROMPT)
        prompt = prompt_template.format(question=question)
        response = self.model.invoke(prompt)
        sql = self._clean_sql(response.content)
        if not sql.lower().startswith("select"):
            raise RuntimeError(f"Refusing to run non-SELECT SQL: {sql}")
        if not references_allowed_tables(sql):
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
            return sql, "No matching student rows were found.", df, None

        context_text = df.to_string(index=False)
        chart_path = None
        if wants_chart(question):
            chart_path = render_chart(df, question)
            prompt_template = ChatPromptTemplate.from_template(STUDENT_CHART_ANSWER_PROMPT)
        else:
            prompt_template = ChatPromptTemplate.from_template(STUDENT_ANSWER_PROMPT)

        prompt = prompt_template.format(context=context_text, question=question)
        response = self.model.invoke(prompt)
        return sql, response.content, df, chart_path


def wants_chart(question: str) -> bool:
    lowered = question.lower()
    return any(keyword in lowered for keyword in CHART_KEYWORDS)


def references_allowed_tables(sql: str) -> bool:
    lowered = sql.lower()
    return not bool(re.search(r"\bfrom\s+(?!student\b|student_exam\b|student_fees\b)", lowered)) and \
        not bool(re.search(r"\bjoin\s+(?!student\b|student_exam\b|student_fees\b)", lowered))


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
    query_mode = os.environ.get("QUERY_MODE", "documentation").strip().lower()
    if query_mode == "student":
        run_student_mode()
    else:
        run_documentation_mode()
