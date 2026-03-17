FROM python:3.11.1

RUN apt-get update -y && \
    apt-get install -y librdkafka-dev

COPY requirements.txt requirements.txt
COPY loader.py loader.py
COPY genai.py genai.py
COPY student_loader.py student_loader.py
COPY student_exam_loader.py student_exam_loader.py
COPY student_fees_loader.py student_fees_loader.py
COPY cdr_loader.py cdr_loader.py

RUN pip install --upgrade pip
RUN python3 --version && pip install -r requirements.txt
