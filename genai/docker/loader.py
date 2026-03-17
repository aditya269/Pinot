#!/usr/bin/env python3

import json
import os
import sys

import requests
from openai import OpenAI
from langchain_community.document_loaders.recursive_url_loader import RecursiveUrlLoader
from langchain_core.documents import Document
from bs4 import BeautifulSoup as Soup

from confluent_kafka import Producer

clock = ['>', '=>', '==>', '===>', '====>', '=====>', '======>', '=======>']


def build_openai_client():
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError(
            "OPENAI_API_KEY is not set. Export it before running `make loader`."
        )

    return OpenAI(api_key=api_key)

class Writer:
    def __init__(self) -> None:
        pass

    def write(self, data:dict): pass

class StdOut(Writer):
    def __init__(self, meta_only=False) -> None:
        super().__init__()
        self.meta_only = meta_only

    def write(self, data:dict):
        print(json.dumps(data['metadata']))
        print(len(data['embedding']))
        if not self.meta_only:
            print(data['embedding'][0:2])


class Kafka(Writer):
    def __init__(self, config:dict) -> None:
        super().__init__()
        self.p = Producer(config)
        self.count = 0

    
    def delivery_report(self, err, msg):
        """ Called once for each message produced to indicate delivery result.
            Triggered by poll() or flush(). """
        if err is not None:
            print('Message delivery failed: {}'.format(err))
        else:
            print(f'====> Streaming to Kafka {msg.topic()} [{msg.partition()}]  {clock[self.count % len(clock)]}            ', end='\r')
            self.count += 1


    def write(self, data: dict):
        self.p.produce(
                key=data['source'],
                topic='documentation', 
                value=json.dumps(data), 
                on_delivery=self.delivery_report)
            
        self.p.flush()

    def records_written(self):
        return self.count

class Loader:
    def __init__(self, url, writer:Writer, model='text-embedding-ada-002', max_depth=0) -> None:
        self.url = url
        self.model = model
        self.client = build_openai_client()
        self.writer = writer
        self.max_depth = max_depth

    def get_embedding(self, text):
        text = text.replace("\n", " ")
        return self.client.embeddings.create(input = [text], model=self.model).data[0]

    def load_docs(self):
        if self.max_depth == 0:
            response = requests.get(self.url, timeout=30)
            response.raise_for_status()
            return [
                Document(
                    page_content=Soup(response.text, "html.parser").text,
                    metadata={"source": response.url},
                )
            ]

        loader = RecursiveUrlLoader(
            url=self.url,
            use_async=True,
            max_depth=self.max_depth,
            extractor=lambda x: Soup(x, "html.parser").text,
        )
        return loader.load()

    def run(self):
        docs = self.load_docs()
        print(f"Loaded {len(docs)} document(s) from {self.url}")

        for doc in docs:
            message = {
                "source": doc.metadata['source'],
                "content": doc.page_content,
                "metadata": doc.metadata,
                "embedding": self.get_embedding(doc.page_content).embedding
            }

            self.writer.write(message)

        records_written = getattr(self.writer, "records_written", lambda: len(docs))()
        print(f"\nSent {records_written} record(s) to Kafka")


if __name__ == '__main__':

    args = sys.argv

    writer = Kafka({'bootstrap.servers': 'kafka:9092'})
    # writer = StdOut(meta_only=True)

    max_depth = int(os.environ.get("MAX_DEPTH", "0"))
    l = Loader(url=args[1], writer=writer, max_depth=max_depth)
    l.run()
