import chromadb
from chromadb.utils import embedding_functions
from typing import List, Dict, Tuple
import numpy as np
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class VectorDB:
    def __init__(self, collection_name: str = "documents", persist_dir: str = "db"):
        self.client = chromadb.PersistentClient(path=persist_dir)

        self.embedding_func = embedding_functions.DefaultEmbeddingFunction()

        self.collection = self.client.get_or_create_collection(
            name=collection_name,
            embedding_function=self.embedding_func
        )

    def add_documents(self, documents: Dict[str, str]) -> None:
        """Добавление документов в векторную БД"""
        logger.info("STARTED adding docs")
        ids = []
        texts = []
        metadatas = []

        for doc_id, content in documents.items():
            logger.info(f"\t {doc_id} - {content}")
            ids.append(doc_id)
            texts.append(content)
            metadatas.append({"source": doc_id})

        self.collection.add(
            documents=texts,
            metadatas=metadatas,
            ids=ids
        )

    def query(self, query_text: str, top_k: int = 3) -> List[Tuple[str, float]]:
        """Поиск по векторной БД"""
        results = self.collection.query(
            query_texts=[query_text],
            n_results=top_k
        )

        return list(zip(
            results["documents"][0],
            results["distances"][0]
        ))

    def clear(self) -> None:
        """Очистка коллекции"""
        self.collection.delete()