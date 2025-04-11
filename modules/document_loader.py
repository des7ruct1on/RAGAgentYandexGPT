import os
import glob
from typing import Dict, List
import markdown
from bs4 import BeautifulSoup


class DocumentLoader:
    def __init__(self, data_path: str = "data", chunk_size: int = 1000):
        self.data_path = data_path
        self.chunk_size = chunk_size

    def load_and_chunk_documents(self) -> Dict[str, List[str]]:
        """Загрузка и разбиение документов на чанки"""
        documents = {}
        for file_path in glob.glob(os.path.join(self.data_path, "*.md")):
            with open(file_path, 'r', encoding='utf-8') as file:
                file_name = os.path.basename(file_path)
                md_content = file.read()
                clean_text = self._clean_markdown(md_content)
                chunks = self._split_text(clean_text)
                documents[file_name] = chunks
        return documents

    def _clean_markdown(self, text: str) -> str:
        """Очистка markdown текста"""
        html = markdown.markdown(text)
        soup = BeautifulSoup(html, 'html.parser')
        return soup.get_text(separator='\n', strip=True)

    def _split_text(self, text: str) -> List[str]:
        """Разбиение текста на чанки"""
        words = text.split()
        chunks = []
        current_chunk = []
        current_length = 0

        for word in words:
            if current_length + len(word) + 1 > self.chunk_size and current_chunk:
                chunks.append(" ".join(current_chunk))
                current_chunk = []
                current_length = 0

            current_chunk.append(word)
            current_length += len(word) + 1

        if current_chunk:
            chunks.append(" ".join(current_chunk))

        return chunks