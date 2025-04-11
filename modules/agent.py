from typing import List, Optional
from yandex_cloud_ml_sdk import YCloudML
from .document_loader import DocumentLoader
from .vector_db import VectorDB
import logging

logger = logging.getLogger(__name__)

class Agent:
    def __init__(
            self,
            yandex_folder_id: str,
            yandex_api_key: str,
            document_loader: DocumentLoader,
            vector_db: VectorDB,
            model_name: str = "yandexgpt",
            model_version: str = "rc"
    ):
        logger.info("Started init Agent")
        self.document_loader = document_loader
        logger.info("FINISHED document_loader")
        self.vector_db = vector_db
        logger.info("FINISHED vector_db")
        self._init_models(yandex_folder_id, yandex_api_key, model_name, model_version)
        logger.info("FINISHED _init_models")
        self._load_knowledge_base()
        logger.info("FINISHED _load_knowledge_base")

    def _init_models(self, folder_id: str, api_key: str, model_name: str, model_version: str):
        """Инициализация моделей"""
        logger.info("STARTED _init_models")
        self.sdk = YCloudML(folder_id=folder_id, auth=api_key)
        logger.info("FINISHED sdk")
        self.model = self.sdk.models.completions(model_name, model_version=model_version)
        logger.info("FINISHED sdk.models.completions")

    def _load_knowledge_base(self):
        """Загрузка документов в векторную БД"""
        logger.info("STARTED _load_knowledge_base")
        documents = {}
        for doc_name, chunks in self.document_loader.load_and_chunk_documents().items():
            logger.info(f"\t {doc_name} - {chunks}")
            for i, chunk in enumerate(chunks):
                doc_id = f"{doc_name}_chunk_{i}"
                documents[doc_id] = chunk

        logger.info("STARTED add_documents")
        self.vector_db.add_documents(documents)

    def generate_response(self, query: str, temperature: float = 0.7) -> str:
        """Генерация ответа с RAG"""
        try:
            # 1. Векторный поиск релевантных чанков
            context_chunks = self.vector_db.query(query)

            # 2. Формирование промпта
            prompt = self._build_prompt(query, [chunk for chunk, _ in context_chunks])

            # 3. Генерация ответа
            response = self.model.run(prompt).text

            return self._postprocess_response(response, query)
        except Exception as e:
            return f"Ошибка при обработке запроса: {str(e)}"

    def _build_prompt(self, query: str, context_chunks: List[str]) -> str:
        """Формирование промпта с контекстом"""
        context = "\n\n".join(
            f"Контекст {i + 1}:\n{chunk}"
            for i, chunk in enumerate(context_chunks)
        ) if context_chunks else "Контекст не найден"

        return f"""
        Ты - бот, который помогает пользователям выбрать страну для путешествий. Ты рекомендуешь страны, основываясь на интересных фактах, достопримечательностях, местных традициях и уникальных особенностях. В каждом ответе ты должны красиво описывать страну, упоминать её культуру, природу, популярные места для посещения, а также советы для путешественников.

        Пример ответа:
        1. **Италия** — страна, которая очарует тебя с первого взгляда! Вдохни воздух Рима, наслаждайся винами Тосканы и заблуди на узких улочках Венеции. Ты не можешь пропустить Колизей и Ватикан, а также обязательно попробуй местное мороженое и пасту. В Италии каждый уголок — это настоящая история, и путешествия здесь будут захватывающими и незабываемыми!
        
        Не забывай упоминать о:
        - Природных красотах (горы, пляжи, озера)
        - Местных традициях и кухне
        - Популярных местах для активного отдыха (экскурсии, пешие маршруты, пляжи)
        - добавляй флаги в ответ страны, которую предлагаешь
        - если пользователь не знает что именно выбрать, то предложи ему несколько стран
        - добавляй смайлики для красивого ответа
        - добавляй в ответ фразы по типу : "я бы тебе порекомендовал" и тп


        {context}

        Вопрос: {query}

        Ответ (будь точны, но не обязательно кратким):
        """

    def _postprocess_response(self, response: str, original_query: str) -> str:
        """
        Постобработка ответа модели:
        - Очистка от лишних пробелов и переносов
        - Проверка релевантности ответа
        - Форматирование
        """
        # Очистка текста
        response = ' '.join(response.split()).strip()

        # Проверка на пустой ответ
        if not response:
            return "Не удалось сформировать ответ. Попробуйте переформулировать вопрос."

        # Проверка на отсутствие информации в контексте
        if "не могу дать точный ответ" in response.lower():
            return "К сожалению, в моей базе знаний нет информации по этому вопросу."

        # Удаление повторяющихся фраз
        sentences = [s.strip() for s in response.split('.') if s.strip()]
        unique_sentences = []
        seen = set()
        for s in sentences:
            if s.lower() not in seen:
                seen.add(s.lower())
                unique_sentences.append(s)
        response = '. '.join(unique_sentences) + ('.' if not response.endswith('.') else '')

        return response

    def update_knowledge_base(self):
        """Обновление базы знаний"""
        self._load_knowledge_base()