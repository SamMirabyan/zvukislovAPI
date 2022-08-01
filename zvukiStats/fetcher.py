import asyncio
import sys
from asyncio import Task
from typing import List

from aiohttp import ClientError, ClientSession, InvalidURL


from .db_manager import DBManager
from .logger import logger


class Fetcher:
    """Выполняет пул запросов к API и заливает данные в БД.

    Атрибуты:
        Класса:
            `url_template`: Шаблон URL, используемый в запросах.
            `verification_url`: URL для проверки структуры ответа API.
                Также используется для получения общего количества
                записей в API.
        Экземпляра:
            `db_manager`: Хендлер базы данных.
            `chunk_size`: Количество записей, возвращаемых в ответе API.

    Пример:
        fetcher = `Fetcher(db_manager, 1000)`
         `await fetcher.execute_tasks()`
    """
    url_template = "https://zvukislov.ru/api/audiobooks/?limit={}&page={}"
    verification_url = url_template.format(1, 1)

    def __init__(self, db_manager: DBManager, chunk_size: int):
        self.db_manager = db_manager
        self.chunk_size = chunk_size

    async def execute_tasks(self):
        """Выполнить список асинхронных задач."""
        async with ClientSession() as session:
            task_list = await self.create_tasks(session)
            await asyncio.gather(*task_list)

    async def create_tasks(self, session: ClientSession) -> List[Task]:
        """Заполнить список асинхронных задач.
        Задачи - выполнение функции `fetch_and_insert` с разными URL.
        """
        task_list = []
        count = await self.get_total_pages(session)
        for page_number in range(1, count):
            url = self.url_template.format(self.chunk_size, page_number)
            task = asyncio.create_task(self.fetch_and_insert(session, url))
            task_list.append(task)
        return task_list

    async def fetch_and_insert(self, session: ClientSession, url: str) -> None:
        """Выполнить запрос к API и залить данные ответа в БД."""
        try:
            async with session.get(url) as response:
                page = await response.json()
                content = page.get("results")
                await self.db_manager.insert_docs(content)
                logger.info(f"Обработан URL `{url}`")
        except InvalidURL as e:
            logger.error(f"Не удалось получить данные по URL `{url}`: {e}")
        except ClientError as e:
            logger.error(
                f"При обработке URL `{url}` возникла проблема "
                f"с Интернет-соединением: {e} "
            )
        except Exception as e:
            logger.error(
                f"Непредвиденная ошибка при обработке URL `{url}`: {e}"
            )

    async def get_total_pages(self, session: ClientSession) -> int:
        """Вычислить общее количество запросов, необходимых
        для обработки всего объема записей в API.
        """
        try:
            async with session.get(self.verification_url) as resp:
                page = await resp.json()
                count = page.get("count")
                return round(count / self.chunk_size) + 1
        except Exception as e:
            logger.error(
                (
                    f"Верификация проверочного url закончилась ошибкой: {e}"
                    "Структура ответа API могла быть изменена."
                )
            )
            sys.exit(1)
