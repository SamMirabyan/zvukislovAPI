from typing import Any, Iterable, MutableMapping, Union

from bson.raw_bson import RawBSONDocument
from bson.son import SON

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCursor

import pymongo
from pymongo.results import InsertManyResult


from .logger import logger


class DBManager():
    """Интерфейс управления базой данных.

    DBManager - это набор асинхронных методов
    управления БД и выполнения запрсов.

    Атрибуты:
        `connection_args`: Кортеж с параметрами для подключения к MongoDB.
        `panel_coroutine_queries`: Список методов, необходимых для
            формирования инфо панелей с помощью `PanelQueryHandler`.
            Все методы данного списка возвращают корутины.
        `panel_numeric_queries`: Список 'числовых' методов для
            формирования инфо панелей. Все методы возвращают числа.

    Разделение на 'корутинные' и 'числовые' методы обсуловлено различиями
        в возвращаемых значениях. Результат выполнения числового метода
        доступен сразу после использования `await`.
        Результат выполнения 'корутинной' фунукции необходимо проитерировать
        либо с помощью `async for` либо с помощью `.to_list(n)`.

    Пример:
        db_manager = `DBManager()`
         `await db_manager.get_db()` => соединяет с БД
         `db_manager.is_empty_collection()`
    """
    def __init__(self, connection_args: tuple = ("localhost", 27888)):
        self.connection_args = connection_args
        self.panel_coroutine_queries = [
            self.find_highest_rated_book,
            self.find_most_reviewed_book,
            self.find_most_prolific_author,
            self.find_most_prolific_narrator,
            self.find_highest_AVG_rated_author,
            self.find_highest_AVG_reviewed_author,
        ]
        self.panel_numeric_queries = [
            self.find_books_total,
        ]

    async def get_db(self) -> None:
        """Привязка клиента БД к интерфейсу."""
        client = AsyncIOMotorClient(*self.connection_args)
        self.db = client.test_database

    async def is_empty_collection(self) -> bool:
        """Проверяет наличие документов в БД."""
        num_docs = await self.db.test_collection.estimated_document_count()
        return num_docs == 0

    async def drop_collection(self) -> None:
        """Удалить все документы из тестовой коллекции."""
        col_name = self.db.test_collection.full_name
        await self.db.test_collection.drop()
        return col_name

    async def insert_docs(
        self,
        documents: Iterable[Union[MutableMapping[str, Any], RawBSONDocument]]
    ) -> InsertManyResult:
        """Добавить в БД несколько документов сразу.

        Параметры:
            `documents`: Набор JSON документов.

        Возвращает:
            Мета информация о выполненной транзакции.
        """
        result = await self.db.test_collection.insert_many(documents)
        logger.info(f'Добавлено {len(result.inserted_ids)} документов')

    async def count_docs(self, instance: str, name: str) -> int:
        """Возвращает количество документов по введенному запросу."""
        return await self.db.test_collection.count_documents(
            {instance: {'$regex': name, '$options': 'i'}}
        )

    async def find_instance_stats(
        self,
        instance: str,
        name: str
    ) -> AsyncIOMotorCursor:
        """Возвращает итерируемую корутину с ответом от БД.

        Для получения результатов необходимо использовать
        `cursor.to_list(n)` или `async for`.

        Параметры:
            `instance`: Сущность, для которой выполняется запрос.
                => 'author' или 'book'.
            `name`: Имя (название) сущности.

        Возвращает:
            Корутину с результатом выполнения запроса к БД.
                Ответ может быть пустым.

        Примеры:
            await find_instance_stats('author', 'лев толстой')
            await find_instance_stats('book', 'каренина')
        """
        search_query = [
            {instance: {'$regex': name, '$options': 'i'}},
            {
                'name': 1,
                '_id': 0,
                'uri': 1,
                'main_actor.cover_name': 1,
                'reviews_count': 1,
            }
        ]
        queryset = self.db.test_collection.find(*search_query).sort(
            'reviews_count', pymongo.DESCENDING
        )
        return queryset

    #############
    #   PANEL   #
    #  QUERIES  #
    #############

    async def find_books_total(self) -> int:
        """Возвращает общее количество документов в БД."""
        return await self.db.test_collection.estimated_document_count()

    async def find_highest_rated_book(self) -> AsyncIOMotorCursor:
        """Возвращает данные об аудиокниге с наиболее высоким рейтингом."""
        search_query = [
            {},
            {
                'name': 1,
                'uri': 1,
                'main_author.cover_name': 1,
                '_id': 0,
                'main_actor.cover_name': 1,
                'global_rating': 1
            }
        ]
        return self.db.test_collection.find(*search_query).sort(
            'global_rating', pymongo.DESCENDING).limit(1)

    async def find_most_reviewed_book(self) -> AsyncIOMotorCursor:
        """Возвращает данные об аудиокниге с наибольшим количеством отзывов."""
        search_query = [
            {},
            {
                'name': 1,
                'uri': 1,
                'main_author.cover_name': 1,
                '_id': 0,
                'main_actor.cover_name': 1,
                'reviews_count': 1
            }
        ]
        return self.db.test_collection.find(*search_query).sort(
            'reviews_count', pymongo.DESCENDING).limit(1)

    async def find_most_prolific_author(self) -> AsyncIOMotorCursor:
        """Возвращает данные об авторе с наибольшим количеством аудиокниг."""
        pipeline = [
            {
                '$group': {
                    '_id': '$main_author.cover_name',
                    'num_books': {'$sum': 1},
                    'uri': {'$first': '$main_author.uri'}
                }
            },
            {'$sort': SON([('num_books', -1)])},
            {'$skip': 1},  # пропускаем 1-ю строку,
            {'$limit': 1}  # потому что там 'Комсомольская правда'
        ]
        return self.db.test_collection.aggregate(pipeline)

    async def find_most_prolific_narrator(self) -> AsyncIOMotorCursor:
        """Возвращает данные о чтеце с наибольшим
        количеством озвученных аудиокниг.
        """
        pipeline = [
            {
                '$group': {
                    '_id': '$main_actor.cover_name',
                    'books_narrated': {'$sum': 1},
                    'uri': {'$first': '$main_actor.uri'}
                }
            },
            {'$sort': SON([('books_narrated', -1)])},
            {'$skip': 1},  # пропускаем 1-ю строку,
            {'$limit': 1}  # потому что там 'Анонимный чтец'
        ]
        return self.db.test_collection.aggregate(pipeline)

    async def find_highest_AVG_rated_author(self) -> AsyncIOMotorCursor:
        """Возвращает данные об авторе с наиболее
        высоким средним рейтингом аудиокниг.
        """
        pipeline = [
            {
                '$group': {
                    '_id': '$main_author.cover_name',
                    'sum_books': {'$sum': 1},
                    'avg_rating': {'$avg': '$global_rating'},
                    'uri': {'$first': '$main_author.uri'}
                }
            },
            {'$match': {'sum_books': {'$gte': 5}}},
            {'$sort': SON([('avg_rating', -1)])},
            {'$limit': 1}
        ]
        return self.db.test_collection.aggregate(pipeline)

    async def find_highest_AVG_reviewed_author(self) -> AsyncIOMotorCursor:
        """Возвращает панель с данными об авторе
        с самым высоким средним количеством отзывов на аудиокнигу.
        """
        pipeline = [
            {
                '$group': {
                    '_id': '$main_author.cover_name',
                    'sum_books': {'$sum': 1},
                    'avg_reviews': {'$avg': '$reviews_count'},
                    'uri': {'$first': '$main_author.uri'}
                }
            },
            {'$match': {'sum_books': {'$gte': 5}}},
            {'$sort': SON([('avg_reviews', -1)])},
            {'$limit': 1}
        ]
        return self.db.test_collection.aggregate(pipeline)
