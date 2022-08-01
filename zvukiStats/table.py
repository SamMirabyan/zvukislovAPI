import itertools
import sys
from typing import Tuple

from motor.motor_asyncio import AsyncIOMotorCursor

from rich import print as rprint
from rich.table import Table

from .db_manager import DBManager
from .info_panel import create_url


async def get_instance_queryset(
    db_manager: DBManager,
    instance: str,
    args: list
) -> Tuple[AsyncIOMotorCursor, str] or str:
    """Возвращает результат запроса к БД.

    Выполняет поиск по БД и возвращает корутину (итерируемая)
    с результатом запроса и тело запроса пользователя,
    если этот результат не пустой.
    Если пустой - возвращает сообщение о неудачном поиске.

    Результат выполнения функции передается в конструктор таблицы.

    Параметры:
        `db_manager`: Хендлер базы данных.
        `instance`: Сущность, для которой выполняется запрос.
                => 'author' или 'book'.
        `args`: Запрос пользователя, по которому производится поиск.

    Возвращает:
        При непустом ответе БД возвращается кортеж из
        корутины с результатами запроса и тело запроса
        пользователя в виде строки.

        При пустом ответе БД возвращается сообщение
        о неудачном поиске.

    Пример:
        `async_queryset`, `header` = `await get_instance_queryset`(
            db_manager, instance, instance_args
        )
         table = await create_rich_table(`async_queryset`, `header`)
    """
    query_string = " ".join(args)
    len_args = len(args)
    if len_args > 9:
        sys.exit(
            "Слишком длинный запрос. Попробуйте сократить запрос "
            "или выделить ключевые слова."
        )
    # При поиске автора, если указаны только имя и фамилия,
    # подкидываем в регулярное выражение опциональное отчетство.
    if instance == "author":
        if len_args == 2:
            args.append("[а-яa-z]* ?")
    for name_combination in itertools.permutations(args, len_args):
        num_docs = await db_manager.count_docs(
            instance, " ".join(name_combination)
        )
        if num_docs > 0:
            async_queryset = await db_manager.find_instance_stats(
                instance, " ".join(name_combination)
            )
            return (async_queryset, query_string)
    return ("По запросу {} ничего не найдено!", query_string)


async def create_table_layout(header="") -> Table:
    """Генерирует макет таблицы."""
    table = Table(
        title=f"Информация по запросу `[{header}]`",
        style="cyan",
        title_style="green bold",
    )
    table.add_column(
        "Название книги",
        style="magenta",
        justify="center",
        vertical="top"
    )
    table.add_column(
        "Читает",
        style="yellow",
        justify="center",
        vertical="middle"
    )
    table.add_column(
        "Количество отзывов",
        style="green",
        justify="center",
        vertical="middle",
        max_width=10
    )
    table.add_column(
        "URL",
        justify="center",
        vertical="middle",
        no_wrap=True
    )
    return table


async def create_rich_table(
    async_queryset: AsyncIOMotorCursor or str,
    header: str
) -> Table or None:
    """Заполнение макета таблицы данными из запроса к БД.

    Получает ответ из БД (корутина), обрабатывает данные ответа
    в цикле, заполняет этими данными таблицу и возвращает ее.

    Параметры:
        `async_queryset`: Корутина с результатами выполнения запроса к БД
            или строка, если результат выполнения запроса оказался пустым.
        `header`: Запрос пользователя, который будет отображаться
            в названии таблицы.

    Пример:
        `table` = `await create_rich_table(async_queryset, header)`
         `console.print(table)`

    Вызвает исключение:
        `sys.exit()`: Выполняется выход из программы, если резульат
            выполнения запроса к БД оказался пустым. Перед выходом
            выводится информационное сообщение.
    """
    if isinstance(async_queryset, str):
        display_empty_qs_message(async_queryset, header)
        sys.exit()
    table = await create_table_layout(header)
    for row in await async_queryset.to_list(length=10):
        table.add_row(
            row["name"],
            row["main_actor"]["cover_name"],
            str(row["reviews_count"]),
            create_url(row["uri"])
        )
    return table


def display_empty_qs_message(error: str, query_string: str) -> None:
    """Выводит стилизованное сообщение о том, что
    результат выполнения запроса оказался пустым.

    Параметры:
        `error`: Сообщение, получаемое функцией `create_rich_table`
            при пустом запросе.
        `query_string`: Сам запрос пользователя.

    Пример:
        python3 main.py --author QUERY_DOESNOT_EXIST
        => [bold red]Внимание! [/bold red] По запросу
        [bold green]`QUERY_DOESNOT_EXIST`[/bold green]
        ничего не найдено!
    """
    formatted_qstring = f"[bold green]`{query_string}`[/bold green]"
    formatted_error = error.format(formatted_qstring)
    rprint("".join(("[bold red]Внимание! [/bold red]", formatted_error)))
