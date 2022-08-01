import asyncio
import sys
import time
from time import perf_counter as timer

from rich.console import Console
from rich.panel import Panel

from .cli import read_console_args
from .db_manager import DBManager
from .fetcher import Fetcher
from .info_panel import PanelCreator, PanelQueryHandler
from .table import create_rich_table, get_instance_queryset

console = Console()
db_manager = DBManager()
fetcher = Fetcher(db_manager, 1000)

INSTANCE_DB_MAPPING = {
    "author": "main_author.cover_name",
    "book": "name"
}

NON_EMPTY_COL_ERROR_MSG = (
    "[red bold]Внимание![/red bold]\nВы пытаетесь загрузить данные "
    "в непустую коллекцию.\nДля повторной загрузки данных воспользуйтесь\n"
    "опцией [green bold]`--drop_collection`[/green bold] "
    "и затем снова [green bold]`--load`[/green bold]"
)

DROP_COL_SUCCESS_MSG = (
    "Все документы коллекции [green bold]`{}`[/green bold] успешно удалены"
)


async def main():
    """Контролирует исполнение программы."""
    await db_manager.get_db()
    console_args = read_console_args()

    # Получить данные от API и загрузить в БД.
    if console_args.load:
        if not await db_manager.is_empty_collection():
            console.log(NON_EMPTY_COL_ERROR_MSG)
            sys.exit(1)
        start_time = timer()
        await fetcher.execute_tasks()
        console.log(
            f"Время заполнения базы данных в секундах: "
            f"[green bold]{time.perf_counter() - start_time:.2f}"
        )

    # Удалить все документы из текущей коллекции.
    elif console_args.drop_collection:
        col_name = await db_manager.drop_collection()
        console.log(DROP_COL_SUCCESS_MSG.format(col_name))

    # Вывести инфо панели со статистикой.
    elif console_args.stats:
        panel_queryset = PanelQueryHandler(db_manager)
        await panel_queryset.await_all_queries()
        stats_panels = PanelCreator(panel_queryset.results).yield_panels()
        console.print(
            "[purple bold]Немного статистики об аудиокнигах[/purple bold]",
            justify="center"
        )
        console.print(Panel(stats_panels))

    # Вывести таблицу с данными об авторе или книге.
    elif instance_args := (console_args.author or console_args.book):
        mode = "author" if console_args.author else "book"
        instance = INSTANCE_DB_MAPPING.get(mode, "author")
        async_queryset, header = await get_instance_queryset(
            db_manager, instance, instance_args
        )
        table = await create_rich_table(async_queryset, header)
        console.print(table)
    elif console_args.test:
        console.log("Тестовые функции не реализованы.")

if __name__ == "__main__":
    asyncio.run(main())
