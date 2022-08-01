from typing import Iterator, List, Tuple, Union

from rich.console import group
from rich.panel import Panel


from .db_manager import DBManager


class PanelQueryHandler:
    """Класс обработки запросов для создания набора инфо панелей.

    Преобразует асинхронные запросы к БД в список результатов
    для последующей передачи в PanelCreator.

    Атрибуты:
        db_manager: Хендлер базы данных.
        results: Список с результатами.
            Заполняется в результате
            вызова метода `await_all_queries`.

    Пример:
        `panel_queryset` = `PanelQueryHandler(db_manager)`
         `await panel_queryset.await_all_queries()`
          `stats_panels` = `PanelCreator(panel_queryset.results)`
    """
    def __init__(self, db_manager: DBManager) -> None:
        self.db_manager = db_manager
        self.results = []

    async def await_all_queries(self) -> None:
        """Запускает выполнение всех запросов к БД."""
        await self._await_all_numeric_queries()
        await self._await_all_coroutine_queries()

    async def _await_numeric_query(self, query: callable) -> None:
        """Обрабатывает запрос к БД, результатом
        выполнения которого является число.

        Параметры:
            query: функция-запрос к БД.
        """
        result = await query()
        self.results.append(result)

    async def _await_all_numeric_queries(self):
        """Запускает выполнение всех `числовых` запросов к БД."""
        for query in self.db_manager.panel_numeric_queries:
            await self._await_numeric_query(query)

    async def _await_coroutine_query(self, query: callable):
        """Обрабатывает запрос к БД, результатом
        выполнения которого является корутина.

        Параметры:
            query: функция-запрос к БД
        """
        cor = await query()
        for data in await cor.to_list(length=1):
            self.results.append(data)

    async def _await_all_coroutine_queries(self):
        """Запускает выполнение всех `корутинных` запросов к БД."""
        for query in self.db_manager.panel_coroutine_queries:
            await self.await_coroutine_query(query)


class PanelCreator:
    """Класс для создания набора инфо панелей.

    PanelCreator - это набор строк с шаблонами оформления
    и функций, которые преобразуют шаблонные строки
    в инфо панели (Panel из rich.panel)

    `yield_panels` запускает процесс преобразования строк в панели.
    На выходе получается генератор панелей, который можно передать
    в `Panel` для вывода готового набора стилизованных инфо панелей.

    Атрибуты:
        `queryset`: Список с данными, передаваемыми в шаблоны.
            Может быть получен с помощью `PanelQueryHandler`.
        `string_templates`: Список строк-шаблонов.
        `format_methods`: Список методов для форматирования шаблонов.
        `render_methods`: Список методов для преобразования форматированных
            строковых шаблонов в панели.

    Пример:
        `stats_panels` = `PanelCreator(results).yield_panels()`
         console.print(Panel(`stats_panels`))
    """
    def __init__(self, queryset: list) -> None:
        self.queryset = queryset
        self.string_templates = {
            "uri": "[cyan]URL[/cyan]: [magenta]{}[/magenta]",
            "author": (
                "[cyan]{}[/cyan]: [magenta]{}[/magenta] "
                "[bold yellow]|[/bold yellow] "
            ),
            "sum_books": (
                "[cyan]Количество книг на портале[/cyan]: "
                "[magenta]{}[/magenta] [bold yellow]|[/bold yellow] "
            ),
            "book": (
                "[cyan]{}[/cyan]: [magenta]{}[/magenta] "
                "[bold yellow]|[/bold yellow] "
            ),
            "book_author": (
                "[cyan]Автор[/cyan]: [magenta]{}[/magenta] "
                "[bold yellow]|[/bold yellow] "
            ),
            "book_narrator": (
                "[cyan]Читает[/cyan]: [magenta]{}[/magenta] "
                "[bold yellow]|[/bold yellow] "
            ),
            "books_total": (
                "[cyan]Всего аудиокниг[/cyan]: "
                "[magenta]{}[/magenta]"
            ),
        }
        self.format_methods = {
            "author": self._create_author_string,
            "uri": self._create_uri_string,
            "sum_books": self._create_sum_books_string,
            "book": self._create_book_string,
            "book_author": self._create_book_author_string,
            "book_narrator": self._create_book_narrator_string,
            "books_total": self._create_books_total_string,
        }
        self.render_methods = [
            self._get_total_books,
            self._get_max_rating_book,
            self._get_most_reviewed_book,
            self._get_most_prolific_author,
            self._get_most_prolific_narrator,
            self._get_highest_AVG_rated_author,
            self._get_highest_AVG_reviewed_author,
        ]

    @group()
    def yield_panels(self) -> Iterator[Panel]:
        """Запускает преобразование шаблонов в панели.

        Возвращает: генератор панелей (Panel).
        """
        for data, method in zip(self.queryset, self.render_methods):
            panel = method(data)
            yield panel

    ##############
    #   RENDER   #
    #  METHODS   #
    ##############

    def _get_total_books(self, total: int) -> Panel:
        """Возвращает панель с общим количеством книг в базе."""
        content = self._generate_content(total, "books_total")
        title = "Общее количество аудиокниг в базе"
        return self._create_green_panel(content, title)

    def _get_max_rating_book(self, data: dict) -> Panel:
        """Возвращает панель с данными о книге,
        имеющей наиболее высокий рейтинг.
        """
        content = self._generate_content(
            data,
            ("book", "global_rating"), "book_author", "book_narrator", "uri"
        )
        title = "Аудиокнига с самым высоким рейтингом"
        return self._create_green_panel(content, title)

    def _get_most_reviewed_book(self, data: dict) -> Panel:
        """Возвращает панель с данными о книге,
        имеющей наибольшее количество отзывов.
        """
        content = self._generate_content(
            data,
            ("book", "reviews_count"), "book_author", "book_narrator", "uri"
        )
        title = "Аудиокнига с наибольшим количеством отзывов"
        return self._create_green_panel(content, title)

    def _get_most_prolific_author(self, data: dict) -> Panel:
        """Возвращает панель с данными об авторе
        с наибольшим количеством аудиокниг.
        """
        content = self._generate_content(
            data,
            ("author", "num_books"), "uri"
        )
        title = "Автор с наибольшим количеством аудиокниг"
        return self._create_green_panel(content, title)

    def _get_most_prolific_narrator(self, data: dict) -> Panel:
        """Возвращает панель с данными о чтеце
        с наибольшим количеством озвученных аудиокниг.
        """
        content = self._generate_content(
            data,
            ("author", "books_narrated"), "uri"
        )
        title = "Чтец с наибольшим количеством озвученных аудиокниг"
        return self._create_green_panel(content, title)

    def _get_highest_AVG_rated_author(self, data: dict) -> Panel:
        """Возвращает панель с данными об авторе
        с самым высоким средним рейтингом аудиокниг.
        """
        content = self._generate_content(
            data,
            ("author", "avg_rating"), "sum_books", "uri"
        )
        title = "Автор с самым высоким средним рейтингом аудиокниг"
        return self._create_green_panel(content, title)

    def _get_highest_AVG_reviewed_author(self, data: dict) -> Panel:
        """Возвращает панель с данными об авторе
        с самым высоким средним количеством отзывов на аудиокнигу.
        """
        content = self._generate_content(
            data,
            ("author", "avg_reviews"), "sum_books", "uri"
        )
        title = (
            "Автор с самым высоким средним количеством "
            "отзывов на аудиокнигу"
        )
        return self._create_green_panel(content, title)

    ##############
    #     MAIN   #
    #  FORMATTER #
    #   METHOD   #
    ##############

    def _generate_content(
        self,
        data: dict,
        *options: Tuple[Union[Tuple[str], List[str], str]]
    ) -> str:
        """Возращает строку, готовую для передачи в Panel.

        Вызывается одним из методов `render_methods`.
        Получает на вход данные для форматирования
        и указание на набор форматирующих  методов.

        Форматирует несколько строк-шаблонов методами `format_methods`.
        Полученный отформатированный шаблон возвращается
        обратно в вызывающий методо `render_methods`.

        Параметры:
            `data`: Словарь с данными для передачи в методы `format_methods`.
                Принимается от однго из методов `render_methods`.
            `options`: Кортеж с набором опций вызова методов `format_methods`.
                Должен содержать кортежи (списки) строк или просто строки.

                Кортеж или список используются, когда помимо метода
                форматирования строки нужно указать дополнительную опцию.
                Например `("author", "num_books")` означает:
                    метод форматирования: "author"
                    дополнительная опция: "num_books"

        Возвращает:
            Форматированная строка с данными, подходящая для
            инфо панели (Panel).

        Вызывает исключение:
            `ValueError`: `options` содержит значения, отличные
            от `str`, `tuple`, `list`

        Пример:
            _generate_content(data, ("author", "num_books"), uri)
            =>
                "[cyan]{data['author']}[/cyan]: [magenta]{data['num_books']}"
                "[/magenta] [bold yellow]|[/bold yellow] "
                "[cyan]URL[/cyan]: [magenta]{data['uri']}[/magenta]"
        """
        results = []
        for option in options:
            if isinstance(option, (tuple, list)):
                method_name, arg = option
                method = self.format_methods.get(method_name, "None")
                if method:
                    results.append(method(data=data, option=arg))
            elif isinstance(option, str):
                method = self.format_methods.get(option, "None")
                if method:
                    results.append(method(data))
            else:
                raise ValueError(
                    "`options` может содержать только строки, "
                    "кортежи или списки."
                )
        return "".join(results)

    ###############
    #    FORMAT   #
    #   METHODS   #
    ###############

    def _create_author_string(self, data: dict, option: str) -> str:
        """Возвращает форматированную строку автора.

        Параметры:
            `data`: Набор данных в виде словаря.
            `option`: Один из дополнительных атрибутов.
            => (`num_books`, `books_narrated`, `avg_reviews`, `avg_rating`).

        Возвращает:
            Форматированную строку с данными об авторе.
        """
        author = data.get("_id")
        author_option = data.get(option)
        tempate = self.string_templates.get("author")
        if tempate:
            return tempate.format(author, author_option)

    def _create_uri_string(self, data: dict) -> str:
        """Возвращает форматированную строку ссылки на аудиокнигу."""
        uri = data.get("uri")
        template = self.string_templates.get("uri")
        if template:
            return template.format(create_url(uri))

    def _create_sum_books_string(self, data: dict) -> str:
        """Возвращает форматированную строку количества аудиокниг."""
        sum_books = data.get("sum_books")
        template = self.string_templates.get("sum_books")
        if template:
            return template.format(sum_books)

    def _create_book_string(self, data: dict, option: str) -> str:
        """Возвращает форматированную строку аудиокниги.
        Параметры:
            `data`: Набор данных в виде словаря.
            `option`: Один из дополнительных атрибутов.
            => (`global_rating`, `reviews_count`).

        Возвращает:
            Форматированную строку с данными об аудиокниге.
        """
        book = data.get("name")
        book_option = data.get(option)
        template = self.string_templates.get("book")
        if template:
            return template.format(book, book_option)

    def _create_books_total_string(self, total: int) -> str:
        """Возвращает форматированную строку о
        количестве аудиокниг автора.
        """
        template = self.string_templates.get("books_total")
        if template:
            return template.format(total)

    def _create_book_author_string(self, data: dict) -> str:
        """Возвращает форматированную строку имени автора
        для отображения в строке об аудиокниге.
        """
        book_author = data.get("main_author").get("cover_name")
        template = self.string_templates.get("book_author")
        if template:
            return template.format(book_author)

    def _create_book_narrator_string(self, data: dict) -> str:
        """Возвращает форматированную строку имени чтеца
        для отображения в строке об аудиокниге.
        """
        book_narrator = data.get("main_actor").get("cover_name")
        template = self.string_templates.get("book_narrator")
        if template:
            return template.format(book_narrator)

    @staticmethod
    def _create_green_panel(content: str, title: str) -> Panel:
        """Создает панель в определенном стиле."""
        return Panel(
            renderable=content,
            title=title,
            padding=(1, 1, 1, 1),
            border_style="green bold"
        )


def create_url(uri: str) -> str:
    """Возвращает полный URL."""
    return "https://zvukislov.ru" + uri
