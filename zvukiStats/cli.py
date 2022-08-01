import argparse


def read_console_args():
    parser = argparse.ArgumentParser(
        prog="zvukiStats",
        description=(
            "Программа для получения данных от API `zvukislov.ru/api/`, "
            "сохранения их в БД и последующей обработки."
        )
    )
    parser.add_argument(
        "-l",
        "--load",
        action="store_true",
        help="получить данные от API и загрузить в базу данных",
    )
    parser.add_argument(
        "-D",
        "--drop_collection",
        action="store_true",
        help="очистить текущую коллекцию"
    )
    parser.add_argument(
        "-a",
        "--author",
        nargs="+",
        help="показать данные об авторе"
    )
    parser.add_argument(
        "-b",
        "--book",
        nargs="+",
        help="показать данные о книге"
    )
    parser.add_argument(
        "-s",
        "--stats",
        action="store_true",
        help="показать интересную статистику"
    )
    parser.add_argument(
        "-t",
        "--test",
        action="store_true",
        help="проверить тестовые функции"
    )
    return parser.parse_args()
