"""
Main script
===========
This script is the main script for the Wikipedia path finder.
It sets up the path from the start title to the end title.
"""

import pathlib

from logger import logger
from dataio import DataIO
from crawler import Crawler
import utils

logger.info(
    "-----------------------------------------"
    "\n                                    "
    "Main Script"
    "\n                                    "
    "-----------------------------------------"
)

PATH = pathlib.Path(__file__).parent.resolve()
DATA = PATH / "DATA.db"
DATAIO = DataIO(DATA, PATH)
CRAWLER = Crawler(DATAIO)


def setup_path(
    start_titles: list[str] = None,
    end_titles: list[str] = None,
    iterations: int = 1,
    continuous: bool = False,
) -> None:
    """
    Sets up the path from the start title to the end title.

    Args:
        start_title (list[str]): The title(s) of the start Wikipedia page(s).
            - Default: None
        end_title (list[str]): The title(s) of the end Wikipedia page(s).
            - Default: ["Adolf_Hitler", "Jesus"]
        iterations (int): The number of iterations to crawl.
            - If start_title is None, iterations is the number of random start titles to crawl.
        continuous (bool): Whether to crawl continously.
            IF True, it will read from queue.json and crawl from the first value there.
            If queue.json is empty, it will crawl from a random start title.
            - Default: False
    """

    print("\n")

    if continuous:
        counter = 0
        print("Setting up continuous crawl.\nPress Ctrl+C to stop.\n")
        logger.info("Setting up continuous crawl.")
        while True:
            counter += 1
            print(f"Crawl number {counter}.")
            logger.info("Crawl number %s.", counter)

            if start_titles is not None:
                for start_title in start_titles:
                    DATAIO.insert_queue(start_title, 9)
            if DATAIO.query_queue_length() != 0:
                start_title = DATAIO.query_queue()
                logger.info(
                    "-----------------------------------------"
                    "\n                                    "
                    "Setting up crawl from queue."
                    "\n                                    "
                    "Start title: %s"
                    "\n                                    "
                    "End titles: %s"
                    "\n                                    "
                    "-----------------------------------------",
                    start_title,
                    end_titles,
                )

            else:
                start_title = utils.get_random_page_title()
                logger.info(
                    "-----------------------------------------"
                    "\n                                    "
                    "Setting up crawl from random start title."
                    "\n                                    "
                    "Start title: %s"
                    "\n                                    "
                    "End titles: %s"
                    "\n                                    "
                    "-----------------------------------------",
                    start_title,
                    end_titles,
                )

            exit_code = CRAWLER.crawl(start_title, end_titles)
            if exit_code == 0:
                print(f"Crawl number {counter} failed.")
                logger.info("Crawl number %s failed.", counter)
            else:
                DATAIO.remove_article_from_queue(start_title)
                print(f"Crawl number {counter} complete.")
                logger.info("Crawl number %s complete.", counter)
            print("\n")

    elif start_titles is None:
        logger.info("Setting up crawl(s) for %s random start titles.", iterations)
        for i in range(iterations):

            print(f"Crawl [{i + 1}/{iterations}].")
            logger.info("Crawl [%s/%s].", i + 1, iterations)
            while not (start_title := utils.get_random_page_title()):
                pass

            logger.info(
                "-----------------------------------------"
                "\n                                    "
                "Setting up crawl:"
                "\n                                    "
                "Start title: %s"
                "\n                                    "
                "End titles: %s"
                "\n                                    "
                "-----------------------------------------",
                start_title,
                end_titles,
            )

            exit_code = CRAWLER.crawl(start_title, end_titles)
            if exit_code == 0:
                print(f"Crawl [{i + 1}/{iterations}] failed.")
                logger.info("Crawl [%s/%s] failed.", i + 1, iterations)
            else:
                print(f"Crawl [{i + 1}/{iterations}] complete.")
                logger.info("Crawl [%s/%s] complete.", i + 1, iterations)
            print("\n")

    else:
        logger.info(
            "Setting up crawl(s) for %s start titles."
            "\n                                    "
            "%s",
            len(start_titles),
            start_titles,
        )

        for start_title in start_titles:
            print(f"Crawl from {start_title}.")
            logger.info("Crawl from %s.", start_title)
            logger.info(
                "-----------------------------------------"
                "\n                                    "
                "Setting up crawl:"
                "\n                                    "
                "Start title: %s"
                "\n                                    "
                "End titles: %s"
                "\n                                    "
                "-----------------------------------------",
                start_title,
                end_titles,
            )

            exit_code = CRAWLER.crawl(start_title, end_titles)
            if exit_code == 0:
                print(f"Crawl from {start_title} failed.")
                logger.info("Crawl from %s failed.", start_title)
            else:
                print(f"Crawl from {start_title} complete.")
                logger.info("Crawl from %s complete.", start_title)
            print("\n")


def main() -> None:
    """
    Main function.
    """

    start_titles = []
    end_titles = []

    print("\033[2J\033[H")
    continuous = input("Crawl continuously? (y/n): ").lower() == "y"

    # if input("Recheck dead ends? (y/n): ").lower() == "y":
    #     DATAIO.recheck_dead_ends()

    if input("Use default start and end titles? (y/n): ").lower() == "y":
        start_titles = None
        end_titles = ["Adolf_Hitler", "Jesus"]
        if input("Fill queue with shared starts? (y/n): ").lower() == "y":
            DATAIO.share_start_articles(end_titles)
        DATAIO.vacuum()
        setup_path(start_titles, end_titles, 1, continuous)

    while start_title := input("Enter the start title: "):
        start_titles.append(start_title)
    while end_title := input("Enter end title: "):
        end_titles.append(end_title)

    DATAIO.vacuum()

    if len(start_titles) > 0 or continuous:
        iterations = None
    else:
        start_titles = None
        if iterations := input("Enter the number of iterations: "):
            iterations = int(iterations)
        else:
            iterations = 1

    if end_titles:
        pass
    else:
        end_titles = [
            "Adolf_Hitler",
            "Jesus",
        ]

    setup_path(start_titles, end_titles, iterations, continuous)


if __name__ == "__main__":
    main()
