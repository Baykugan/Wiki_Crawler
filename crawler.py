"""
Crawler
=======
Crawls Wikipedia pages to find paths between two pages.
"""

import time
import re
from queue import Queue
from datetime import timedelta

from bs4 import BeautifulSoup

from logger import logger
from dataio import DataIO
import utils

logger.info(
    "-----------------------------------------"
    "\n                                    "
    "Wiki Crawler"
    "\n                                    "
    "-----------------------------------------"
)


class Crawler:
    """
    A class to crawl Wikipedia pages.
    """

    def __init__(self, data_io: DataIO):
        self.data_io = data_io

    def extract_links(self, page_title: str) -> list[str] | int:
        """
        Extracts the links from a Wikipedia page.

        Args:
            page_title (str): The title of the Wikipedia page.

        Returns:
            list[str]: The list of links in the Wikipedia page.
            None: If the request to the Wikipedia page fails.
        """

        if self.data_io.query_have_article_links(page_title):
            query = self.data_io.query_article_links(page_title)
            return query, False

        url = f"https://en.wikipedia.org/wiki/{page_title}"
        while not (response := utils.request_url(url)):
            pass

        if response.status_code == 200:
            soup = BeautifulSoup(response.content, "html.parser")

            content_div = soup.find("div", {"id": "mw-content-text"})

            if content_div:
                links = []
                for link in content_div.find_all("a", href=True):
                    if (
                        link.find_parent(class_="navbox") is None
                        and link.find_parent(class_="vertical-navbox") is None
                        and link.find_parent(class_="infobox") is None
                        and link.find_parent(class_="infobox vcard") is None
                        and link.find_parent(class_="thumb") is None
                        and link.find_parent(class_="thumbinner") is None
                        and link.find_parent(class_="metadata") is None
                        and link.find_parent(class_="mbox-small") is None
                        and link.find_parent(class_="hatnote") is None
                        and link.find_parent(class_="shortdescription") is None
                        and link.find_parent(class_="reflist") is None
                        and link.find_parent(class_="noprint") is None
                        and link.find_parent(class_="sidebar-content") is None
                        and link.find_parent(class_="toc") is None
                        and link.find_parent(class_="thumbcaption") is None
                        and link.find_parent(class_="wikitable") is None
                    ):
                        href = link["href"]
                        if href.startswith("/wiki/") and not ":" in href:
                            href = href.split("/wiki/")[1]
                            if "#" in href:
                                href = href.split("#")[0]
                            links.append(href)

                if not links:
                    logger.warning("No links found in %s.", url)
                    if not self.data_io.query_is_dead_end(page_title):
                        self.data_io.insert_dead_end(page_title)
                    return 0

                self.data_io.insert_article_links(page_title, links)

                return links, True

        logger.error("Failed to extract links from %s.", url)
        return 1

    def crawl(self, start_title: str, end_titles: list[str]) -> None | int:
        """
        Crawls from the start title to the end title.

        Args:
            start_title (str): The title of the start Wikipedia page.
            end_title (str): The title of the end Wikipedia page.
        """

        start_time = time.time()
        time.sleep(0.1)
        visited = set()
        paths = {}
        q = Queue()
        q.put((start_title,))
        new_articles = 0

        remaining_ends = end_titles.copy()
        longest_end_title = len(max(end_titles, key=len))
        padding = (
            14
            + len(end_titles)
            + sum(len(path) + 2 for path in paths.values())
            + (0 if paths else 1)
        )

        print("\n" * padding + "\n", end="")
        while q and remaining_ends:

            path = q.get()
            if path[-1] in visited:
                continue

            self.print_info(
                start_title,
                end_titles,
                path,
                paths,
                visited,
                start_time,
                remaining_ends,
                longest_end_title,
                padding,
                new_articles,
            )
            padding = (
                15
                + len(end_titles)
                + len(path)
                + sum(len(path) + 2 for path in paths.values())
                + (0 if paths else 1)
            )

            visited.add(path[-1])
            if (result := self.extract_links(path[-1])) == 0:
                if q.empty():
                    logger.error(
                        "-----------------------------------------"
                        "\n                                    "
                        "No more links to search."
                        "\n                                    "
                        "Start title: %s"
                        "\n                                    "
                        "End titles: %s"
                        "\n                                    "
                        "Remaining end titles: %s"
                        "\n                                    "
                        "Articles searched: %s"
                        "\n                                    "
                        "Time taken: %s"
                        "\n                                    "
                        "-----------------------------------------",
                        start_title,
                        end_titles,
                        remaining_ends,
                        len(visited),
                        timedelta(seconds=int(time.time() - start_time)),
                    )
                    print()
                    return 0

                continue

            elif result == 1:
                continue

            links, new_article = result
            new_articles += new_article

            for link in links:
                q.put(path + (link,))
                if link in remaining_ends:
                    logger.info(
                        "-----------------------------------------"
                        "\n                                    "
                        "%s found:"
                        "\n                                    "
                        "Path: %s"
                        "\n                                    "
                        "Articles searched: %s"
                        "\n                                    "
                        "Time taken: %s"
                        "\n                                    "
                        "-----------------------------------------",
                        link,
                        utils.wiki_link_log(path + (link,)),
                        len(visited),
                        timedelta(seconds=int(time.time() - start_time)),
                    )

                    self.data_io.insert_path(path + (link,))
                    remaining_ends.remove(link)
                    paths[str(len(visited)) + " " + link] = path + (link,)

        self.print_info(
            start_title,
            end_titles,
            path,
            paths,
            visited,
            start_time,
            remaining_ends,
            longest_end_title,
            padding,
            new_articles,
        )
        print()
        return 1

    def print_info(
        self,
        start_title: str,
        end_titles: list[str],
        path: tuple[str],
        paths: dict[str, tuple[str]],
        visited: set[str],
        start_time: int,
        remaining_ends: list[str],
        longest_end_title: int,
        padding: int,
        new_articles: int,
    ) -> None:
        """
        Prints the information about the crawl.

        Args:
            start_title (str): The title of the start Wikipedia page.
            end_title (str): The title of the end Wikipedia page.
            path (tuple[str]): The path from the start title to the end title.
            paths (dict[str, tuple[str]]): The path(s) from the start title to the end title.
            visited (set[str]): The set of visited Wikipedia pages.
            start_time (int): The start time of the crawl.
            remaining_ends (list[str]): The list of remaining end titles.
            longest_end_title (int): The length of the longest end title.
            padding (int): The padding for the print.
            new_articles (int): The number of new articles found.
        """

        line_length = 60

        def line_fill():
            print("\033[2K" + "█" * line_length, end="")
            # time.sleep(0.01)
            print("\033[2K\r", end="")

        def print_info_path(path):
            for i, link in enumerate(path):
                line_fill()
                print(
                    ljust_ansi(
                        f"│ {"╚═►" if i + 1 == len(path) else "╠═►"} {utils.wiki_link(link)}",
                        line_length - 1,
                    )
                    + "│"
                )

        def ljust_ansi(s: str, width: int, fillchar: str = " ") -> str:
            ansi_escape = re.compile(
                r"\033]8;;[\w\W]*?\033\\|\033\[38;2;255;0;0m|\033\[0m|\033\[38;2;0;255;0m|\033\[0m"
            )
            visible_length = len(ansi_escape.sub("", s))
            fill_length = max(0, width - visible_length)
            return s + (fillchar * fill_length)

        # pylint: disable=line-too-long

        print("\033[F" * padding, "\r", sep="", end="")
        line_fill()
        print("┌" + "─" * (line_length - 2) + "┐")
        line_fill()
        print("│ Stats".ljust(line_length - 1) + "│")
        line_fill()
        print("├" + "─" * (line_length - 2) + "┤")
        line_fill()
        print(
            ljust_ansi(f"│ Start: {utils.wiki_link(start_title)}", line_length - 1)
            + "│"
        )
        for i, end_title in enumerate(end_titles):
            line_fill()
            print(
                ljust_ansi(
                    f"│ End {i + 1}: {utils.wiki_link(end_title)}{"".ljust(longest_end_title-len(end_title))}: {"\033[38;2;255;0;0mX\033[0m" if end_title in remaining_ends else "\033[38;2;0;255;0m✓\033[0m"}",
                    line_length - 1,
                )
                + "│"
            )
        print(
            f"│ [{len(end_titles) - len(remaining_ends)}/{len(end_titles)}]: {utils.progress_bar(len(end_titles) - len(remaining_ends), len(end_titles), line_length - len(f"[{len(end_titles) - len(remaining_ends)}/{len(end_titles)}]") - 6)}".ljust(
                line_length - 1
            )
            + "│"
        )
        line_fill()
        print("├" + "─" * (line_length - 2) + "┤")
        line_fill()
        print(f"│ Articles searched: {len(visited)}".ljust(line_length - 1) + "│")
        line_fill()
        print(
            f"│ Time taken: {timedelta(seconds=int(time.time() - start_time))}".ljust(
                line_length - 1
            )
            + "│"
        )
        line_fill()
        print(
            f"│ Time per article: {round((time.time() - start_time) / max(len(visited), 1), 3)} seconds".ljust(
                line_length - 1
            )
            + "│"
        )
        line_fill()
        print(f"│ New articles searched: {new_articles}".ljust(line_length - 1) + "│")
        line_fill()
        print("├" + "─" * (line_length - 2) + "┤")
        line_fill()
        print(f"╞═╗ Current depth: {len(path) - 1}".ljust(line_length - 1) + "│")
        line_fill()
        print(
            ljust_ansi(
                f"│ ║ Current article: {utils.wiki_link(path[-1])}", line_length - 1
            )
            + "│"
        )
        line_fill()
        print_info_path(path)
        line_fill()
        print("├" + "─" * (line_length - 2) + "┤")
        line_fill()
        print("│   Paths:".ljust(line_length - 1) + "│")
        counter = 0
        for key, value in paths.items():
            counter += 1
            line_fill()
            print(
                ljust_ansi(
                    f"╞═╗ Path to {utils.wiki_link(key.split(" ")[1])} (Searched {key.split(" ")[0]} articles):",
                    line_length - 1,
                )
                + "│"
            )
            line_fill()
            print_info_path(value)
            if counter < len(paths):
                line_fill()
                print("│".ljust(line_length - 1) + "│")
        line_fill()
        print("└" + "─" * (line_length - 2) + "┘")

        # pylint: enable=line-too-long
