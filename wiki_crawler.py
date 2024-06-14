"""
Wiki Crawler
============
This script crawls from a start Wikipedia page to an end Wikipedia page.
"""

# pylint: disable=line-too-long

import json
import pathlib
import re
import time
from datetime import timedelta
from queue import Queue

import portalocker
import requests
from bs4 import BeautifulSoup

from logger import logger
from queue_fillers import share_starts, recheck_dead_ends

PATH = pathlib.Path(__file__).parent.resolve()

logger.info(
    "-----------------------------------------"
    "\n                                    "
    "Wiki Crawler"
    "\n                                    "
    "-----------------------------------------"
)


def progress_bar(progress: int, goal: int, bar_length: int = 27) -> str:
    """
    Returns a progress bar string.

    Args:
        progress (int): The progress value.

    Returns:
        str: The progress bar string.
    """

    bracketless_length = bar_length - 2

    if progress > goal:
        return "[" + "█" * bracketless_length + "]"  # 100%

    percent = round(progress / goal * bracketless_length * 2)

    full_blocks = percent // 2
    half_block = percent % 2
    empty_blocks = bracketless_length - full_blocks - (1 if half_block else 0)

    return (
        "[" + "█" * full_blocks + ("▌" if half_block else "") + " " * empty_blocks + "]"
    )


def get_random_page_title() -> str | None:
    """
    Retrieves the title of a random Wikipedia page.

    Returns:
        str: The title of the random Wikipedia page.
        None: If the request to the random page fails.
    """

    response = requests.get("https://en.wikipedia.org/wiki/Special:Random", timeout=100)
    if response.status_code == 200:
        return response.url.split("/")[-1]

    logger.error("Failed to get random page title.")
    return None


def extract_links(page_title: str) -> list[str] | int:
    """
    Extracts the links from a Wikipedia page.

    Args:
        page_title (str): The title of the Wikipedia page.

    Returns:
        list[str]: The list of links in the Wikipedia page.
        None: If the request to the Wikipedia page fails.
    """

    with open(PATH / "links.json", "r+", encoding="UTF-8") as file:
        portalocker.lock(file, portalocker.LOCK_EX)
        data = json.load(file)

        if page_title in data:
            logger.info("Links already extracted for %s.", page_title)
            portalocker.unlock(file)
            return data[page_title], False

    url = f"https://en.wikipedia.org/wiki/{page_title}"
    response = requests.get(url, timeout=100)

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
                with open(PATH / "dead_ends.json", "r+", encoding="UTF-8") as file:
                    portalocker.lock(file, portalocker.LOCK_EX)
                    data = json.load(file)

                    if url not in data["dead_ends"]:
                        data["dead_ends"].append(url)

                    file.seek(0)
                    json.dump(data, file, indent=4)
                    file.truncate()
                    portalocker.unlock(file)
                return 0

            with open(PATH / "links.json", "r+", encoding="UTF-8") as file:
                portalocker.lock(file, portalocker.LOCK_EX)
                data = json.load(file)

                data[page_title] = links

                data = dict(sorted(data.items(), key=lambda x: x[0]))

                file.seek(0)
                json.dump(data, file, indent=4)
                file.truncate()
                portalocker.unlock(file)

                logger.info("Links extracted for %s.", page_title)

            return links, True

    logger.error("Failed to extract links from %s.", url)
    return 1


def crawl(start_title: str, end_titles: list[str]) -> None | int:
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

        print_info(
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
        if (result := extract_links(path[-1])) == 0:
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
                    wiki_link_log(path + (link,)),
                    len(visited),
                    timedelta(seconds=int(time.time() - start_time)),
                )

                save_path(path + (link,))
                remaining_ends.remove(link)
                paths[str(len(visited)) + " " + link] = path + (link,)

    print_info(
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
                    f"│ {"╚═►" if i + 1 == len(path) else "╠═►"} {wiki_link(link)}",
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

    print("\033[F" * padding, "\r", sep="", end="")
    line_fill()
    print("┌" + "─" * (line_length - 2) + "┐")
    line_fill()
    print("│ Stats".ljust(line_length - 1) + "│")
    line_fill()
    print("├" + "─" * (line_length - 2) + "┤")
    line_fill()
    print(ljust_ansi(f"│ Start: {wiki_link(start_title)}", line_length - 1) + "│")
    for i, end_title in enumerate(end_titles):
        line_fill()
        print(
            ljust_ansi(
                f"│ End {i + 1}: {wiki_link(end_title)}{"".ljust(longest_end_title-len(end_title))}: {f"\033[38;2;255;0;0m{"X"}\033[0m" if end_title in remaining_ends else f"\033[38;2;0;255;0m{"✓"}\033[0m"}",
                line_length - 1,
            )
            + "│"
        )
    print(
        f"│ [{len(end_titles) - len(remaining_ends)}/{len(end_titles)}]: {progress_bar(len(end_titles) - len(remaining_ends), len(end_titles), line_length - len(f"[{len(end_titles) - len(remaining_ends)}/{len(end_titles)}]") - 6)}".ljust(
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
    print(f"│ New articles found: {new_articles}".ljust(line_length - 1) + "│")
    line_fill()
    print("├" + "─" * (line_length - 2) + "┤")
    line_fill()
    print(f"╞═╗ Current depth: {len(path) - 1}".ljust(line_length - 1) + "│")
    line_fill()
    print(
        ljust_ansi(f"│ ║ Current article: {wiki_link(path[-1])}", line_length - 1) + "│"
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
                f"╞═╗ Path to {wiki_link(key.split(" ")[1])} (Searched {key.split(" ")[0]} articles):",
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


def path_to_string(path: tuple[str]) -> str:
    """
    Converts a path to a string.

    Args:
        path (tuple[str]): The path from the start title to the end title.

    Returns:
        str: The string representation of the path.
    """

    return " -> ".join((wiki_link(link) for link in path))


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
            with open(PATH / "queue.json", "r+", encoding="UTF-8") as file:
                portalocker.lock(file, portalocker.LOCK_EX)
                data = json.load(file)

                if start_titles is not None:
                    data["queue"] = start_titles + data["queue"]
                    start_titles = None
                if data["queue"] != []:
                    start_title = data["queue"].pop(0)
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
                    start_title = get_random_page_title()
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

                file.seek(0)
                json.dump(data, file, indent=4)
                file.truncate()
                portalocker.unlock(file)

            exit_code = crawl(start_title, end_titles)
            if exit_code == 0:
                print(f"Crawl number {counter} failed.")
                logger.info("Crawl number %s failed.", counter)
            else:
                print(f"Crawl number {counter} complete.")
                logger.info("Crawl number %s complete.", counter)
            print("\n")

    elif start_titles is None:
        logger.info("Setting up crawl(s) for %s random start titles.", iterations)
        for i in range(iterations):

            print(f"Crawl [{i + 1}/{iterations}].")
            logger.info("Crawl [%s/%s].", i + 1, iterations)
            while not (start_title := get_random_page_title()):
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

            exit_code = crawl(start_title, end_titles)
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

            exit_code = crawl(start_title, end_titles)
            if exit_code == 0:
                print(f"Crawl from {start_title} failed.")
                logger.info("Crawl from %s failed.", start_title)
            else:
                print(f"Crawl from {start_title} complete.")
                logger.info("Crawl from %s complete.", start_title)
            print("\n")


def wiki_link(title: str) -> str:
    """
    Returns the Wikipedia link for the title.

    Args:
        title (str): The title of the Wikipedia page.

    Returns:
        str: The Wikipedia link for the title.
    """

    return f"\x1b]8;;https://en.wikipedia.org/wiki/{title}\x1b\\{title}\x1b]8;;\x1b\\"


def wiki_link_log(path: list[str] | tuple[str]) -> str:
    """
    Returns the Wikipedia links for the path.

    Args:
        path (list[str] | tuple[str]): The path from the start title to the end title.

    Returns:
        str: The Wikipedia links for the path.
    """

    links = [
        "\n                                        "
        + f"https://en.wikipedia.org/wiki/{title}"
        for title in path
    ]
    link_str = "[" + ("".join(links)) + "\n                                    ]"

    return link_str


def save_path(path: tuple[str]) -> None:
    """
    Saves the path from the start title to the end title.

    Args:
        path (tuple[str]): The path from the start title to the end title.
    """

    path_length = str(len(path))

    with open(PATH / "paths.json", "r+", encoding="UTF-8") as file:
        portalocker.lock(file, portalocker.LOCK_EX)
        data = json.load(file)

        if path[-1] not in data:
            data[path[-1]] = {}
            data = dict(sorted(data.items(), key=lambda x: x[0]))
        if path_length not in data[path[-1]]:
            data[path[-1]][path_length] = {}
            data[path[-1]] = dict(sorted(data[path[-1]].items(), key=lambda x: x[0]))
        if path[0] not in data[path[-1]][path_length]:
            data[path[-1]][path_length][path[0]] = {}
            data[path[-1]][path_length] = dict(
                sorted(data[path[-1]][path_length].items(), key=lambda x: x[0])
            )
        else:
            logger.info(
                "Path already saved: %s",
                path,
            )
            portalocker.unlock(file)
            return

        data[path[-1]][path_length][path[0]] = path

        file.seek(0)
        json.dump(data, file, indent=4)
        file.truncate()
        portalocker.unlock(file)

        logger.info("Path saved: %s", path)

    if len(path) >= 3:
        path = path[1:]
        save_path(path)


def main() -> None:
    """
    Main function.
    """

    start_titles = []
    end_titles = []

    print("\033[2J")
    continuous = input("Crawl continuously? (y/n): ").lower() == "y"

    # if input("Recheck dead ends? (y/n): ").lower() == "y":
    #     recheck_dead_ends()

    if input("Use default start and end titles? (y/n): ").lower() == "y":
        start_titles = None
        end_titles = ["Adolf_Hitler", "Jesus"]
        if input("Fill queue with shared starts? (y/n): ").lower() == "y":
            share_starts(
                end_titles=end_titles,
                paths=PATH / "paths.json",
                queue=PATH / "queue.json",
            )
        setup_path(start_titles, end_titles, 1, continuous)

    while start_title := input("Enter the start title: "):
        start_titles.append(start_title)
    while end_title := input("Enter end title: "):
        end_titles.append(end_title)

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
