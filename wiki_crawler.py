"""
Wiki Crawler
============
This script crawls from a start Wikipedia page to an end Wikipedia page.
"""

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

PATH = pathlib.Path(__file__).parent.resolve()




def progress_bar(progress: int, goal: int) -> str:
    """
    Returns a progress bar string.

    Args:
        progress (int): The progress value.

    Returns:
        str: The progress bar string.
    """
    pbar = {0: "          ",
            1:  "▌         ",
            2:  "█         ",
            3:  "█▌        ",
            4:  "██        ",
            5:  "██▌       ",
            6:  "███       ",
            7:  "███▌      ",
            8:  "████      ",
            9:  "████▌     ",
            10: "█████     ",
            11: "█████▌    ",
            12: "██████    ",
            13: "██████▌   ",
            14: "███████   ",
            15: "███████▌  ",
            16: "████████  ",
            17: "████████▌ ",
            18: "█████████ ",
            19: "█████████▌",
            20: "██████████",
           }

    if progress > goal:
        return pbar[20]
    else:
        percent = round(progress / goal * 20)

    return "[" + pbar[percent] + "]"





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

def extract_links(page_title: str) -> list[str] | None:
    """
    Extracts the links from a Wikipedia page.

    Args:
        page_title (str): The title of the Wikipedia page.

    Returns:
        list[str]: The list of links in the Wikipedia page.
        None: If the request to the Wikipedia page fails.
    """

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
            return links

    logger.error("Failed to extract links from %s.", url)
    return None


def crawl(start_title: str, end_titles: list[str]) -> dict[str, tuple[str]]:
    """
    Crawls from the start title to the end title.

    Args:
        start_title (str): The title of the start Wikipedia page.
        end_title (str): The title of the end Wikipedia page.

    Returns:
        dict[str, tuple[str]]: The path(s) from the start title to the end title.
    """

    start_time = time.time()
    time.sleep(0.1)
    visited = set()
    paths = {}
    q = Queue()
    q.put((start_title,))

    remaining_ends = end_titles.copy()
    longest_end_title = len(max(end_titles, key=len))
    padding = 16 + len(end_titles) + sum(len(path) + 1 for path in paths.values())


    print("\n" * padding + "\n", end="")
    while q and remaining_ends:


        path = q.get()
        if path[-1] in visited:
            continue
        visited.add(path[-1])


        print_info(start_title,
                   end_titles,
                   path,
                   paths,
                   visited,
                   start_time,
                   remaining_ends,
                   longest_end_title,
                   padding
        )
        padding = 16 + len(end_titles) + len(path) + sum(len(path) + 1 for path in paths.values())

        while not (links := extract_links(path[-1])):
            pass
        for link in links:
            q.put(path + (link,))
            if link in remaining_ends:
                logger.info(
                    "-----------------------------------------"
                    "\n                                    "
                    "End title found: %s"
                    "\n                                    "
                    "Path: %s"
                    "\n                                    "
                    "Articles searched: %s"
                    "\n                                    "
                    "Time taken: %s seconds"
                    "\n                                    "
                    "-----------------------------------------",
                    link, path + (link,), len(visited), timedelta(seconds=int(time.time() - start_time)))
                remaining_ends.remove(link)
                paths[str(len(visited)) + " " + link] = path + (link,)

    print_info(start_title,
               end_titles,
               path,
               paths,
               visited,
               start_time,
               remaining_ends,
               longest_end_title,
               padding
    )


    return paths


def print_info(start_title: str,
               end_titles: list[str],
               path: tuple[str],
               paths: dict[str, tuple[str]],
               visited: set[str],
               start_time: int,
               remaining_ends: list[str],
               longest_end_title: int,
               padding: int
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
    """

    line_length = 60

    def line_fill():
        print("\033[2K" + "█"*line_length, end="")
        time.sleep(0.01)
        print("\033[2K\r", end="")

    def print_info_path(path):
        for i, link in enumerate(path):
            line_fill()
            print(ljust_ansi(f"│  {"╚═►" if i + 1 == len(path) else "╠═►"} {wiki_link(link)}", line_length - 1) + "│")

    def ljust_ansi(s: str, width: int, fillchar: str = " ") -> str:
        ansi_escape = re.compile(r'\033]8;;[\w\W]*?\033\\|\033\[38;2;255;0;0m|\033\[0m|\033\[38;2;0;255;0m|\033\[0m')
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
        print(ljust_ansi(f"│ End {i + 1}: {wiki_link(end_title)}{"".ljust(longest_end_title-len(end_title))}: {f"\033[38;2;255;0;0m{"X"}\033[0m" if end_title in remaining_ends else f"\033[38;2;0;255;0m{"✓"}\033[0m"}", line_length - 1) + "│")
    print(f"│ [{len(end_titles) - len(remaining_ends)}/{len(end_titles)}]: {progress_bar(len(end_titles) - len(remaining_ends), len(end_titles))}".ljust(line_length - 1) + "│")
    line_fill()
    print("├" + "─" * (line_length - 2) + "┤")
    line_fill()
    print(f"│ Articles searched: {len(visited)}".ljust(line_length - 1) + "│")
    line_fill()
    print(f"│ Time taken: {timedelta(seconds=int(time.time() - start_time))} seconds".ljust(line_length - 1) + "│")
    line_fill()
    print(
        f"│ Time per article: {round((time.time() - start_time) / len(visited), 3)} seconds".ljust(line_length - 1) + "│"
    )
    line_fill()
    print("├" + "─" * (line_length - 2) + "┤")
    line_fill()
    print(f"│ Current depth: {len(path) - 1}".ljust(line_length - 1) + "│")
    line_fill()
    print(ljust_ansi(f"│ Current article: {wiki_link(path[-1])}", line_length - 1) + "│")
    line_fill()
    print("│ Path:".ljust(line_length - 1) + "│")
    line_fill()
    print_info_path(path)
    line_fill()
    print("├" + "─" * (line_length - 2) + "┤")
    line_fill()
    print("│ Paths:".ljust(line_length - 1) + "│")
    for key, value in paths.items():
        line_fill()
        print(ljust_ansi(f"│ Path to {wiki_link(key.split(" ")[1])} (Searched {key.split(" ")[0]} articles):", line_length - 1) + "│")
        line_fill()
        print_info_path(value)
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
    start_titles: list[str] = None, end_titles: list[str] = None, iterations: int = 1
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
    """

    if start_titles is None:
        logger.info("Setting up crawl(s) for %s random start titles.", iterations)
        for i in range(iterations):

            print("Iteration: ", i + 1)
            while not (start_title := get_random_page_title()):
                pass

            logger.info(
                "-----------------------------------------"
                "\n                                    "
                "Setting up crawl number [%s/%s]."
                "\n                                    "
                "First random title: %s"
                "\n                                    "
                "End titles: %s"
                "\n                                    "
                "-----------------------------------------",
                i + 1, iterations, start_title, end_titles)

            show_and_save_path(start_title, end_titles)
            logger.info("Crawl number %s complete.", i + 1)
    else:
        logger.info("Setting up crawl(s) for %s start titles."
                    "\n                                    "
                    "%s",
                    len(start_titles), start_titles)

        for title in start_titles:
            logger.info(
                "-----------------------------------------"
                "\n                                    "
                "Setting up crawl."
                "\n                                    "
                "Start title: %s"
                "\n                                    "
                "End titles: %s"
                "\n                                    "
                "-----------------------------------------",
                title, end_titles)

            show_and_save_path(title, end_titles)
            logger.info("Crawl complete.")


def show_and_save_path(start_title: str, end_titles: list[str]) -> None:
    """
    Shows and saves the path from the start title to the end title.

    Args:
        start_title (str): The title of the start Wikipedia page.
        end_title (str): The title of the end Wikipedia page.
    """

    paths = crawl(start_title, end_titles)

    for path in paths.values():
        print(
            "Path: " + path_to_string(path)
        )
        save_path(path)


def wiki_link(title: str) -> str:
    """
    Returns the Wikipedia link for the title.

    Args:
        title (str): The title of the Wikipedia page.

    Returns:
        str: The Wikipedia link for the title.
    """

    return f"\x1b]8;;https://en.wikipedia.org/wiki/{title}\x1b\\{title}\x1b]8;;\x1b\\"


def save_path(path: tuple[str]) -> None:
    """
    Saves the path from the start title to the end title.

    Args:
        path (tuple[str]): The path from the start title to the end title.
    """

    path_length = str(len(path))
    save_loc = PATH / "paths.json"

    with open(save_loc, "r", encoding="UTF-8") as file:
        portalocker.lock(file, portalocker.LOCK_SH)
        data = json.load(file)
        portalocker.unlock(file)


    if path[-1] not in data:
        data[path[-1]] = {}
        data = {k: v for k, v in sorted(data.items(), key=lambda x: x[0])}
    if path_length not in data[path[-1]]:
        data[path[-1]][path_length] = {}
        data[path[-1]] = {
            k: v for k, v in sorted(data[path[-1]].items(), key=lambda x: x[0])
        }
    if path[0] not in data[path[-1]][path_length]:
        data[path[-1]][path_length][path[0]] = {}
        data[path[-1]][path_length] = {
            k: v
            for k, v in sorted(data[path[-1]][path_length].items(), key=lambda x: x[0])
        }

    data[path[-1]][path_length][path[0]] = path

    logger.info("Path saved: %s", path)
    with open(save_loc, "w", encoding="UTF-8") as file:
        portalocker.lock(file, portalocker.LOCK_EX)
        json.dump(data, file, indent=4)
        portalocker.unlock(file)

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
    while start_title := input("Enter the start title: "):
        start_titles.append(start_title)
    while end_title := input("Enter the end title: "):
        end_titles.append(end_title)

    if len(start_titles) > 0:
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
        end_titles = ["Adolf_Hitler",
                      "Jesus",
                      "Napoleon",
                      ]

    setup_path(start_titles, end_titles, iterations)


if __name__ == "__main__":
    main()
