"""
Utilities
=========
This module contains utility functions for the Wikipedia crawler.
"""

import time

import requests

from logger import logger

logger.info(
    "-----------------------------------------"
    "\n                                    "
    "Utilities"
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


def request_url(url: str, timeout: int = 10) -> str | None:
    """
    Requests the content of a URL.

    Args:
        title (str): The title of the Wikipedia page.

    Returns:
        str: The content of the Wikipedia page.
        None: If the request to the Wikipedia page fails.
    """

    try:
        response = requests.get(url, timeout=200)
        response.raise_for_status()
        return response
    except requests.exceptions.ConnectTimeout:
        logger.error("Connection timeout occurred.")
        logger.error("Retrying in %s seconds.", timeout)
        time.sleep(timeout)
        logger.error("Retrying...")
        return None
    except requests.exceptions.HTTPError as http_err:
        logger.error("HTTP error occurred: %s", http_err)
        return None
    except requests.exceptions.RetryError as retry_err:
        logger.error("Retry error occurred: %s", retry_err)
        return None
    except Exception as err:
        logger.error("An error occurred: %s", err)
        raise err


def get_random_page_title() -> str | None:
    """
    Retrieves the title of a random Wikipedia page.

    Returns:
        str: The title of the random Wikipedia page.
        None: If the request to the random page fails.
    """

    url = "https://en.wikipedia.org/wiki/Special:Random"
    while not (response := request_url(url)):
        pass

    if response.status_code == 200:
        return response.url.split("/")[-1]

    logger.error("Failed to get random page title.")
    return None


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


def path_to_string(path: tuple[str]) -> str:
    """
    Converts a path to a string.

    Args:
        path (tuple[str]): The path from the start title to the end title.

    Returns:
        str: The string representation of the path.
    """

    return " -> ".join((wiki_link(link) for link in path))
