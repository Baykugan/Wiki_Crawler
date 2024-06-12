from logger import logger
import json
import pathlib
import portalocker

logger.info(
    "-----------------------------------------"
    "\n                                    "
    "Queue Fillers"
    "\n                                    "
    "-----------------------------------------"
)


def share_starts(
    end_titles: list[str], paths: pathlib.Path, queue: pathlib.Path
) -> None:
    """
    This function takes in a list of end_titles and filles queue.json
    with the start_titles that is not shared by the end_titles.

    Args:
        end_titles (list): A list of end_titles.
        paths (pathlib.Path): The path to the paths.json file.
        queue (pathlib.Path): The path to the queue.json file.
    """

    print("\n\nSharing beginnings...")
    logger.info(
        "-----------------------------------------"
        "\n                                    "
        "Sharing beginnings..."
    )

    with open(paths, "r", encoding="UTF-8") as file:
        portalocker.lock(file, portalocker.LOCK_SH)
        paths = json.load(file)
        portalocker.unlock(file)

    possible_start_titles = []

    for end_title in end_titles:
        if end_title in paths:
            titles = []
            for start_titles in paths[end_title].values():
                titles += start_titles.keys()

        possible_start_titles.append(set(titles))

    union_set = set.union(*map(set, possible_start_titles))

    # Find the intersection of all elements in the lists
    intersection_set = set.intersection(*possible_start_titles)

    # Subtract the intersection from the union to get unique elements
    unique_elements = list(union_set - intersection_set)

    


    with open(queue, "r+", encoding="UTF-8") as file:
        portalocker.lock(file, portalocker.LOCK_EX)
        queue = json.load(file)
        old_queue_len = len(queue["queue"])
        for title in unique_elements:
            if title not in queue["queue"]:
                queue["queue"].append(title)
        new_queue_len = len(queue["queue"])
        file.seek(0)
        json.dump(queue, file, indent=4)
        file.truncate()
        portalocker.unlock(file)

    print(f"New titles added to queue: {new_queue_len - old_queue_len}")
    logger.info(
        "New titles added to queue: %s"
        "\n                                    "
        "-----------------------------------------",
        new_queue_len - old_queue_len,
    )

    print("Beginnings shared.")
    logger.info(
        "Beginnings shared."
        "\n                                    "
        "-----------------------------------------"
    )
