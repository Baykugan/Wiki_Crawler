from logger import logger
from dataio import DataIO

logger.info(
    "-----------------------------------------"
    "\n                                    "
    "Queue Fillers"
    "\n                                    "
    "-----------------------------------------"
)


def share_starts(data_io: DataIO, end_titles: list[str]) -> None:
    """
    This function adds the start titles without a complete
    path to all end titles to the queue.json file.

    Args:
        data_io (DataIO): The DataIO object to interact with the database.
        end_titles (list[str]): The list of end titles to share the start titles with.
    """

    print("\n\nSharing beginnings...")
    logger.info(
        "-----------------------------------------"
        "\n                                    "
        "Sharing beginnings..."
    )

    old_queue_len = data_io.query_queue_length()
    for title in data_io.query_non_complete_start_articles(end_titles):
        data_io.insert_queue(title, 5)
    new_queue_len = data_io.query_queue_length()

    print(f"New titles added to queue: {new_queue_len - old_queue_len}")
    logger.info(
        "New titles added to queue: %s",
        new_queue_len - old_queue_len,
    )

    print("Beginnings shared.")
    logger.info(
        "Beginnings shared."
        "\n                                    "
        "-----------------------------------------"
    )


def recheck_dead_ends(data_io: DataIO) -> None:
    """
    This function adds the dead ends from the database
    to the beginning of the queue.json file.

    Args:
        data_io (DataIO): The DataIO object to interact with the database.
    """

    print("\n\nRechecking dead ends...")
    logger.info(
        "-----------------------------------------"
        "\n                                    "
        "Rechecking dead ends..."
    )

    old_queue_len = data_io.query_queue_length()
    for title in data_io.query_dead_ends():
        data_io.insert_queue(title, 7)
    new_queue_len = data_io.query_queue_length()

    print(f"New titles added to queue: {new_queue_len - old_queue_len}")
    logger.info(
        "New titles added to queue: %s",
        new_queue_len - old_queue_len,
    )

    print("Dead ends rechecked.\n\n")
    logger.info(
        "Dead ends rechecked."
        "\n                                    "
        "-----------------------------------------"
    )


