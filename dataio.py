"""
DataIO class for handling database operations.
"""

import time

import sqlite3 as sqlite
from pathlib import Path
from sqlite3.dbapi2 import Connection
from logger import logger

logger.info(
    "-----------------------------------------"
    "\n                                    "
    "DataIO"
    "\n                                    "
    "-----------------------------------------"
)


class DataIO:
    """
    DataIO class for handling database operations.
    """

    #########################################
    ##### Initialization and Connection #####
    #########################################

    def __init__(self, db_file: Path, path: Path) -> None:
        self.db_file = db_file
        self.path = path
        self.conn = self.create_connection(db_file)
        self.ensure_tables()

    def create_connection(self, db_file: Path) -> Connection:
        """
        Create a database connection to the SQLite database.
        """

        try:
            conn = sqlite.connect(db_file)
            return conn
        except sqlite.Error as e:
            logger.error(e)

        return None

    def ensure_tables(self) -> None:
        """
        Ensure the tables exist in the database.
        """

        queries = [
            """
        CREATE TABLE IF NOT EXISTS articles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            article TEXT UNIQUE NOT NULL
        );
        """,
            """
        CREATE TABLE IF NOT EXISTS paths (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            start_article_id INTEGER NOT NULL,
            end_article_id INTEGER NOT NULL,
            path_length INTEGER NOT NULL CHECK (path_length > 0),
            FOREIGN KEY (start_article_id) REFERENCES articles(id)
                ON DELETE CASCADE
                ON UPDATE CASCADE,
            FOREIGN KEY (end_article_id) REFERENCES articles(id)
                ON DELETE CASCADE
                ON UPDATE CASCADE
        );
        """,
            """
        CREATE TABLE IF NOT EXISTS steps (
            path_id INTEGER NOT NULL,
            step_number INTEGER NOT NULL CHECK (step_number >= 0),
            article_id INTEGER NOT NULL,
            PRIMARY KEY (path_id, step_number),
            FOREIGN KEY (path_id) REFERENCES paths(id)
                ON DELETE CASCADE
                ON UPDATE CASCADE,
            FOREIGN KEY (article_id) REFERENCES articles(id)
                ON DELETE CASCADE
                ON UPDATE CASCADE
            
        );
        """,
            """
        CREATE TABLE IF NOT EXISTS links_in_article (
            article_id INTEGER NOT NULL,
            link_number INTEGER NOT NULL CHECK (link_number >= 0),
            link_id INTEGER NOT NULL,
            PRIMARY KEY (article_id, link_number),
            UNIQUE (article_id, link_id),
            FOREIGN KEY (article_id) REFERENCES articles(id)
                ON DELETE CASCADE
                ON UPDATE CASCADE,
            FOREIGN KEY (link_id) REFERENCES articles(id)
                ON DELETE CASCADE
                ON UPDATE CASCADE
        );
        """,
            """
        CREATE TABLE IF NOT EXISTS dead_ends (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            article_id INTEGER NOT NULL,
            FOREIGN KEY (article_id) REFERENCES articles(id)
                ON DELETE CASCADE
                ON UPDATE CASCADE
        );
        """,
            """
        CREATE TABLE IF NOT EXISTS queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            article_id INTEGER UNIQUE NOT NULL,
            priority INTEGER NOT NULL CHECK (priority >= 0 AND priority <= 9),
            FOREIGN KEY (article_id) REFERENCES articles(id)
                ON DELETE CASCADE
                ON UPDATE CASCADE
        );


        """,
            """
        CREATE INDEX IF NOT EXISTS idx_paths_start_article_id ON paths(start_article_id);
        CREATE INDEX IF NOT EXISTS idx_paths_end_article_id ON paths(end_article_id);
        CREATE INDEX IF NOT EXISTS idx_steps_path_id ON steps(path_id);
        CREATE INDEX IF NOT EXISTS idx_steps_article_id ON steps(article_id);
        CREATE INDEX IF NOT EXISTS idx_links_in_article_article_id ON links_in_article(article_id);
        CREATE INDEX IF NOT EXISTS idx_links_in_article_link_id ON links_in_article(link_id);
        CREATE INDEX IF NOT EXISTS idx_dead_ends_article_id ON dead_ends(article_id);
        CREATE INDEX IF NOT EXISTS idx_queue_priority ON queue(priority) DESC;
        """,
        ]

        try:
            c = self.conn.cursor()
            for query in queries:
                c.execute(query)
            c.close()
            self.conn.commit()
        except sqlite.Error as e:
            print(e)

        logger.info("Connection to the database closed.")

    def close(self):
        """
        Close the database connection.
        """

        self.conn.close()
        logger.info("Connection to the database closed.")

    #######################
    ##### Maintenance #####
    #######################

    def vacuum(self) -> None:
        """
        Vacuum the database.
        """

        print("\n\nVacuuming database...")
        logger.info(
            "-----------------------------------------"
            "\n                                    "
            "Vacuuming database..."
        )

        try:
            start = time.time()

            c = self.conn.cursor()
            c.execute("VACUUM")
            c.close()
            self.conn.commit()

            end = time.time()

            print(f"Database vacuumed in {end - start:.2f} seconds.")
            logger.info(
                "Database vacuumed in %s seconds.",
                round(end - start, 2),
            )
        except sqlite.Error as e:
            print(e)
            print("Database vacuum failed.")
            logger.error(
                "Database vacuum failed."
                "\n                                    "
                "-----------------------------------------"
            )

    def generate_report(self) -> dict:
        """
        Generate a report of the database.
        """

        print("\n\nGenerating report...")
        logger.info(
            "-----------------------------------------"
            "\n                                    "
            "Generating report..."
        )

        report = {
            "articles_count": 0,
            "paths_count": 0,
            "dead_ends_count": 0,
            "links_in_article_count": 0,
        }

        try:
            start = time.time()

            c = self.conn.cursor()

            c.execute("SELECT COUNT(*) FROM articles")
            report["articles_count"] = c.fetchone()[0]

            c.execute("SELECT COUNT(*) FROM paths")
            report["paths_count"] = c.fetchone()[0]

            c.execute("SELECT COUNT(*) FROM dead_ends")
            report["dead_ends_count"] = c.fetchone()[0]

            c.execute("SELECT COUNT(*) FROM links_in_article")
            report["links_in_article_count"] = c.fetchone()[0]

            c.close()

            end = time.time()

            print(f"Articles: {report['articles_count']}")
            print(f"Paths: {report['paths_count']}")
            print(f"Dead ends: {report['dead_ends_count']}")
            print(f"Links in article: {report['links_in_article_count']}")
            print(f"Report generated in {end - start:.2f} seconds.")
            logger.info(
                "Articles: %s"
                "\n                                    "
                "Paths: %s"
                "\n                                    "
                "Dead ends: %s"
                "\n                                    "
                "Links in article: %s"
                "\n                                    "
                "Report generated in %s seconds.",
                report["articles_count"],
                report["paths_count"],
                report["dead_ends_count"],
                report["links_in_article_count"],
                round(end - start, 2),
            )

            print("\n\nReport generated.")
            logger.info(
                "Report generated."
                "\n                                    "
                "-----------------------------------------"
            )

        except sqlite.Error as e:
            print(e)
            print("Report generation failed.")
            logger.error(
                "Report generation failed."
                "\n                                    "
                "-----------------------------------------"
            )

        return report

    ########################
    ##### Data Helpers #####
    ########################

    def get_or_create_article_id(self, article: str) -> int:
        """
        Get the article id from the database or create it if it does not exist.
        """

        c = self.conn.cursor()
        c.execute("SELECT id FROM articles WHERE article=?", (article,))
        if row := c.fetchone():
            article_id = row[0]
        else:
            c.execute("INSERT INTO articles (article) VALUES (?)", (article,))
            article_id = c.lastrowid
            self.conn.commit()
        c.close()
        return article_id

    def get_article_ids(self, articles: list[str]) -> list[int]:
        """
        Get the IDs of the articles in a list.
        """

        c = self.conn.cursor()
        path_article_ids = []
        for article in articles:
            article_id = self.get_or_create_article_id(article)
            path_article_ids.append(article_id)
        c.close()
        return path_article_ids

    ########################
    ##### Main Inserts #####
    ########################

    def insert_path(self, path: tuple[str], deep_save: bool = False) -> None:
        """
        Insert a path into the database. The path is associated with a start and end article.
        """

        c = self.conn.cursor()

        def insert(path: tuple[str]) -> None:
            if self.query_path_exists(path[1], path[-1]):
                logger.info("Path already saved: %s", path)
                return

            article_ids = self.get_article_ids(path)

            start_article_id = article_ids[0]
            end_article_id = article_ids[-1]

            c.execute(
                """
                INSERT INTO paths (start_article_id, end_article_id, path_length)
                VALUES (?, ?, ?)
                """,
                (start_article_id, end_article_id, len(path)),
            )
            path_id = c.lastrowid

            for i, article_id in enumerate(article_ids):
                c.execute(
                    "INSERT INTO steps (path_id, step_number, article_id) VALUES (?, ?, ?)",
                    (path_id, i, article_id),
                )

            logger.info("Path saved: %s", path)

        insert(path)
        if deep_save:
            for i in range(1, len(path) - 2):
                insert(path[i:])

        self.conn.commit()
        c.close()

    def insert_article_links(self, article: str, links: list[str]) -> None:
        """
        Insert a list of links into the database. The links are associated with an article.
        """

        c = self.conn.cursor()
        article_id = self.get_or_create_article_id(article)

        for i, link in enumerate(links):
            link_id = self.get_or_create_article_id(link)
            c.execute(
                """
                INSERT OR IGNORE INTO links_in_article (article_id, link_number, link_id)
                VALUES (?, ?, ?)
                """,
                (article_id, i, link_id),
            )

        self.conn.commit()
        c.close()

    def insert_dead_end(self, article: str) -> None:
        """
        Insert a dead end into the database.
        """

        article_id = self.get_or_create_article_id(article)

        c = self.conn.cursor()
        c.execute(
            "INSERT INTO dead_ends (article_id) VALUES (?)",
            (article_id,),
        )
        self.conn.commit()
        c.close()

    def insert_queue(
        self, article: str, priority: int, condition: str = "higher"
    ) -> None:
        """
        Insert an article into the queue.
        """

        article_id = self.get_or_create_article_id(article)

        c = self.conn.cursor()

        match condition:
            case "higher":
                c.execute(
                    """
                    INSERT INTO queue (article_id, priority)
                    VALUES (?, ?)
                    ON CONFLICT(article_id) 
                    DO UPDATE SET priority = excluded.priority
                    WHERE excluded.priority > queue.priority;
                    """,
                    (article_id, priority),
                )
            case "lower":
                c.execute(
                    """
                    INSERT INTO queue (article_id, priority)
                    VALUES (?, ?)
                    ON CONFLICT(article_id) 
                    DO UPDATE SET priority = excluded.priority
                    WHERE excluded.priority < queue.priority;
                    """,
                    (article_id, priority),
                )
            case "both":
                c.execute(
                    """
                    INSERT INTO queue (article_id, priority)
                    VALUES (?, ?)
                    ON CONFLICT(article_id) 
                    DO UPDATE SET queue.priority = excluded.priority;
                    """,
                    (article_id, priority),
                )
            case "none":
                c.execute(
                    """
                    INSERT OR IGNORE INTO queue (article_id, priority)
                    VALUES (?, ?)
                    """,
                    (article_id, priority),
                )

        self.conn.commit()
        c.close()

    ########################
    ##### Main Queries #####
    ########################

    def query_have_article_links(self, article: str) -> bool:
        """
        Query if an article has been searched.
        """

        c = self.conn.cursor()
        c.execute(
            """
            SELECT 1
            FROM links_in_article lia
            JOIN articles a ON lia.article_id = a.id
            WHERE a.article = ?;
            """,
            (article,),
        )

        result = c.fetchone()
        c.close()
        return result is not None

    def query_article_links(self, article: str) -> list[str]:
        """
        Query the links associated with a given article if it exists.
        """

        c = self.conn.cursor()
        c.execute(
            """
            SELECT a2.article
            FROM (
                SELECT id
                FROM articles
                WHERE article = ?
            ) a1
            JOIN links_in_article lia ON a1.id = lia.article_id
            JOIN articles a2 ON lia.link_id = a2.id
            ORDER BY lia.link_number ASC;
            """,
            (article,),
        )

        results = c.fetchall()
        c.close()
        return [r[0] for r in results]

    def query_path_exists(self, start_article: str, end_article: str) -> list:
        """
        Query if a path exists between two articles.
        """

        c = self.conn.cursor()
        c.execute(
            """
            SELECT 1
            FROM paths p
            JOIN articles sa ON p.start_article_id = sa.id
            JOIN articles ea ON p.end_article_id = ea.id
            WHERE sa.article = ? AND ea.article = ?;
            """,
            (start_article, end_article),
        )

        result = c.fetchone()
        c.close()
        return result is not None

    def query_is_dead_end(self, article: str) -> bool:
        """
        Query if a dead end exists in the database.
        """

        c = self.conn.cursor()
        c.execute(
            """
            SELECT 1
            FROM dead_ends de
            JOIN articles a ON de.article_id = a.id
            WHERE a.article = ?;
            """,
            (article,),
        )

        result = c.fetchone()
        c.close()
        return result is not None

    def query_dead_ends(self) -> list[str]:
        """
        Query the dead ends in the database.
        """

        c = self.conn.cursor()
        c.execute(
            """
            SELECT a.article
            FROM dead_ends de
            JOIN articles a ON de.article_id = a.id;
            """
        )
        results = c.fetchall()
        c.close()
        return [r[0] for r in results]

    def query_non_complete_start_articles(self, end_articles: list[str]) -> list[str]:
        """
        Query the start articles that do not have a complete path to all end articles.
        """

        c = self.conn.cursor()
        if end_articles == []:
            query = """
            SELECT sa.article
            FROM paths p1
            JOIN articles sa ON p1.start_article_id = sa.id
            
            WHERE (
                SELECT 1
                FROM paths p2
                JOIN articles ea ON p2.end_article_id = ea.id

                WHERE NOT EXISTS (
                    SELECT 1
                    FROM paths p3
                    WHERE p3.start_article_id = sa.id AND p3.end_article_id = ea.id
                )
            );
            """

        else:
            query = f"""
            SELECT sa.article
            FROM paths p1
            JOIN articles sa ON p1.start_article_id = sa.id
            
            WHERE (
                SELECT 1
                FROM paths p2
                JOIN articles ea ON p2.end_article_id = ea.id

                WHERE ea.article IN ({', '.join(['?'] * len(end_articles))})
                AND NOT EXISTS (
                    SELECT 1
                    FROM paths p3
                    WHERE p3.start_article_id = sa.id AND p3.end_article_id = ea.id
                )
            );
            """

        c.execute(query, end_articles)
        results = c.fetchall()
        c.close()
        return [r[0] for r in results]

    def query_start_articles(self) -> list[str]:
        """
        Query the start articles in the database.
        """

        c = self.conn.cursor()
        c.execute(
            """
            SELECT DISTINCT a.article
            FROM paths p
            JOIN articles a ON p.start_article_id = a.id;
            """
        )
        results = c.fetchall()
        c.close()
        return [r[0] for r in results]

    def query_end_articles(self) -> list[str]:
        """
        Query the end articles in the database.
        """

        c = self.conn.cursor()
        c.execute(
            """
            SELECT DISTINCT a.article
            FROM paths p
            JOIN articles a ON p.end_article_id = a.id;
            """
        )
        results = c.fetchall()
        c.close()
        return [r[0] for r in results]

    def query_queue(self) -> str:
        """
        Query the queue.
        """

        c = self.conn.cursor()
        c.execute(
            """
            SELECT a.article
            FROM queue q
            JOIN articles a ON q.article_id = a.id
            ORDER BY q.priority DESC;
            """
        )

        result = c.fetchone()
        c.close()
        return result[0]

    def remove_article_from_queue(self, article: str) -> None:
        """
        Remove an article from the queue.
        """

        c = self.conn.cursor()
        c.execute(
            """
            DELETE FROM queue
            WHERE article_id = (
                SELECT id
                FROM articles
                WHERE article = ?
            );
            """,
            (article,),
        )
        self.conn.commit()
        c.close()

    ########################
    ##### Misc Queries #####
    ########################

    def query_queue_length(self) -> int:
        """
        Query the length of the queue.
        """

        c = self.conn.cursor()
        c.execute("SELECT COUNT(*) FROM queue")
        result = c.fetchone()[0]
        c.close()
        return result
