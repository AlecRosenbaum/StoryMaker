"""provides an interface for a sentence subject database"""

import sqlite3


class Database():
    """sets up a custom database interface for sentences and subjects"""

    # set up database
    create_tables = """
    CREATE TABLE IF NOT EXISTS SUBJECT(
        id integer PRIMARY KEY,
        subject text
    );
    CREATE TABLE IF NOT EXISTS SENTENCE(
        id integer PRIMARY KEY,
        sentence_obj BLOB,
        link text,
        batch text
    );
    CREATE TABLE IF NOT EXISTS SENTENCE_SUBJECT(
        id integer PRIMARY KEY,
        subject_id integer,
        sentence_id integer,
        FOREIGN KEY(subject_id) REFERENCES subject(id),
        FOREIGN KEY(sentence_id) REFERENCES sentence(id)
    );
    CREATE TABLE IF NOT EXISTS STORIES(
        id integer PRIMARY KEY,
        subject_id integer,
        sentence_id integer,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
        FOREIGN KEY(subject_id) REFERENCES subject(id),
        FOREIGN KEY(sentence_id) REFERENCES sentence(id)
    );
    """

    def __init__(self):
        """init -> create the tables in the database"""
        self._conn = sqlite3.connect("db.sqlite3")
        self._cur = self._conn.cursor()

        for stmt in self.create_tables.split(";"):
            self._cur.execute(stmt)

        self._conn.commit()

    def commit(self):
        """commit before disconnect"""
        self._conn.commit()

    def insert(self, subjects, sentence, link, batch):
        """insert into database"""

        # define some helpful SQL
        insert_subject = """
        INSERT INTO SUBJECT(subject) VALUES(?);
        """
        insert_sentence = """
        INSERT INTO SENTENCE(sentence_obj, link, batch) VALUES(?, ?, ?);
        """
        insert_sentence_subject = """
        INSERT INTO SENTENCE_SUBJECT(subject_id, sentence_id) VALUES (?, ?)
        """
        check_subject = """
        SELECT id FROM SUBJECT WHERE subject = ?
        """

        self._cur.execute(insert_sentence, (sentence, link, batch))
        sentence_id = self._cur.lastrowid

        for i in subjects:
            self._cur.execute(check_subject, (i,))
            res = self._cur.fetchall()
            if len(res) > 0:
                if len(res) > 1:
                    raise ValueError("more than one subject: {} - {}".format(i, res))
                subject_id = res[0][0]
            else:
                self._cur.execute(insert_subject, (i,))
                subject_id = self._cur.lastrowid

            self._cur.execute(insert_sentence_subject, (subject_id, sentence_id))

        self._conn.commit()

    def get_by_subject(self, subject):
        """return sentences based on their subject"""

        # sql statement
        get_sql = """
            SELECT
                sen.id,
                sen.sentence_obj,
                sen.link,
                sen.batch
            FROM (
                SELECT
                    med.sentence_id
                FROM
                    SUBJECT sub
                LEFT JOIN
                    SENTENCE_SUBJECT med
                ON
                    sub.id = med.subject_id
                WHERE
                    sub.subject = ?
            ) sen_ids
            LEFT JOIN
                SENTENCE sen
            ON
                sen.id = sen_ids.sentence_id
        """
        self._cur.execute(get_sql, (subject,))
        return self._cur.fetchall()

    def get_sentences_by_subject_id(self, subject_id):
        """return sentences based on their subject"""
        get_sql = """
            SELECT
                sen.id,
                sen.sentence_obj,
                sen.link,
                sen.batch
            FROM
                SENTENCE_SUBJECT SS
            LEFT JOIN
                SENTENCE sen
            ON
                sen.id = SS.sentence_id
            WHERE
                SS.subject_id = ?
        """
        self._cur.execute(get_sql, (subject_id,))
        return self._cur.fetchall()

    def popular_subjects(self, limit=3):
        """returns the 3 most popular subjects (with the most sentences)"""

        select_stmt = """
            SELECT
                SUBJECT.subject,
                SUBJECT.id,
                CNTS.cnt
            FROM (
                SELECT
                    subject_id,
                    COUNT(*) as CNT
                FROM
                    SENTENCE_SUBJECT
                GROUP BY
                    subject_id
            ) CNTS LEFT JOIN SUBJECT
            ON
                SUBJECT.id = CNTS.subject_id
            ORDER BY
                CNTS.CNT DESC
            LIMIT {}
        """

        self._cur.execute(select_stmt.format(limit))
        return self._cur.fetchall()

    def append_to_story(self, subject_id, sentence_id):
        """used to add a sentence id to a subject's story. auto adds timestamp"""
        # select_stmt = """
        #     SELECT
        #         created_at
        #     FROM
        # """ EVENTUALLY USE FOR TIMESTAMP CHECKING
        insert_stmt = """
            INSERT INTO STORIES(subject_id, sentence_id) VALUES (?, ?)
        """
        self._cur.execute(insert_stmt, (subject_id, sentence_id))
        self._conn.commit()

    def get_story(self, subject_id):
        """return all sentences related to a subject

        Returns:
            list of sentences:
                [
                    (<used>, <id>, <sentence_obj>, <link>),
                    ...
                ]
            used will be either 0 or 1.
            results will be sorted by used, then story addition timestamp (to retain order)
        """
        select_stmt = """
            SELECT
                SS_SEN.sentence_id,
                SS_SEN.sentence_obj,
                SS_SEN.link,
                CASE WHEN STO.sentence_id IS NOT NULL THEN 1 ELSE 0 END AS USED
            FROM (
                SELECT
                    SS.subject_id,
                    sen.id AS sentence_id,
                    sen.sentence_obj,
                    sen.link
                FROM
                    SENTENCE_SUBJECT SS
                LEFT JOIN
                    SENTENCE SEN
                ON
                    SS.sentence_id = SEN.id
                WHERE
                    SS.subject_id = ?
            ) SS_SEN
            LEFT JOIN STORIES STO ON
                SS_SEN.sentence_id = STO.sentence_id AND SS_SEN.subject_id = STO.subject_id
            ORDER BY
                STO.CREATED_AT
        """
        self._cur.execute(select_stmt, (subject_id,))
        return self._cur.fetchall()

    def get_subject_by_id(self, subject_id):
        select_stmt = """
            SELECT
                id,
                subject
            FROM SUBJECT
            WHERE
                id = ?
        """
        self._cur.execute(select_stmt, (subject_id, ))
        return self._cur.fetchall()[0]

    def get_topics(self, order="posts", offset=0, per_page=0):
        get_sql = """
            SELECT
                CNT.id,
                CNT.subject,
                MAX(STO.CREATED_AT) AS ACTIVITY,
                COUNT(STO.SENTENCE_ID) AS POSTS,
                CNT.NUM_SENTENCES - COUNT(STO.SENTENCE_ID) AS NUM_SENTENCES
            FROM (
                SELECT
                    SUB.id,
                    SUB.subject,
                    COUNT(*) as NUM_SENTENCES
                FROM
                    SENTENCE_SUBJECT SEN
                LEFT JOIN
                    SUBJECT SUB
                ON 
                    SEN.subject_id = SUB.id
                GROUP BY
                    subject_id
            ) CNT
            LEFT JOIN
                STORIES STO
            ON
                CNT.id = STO.subject_id
            GROUP BY
                CNT.id
            ORDER BY
                {} DESC,
                NUM_SENTENCES DESC
            LIMIT
                {}, {}
        """

        if order == "posts":
            order_text = "POSTS"
        elif order == "time":
            order_text = "ACTIVITY"

        self._cur.execute(get_sql.format(order_text, offset, per_page))
        return self._cur.fetchall()

    def get_num_topics(self):
        select_stmt = """
            SELECT
                COUNT(*)
            FROM
                SUBJECT
        """
        self._cur.execute(select_stmt)
        return self._cur.fetchall()[0][0]
