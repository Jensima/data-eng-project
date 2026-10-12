import json


def log_error(cursor, row, reason):

    cursor.execute(
        """
        INSERT INTO error_stage
        (
            url,
            title,
            reason,
            raw_record
        )
        VALUES
        (
            ?,?,?,?
        )
        """,
        (
            row["url"],
            row["title"],
            reason,
            json.dumps(row.to_dict(), default=str)
        )
    )