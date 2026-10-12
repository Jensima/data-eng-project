import sqlite3
import json

DATABASE = "warehouse/warehouse.db"


def replay_errors():

    conn = sqlite3.connect(DATABASE)

    cursor = conn.cursor()

    cursor.execute("""
        SELECT error_id,
               raw_record
        FROM error_stage
    """)

    rows = cursor.fetchall()

    print(f"\nRecords Available For Replay : {len(rows)}")

    for error_id, raw in rows:

        record = json.loads(raw)

        print(f"\nReplay Record : {record['title']}")

        # In production, you would re-run the validation
        # after correcting the data.

        cursor.execute(
            "DELETE FROM error_stage WHERE error_id=?",
            (error_id,)
        )

    conn.commit()

    conn.close()

    print("\nReplay Completed")


if __name__ == "__main__":

    replay_errors()