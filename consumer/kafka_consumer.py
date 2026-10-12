def consumer_main():
    import json
    import sqlite3

    from kafka import KafkaConsumer

    DATABASE = "warehouse/warehouse.db"

    TOPIC = "amazon-products"

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers="localhost:9092",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode("utf-8"))
    )

    conn = sqlite3.connect(DATABASE)

    cursor = conn.cursor()

    cursor.execute("DELETE FROM raw_stage")
    conn.commit()

    print("Listening for Kafka messages...\n")

    count = 0

    for message in consumer:

        row = message.value

        cursor.execute(
            """
            INSERT INTO raw_stage
            (
                url,
                title,
                price,
                mrp,
                discount,
                rating,
                rating_count,
                description,
                highlights,
                specifications,
                image_urls,
                video_urls
            )

            VALUES
            (
                ?,?,?,?,?,?,?,?,?,?,?,?
            )
            """,
            (
                row.get("url"),
                row.get("title"),
                row.get("price"),
                row.get("mrp"),
                row.get("discount"),
                row.get("rating"),
                row.get("rating_count_text"),
                row.get("description"),
                row.get("highlights"),
                row.get("specifications"),
                row.get("image_urls"),
                row.get("video_urls"),
            ),
        )

        conn.commit()

        count += 1

        print(f"Inserted Record : {count}")

        if count == 100:
            break

    conn.close()

    consumer.close()

    print("\nFinished consuming all records.")

if __name__ == "__main__":
    consumer_main()