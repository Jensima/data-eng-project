import re
import json
import sqlite3
import pandas as pd

DATABASE = "warehouse/warehouse.db"


def get_connection():
    return sqlite3.connect(DATABASE)


# -----------------------------
# Price
# -----------------------------
def clean_price(value):

    if pd.isna(value):
        return None

    value = str(value)

    match = re.search(r"₹\s*([\d,]+)", value)

    if match:

        price = match.group(1)

        price = price.replace(",", "")

        return float(price)

    # fallback

    match = re.search(r"([\d,]+)", value)

    if match:

        return float(match.group(1).replace(",", ""))

    return None


# -----------------------------
# Rating
# -----------------------------
def clean_rating(value):

    if pd.isna(value):

        return None

    match = re.search(r"\d+\.\d+", str(value))

    if match:

        return float(match.group())

    return None


# -----------------------------
# Rating Count
# -----------------------------
def clean_rating_count(value):

    if pd.isna(value):

        return 0

    value = str(value)

    match = re.search(r"([\d,]+)", value)

    if match:

        return int(match.group(1).replace(",", ""))

    return 0


# -----------------------------
# Discount
# -----------------------------
def clean_discount(value):

    if pd.isna(value):

        return None

    value = str(value)

    value = value.replace("%", "")

    value = value.strip()

    if value == "":

        return None

    return float(value)


# -----------------------------
# Error Table
# -----------------------------
def log_error(cursor, record, reason):

    cursor.execute(
        """
        INSERT INTO error_stage
        (
            url,
            title,
            reason,
            raw_record
        )
        VALUES (?,?,?,?)
        """,
        (
            record.get("url"),
            record.get("title"),
            reason,
            json.dumps(record),
        ),
    )


# -----------------------------
# Validation
# -----------------------------
def validate_record(record, cursor):

    record["price"] = clean_price(record["price"])

    record["mrp"] = clean_price(record["mrp"])

    record["rating"] = clean_rating(record["rating"])

    record["discount"] = clean_discount(record["discount"])

    record["rating_count"] = clean_rating_count(
        record["rating_count"]
    )

    # ------------------------
    # Mandatory
    # ------------------------

    mandatory = [

        "url",

        "title",

        "price",

        "rating"

    ]

    for col in mandatory:

        if record[col] is None or record[col] == "":

            log_error(record, f"{col} missing")

            return None

    # ------------------------
    # Business Rules
    # ------------------------

    if record["price"] <= 0:

        log_error(cursor, record, "Invalid Price")

        return None

    if record["rating"] < 0 or record["rating"] > 5:

        log_error(cursor, record, "Invalid Rating")

        return None

    if record["mrp"] is not None:

        if record["mrp"] < record["price"]:

            log_error(cursor, record, "MRP < Price")

            return None

    return record