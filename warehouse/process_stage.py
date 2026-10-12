import sqlite3
import pandas as pd
import re
from warehouse.error_handler import log_error

DATABASE = "warehouse/warehouse.db"


# ------------------------------
# Cleaning Functions
# ------------------------------

def clean_price(value):

    if pd.isna(value):
        return None

    value = str(value)

    match = re.search(r'(\d[\d,]*)', value)

    if match:
        return float(match.group(1).replace(",", ""))

    return None


def clean_rating(value):

    if pd.isna(value):
        return None

    match = re.search(r'(\d+(\.\d+)?)', str(value))

    if match:
        return float(match.group(1))

    return None


def clean_discount(value):

    if pd.isna(value):
        return None

    match = re.search(r'(\d+)', str(value))

    if match:
        return float(match.group(1))

    return None


def clean_rating_count(value):

    if pd.isna(value):
        return 0

    value = str(value).replace(",", "")

    match = re.search(r'(\d+)', value)

    if match:
        return int(match.group(1))

    return 0


# ------------------------------
# Pipeline
# ------------------------------

def process_stage():

    conn = sqlite3.connect(DATABASE)

    cursor = conn.cursor()

    # Clear previous processed data
    cursor.execute("DELETE FROM validated_stage")
    cursor.execute("DELETE FROM error_stage")
    conn.commit()


    df = pd.read_sql("SELECT * FROM raw_stage", conn)

    valid = 0
    invalid = 0

    for _, row in df.iterrows():
        price = clean_price(row["price"])
        mrp = clean_price(row["mrp"])
        discount = clean_discount(row["discount"])
        rating = clean_rating(row["rating"])
        rating_count = clean_rating_count(row["rating_count"])

        # -------------------------
        # Validation
        # -------------------------

        if pd.isna(row["title"]):

            log_error(cursor, row, "Missing Title")

            invalid += 1

            continue


        if pd.isna(row["url"]):

            log_error(cursor, row, "Missing URL")

            invalid += 1

            continue


        if price is None or price <= 0:

            log_error(cursor, row, "Invalid Price")

            invalid += 1

            continue


        if rating is None:

            log_error(cursor, row, "Missing Rating")

            invalid += 1

            continue


        if rating < 0 or rating > 5:

            log_error(cursor, row, "Invalid Rating")

            invalid += 1

            continue

        cursor.execute(
            """
            INSERT OR REPLACE INTO validated_stage
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
                row["url"],
                row["title"],
                price,
                mrp,
                discount,
                rating,
                rating_count,
                row["description"],
                row["highlights"],
                row["specifications"],
                row["image_urls"],
                row["video_urls"],
            ),
        )

        valid += 1

    conn.commit()

    conn.close()

    print("\n========== SUMMARY ==========")
    print("Valid Records   :", valid)
    print("Invalid Records :", invalid)

# ------------------------------
# Derived Columns
# ------------------------------

def get_brand(title):

    brands = [
        "Apple","Samsung","OnePlus","Xiaomi","Redmi",
        "Realme","Motorola","Nothing","Google",
        "POCO","Vivo","Oppo","IQOO","Honor"
    ]

    title = str(title)

    for brand in brands:

        if brand.lower() in title.lower():
            return brand

    return "Other"


def get_savings(price, mrp):

    if price is None or mrp is None:
        return 0

    return round(mrp - price, 2)


def get_discount_percentage(price, mrp):

    if price is None or mrp is None or mrp == 0:
        return 0

    return round(((mrp - price) / mrp) * 100, 2)


def get_price_category(price):

    if price is None:
        return "Unknown"

    if price < 15000:
        return "Budget"

    elif price < 30000:
        return "Mid Range"

    elif price < 60000:
        return "Premium"

    else:
        return "Ultra Premium"


def get_rating_category(rating):

    if rating is None:
        return "Unknown"

    if rating >= 4.5:
        return "Excellent"

    elif rating >= 4:
        return "Good"

    elif rating >= 3:
        return "Average"

    else:
        return "Poor"




def get_popularity(count):

    if count is None:
        return "Unknown"

    if count < 500:
        return "New"

    elif count < 2000:
        return "Popular"

    else:
        return "Best Seller"


def get_ram(title):

    match = re.search(r'(\d+)\s*GB', str(title), re.IGNORECASE)

    if match:
        return match.group(1) + " GB"

    return "Unknown"


def get_storage(title):

    match = re.search(r'(64|128|256|512|1024)\s*GB', str(title), re.IGNORECASE)

    if match:
        return match.group(1) + " GB"

    return "Unknown"


def get_processor(text):

    text = str(text).lower()

    processors = [
        "snapdragon",
        "dimensity",
        "exynos",
        "tensor",
        "mediatek",
        "apple"
    ]

    for p in processors:

        if p in text:
            return p.title()

    return "Unknown"


def get_os(title):

    title = str(title).lower()

    if "iphone" in title:
        return "iOS"

    return "Android"


if __name__ == "__main__":

    process_stage()