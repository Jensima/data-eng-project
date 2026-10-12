import sqlite3
import re

DATABASE = "warehouse/warehouse.db"


# ---------------------------------------------------
# Helper Functions
# ---------------------------------------------------

def get_brand(title):

    brands = [
        "Apple",
        "Samsung",
        "OnePlus",
        "Xiaomi",
        "Redmi",
        "Realme",
        "Motorola",
        "Nothing",
        "Google",
        "POCO",
        "Vivo",
        "Oppo",
        "IQOO",
        "Honor"
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

    title = str(title)

    match = re.search(r'(\d+)\s*GB\s*RAM', title, re.IGNORECASE)
    if match:
        return match.group(1) + " GB"

    match = re.search(r'(\d+)\s*GB\s*\+\s*(\d+)\s*GB', title, re.IGNORECASE)
    if match:
        return match.group(1) + " GB"

    match = re.search(r'(\d+)\+(\d+)', title)
    if match:
        return match.group(1) + " GB"

    return "Unknown"


def get_storage(title):

    title = str(title)

    match = re.search(r'(\d+)\s*GB\s*Storage', title, re.IGNORECASE)
    if match:
        return match.group(1) + " GB"

    match = re.search(r'(\d+)\s*GB\s*\+\s*(\d+)\s*GB', title, re.IGNORECASE)
    if match:
        return match.group(2) + " GB"

    match = re.search(r'(\d+)\+(\d+)', title)
    if match:
        return match.group(2) + " GB"

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


# ---------------------------------------------------
# Merge Pipeline
# ---------------------------------------------------

def merge_data():

    conn = sqlite3.connect(DATABASE)

    cursor = conn.cursor()

    try:

        cursor.execute("BEGIN TRANSACTION")

        cursor.execute("SELECT * FROM validated_stage")

        rows = cursor.fetchall()

        merged = 0

        for row in rows:

            url = row[1]
            title = row[2]
            price = row[3]
            mrp = row[4]
            discount = row[5]
            rating = row[6]
            rating_count = row[7]
            description = row[8]
            highlights = row[9]
            specifications = row[10]
            image_urls = row[11]
            video_urls = row[12]

            brand = get_brand(title)

            savings = get_savings(price, mrp)

            discount_percentage = get_discount_percentage(price, mrp)

            price_category = get_price_category(price)

            rating_category = get_rating_category(rating)

            popularity = get_popularity(rating_count)

            ram = get_ram(title)

            storage = get_storage(title)

            processor = get_processor(
                title + " " + str(highlights)
            )

            operating_system = get_os(title)

            cursor.execute(
                """
                INSERT INTO warehouse_products
                (
                    url,
                    title,
                    brand,
                    price,
                    mrp,
                    savings,
                    discount,
                    discount_percentage,
                    price_category,
                    rating,
                    rating_category,
                    rating_count,
                    popularity,
                    ram,
                    storage,
                    processor,
                    operating_system,
                    description,
                    highlights,
                    specifications,
                    image_urls,
                    video_urls
                )

                VALUES
                (
                    ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
                )

                ON CONFLICT(url)

                DO UPDATE SET

                    title=excluded.title,
                    brand=excluded.brand,
                    price=excluded.price,
                    mrp=excluded.mrp,
                    savings=excluded.savings,
                    discount=excluded.discount,
                    discount_percentage=excluded.discount_percentage,
                    price_category=excluded.price_category,
                    rating=excluded.rating,
                    rating_category=excluded.rating_category,
                    rating_count=excluded.rating_count,
                    popularity=excluded.popularity,
                    ram=excluded.ram,
                    storage=excluded.storage,
                    processor=excluded.processor,
                    operating_system=excluded.operating_system,
                    description=excluded.description,
                    highlights=excluded.highlights,
                    specifications=excluded.specifications,
                    image_urls=excluded.image_urls,
                    video_urls=excluded.video_urls,
                    last_updated=CURRENT_TIMESTAMP
                """,
                (
                    url,
                    title,
                    brand,
                    price,
                    mrp,
                    savings,
                    discount,
                    discount_percentage,
                    price_category,
                    rating,
                    rating_category,
                    rating_count,
                    popularity,
                    ram,
                    storage,
                    processor,
                    operating_system,
                    description,
                    highlights,
                    specifications,
                    image_urls,
                    video_urls,
                ),
            )

            merged += 1

        conn.commit()

        print("\nTransaction Successful")
        print(f"Merged Records : {merged}")

    except Exception as e:

        conn.rollback()

        print("\nTransaction Failed")
        print(e)

    finally:

        conn.close()


if __name__ == "__main__":

    merge_data()