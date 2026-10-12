import sys
import pandas as pd
import re


# ==========================================================
# CLEANING FUNCTIONS FOR EDA ONLY
# ==========================================================

def extract_price(value):

    if pd.isna(value):
        return None

    match = re.search(r'₹\s*([\d,]+)', str(value))

    if match:
        return float(match.group(1).replace(",", ""))

    # fallback for plain numeric values
    match = re.search(r'([\d,]+)', str(value))

    if match:
        return float(match.group(1).replace(",", ""))

    return None


def extract_rating(value):

    if pd.isna(value):
        return None

    match = re.search(r'(\d+(?:\.\d+)?)\s*out of\s*5', str(value), re.IGNORECASE)

    if match:
        return float(match.group(1))

    # fallback
    match = re.search(r'(\d+(?:\.\d+)?)', str(value))

    if match:
        rating = float(match.group(1))

        if 0 <= rating <= 5:
            return rating

    return None


def extract_discount(value):

    if pd.isna(value):
        return None

    match = re.search(r'(\d+(?:\.\d+)?)\s*%', str(value))

    if match:
        return float(match.group(1))

    return None


def extract_rating_count(value):

    if pd.isna(value):
        return None

    match = re.search(r'([\d,]+)', str(value))

    if match:
        return int(match.group(1).replace(",", ""))

    return None


# ==========================================================
# BRAND EXTRACTION
# ==========================================================

def extract_brand(title):

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


# ==========================================================
# OUTLIER DETECTION
# ==========================================================

def detect_outliers(series):

    series = series.dropna()

    if len(series) == 0:
        return 0

    q1 = series.quantile(0.25)
    q3 = series.quantile(0.75)

    iqr = q3 - q1

    lower_bound = q1 - (1.5 * iqr)
    upper_bound = q3 + (1.5 * iqr)

    return (
        (series < lower_bound) |
        (series > upper_bound)
    ).sum()


# ==========================================================
# EDA
# ==========================================================

def run_eda(file_path):

    print("\n" + "=" * 60)
    print("        AMAZON PRODUCTS - EXPLORATORY DATA ANALYSIS")
    print("=" * 60)

    # ------------------------------------------------------
    # LOAD DATA
    # ------------------------------------------------------

    try:

        df = pd.read_csv(file_path)

    except FileNotFoundError:

        print("\nERROR: File not found:", file_path)

        return

    # ------------------------------------------------------
    # 1. DATASET OVERVIEW
    # ------------------------------------------------------

    print("\n[1] DATASET OVERVIEW")
    print("-" * 60)

    print("Number of Rows    :", df.shape[0])
    print("Number of Columns :", df.shape[1])

    print("\nColumn Names:")

    for column in df.columns:
        print("-", column)

    # ------------------------------------------------------
    # 2. DATA TYPES
    # ------------------------------------------------------

    print("\n[2] DATA TYPES")
    print("-" * 60)

    print(df.dtypes.to_string())

    # ------------------------------------------------------
    # 3. MISSING VALUES
    # ------------------------------------------------------

    print("\n[3] MISSING VALUE ANALYSIS")
    print("-" * 60)

    # IMPORTANT:
    # Only original dataset columns are used here.

    missing = df.isnull().sum()

    missing_percentage = (
        df.isnull().sum() / len(df) * 100
    ).round(2)

    missing_report = pd.DataFrame({
        "Missing Values": missing,
        "Missing Percentage": missing_percentage
    })

    print(missing_report.to_string())

    # ------------------------------------------------------
    # 4. DUPLICATES
    # ------------------------------------------------------

    print("\n[4] DUPLICATE ANALYSIS")
    print("-" * 60)

    duplicate_rows = df.duplicated().sum()

    print("Duplicate Rows :", duplicate_rows)

    if "url" in df.columns:

        duplicate_urls = df["url"].duplicated().sum()

        print("Duplicate URLs :", duplicate_urls)

    # ------------------------------------------------------
    # 5. CREATE TEMPORARY ANALYTICAL COLUMNS
    # ------------------------------------------------------

    df["price_numeric"] = df["price"].apply(extract_price)

    df["mrp_numeric"] = df["mrp"].apply(extract_price)

    df["discount_numeric"] = df["discount"].apply(
        extract_discount
    )

    df["rating_numeric"] = df["rating"].apply(
        extract_rating
    )

    df["rating_count_numeric"] = df[
        "rating_count_text"
    ].apply(extract_rating_count)

    # ------------------------------------------------------
    # 6. NUMERICAL STATISTICS
    # ------------------------------------------------------

    print("\n[5] NUMERICAL STATISTICS")
    print("-" * 60)

    numeric_columns = [
        "price_numeric",
        "mrp_numeric",
        "discount_numeric",
        "rating_numeric",
        "rating_count_numeric"
    ]

    statistics = df[numeric_columns].describe().T

    statistics["median"] = df[
        numeric_columns
    ].median()

    print(
        statistics[
            [
                "count",
                "mean",
                "median",
                "std",
                "min",
                "max"
            ]
        ].round(2).to_string()
    )

    # ------------------------------------------------------
    # 7. PRICE ANALYSIS
    # ------------------------------------------------------

    print("\n[6] PRICE ANALYSIS")
    print("-" * 60)

    price = df["price_numeric"].dropna()

    if len(price) > 0:

        print("Minimum Price :", round(price.min(), 2))
        print("Maximum Price :", round(price.max(), 2))
        print("Average Price :", round(price.mean(), 2))
        print("Median Price  :", round(price.median(), 2))

    # ------------------------------------------------------
    # 8. RATING ANALYSIS
    # ------------------------------------------------------

    print("\n[7] RATING ANALYSIS")
    print("-" * 60)

    rating = df["rating_numeric"].dropna()

    if len(rating) > 0:

        print("Minimum Rating :", round(rating.min(), 2))
        print("Maximum Rating :", round(rating.max(), 2))
        print("Average Rating :", round(rating.mean(), 2))
        print("Median Rating  :", round(rating.median(), 2))

        invalid_ratings = (
            (df["rating_numeric"] < 0) |
            (df["rating_numeric"] > 5)
        ).sum()

        print(
            "Invalid Ratings (outside 0-5) :",
            invalid_ratings
        )

    # ------------------------------------------------------
    # 9. DISCOUNT ANALYSIS
    # ------------------------------------------------------

    print("\n[8] DISCOUNT ANALYSIS")
    print("-" * 60)

    discount = df["discount_numeric"].dropna()

    if len(discount) > 0:

        print(
            "Minimum Discount :",
            round(discount.min(), 2)
        )

        print(
            "Maximum Discount :",
            round(discount.max(), 2)
        )

        print(
            "Average Discount :",
            round(discount.mean(), 2)
        )

    # ------------------------------------------------------
    # 10. PRICE VS MRP
    # ------------------------------------------------------

    print("\n[9] PRICE vs MRP ANALYSIS")
    print("-" * 60)

    valid_price_mrp = df[
        [
            "price_numeric",
            "mrp_numeric"
        ]
    ].dropna()

    if len(valid_price_mrp) > 0:

        invalid_price_mrp = (
            valid_price_mrp["price_numeric"]
            >
            valid_price_mrp["mrp_numeric"]
        ).sum()

        print(
            "Products where Price > MRP :",
            invalid_price_mrp
        )

    # ------------------------------------------------------
    # 11. BRAND ANALYSIS
    # ------------------------------------------------------

    print("\n[10] BRAND EXPLORATION")
    print("-" * 60)

    df["brand_eda"] = df[
        "title"
    ].apply(extract_brand)

    brand_counts = (
        df["brand_eda"]
        .value_counts()
    )

    print(brand_counts.to_string())

    # ------------------------------------------------------
    # 12. OUTLIER ANALYSIS
    # ------------------------------------------------------

    print("\n[11] OUTLIER ANALYSIS")
    print("-" * 60)

    print(
        "Price Outliers :",
        detect_outliers(df["price_numeric"])
    )

    print(
        "Rating Outliers :",
        detect_outliers(df["rating_numeric"])
    )

    print(
        "Discount Outliers :",
        detect_outliers(df["discount_numeric"])
    )

    # ------------------------------------------------------
    # 13. DATA QUALITY SUMMARY
    # ------------------------------------------------------

    print("\n[12] DATA QUALITY SUMMARY")
    print("-" * 60)

    print("Total Records :", len(df))

    print(
        "Rows with Missing Values :",
        df[
            [
                column
                for column in [
                    "url",
                    "title",
                    "price",
                    "mrp",
                    "discount",
                    "rating",
                    "rating_count_text",
                    "description",
                    "highlights",
                    "specifications",
                    "image_urls",
                    "video_urls"
                ]
                if column in df.columns
            ]
        ].isnull().any(axis=1).sum()
    )

    print(
        "Duplicate Rows :",
        df.duplicated(
            subset=[
                column
                for column in df.columns
                if not column.endswith("_numeric")
                and column != "brand_eda"
            ]
        ).sum()
    )

    print(
        "Duplicate URLs :",
        df["url"].duplicated().sum()
    )

    print(
        "Missing Ratings :",
        df["rating_numeric"].isna().sum()
    )

    print(
        "Invalid Ratings :",
        (
            (df["rating_numeric"] < 0) |
            (df["rating_numeric"] > 5)
        ).sum()
    )

    print(
        "Missing Prices :",
        df["price_numeric"].isna().sum()
    )

    print(
        "Invalid Prices :",
        (
            df["price_numeric"] <= 0
        ).sum()
    )

    print("\n" + "=" * 60)
    print("              EDA COMPLETED")
    print("=" * 60)


# ==========================================================
# MAIN
# ==========================================================

if __name__ == "__main__":

    if len(sys.argv) != 2:

        print(
            "\nUsage:"
            "\npython eda/exploratory_analysis.py "
            "<csv_file>"
        )

        sys.exit(1)

    file_path = sys.argv[1]

    run_eda(file_path)