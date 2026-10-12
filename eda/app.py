import streamlit as st
import pandas as pd
import numpy as np
import re
import os
import json
import subprocess


# ==========================================================
# PROJECT CONFIGURATION
# ==========================================================

PROJECT_ROOT = "/Users/i.seviantojensima/Desktop/Sem 9/Data Engineering/Project eda "

INCOMING_DIR = os.path.join(
    PROJECT_ROOT,
    "data",
    "incoming"
)

os.makedirs(INCOMING_DIR, exist_ok=True)


# ==========================================================
# PAGE CONFIGURATION
# ==========================================================

st.set_page_config(
    page_title="Amazon Product EDA",
    page_icon="📊",
    layout="wide"
)


# ==========================================================
# CUSTOM CSS
# ==========================================================

st.markdown(
    """
    <style>

    .main {
        background-color: #0b0f14;
    }

    .block-container {
        padding-top: 2rem;
    }

    h1, h2, h3 {
        color: #4da6ff;
    }

    </style>
    """,
    unsafe_allow_html=True
)


# ==========================================================
# TITLE
# ==========================================================

st.title("Amazon Product — Exploratory Data Analysis")

st.caption(
    "Dynamic analysis of the incoming Amazon product dataset"
)


# ==========================================================
# FILE INPUT
# ==========================================================

st.sidebar.header("Dataset")

uploaded_file = st.sidebar.file_uploader(
    "Upload a new CSV file",
    type=["csv"]
)


# ==========================================================
# FILE PATH + LOAD DATA
# ==========================================================

input_file_path = None
is_uploaded_file = False


if uploaded_file is not None:

    # ------------------------------------------------------
    # Save uploaded file
    # ------------------------------------------------------

    input_file_path = os.path.join(
        INCOMING_DIR,
        uploaded_file.name
    )

    with open(input_file_path, "wb") as f:
        f.write(uploaded_file.getbuffer())

    is_uploaded_file = True

    st.sidebar.success(
        f"Uploaded: {uploaded_file.name}"
    )

    st.sidebar.caption(
        "File saved to incoming folder"
    )

    st.sidebar.code(
        input_file_path
    )

    # ------------------------------------------------------
    # Read uploaded file
    # ------------------------------------------------------

    try:

        df = pd.read_csv(
            uploaded_file
        )

    except Exception as e:

        st.error("Unable to read the uploaded CSV file.")

        st.error(
            f"Reason: {e}"
        )

        st.stop()

else:

    # ------------------------------------------------------
    # Default dataset
    # ------------------------------------------------------

    input_file_path = os.path.join(
        PROJECT_ROOT,
        "data",
        "amazon_products_raw.csv"
    )

    try:

        df = pd.read_csv(
            input_file_path
        )

        st.sidebar.success(
            "Using default dataset"
        )

    except FileNotFoundError:

        st.warning(
            "Please upload an Amazon product CSV file."
        )

        st.stop()


# ==========================================================
# RUN AIRFLOW PIPELINE
# ==========================================================

st.sidebar.markdown("---")

st.sidebar.header("ETL Pipeline")

if is_uploaded_file:

    st.sidebar.write(
        "The uploaded file is ready to be processed."
    )

    if st.sidebar.button(
        "Run Airflow Pipeline",
        type="primary"
    ):

        st.sidebar.info(
            "Triggering Airflow..."
        )

        airflow_conf = json.dumps({
            "input_file": input_file_path
        })

        try:

            result = subprocess.run(
                [
                    "/Users/i.seviantojensima/Desktop/Sem 9/Data Engineering/Project/airflow_venv/bin/airflow",
                    "dags",
                    "trigger",
                    "amazon_etl_pipeline",
                    "--conf",
                    airflow_conf
                ],
                capture_output=True,
                text=True
            )

            if result.returncode == 0:

                st.sidebar.success(
                    "Airflow pipeline triggered successfully!"
                )

                st.sidebar.write(
                    "Input file sent to Airflow:"
                )

                st.sidebar.code(
                    input_file_path
                )

                with st.expander(
                    "Airflow Trigger Response"
                ):

                    st.code(
                        result.stdout
                    )

            else:

                st.sidebar.error(
                    "Airflow pipeline could not be triggered."
                )

                with st.expander(
                    "Airflow Error"
                ):

                    st.code(
                        result.stderr
                    )

        except FileNotFoundError:

            st.sidebar.error(
                "Airflow command was not found."
            )

            st.sidebar.write(
                "Make sure the Airflow virtual environment "
                "is activated before starting Streamlit."
            )

else:

    st.sidebar.info(
        "Upload a new file to enable the Airflow pipeline."
    )


# ==========================================================
# COLUMN NORMALIZATION
# ==========================================================

COLUMN_ALIASES = {

    "url": [
        "url",
        "product_url",
        "product_link",
        "link"
    ],

    "title": [
        "title",
        "product_title",
        "product_name",
        "name"
    ],

    "price": [
        "price",
        "selling_price",
        "current_price",
        "sale_price"
    ],

    "mrp": [
        "mrp",
        "original_price",
        "maximum_retail_price",
        "list_price"
    ],

    "discount": [
        "discount",
        "discount_percentage",
        "discount_percent",
        "discount_pct"
    ],

    "rating": [
        "rating",
        "ratings",
        "product_rating"
    ],

    "rating_count_text": [
        "rating_count_text",
        "rating_count",
        "review_count",
        "reviews",
        "number_of_reviews"
    ],

    "description": [
        "description",
        "product_description",
        "details"
    ],

    "highlights": [
        "highlights",
        "features",
        "key_features"
    ],

    "specifications": [
        "specifications",
        "specs",
        "technical_details"
    ],

    "image_urls": [
        "image_urls",
        "image_url",
        "images",
        "image_links"
    ],

    "video_urls": [
        "video_urls",
        "video_url",
        "videos",
        "video_links"
    ]
}


# ==========================================================
# NORMALIZE COLUMN NAMES
# ==========================================================

existing_columns = {
    column.lower().strip(): column
    for column in df.columns
}

rename_map = {}

for standard_name, possible_names in COLUMN_ALIASES.items():

    for possible_name in possible_names:

        if possible_name.lower() in existing_columns:

            actual_column = existing_columns[
                possible_name.lower()
            ]

            rename_map[
                actual_column
            ] = standard_name

            break


df = df.rename(
    columns=rename_map
)


# ==========================================================
# REQUIRED COLUMN VALIDATION
# ==========================================================

required_columns = [
    "title",
    "price"
]

missing_required = [
    column
    for column in required_columns
    if column not in df.columns
]


if missing_required:

    st.error(
        "Format Not Supported"
    )

    st.write(
        "The uploaded file does not contain enough "
        "Amazon product information."
    )

    st.write(
        "Missing required columns:"
    )

    for column in missing_required:

        st.write(
            f"- {column}"
        )

    st.write(
        "The file should contain at least product "
        "title/name and price information."
    )

    st.stop()


# ==========================================================
# OPTIONAL COLUMNS
# ==========================================================

optional_columns = [
    "url",
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


for column in optional_columns:

    if column not in df.columns:

        df[column] = np.nan


# ==========================================================
# CLEANING FUNCTIONS
# ==========================================================

def extract_price(value):

    if pd.isna(value):

        return None

    match = re.search(
        r'₹\s*([\d,]+)',
        str(value)
    )

    if match:

        return float(
            match.group(1).replace(",", "")
        )

    match = re.search(
        r'([\d,]+)',
        str(value)
    )

    if match:

        return float(
            match.group(1).replace(",", "")
        )

    return None


def extract_rating(value):

    if pd.isna(value):

        return None

    match = re.search(
        r'(\d+(?:\.\d+)?)\s*out of\s*5',
        str(value),
        re.IGNORECASE
    )

    if match:

        return float(
            match.group(1)
        )

    match = re.search(
        r'(\d+(?:\.\d+)?)',
        str(value)
    )

    if match:

        rating = float(
            match.group(1)
        )

        if 0 <= rating <= 5:

            return rating

    return None


def extract_discount(value):

    if pd.isna(value):

        return None

    match = re.search(
        r'(\d+(?:\.\d+)?)\s*%',
        str(value)
    )

    if match:

        return float(
            match.group(1)
        )

    return None


def extract_rating_count(value):

    if pd.isna(value):

        return None

    match = re.search(
        r'([\d,]+)',
        str(value)
    )

    if match:

        return int(
            match.group(1).replace(",", "")
        )

    return None


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
# CREATE ANALYTICAL COLUMNS
# ==========================================================

df["price_numeric"] = df[
    "price"
].apply(
    extract_price
)

df["mrp_numeric"] = df[
    "mrp"
].apply(
    extract_price
)

df["discount_numeric"] = df[
    "discount"
].apply(
    extract_discount
)

df["rating_numeric"] = df[
    "rating"
].apply(
    extract_rating
)

df["rating_count_numeric"] = df[
    "rating_count_text"
].apply(
    extract_rating_count
)

df["brand_eda"] = df[
    "title"
].apply(
    extract_brand
)


# ==========================================================
# SIDEBAR FILTERS
# ==========================================================

st.sidebar.header("Filters")

brands = sorted(
    df["brand_eda"]
    .dropna()
    .unique()
)

selected_brands = st.sidebar.multiselect(
    "Brand",
    brands,
    default=brands
)


filtered_df = df[
    df["brand_eda"].isin(
        selected_brands
    )
]


# ==========================================================
# DATASET OVERVIEW
# ==========================================================

st.header(
    "1. Dataset Overview"
)

col1, col2, col3, col4 = st.columns(4)

with col1:

    st.metric(
        "Total Products",
        len(filtered_df)
    )

with col2:

    st.metric(
        "Columns",
        len(df.columns) - 6
    )

with col3:

    st.metric(
        "Duplicate Rows",
        filtered_df.duplicated().sum()
    )

with col4:

    st.metric(
        "Duplicate URLs",
        filtered_df["url"].duplicated().sum()
    )


# ==========================================================
# MISSING VALUES
# ==========================================================

st.header(
    "2. Missing Value Analysis"
)

original_columns = [
    column
    for column in df.columns
    if not column.endswith("_numeric")
    and column != "brand_eda"
]

missing_report = pd.DataFrame({

    "Column": original_columns,

    "Missing Values": [
        filtered_df[column].isnull().sum()
        for column in original_columns
    ]

})

missing_report[
    "Missing Percentage"
] = (

    missing_report["Missing Values"]
    / len(filtered_df)
    * 100

).round(2)

st.dataframe(
    missing_report,
    use_container_width=True,
    hide_index=True
)


# ==========================================================
# DATA TYPES
# ==========================================================

st.header(
    "3. Data Types"
)

dtype_report = pd.DataFrame({

    "Column": original_columns,

    "Data Type": [
        str(df[column].dtype)
        for column in original_columns
    ]

})

st.dataframe(
    dtype_report,
    use_container_width=True,
    hide_index=True
)


# ==========================================================
# NUMERICAL STATISTICS
# ==========================================================

st.header(
    "4. Numerical Statistics"
)

numeric_columns = [
    "price_numeric",
    "mrp_numeric",
    "discount_numeric",
    "rating_numeric",
    "rating_count_numeric"
]

statistics = filtered_df[
    numeric_columns
].describe().T

statistics["median"] = filtered_df[
    numeric_columns
].median()

statistics = statistics[
    [
        "count",
        "mean",
        "median",
        "std",
        "min",
        "max"
    ]
].round(2)

st.dataframe(
    statistics,
    use_container_width=True
)


# ==========================================================
# PRICE ANALYSIS
# ==========================================================

st.header(
    "5. Price Analysis"
)

price = filtered_df[
    "price_numeric"
].dropna()

if len(price) > 0:

    col1, col2, col3, col4 = st.columns(4)

    with col1:

        st.metric(
            "Minimum Price",
            f"₹{price.min():,.2f}"
        )

    with col2:

        st.metric(
            "Maximum Price",
            f"₹{price.max():,.2f}"
        )

    with col3:

        st.metric(
            "Average Price",
            f"₹{price.mean():,.2f}"
        )

    with col4:

        st.metric(
            "Median Price",
            f"₹{price.median():,.2f}"
        )


st.subheader(
    "Average Price by Brand"
)

price_by_brand = (
    filtered_df
    .groupby("brand_eda")[
        "price_numeric"
    ]
    .mean()
    .sort_values(
        ascending=False
    )
    .round(2)
)

st.bar_chart(
    price_by_brand,
    use_container_width=True
)


# ==========================================================
# RATING ANALYSIS
# ==========================================================

st.header(
    "6. Rating Analysis"
)

rating = filtered_df[
    "rating_numeric"
].dropna()

if len(rating) > 0:

    col1, col2, col3 = st.columns(3)

    with col1:

        st.metric(
            "Minimum Rating",
            f"{rating.min():.2f}"
        )

    with col2:

        st.metric(
            "Maximum Rating",
            f"{rating.max():.2f}"
        )

    with col3:

        st.metric(
            "Average Rating",
            f"{rating.mean():.2f}"
        )


st.subheader(
    "Rating Distribution"
)

rating_distribution = (
    rating
    .round(1)
    .value_counts()
    .sort_index()
)

st.bar_chart(
    rating_distribution
)


# ==========================================================
# DISCOUNT ANALYSIS
# ==========================================================

st.header(
    "7. Discount Analysis"
)

discount = filtered_df[
    "discount_numeric"
].dropna()

if len(discount) > 0:

    col1, col2, col3 = st.columns(3)

    with col1:

        st.metric(
            "Minimum Discount",
            f"{discount.min():.2f}%"
        )

    with col2:

        st.metric(
            "Maximum Discount",
            f"{discount.max():.2f}%"
        )

    with col3:

        st.metric(
            "Average Discount",
            f"{discount.mean():.2f}%"
        )


st.subheader(
    "Average Discount by Brand"
)

discount_by_brand = (
    filtered_df
    .groupby("brand_eda")[
        "discount_numeric"
    ]
    .mean()
    .sort_values(
        ascending=False
    )
    .round(2)
)

st.bar_chart(
    discount_by_brand,
    use_container_width=True
)


# ==========================================================
# BRAND ANALYSIS
# ==========================================================

st.header(
    "8. Brand Distribution"
)

brand_counts = (
    filtered_df["brand_eda"]
    .value_counts()
    .sort_values(
        ascending=False
    )
)

st.bar_chart(
    brand_counts
)


# ==========================================================
# PRICE VS MRP
# ==========================================================

st.header(
    "9. Price vs MRP Analysis"
)

price_mrp = filtered_df[
    [
        "price_numeric",
        "mrp_numeric"
    ]
].dropna()

invalid_price_mrp = (
    price_mrp["price_numeric"]
    >
    price_mrp["mrp_numeric"]
).sum()

st.metric(
    "Products where Price > MRP",
    int(invalid_price_mrp)
)


# ==========================================================
# OUTLIER ANALYSIS
# ==========================================================

def detect_outliers(series):

    series = series.dropna()

    if len(series) == 0:

        return 0

    q1 = series.quantile(0.25)

    q3 = series.quantile(0.75)

    iqr = q3 - q1

    lower = q1 - 1.5 * iqr

    upper = q3 + 1.5 * iqr

    return int(
        (
            (series < lower)
            |
            (series > upper)
        ).sum()
    )


st.header(
    "10. Outlier Analysis"
)

col1, col2, col3 = st.columns(3)

with col1:

    st.metric(
        "Price Outliers",
        detect_outliers(
            filtered_df[
                "price_numeric"
            ]
        )
    )

with col2:

    st.metric(
        "Rating Outliers",
        detect_outliers(
            filtered_df[
                "rating_numeric"
            ]
        )
    )

with col3:

    st.metric(
        "Discount Outliers",
        detect_outliers(
            filtered_df[
                "discount_numeric"
            ]
        )
    )


# ==========================================================
# DATA QUALITY
# ==========================================================

st.header(
    "11. Data Quality Summary"
)

missing_rows = filtered_df[
    original_columns
].isnull().any(
    axis=1
).sum()

missing_ratings = (
    filtered_df[
        "rating_numeric"
    ]
    .isna()
    .sum()
)

invalid_ratings = (
    (
        filtered_df[
            "rating_numeric"
        ] < 0
    )
    |
    (
        filtered_df[
            "rating_numeric"
        ] > 5
    )
).sum()

missing_prices = (
    filtered_df[
        "price_numeric"
    ]
    .isna()
    .sum()
)

invalid_prices = (
    filtered_df[
        "price_numeric"
    ] <= 0
).sum()

quality_report = pd.DataFrame({

    "Check": [

        "Total Records",
        "Rows with Missing Values",
        "Duplicate Rows",
        "Duplicate URLs",
        "Missing Ratings",
        "Invalid Ratings",
        "Missing Prices",
        "Invalid Prices"

    ],

    "Result": [

        len(filtered_df),

        missing_rows,

        filtered_df[
            original_columns
        ].duplicated().sum(),

        filtered_df[
            "url"
        ].duplicated().sum(),

        missing_ratings,

        invalid_ratings,

        missing_prices,

        invalid_prices

    ]

})

st.dataframe(
    quality_report,
    use_container_width=True,
    hide_index=True
)


# ==========================================================
# RAW DATA
# ==========================================================

st.header(
    "12. Dataset Preview"
)

st.dataframe(
    filtered_df[
        original_columns
    ],
    use_container_width=True,
    height=400
)


# ==========================================================
# PIPELINE INFORMATION
# ==========================================================

st.header(
    "13. Pipeline Information"
)

st.write(
    "Current input file:"
)

st.code(
    input_file_path
)

if is_uploaded_file:

    st.success(
        "This dataset was uploaded through Streamlit."
    )

    st.info(
        "Click 'Run Airflow Pipeline' in the sidebar "
        "to send this file through the ETL pipeline."
    )

else:

    st.info(
        "Upload a new CSV file to send a new dataset "
        "through the Airflow ETL pipeline."
    )


# ==========================================================
# FOOTER
# ==========================================================

st.markdown("---")

st.caption(
    "Amazon Product EDA | "
    "Generated dynamically from the uploaded dataset"
)