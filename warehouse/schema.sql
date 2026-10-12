-- ==========================================================
-- RAW STAGING TABLE
-- ==========================================================

CREATE TABLE IF NOT EXISTS raw_stage (

    url TEXT,

    title TEXT,

    price REAL,

    mrp REAL,

    discount REAL,

    rating REAL,

    rating_count INTEGER,

    description TEXT,

    highlights TEXT,

    specifications TEXT,

    image_urls TEXT,

    video_urls TEXT,

    load_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-------------------------------------------------------------

CREATE TABLE IF NOT EXISTS validated_stage (

    product_id INTEGER PRIMARY KEY AUTOINCREMENT,

    url TEXT UNIQUE,

    title TEXT,

    price REAL,

    mrp REAL,

    discount REAL,

    rating REAL,

    rating_count INTEGER,

    description TEXT,

    highlights TEXT,

    specifications TEXT,

    image_urls TEXT,

    video_urls TEXT,

    validated_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-------------------------------------------------------------

CREATE TABLE IF NOT EXISTS error_stage (

    error_id INTEGER PRIMARY KEY AUTOINCREMENT,

    url TEXT,

    title TEXT,

    reason TEXT,

    raw_record TEXT,

    error_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-------------------------------------------------------------

CREATE TABLE IF NOT EXISTS warehouse_products (

    url TEXT PRIMARY KEY,

    title TEXT,

    brand TEXT,

    price REAL,

    mrp REAL,

    savings REAL,

    discount REAL,

    discount_percentage REAL,

    price_category TEXT,

    rating REAL,

    rating_category TEXT,

    rating_count INTEGER,

    popularity TEXT,

    ram TEXT,

    storage TEXT,

    processor TEXT,

    operating_system TEXT,

    description TEXT,

    highlights TEXT,

    specifications TEXT,

    image_urls TEXT,

    video_urls TEXT,

    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);