CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    customer_name VARCHAR(255) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    country VARCHAR(100),
    created_at TIMESTAMP DEFAULT NOW()
) WITH (OIDS=FALSE)
DISTRIBUTED BY (customer_id);
-- Indexes
CREATE INDEX idx_customers_id ON customers(id);
CREATE INDEX idx_customers_email ON customers(email);


CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    price DECIMAL(10, 2) NOT NULL CHECK (price >= 0),
    category VARCHAR(100),
    created_at TIMESTAMP DEFAULT NOW()
) WITH (OIDS=FALSE)
DISTRIBUTED BY (product_id);
-- Indexes
CREATE INDEX idx_products_category ON products(category); -- Supposed that analytic queries could be by product category.
CREATE INDEX idx_products_id ON products(product_id);


CREATE TABLE sales_transactions (
    transaction_id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES customers(customer_id),
    product_id INT REFERENCES products(product_id),
    purchase_date DATE NOT NULL,
    quantity_purchased INT NOT NULL,  -- Return is possible? If not, we can add CHECK > 0
    created_at TIMESTAMP DEFAULT NOW()
)
WITH (
    appendonly='true',
	compresslevel='1',
	orientation='column',
	compresstype=zstd
)   -- In append optimized tables there isn't much sense in indexes as they are already optimized, so I didn't add any.
    -- Partitions are better.
    -- I chose Append Optimized because I assume that these tables will have many write operations and few update operations.
DISTRIBUTED BY (purchase_date)
PARTITION BY RANGE (purchase_date)
(
    PARTITION sales_transactions_202401 START ('2024-01-01'::date) END ('2024-01-31'::date),
    PARTITION sales_transactions_202402 START ('2024-02-01'::date) END ('2024-02-29'::date),
    -- Add another partitions
	DEFAULT PARTITION other
);


CREATE TABLE shipping_details (
    transaction_id INT REFERENCES sales_transactions(transaction_id),
    shipping_date DATE NOT NULL,
    shipping_address VARCHAR(255),
    city VARCHAR(100),
    country VARCHAR(100),
    PRIMARY KEY (transaction_id) -- if there cannot be several addresses for one transaction
)
WITH (
    appendonly='true',
	compresslevel='1',
	orientation='column',
	compresstype=zstd
)   -- In append optimized tables there isn't much sense in indexes as they are already optimized, so I didn't add any.
    -- Partitions are better.
    -- I chose Append Optimized because I assume that these tables will have many write operations and few update operations.
DISTRIBUTED BY (transaction_id)
PARTITION BY RANGE (shipping_date)
(
    PARTITION shipping_details_202401 START ('2024-01-01'::date) END ('2024-01-31'::date),
    PARTITION shipping_details_202402 START ('2024-02-01'::date) END ('2024-02-29'::date),
    -- Add another partitions
	DEFAULT PARTITION other
);


-- Query Task
WITH monthly_sales AS (
    SELECT
        DATE_TRUNC('month', st.purchase_date) AS sale_month,
        SUM(st.quantity_purchased * p.price) AS total_sales,
        COUNT(transaction_id) AS total_transactions
    FROM sales_transactions st
    JOIN products p ON st.product_id = p.product_id
    GROUP BY sale_month
),
moving_avg AS (
    SELECT
        sale_month,
        total_sales,
        total_transactions,
        AVG(total_sales) OVER (ORDER BY sale_month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS moving_avg_sales
    FROM monthly_sales
)
SELECT
    sale_month,
    total_sales,
    total_transactions,
    moving_avg_sales
FROM moving_avg
ORDER BY sale_month;
