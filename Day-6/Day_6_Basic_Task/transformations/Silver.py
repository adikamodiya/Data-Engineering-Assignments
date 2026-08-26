## Data Cleaning & Standardization Steps

# 1. **String Trimming:** Strips hidden leading and trailing whitespaces from raw string fields using `trim()`.

# 2. **Safe Type Casting:** Converts raw string data into proper data types (`INTEGER` and `DECIMAL(10,2)`) using `try_cast()`.

# 3. **Corrupt Data Handling:** Automatically converts un-castable or invalid raw values into `NULL` instead of throwing pipeline runtime errors.

# 4. **Multi-Format Date Standardization:** Normalizes mixed date string formats (`yyyy-MM-dd`, `MM/dd/yyyy`, `dd-MM-yyyy`, etc.) into a single standard `DATE` type using `coalesce()` and `try_to_date()`.

# 5. **Business Logic Value Normalization:** Cleans invalid negative monetary values by overriding any `total_amount < 0` to `0.00` using `when()` and `otherwise()`.

# 6. **Data Deduplication:** Eliminates duplicate entries across streaming records using `dropDuplicates()`.

# 7. **Schema Structuring:** Filters out unused raw metadata and selects only the required target columns using `select()`.



import dlt
from pyspark.sql.functions import *
from pyspark.sql.functions import col, coalesce, lit, when, trim, expr
@dlt.table(
    name="cyntexa_dev.medallion_day_6.silver_sales",
    comment="Cleaned, typed, and deduplicated sales data from Bronze"
)
# Check for valid required keys after safe casting
@dlt.expect_or_drop("valid_order_id", "order_id IS NOT NULL")
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_order_date", "order_date IS NOT NULL")
def create_silver_sales():
    return (
        dlt.read_stream("bronze_sales")
        
        # 1. Trim whitespace first, then safely cast types
        .withColumn("order_id", expr("try_cast(trim(order_id) as integer)"))
        .withColumn("customer_id", expr("try_cast(trim(customer_id) as integer)"))
        .withColumn("transaction_id", trim(col("transaction_id")))
        .withColumn("product_id", expr("try_cast(trim(product_id) as integer)"))
        .withColumn("quantity", expr("try_cast(trim(quantity) as integer)"))
        .withColumn("discount_amount", expr("try_cast(trim(discount_amount) as decimal(10,2))"))
        .withColumn("total_amount", expr("try_cast(trim(total_amount) as decimal(10,2))"))
        
        # 2. Parse multi-format dates safely on trimmed string values
        .withColumn(
            "order_date",
            coalesce(
    expr("try_to_date(trim(order_date), 'yyyy-MM-dd')"),
    expr("try_to_date(trim(order_date), 'MM/dd/yyyy')"),
    expr("try_to_date(trim(order_date), 'dd-MM-yyyy')"),
    expr("try_to_date(trim(order_date), 'dd/MM/yyyy')"),
    expr("try_to_date(trim(order_date), 'dd/MM/yy')"),
    expr("try_to_date(trim(order_date), 'yyyy/MM/dd')"),
    expr("try_to_date(trim(order_date), 'MM-dd-yyyy')"),
    expr("try_to_date(trim(order_date), 'dd.MM.yyyy')"),
    expr("try_to_date(trim(order_date), 'MM.dd.yyyy')"),
    expr("try_to_date(trim(order_date), 'yyyy.MM.dd')"),
    expr("try_to_date(trim(order_date), 'dd MMM yyyy')"),
    expr("try_to_date(trim(order_date), 'dd MMMM yyyy')"),
    expr("try_to_date(trim(order_date), 'MMM dd, yyyy')"),
    expr("try_to_date(trim(order_date), 'MMMM dd, yyyy')"),
    expr("try_to_date(trim(order_date), 'yyyyMMdd')"),
    expr("try_to_date(trim(order_date), 'ddMMyyyy')")
)
        )
        
        # 3. Handle negative amounts on cleanly typed numeric columns
        .withColumn(
            "total_amount",
            when(col("total_amount") < 0, lit(0.00).cast("decimal(10,2)"))
            .otherwise(col("total_amount"))
        )
        
        # 4. Select expected Silver schema
        .select(
            "order_id",
            "customer_id",
            "transaction_id",
            "product_id",
            "quantity",
            "discount_amount",
            "total_amount",
            "order_date"
        )
        
        # 5. Deduplicate on streaming key identifiers
        .dropDuplicates()
    )