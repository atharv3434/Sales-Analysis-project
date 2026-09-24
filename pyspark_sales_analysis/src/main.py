from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_date,
    month,
    sum as spark_sum,
    round as spark_round
)

def create_spark_session():
    return (
        SparkSession.builder
        .appName("PySpark Sales Analysis")
        .master("local[*]")
        .getOrCreate()
    )

def read_sales_data(spark, path):
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(path)
    )

def clean_data(df):
    required_columns = [
        "order_id",
        "order_date",
        "product",
        "category",
        "city",
        "quantity",
        "unit_price",
        "unit_cost",
    ]

    df = df.dropna(subset=required_columns)

    df = (
        df.withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))
          .withColumn("quantity", col("quantity").cast("int"))
          .withColumn("unit_price", col("unit_price").cast("double"))
          .withColumn("unit_cost", col("unit_cost").cast("double"))
    )

    return df

def add_calculated_columns(df):
    return (
        df.withColumn(
            "revenue",
            spark_round(col("quantity") * col("unit_price"), 2)
        )
        .withColumn(
            "cost",
            spark_round(col("quantity") * col("unit_cost"), 2)
        )
        .withColumn(
            "profit",
            spark_round(col("revenue") - col("cost"), 2)
        )
    )

def product_sales(df):
    return (
        df.groupBy("product")
        .agg(
            spark_round(spark_sum("quantity"), 0).alias("total_quantity"),
            spark_round(spark_sum("revenue"), 2).alias("total_revenue"),
            spark_round(spark_sum("profit"), 2).alias("total_profit"),
        )
        .orderBy(col("total_revenue").desc())
    )

def city_sales(df):
    return (
        df.groupBy("city")
        .agg(
            spark_round(spark_sum("revenue"), 2).alias("total_revenue"),
            spark_round(spark_sum("profit"), 2).alias("total_profit"),
        )
        .orderBy(col("total_revenue").desc())
    )

def monthly_sales(df):
    return (
        df.withColumn("month", month(col("order_date")))
        .groupBy("month")
        .agg(
            spark_round(spark_sum("revenue"), 2).alias("total_revenue"),
            spark_round(spark_sum("profit"), 2).alias("total_profit"),
        )
        .orderBy("month")
    )

def write_output(df, path):
    (
        df.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(path)
    )

def main():
    spark = create_spark_session()

    try:
        input_path = "data/sales.csv"
        output_base = "output"

        print("\n=== Reading Sales Data ===")
        sales_df = read_sales_data(spark, input_path)
        sales_df.show(truncate=False)

        print("\n=== Schema ===")
        sales_df.printSchema()

        print("\n=== Cleaning Data ===")
        clean_df = clean_data(sales_df)
        print(f"Rows after cleaning: {clean_df.count()}")

        print("\n=== Adding Revenue, Cost and Profit ===")
        final_df = add_calculated_columns(clean_df)
        final_df.show(truncate=False)

        print("\n=== Sales by Product ===")
        product_df = product_sales(final_df)
        product_df.show()

        print("\n=== Sales by City ===")
        city_df = city_sales(final_df)
        city_df.show()

        print("\n=== Monthly Sales ===")
        monthly_df = monthly_sales(final_df)
        monthly_df.show()

        write_output(product_df, f"{output_base}/product_sales")
        write_output(city_df, f"{output_base}/city_sales")
        write_output(monthly_df, f"{output_base}/monthly_sales")

        print("\nResults written to the output/ directory.")

    finally:
        spark.stop()

if __name__ == "__main__":
    main()
