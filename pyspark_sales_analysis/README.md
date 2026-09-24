# PySpark Sales Analysis Project

A beginner-friendly PySpark project that reads sales data from CSV, cleans it,
calculates revenue and profit, performs aggregations, and writes the results
to CSV.

## Project structure

```text
pyspark_sales_analysis/
├── data/
│   └── sales.csv
├── output/
├── src/
│   └── main.py
├── tests/
│   └── test_main.py
├── requirements.txt
├── run.sh
└── README.md
```

## Requirements

- Python 3
- Java 8 or 11
- Apache Spark / PySpark

## Install PySpark

```bash
pip install -r requirements.txt
```

## Run the project

From the project directory:

```bash
chmod +x run.sh
./run.sh
```

Or directly:

```bash
spark-submit src/main.py
```

## What the project does

1. Creates a SparkSession.
2. Reads `data/sales.csv`.
3. Displays the raw data.
4. Removes rows with missing required values.
5. Converts numeric columns to proper data types.
6. Calculates:
   - `revenue = quantity × unit_price`
   - `cost = quantity × unit_cost`
   - `profit = revenue - cost`
7. Finds total sales by product.
8. Finds total sales by city.
9. Finds monthly sales.
10. Writes the summary results into the `output/` folder.

## Output

After running, you will find:

```text
output/
├── product_sales/
├── city_sales/
└── monthly_sales/
```

Each output directory contains Spark-generated CSV part files.

## Important

Do not put files manually inside the output directories before running.
Spark may overwrite them when the application runs.
