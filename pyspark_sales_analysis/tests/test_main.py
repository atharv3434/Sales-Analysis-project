import unittest

from pyspark.sql import SparkSession
from src.main import clean_data, add_calculated_columns

class TestSalesAnalysis(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .appName("PySparkTest")
            .master("local[2]")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_calculated_columns(self):
        data = [
            (
                1,
                "2026-01-01",
                "Laptop",
                "Electronics",
                "Pune",
                2,
                100.0,
                60.0,
            )
        ]

        columns = [
            "order_id",
            "order_date",
            "product",
            "category",
            "city",
            "quantity",
            "unit_price",
            "unit_cost",
        ]

        df = self.spark.createDataFrame(data, columns)
        result = add_calculated_columns(clean_data(df)).collect()[0]

        self.assertEqual(result["revenue"], 200.0)
        self.assertEqual(result["cost"], 120.0)
        self.assertEqual(result["profit"], 80.0)

if __name__ == "__main__":
    unittest.main()
