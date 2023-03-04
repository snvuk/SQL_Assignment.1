
import unittest
from pyspark.sql import SparkSession
from Assignment2.core.util import *
from pyspark.sql.types import *
class SparkETLTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession
                     .builder
                     .master("local[*]")
                     .appName("PySpark-unit-test")
                     .config('spark.port.maxRetries', 30)
                     .getOrCreate())

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_etl(self):
        input_Schema = StructType([
            StructField("Product", StringType(), True),
            StructField("Amount", IntegerType(), True),
            StructField("Country", StringType(), True)
        ])

        input_data = [("Banana", 1000, "USA"), ("Carrots", 1500, "INDIA"), ("Beans", 1600, "Sweden"),
                ("Orange", 2000, "UK"), ("Orange", 2000, "USA"), ("Banana", 400, "China"),
                ("Carrots", 1200, "China")]
        input_df = self.spark.createDataFrame(data=input_data, schema=input_Schema)

        # Unit Test Case How many lines does the RDD contain
        expected_schema = StructType([
            StructField("Product", StringType(), True),
            StructField("China", IntegerType(), True),
            StructField("INDIA", IntegerType(), True),
            StructField("Sweden", IntegerType(), True),
            StructField("UK", IntegerType(), True),
            StructField("USA", IntegerType(), True)
        ])
        expected_data = [("Orange",None,None,None,2000,2000),
                         ("Beans", None, None, 1600, None, None),
                         ("Banana", 400, None, None, None, 1000),
                         ("Carrots", 1200, 1500, None, None, None)
                         ]

        expected_df = self.spark.createDataFrame(data=expected_data, schema=expected_schema)
        transformed_df = pivoitdata(input_df)
        transformed_df.show()


        input_Schema = StructType([
            StructField("Product", StringType(), True),
            StructField("China", IntegerType(), True),
            StructField("INDIA", IntegerType(), True),
            StructField("Sweden", IntegerType(), True),
            StructField("UK", IntegerType(), True),
            StructField("USA", IntegerType(), True)
        ])
        input_data = [("Orange", None, None, None, 2000, 2000),
                         ("Beans", None, None, 1600, None, None),
                         ("Banana", 400, None, None, None, 1000),
                         ("Carrots", 1200, 1500, None, None, None)
                         ]
        input_df = self.spark.createDataFrame(data=input_data, schema=input_Schema)

        # Unit Test Case How many lines does the RDD contain
        expected_schema = StructType([
            StructField("Product", StringType(), True),
            StructField("Country", StringType(), True),
            StructField("Total", IntegerType(), True)
        ])
        expected_data = [("Orange", 'USA', 2000),
                         ("Banana", 'China', 400),
                         ("Banana", 'USA', 1000),
                         ('Beans', 'SWeden', 1600),
                         ("Carrots", 'China', 1200),
                         ("Carrots", 'INDIA', 1500)]

        expected_df = self.spark.createDataFrame(data=expected_data, schema=expected_schema)
        transformed_df = unpivoitdata(input_df)
        transformed_df.show()

        self.assertEqual(sorted(expected_df.collect()), sorted(transformed_df.collect()))





