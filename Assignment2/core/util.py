
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from pyspark.sql.functions import col, avg, sum, min, max, row_number

def sparkSe():
    spark = SparkSession.builder.config("spark.driver.host", "localhost").getOrCreate()
    return spark

def fruits_details(spark):

    data = [("Banana",1000,"USA"),
            ("Carrots",1500,"INDIA"),
            ("Beans",1600,"Sweden"),
            ("Orange",2000,"UK"),
            ("Orange",2000,"USA"),
            ("Banana",400,"China"),
            ("Carrots",1200,"China")]

    columns= ["Product","Amount","Country"]
    fruits_details_df = spark.createDataFrame(data = data, schema = columns)
    return fruits_details_df
def pivoitdata(fruits_details_df):
    pivotdf = fruits_details_df.groupBy("Product").pivot("Country").sum("Amount")
    pivotdf.show(truncate=False)
    return pivotdf

def unpivoitdata(pivotdf):
    unpivotexpr = "stack(4, 'China', china, 'INDIA', india, 'SWeden', sweden,'USA',usa) as (Country,Total)"
    unpivotdf = pivotdf.select("Product", expr(unpivotexpr)) \
        .where("Total is not null")
    unpivotdf.show()
    return unpivotdf


