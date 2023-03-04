from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

def spark():
    spark = SparkSession\
        .builder\
    .appName("SQL_Assignment")\
    .getOrCreate()
    return spark



schema = StructType([StructField("name",MapType(StringType(),StringType()),True),\
                     StructField("dob",StringType(),True),\
                     StructField("gender",StringType(),True),\
                     StructField("salary",LongType(),True)])

dict1 = dict(firstName = "James", middleName ="" ,lastName ="Smith")
dict2 = dict(firstName = "Michael", middleName ="Rose" ,lastName ="")
dict3 = dict(firstName = "Robert", middleName ="" ,lastName ="Williams")
dict4 = dict(firstName = "Maria", middleName ="Anne" ,lastName ="Jones")
dict5 = dict(firstName = "Jen", middleName ="Mary",lastName ="Brown")

data = [(dict1,"03011988","M",3000),\
        (dict2,"10111998","M",20000),\
        (dict3,"02012000","M",3000),\
        (dict4,"03011998","F",11000),\
        (dict5,"04101988","F",10000)]

spark.sparkContext.setLogLevel("ERROR")

def makeDF(data,schema):
    df = spark.createDataFrame(data=data, schema=schema)
    return df

def flattenDF(makeDF):
    df = makeDF(data,schema)
    df = df.withColumn("firstName", df.name.getItem("firstName")) \
        .withColumn("middleName", df.name.getItem("middleName")) \
        .withColumn("lastName", df.name.getItem("lastName")) \
        .withColumn("dob", df.dob.cast(LongType())) \
        .drop("name")
    return df
    # df.show(truncate=False)
# flattenDf(makeDF)
def createTempView(flattenDF):
    df = flattenDF(makeDF)
    tempView = df.createOrReplaceTempView("tbname")
    return tempView

# Q1
def selectStatement():
    createTempView(flattenDF)
    df = spark.sql("SELECT firstName,lastName,salary FROM tbname")
    df.show()
    return df
# Q2
def createColumns():
    createTempView(flattenDF)
    df = spark.sql("SELECT * , '' as Country,''as Department,''as age FROM tbname")
    df.show()
    return df
# Q3
def updateColumns():
    createColumns()
    df = spark.sql("SELECT *,salary*10 as salary FROM tbname")
    df.show()
    return df
# Q4
def castColumn():
    createColumns()
    df = spark.sql("SELECT STRING(salary),STRING(dob) FROM tbname")
    df.show()
    return df
# Q5
def highestWageEmp(flattenDF):
    df = flattenDF(makeDF)
    df = df.withColumn("newColumn",col("salary"))
    df.show()
    return df
# Q6
def maxSalary():
    createColumns()
    df = spark.sql("SELECT firstName, middleName, lastName,salary FROM tbname WHERE salary = (SELECT MAX(salary) FROM tbname)")
    df.show()
    return df
# Q7
def orderBy():
    createColumns()
    df = spark.sql("SELECT * FROM tbname ORDER BY salary DESC")
    df.show()
    return df
# Q8
def dropColumn(flattenDF):
    df = flattenDF(makeDF)
    df = df.drop("salary", "dob")
    df.show()
    return df