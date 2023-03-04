from Assignment01.core.util import *
import unittest

class testCases(unittest.TestCase):
    # 1
    def testSelectStatement(self):
        def checkDF():
            Schema = StructType([StructField('firstName', StringType(), True),\
                                 StructField('lastName', StringType(), True),\
                                 StructField('salary', LongType(), True)])
            data = [("James","Smith",3000), \
                    ("Michael","",20000), \
                    ("Robert","Williams",3000),\
                    ("Maria","Jones",11000),\
                    ("Jen","Brown",10000)]
            df = spark.createDataFrame(data=data,schema=Schema)
            return df
        self.assertEqual(selectStatement().collect(),checkDF().collect())
    # 2
    def testcreateColumns(self):
        def checkDF():
            Schema= StructType([StructField('firstName', StringType(), True),\
                                StructField('lastName', StringType(), True),\
                                StructField('salary', LongType(), True),\
                                StructField('Country', StringType(), False),\
                                StructField('Department', StringType(), False),\
                                StructField('age', StringType(), False)])

            data = [("James", "Smith", 3000,"","",""), \
                    ("Michael", "", 20000,"","",""), \
                    ("Robert", "Williams", 3000,"","",""), \
                    ("Maria", "Jones", 11000,"","",""), \
                    ("Jen", "Brown", 10000,"","","")]
            df = spark.createDataFrame(data=data, schema=Schema)
            return df

        self.assertEqual(createColumns().collect(), checkDF().collect())
    # 3
    def testupdateColumns(self):
        def checkDF():
            Schema= StructType([StructField('firstName', StringType(), True),\
                                StructField('lastName', StringType(), True),\
                                StructField('salary', LongType(), True),\
                                StructField('salary', LongType(), True)])
            data = [("James", "Smith", 3000,30000), \
                    ("Michael", "", 20000,200000), \
                    ("Robert", "Williams", 3000,30000), \
                    ("Maria", "Jones", 11000,110000), \
                    ("Jen", "Brown", 10000,100000)]
            df = spark.createDataFrame(data=data, schema=Schema)
            return df
        self.assertEqual(updateColumns().collect(), checkDF().collect())
    # 4
    def testcastColumn(self):
        def checkDF():
            Schema= StructType([StructField('salary', StringType(), True),\
                                StructField('dob', StringType(), True)])

            data = [(3000,""), \
                    (20000,""), \
                    (3000,""), \
                    (11000,""), \
                    (10000,"")]
            df = spark.createDataFrame(data=data, schema=Schema)
            return df
        self.assertEqual(castColumn().collect(), checkDF().collect())
    # 6
    def testmaxSalary(self):
        def checkDF():
            Schema= StructType([StructField('firstName', StringType(), True),\
                                StructField('middleName', StringType(), True),\
                                StructField('lastName', StringType(), True),\
                                StructField('salary', LongType(), True)])

            data = [("Michael", "Rose","", 20000)]
            df = spark.createDataFrame(data=data, schema=Schema)
            return df
        self.assertEqual(maxSalary().collect(), checkDF().collect())

    # 5
    def testhighestWageEmp(self):
        def checkDF():
            Schema= StructType([StructField('dob', LongType(), True),\
                                StructField('gender', StringType(), True),\
                                StructField('salary', LongType(), True),\
                                StructField('firstName', StringType(), True),\
                                StructField('middleName', StringType(), True),\
                                StructField('lastName', StringType(), True),\
                                StructField('newColumn', LongType(), True)])

            data = [(3011988,"M",3000,"James", "", "Smith", 3000), \
                    (10111998,"M",3000,"Michael", "Rose", "", 20000), \
                    (2012000,"M",3000,"Robert", "", "Williams", 3000), \
                    (3011988,"F",3000,"Maria", "Anne", "Jones", 11000), \
                    (4101988,"F",3000,"Jen", "Mary", "Brown", 10000)]
            df = spark.createDataFrame(data=data, schema=Schema)
            return df
        self.assertEqual(highestWageEmp(flattenDF).collect(), checkDF().collect())
    # 7
    def testorderBy(self):
        def checkDF():
            Schema= StructType([StructField('dob', LongType(), True),\
                                StructField('gender', StringType(), True),\
                                StructField('salary', LongType(), True),\
                                StructField('firstName', StringType(), True),\
                                StructField('middleName', StringType(), True),\
                                StructField('lastName', StringType(), True)])

            data = [(10111998,"M",20000,"Michael", "Rose",""), \
                    (3011998,"F",11000,"Michael", "Anne","Jones"), \
                    (4101988,"F",10000,"Michael", "Mary","Brown"), \
                    (2012000,"M",3000,"Michael", "","Williams"), \
                    (3011998,"M",3000,"James", "","Smith")]
            df = spark.createDataFrame(data=data, schema=Schema)
            return df
        self.assertEqual(orderBy().collect(), checkDF().collect())
    # 8
    def testdropColumn(self):
        def checkDF():
            Schema= StructType([StructField('gender', StringType(), True),\
                                StructField('firstName', StringType(), True),\
                                StructField('middleName', StringType(), True),\
                                StructField('lastName', StringType(), True)])

            data = [("M","James", "","Smith"), \
                    ("M","Michael", "Rose", ""), \
                    ("M","Robert", "","Williams"), \
                    ("F","Maria", "Anne","Jones"), \
                    ("F","Jen", "Mary","Brown")]
            df = spark.createDataFrame(data=data, schema=Schema)
            return df
        self.assertEqual(dropColumn(flattenDF).collect(), checkDF().collect())

if __name__ == 'main':
    unittest.main()


