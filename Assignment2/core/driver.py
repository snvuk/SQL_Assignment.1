from Assignment2.core.util import *
# create SparkSession
spark = sparkSe()
# create dataframe
fruits_details_df=fruits_details(spark)
fruits_details_df.show()

pivoitdata_df=pivoitdata(fruits_details_df)
print(pivoitdata_df)

unpivoitdata_df=unpivoitdata(pivoitdata_df)
print(unpivoitdata_df)

