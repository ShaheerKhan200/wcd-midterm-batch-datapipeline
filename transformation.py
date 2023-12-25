from pyspark.sql import SparkSession
import sys
from argparse import ArgumentParser
import time
from datetime import datetime

parser = ArgumentParser()
parser.add_argument('-d', help='gets the date')
parser.add_argument('-data',nargs='+', type=str, help='gets the data list') 
args = parser.parse_args()

data = args.data
date_str_raw = args.d

print(data)
print("data[0]",data[0])

sales_df_add = data[0]
calendar_df_add = data[1]
inventory_df_add = data[2]
store_df_add = data[3]
product_df_add = data[4]

print(sales_df_add)

print("date",date_str_raw)
# Convert to a time struct using time.strptime
time_struct = time.strptime(date_str_raw, "%Y-%m-%d")
# Convert the time struct to a datetime object
date_object = datetime.fromtimestamp(time.mktime(time_struct))
# Convert back to the desired string format
date_str = date_object.strftime("%Y%m%d")
# Print the result
print("date_str",date_str)


#creating Spark Session
#change to yarn for EMR Cluster
spark = SparkSession.builder.master("local[*]").appName("demo").getOrCreate()
#spark = SparkSession.builder.master("yarn").appName("demo").getOrCreate()

#read a file from S3
#change to s3 bucket when running in EMR
# for local file running
#sales_df = spark.read.option("header", "true").option("delimiter", ",").csv(f"file:////home/ec2-user/midterm/data/sales_20230723.csv")
# working 
sales_df = spark.read.option("header", "true").option("delimiter", ",").csv("/home/ec2-user/midterm/data/result/sales_20230723.csv")
#sales_df = spark.read.option("header", "true").option("delimiter", ",").csv(f"s3://midterm-data-dump-sk/sales_20230725.csv")

#sales_df = spark.read.option("header", "true").option("delimiter", ",").csv(f"{sales_df_add}")

# calendar_df
# calendar_df = spark.read.option("header", "true").option("delimiter", ",").csv(f"{calendar_df_add}")

# # inventory_df
# inventory_df = spark.read.option("header", "true").option("delimiter", ",").csv(f"{inventory_df_add}")

# # store_df
# store_df = spark.read.option("header", "true").option("delimiter", ",").csv(f"{store_df_add}")

# # product_df
# product_df = spark.read.option("header", "true").option("delimiter", ",").csv(f"{product_df_add}")
                                                                            
#total sales quantity by week, store, and product
sales_df.createOrReplaceTempView("sales")

df_sum_sales_quantity = spark.sql("select * from sales limit 5;")
df_sum_sales_quantity.show()                                                                        

#write the file back to S3
#change to s3 bucket when running in EMR
# for local file running
#df_sum_sales_quantity.repartition(1).write.mode("overwrite").option("compression", "gzip").parquet(f"file:////home/ec2-user/midterm/data/result/date=#{date_str}")
#works
#df_sum_sales_quantity.repartition(1).write.mode("overwrite").option("compression", "gzip").parquet("/home/ec2-user/midterm/data/result/date=2023-07-23")
#df_sum_sales_quantity.repartition(1).write.mode("overwrite").option("compression", "gzip").parquet(f"s3://midterm-result-sk/date={date_str}")

df_sum_sales_quantity.repartition(1).write.mode("overwrite").option("compression", "gzip").parquet(f"s3://midterm-result-sk/date={date_str}")

#df.write.format(“parquet”).partitionBy(“whatever”).mode(“overwrite")
#     .save("s3a://mylocation”)