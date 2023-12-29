from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
from pyspark import SparkConf, SparkContext
from pyspark.sql.types import *

spark = SparkSession.builder.appName("Visualize").getOrCreate()
hdfs_csv_path = "hdfs://master:9000/trung/output.csv"

df = spark.read.csv(hdfs_csv_path, header=True, inferSchema=True)

df.printSchema()

df.show()

symbol_col = 'Symbol'
last_col = 'Last'

df = df.withColumn(last_col, df[last_col].cast('double'))

data_to_plot = df.select(symbol_col, last_col).toPandas()

plt.figure(figsize=(12, 6))
plt.bar(data_to_plot[symbol_col], data_to_plot[last_col], color='blue')
plt.xlabel('Symbol')
plt.ylabel('Last price')
plt.title('Stock Prices')
plt.xticks(rotation=90)
plt.tight_layout()

plt.show()

spark.stop()