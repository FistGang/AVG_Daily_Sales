from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, round

spark = (
    SparkSession.builder.master("local[*]")
    .appName("Calculate Average Daily Sales")
    .getOrCreate()
)

file_path = "./daily_sales.csv"
data = spark.read.csv(file_path, header=True, inferSchema=True)
average_sale = (
    data.groupBy("store_id")
    .agg(round(avg("sales_amount"), 2).alias("average_daily_sales"))
    .orderBy("store_id")
)

average_sale.show()

output_path = "average_daily_sales.csv"
average_sale.write.mode("overwrite").csv(output_path, header=True)

spark.stop()
