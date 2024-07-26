from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

spark = (
    SparkSession.builder.master("local")
    .appName("Calculate Average Daily Sales")
    .getOrCreate()
)

file_path = "./daily_sales.csv"
data = spark.read.csv(file_path, header=True, inferSchema=True)
average_sale = data.groupBy("store_id").agg(
    avg("sales_amount").alias("average_daily_sales")
)

average_sale.show()

output_path = "average_daily_sales.csv"
average_sale.write.csv(output_path, header=True)

spark.stop()
