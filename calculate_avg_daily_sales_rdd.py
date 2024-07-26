from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.master("local[*]")
    .appName("Calculate Average Daily Sales")
    .getOrCreate()
)

file_path = "./daily_sales.csv"
data = spark.read.csv(file_path, header=True, inferSchema=True)

rdd = data.rdd

average_sale_rdd = (
    rdd.map(lambda row: (row["store_id"], row["sales_amount"]))
    .groupByKey()
    .mapValues(lambda amounts: round(sum(amounts) / len(amounts), 2))
)

average_sale_df = average_sale_rdd.toDF(["store_id", "average_daily_sales"]).orderBy(
    "store_id"
)

average_sale_df.show()

output_path = "average_daily_sales_rdd.csv"
average_sale_df.write.mode("overwrite").csv(output_path, header=True)

spark.stop()
