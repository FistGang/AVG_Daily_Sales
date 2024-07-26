# AVG_Daily_Sales

Calculate average daily sales using PySpark (just a toy demo)

## Generate sample data

Use this script to generate sample sales data.

```bash
python3 generate_sample_data.py
```

## Script

The generated data has three columns: `store_id`, `sales_amount` and `date`.
Our target is computing the daily average sales of each store based on store ID.

```bash
python3 calculate_avg_sales.py
```

The script using DataFrame API action and transformation methods.

Or you can use the RDD API to do the same

```bash
python3 calculate_avg_daily_sales_rdd.py
```
