import random
import csv
from datetime import datetime, timedelta

file_path = "daily_sales.csv"
store_ids = [i for i in range(1, 101)]
start_date = datetime(2023, 1, 1)
end_date = datetime(2023, 12, 31)
delta = end_date - start_date


with open(file_path, mode="w", newline="") as file:
    writer = csv.writer(file)
    writer.writerow(["store_id", "sales_amount", "date"])
    for i in range(10000):
        store_id = random.choice(store_ids)
        sales_amount = round(random.uniform(100, 5000), 2)
        date = start_date + timedelta(days=random.randint(0, delta.days))
        writer.writerow([store_id, sales_amount, date.strftime("%Y-%m-%d")])

print(f"Sample sales data file created as {file_path}")
