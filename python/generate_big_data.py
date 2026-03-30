import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

# number of rows
NUM_ROWS = 15000

# sample data
customers = [f"C{str(i).zfill(4)}" for i in range(1, 501)]  # 500 customers
products = ["P1", "P2", "P3", "P4", "P5", "P6", "P7", "P8"]
categories = ["Electronics", "Clothing", "Home", "Sports"]
cities = ["Delhi", "Mumbai", "Bangalore", "Chennai", "Kolkata"]

start_date = datetime(2023, 1, 1)

data = []

for i in range(NUM_ROWS):
    order_id = 1000 + i
    customer = random.choice(customers)
    product = random.choice(products)
    category = random.choice(categories)
    city = random.choice(cities)

    order_date = start_date + timedelta(days=random.randint(0, 365))

    quantity = random.randint(1, 5)
    price = random.randint(100, 2000)
    revenue = quantity * price

    data.append([
        customer,
        order_id,
        order_date,
        product,
        category,
        city,
        quantity,
        price,
        revenue
    ])

df = pd.DataFrame(data, columns=[
    "customer_id",
    "order_id",
    "order_date",
    "product_id",
    "category",
    "city",
    "quantity",
    "price",
    "revenue"
])

# save file
df.to_csv("data/big_data.csv", index=False)

print("✅ Big dataset generated successfully!")