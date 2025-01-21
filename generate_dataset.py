import pandas as pd
import numpy as np
from faker import Faker

fake = Faker()

# Define the number of rows
# n_rows = 10_000_000  # 10m
# n_rows = 1_000_000  # 1m
n_rows = 100_000  # 100k

# Generate synthetic data
data = {
    "id": np.arange(1, n_rows + 1),
    "name": [fake.name() for _ in range(n_rows)],
    "age": np.random.randint(18, 70, size=n_rows),
    "city": [fake.city() for _ in range(n_rows)],
    "salary": np.random.uniform(30_000, 120_000, size=n_rows),
    "signup_date": [fake.date_this_decade() for _ in range(n_rows)],
}

# Create a DataFrame and save as CSV
df = pd.DataFrame(data)
df.to_csv("synthetic_100k.csv", index=False)
