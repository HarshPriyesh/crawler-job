import pandas as pd

data = {
    'name': ['Alice', 'Bob', 'Charlie'],
    'age': [25, 30, 35],
    'address': [
        {'street': '123 Main St', 'city': 'New York', 'zipcode': '10001'},
        {'street': '456 Elm St', 'city': 'Los Angeles', 'zipcode': '90001'},
        {'street': '789 Oak St', 'city': 'Chicago', 'zipcode': '60001'}
    ],
    'contacts': [
        {'phone': '123-456-7890', 'email': 'alice@example.com'},
        {'phone': '987-654-3210', 'email': 'bob@example.com'},
        {'phone': '555-555-5555', 'email': 'charlie@example.com'}
    ]
}

df = pd.DataFrame(data)

parquet_file_path = "user_details.parquet"

df.to_parquet(parquet_file_path, engine="pyarrow")

print(f"Data successfully saved as Parquet file at: {parquet_file_path}")
