import pandas as pd

import pandas as pd

data = {
    "name": ["Alice", "Bob", "Charlie"],
    "age": [25, 30, 35],
    "address": [
        {
            "street": "123 Main St",
            "city": "New York",
            "zipcode": "10001",
            "coordinates": {"lat": 40.7128, "long": -74.0060},
        },
        {
            "street": "456 Elm St",
            "city": "Los Angeles",
            "zipcode": "90001",
            "coordinates": {"lat": 34.0522, "long": -118.2437},
        },
        {
            "street": "789 Oak St",
            "city": "Chicago",
            "zipcode": "60001",
            "coordinates": {"lat": 41.8781, "long": -87.6298},
        },
    ],
    "contacts": [
        {
            "phone": "123-456-7890",
            "email": "alice@example.com",
            "social_media": {"facebook": "alice_fb", "twitter": "alice_twitter"},
        },
        {
            "phone": "987-654-3210",
            "email": "bob@example.com",
            "social_media": {"facebook": "bob_fb", "twitter": "bob_twitter"},
        },
        {
            "phone": "555-555-5555",
            "email": "charlie@example.com",
            "social_media": {"facebook": "charlie_fb", "twitter": "charlie_twitter"},
        },
    ],
    "test_scores": [
        {
            "semester": "Fall 2023",
            "subjects": [
                {"subject": "Math", "score": 90},
                {"subject": "Science", "score": 88},
            ],
        },
        {
            "semester": "Spring 2023",
            "subjects": [
                {"subject": "Math", "score": 85},
                {"subject": "Science", "score": 92},
            ],
        },
        {
            "semester": "Fall 2022",
            "subjects": [
                {"subject": "Math", "score": 95},
                {"subject": "Science", "score": 89},
            ],
        },
    ],
}

df = pd.DataFrame(data)

print(df)


df = pd.DataFrame(data)

parquet_file_path = "user_details.parquet"

df.to_parquet(parquet_file_path, engine="pyarrow")

print(f"Data successfully saved as Parquet file at: {parquet_file_path}")
