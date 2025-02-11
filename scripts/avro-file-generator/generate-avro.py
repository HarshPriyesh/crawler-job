import avro.schema
from avro.datafile import DataFileWriter
from avro.io import DatumWriter

schema_path = "account_schema.avsc"
schema = avro.schema.parse(open(schema_path, "r").read())

data = [
    {
        "AccountId": 1,
        "AccountName": "Primary Account",
        "Accounts": [
            {"AccountId": 101, "AccountType": "Savings"},
            {"AccountId": 102, "AccountType": "Checking"},
        ],
    },
    {
        "AccountId": 2,
        "AccountName": "Secondary Account",
        "Accounts": [
            {"AccountId": 201, "AccountType": "Business"},
            {"AccountId": 202, "AccountType": None},
        ],
    },
    {"AccountId": 3, "AccountName": "Tertiary Account", "Accounts": None},
]


output_file = "account.avro"
with DataFileWriter(open(output_file, "wb"), DatumWriter(), schema) as writer:
    for record in data:
        writer.append(record)

print(f"Data written to {output_file}")
