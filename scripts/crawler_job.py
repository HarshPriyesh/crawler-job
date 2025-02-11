import logging
import argparse
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
import apache_beam as beam
from google.cloud import bigquery, storage
import os
import json
from google.api_core.exceptions import NotFound
import pandas as pd
import re
import fastavro
import pyarrow.parquet as pq
from datetime import datetime
import pytz

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (
    r"C:\Users\HarshPriyesh\Downloads\thehindu-411006-e7936a3c901c.json"
)


def flatten_json(record, parent_key=""):
    items = []
    for key, value in record.items():
        new_key = f"{parent_key}.{key}" if parent_key else key

        if isinstance(value, dict):
            items.extend(flatten_json(value, new_key))
        elif isinstance(value, list) and value and isinstance(value[0], dict):
            items.extend(flatten_json(value[0], new_key))
        else:
            items.append(new_key)

    return items


def parse_gcs_path(input_path):
    if input_path.startswith("gs://"):
        input_path = input_path[5:]
        bucket_name, prefix = input_path.split("/", 1)
        return bucket_name, prefix
    else:
        logging.error("Invalid GCS path. It should start with 'gs://'.")


def get_master_schema(schema_file_path):
    bucket_name, file_path = parse_gcs_path(schema_file_path)
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(file_path)
    if not blob.exists():
        logging.warning(f"Schema file not found: {schema_file_path}")
        return []
    raw_master_schema = blob.download_as_string().decode("utf-8")
    master_schema = raw_master_schema.splitlines()
    return master_schema


def extract_column_names(schema, prefix=""):
    column_names = []
    for field in schema.get("fields", []):
        field_name = field["name"]
        field_type = field["type"]

        if isinstance(field_type, list):
            # Check for non-null type and returns first field_type
            field_type = next((t for t in field_type if t != "null"), "null")

        if isinstance(field_type, dict) and field_type["type"] == "record":
            nested_prefix = f"{prefix}{field_name}."
            column_names.extend(extract_column_names(field_type, nested_prefix))
        elif isinstance(field_type, dict) and field_type["type"] == "array":
            items_type = field_type["items"]
            if isinstance(items_type, dict) and items_type["type"] == "record":
                nested_prefix = f"{prefix}{field_name}."
                column_names.extend(extract_column_names(items_type, nested_prefix))
        else:
            column_names.append(f"{prefix}{field_name}")
    return column_names


def get_columns_from_avro(file_path):
    """
    Reads all columns from an Avro file and stores them in a set.

    :param file_path: Path to the Avro file.
    :return: Set of column names.
    """
    columns = []
    try:
        with open(file_path, "rb") as file:
            reader = fastavro.reader(file)
            schema = reader.writer_schema
            if not schema:
                raise ValueError("No schema found in the Avro file.")
            columns = extract_column_names(schema)

    except fastavro._reader.common.SchemaResolutionError as e:
        logging.error("Schema resolution error:", e)
    except fastavro._reader.reader.ReaderError as e:
        logging.error("Reader error:", e)
    except Exception as e:
        logging.error("An error occurred while reading the Avro file:", e)

    return columns


def flatten_schema(schema, prefix=""):
    """
    Recursively flattens the schema to extract column names, including nested structs and lists.

    :param schema: Arrow schema or a nested field.
    :param prefix: The current prefix for the column name.
    :return: A list of fully qualified column names.
    """
    columns = []
    for field in schema:
        field_name = f"{prefix}.{field.name}" if prefix else field.name

        # Handle struct types
        if field.type.__class__.__name__ == "StructType":
            columns.extend(flatten_schema(field.type, prefix=field_name))
        # Handle list types
        elif field.type.__class__.__name__ == "ListType":
            # Drill down into the list element type
            list_element = field.type.value_type
            if list_element.__class__.__name__ == "StructType":
                columns.extend(flatten_schema(list_element, prefix=field_name))
            else:
                columns.append(field_name)  # Primitive type in list
        else:
            columns.append(field_name)  # Primitive type
    return columns


def get_columns_from_parquet(file_path):
    """
    Reads all column names from a Parquet file schema using PyArrow.

    :param file_path: Path to the Parquet file.
    :return: List of column names.
    """
    try:
        # Open the Parquet file and extract the schema
        parquet_file = pq.ParquetFile(file_path)
        schema = parquet_file.schema_arrow

        # Flatten the schema to extract all column names
        return flatten_schema(schema)
    except Exception as e:
        logging.error("An error occurred while reading the Parquet file schema:", e)
        return []


class GenerateColumnNames(beam.DoFn):
    def __init__(self, file_format):
        self.file_format = file_format

    def process(self, element):
        input_path = element

        logging.info(f"********** STARTED for {self.file_format.upper()} **********")
        bucket_name, prefix = parse_gcs_path(input_path)
        logging.info(f"bucket_name: {bucket_name}")
        logging.info(f"prefix: {prefix}")

        client = storage.Client()
        bucket = client.get_bucket(bucket_name)
        blobs = bucket.list_blobs(prefix=prefix)

        files = [
            blob.name
            for blob in blobs
            if blob.name.endswith(f".{self.file_format.lower()}")
        ]
        columns = set()

        match self.file_format.lower():
            case "json":
                if files:
                    for file in files:
                        logging.info(f"Processing {os.path.basename(file)}...")
                        temp_file = "temp_" + os.path.basename(file)
                        try:
                            blob = bucket.blob(file)
                            blob.download_to_filename(temp_file)

                            with open(temp_file, "r") as json_file:
                                for line in json_file:
                                    if line.strip():
                                        try:
                                            record = json.loads(line)
                                            columns.update(flatten_json(record))
                                        except json.JSONDecodeError as e:
                                            logging.error(
                                                f"Error decoding JSON: {e} on line: {line}"
                                            )

                            os.remove(temp_file)
                        except Exception as e:
                            logging.error(
                                "Error during processing of %s: %s", file, str(e)
                            )
                            os.remove(temp_file)

            case "csv":
                if files:
                    for file in files:
                        logging.info(f"Processing {os.path.basename(file)}...")
                        print(input_path + os.path.basename(file))
                        df = pd.read_csv(input_path + os.path.basename(file))
                        columns.update(df.columns)

            case "avro":
                if files:
                    for file in files:
                        logging.info(f"Processing {os.path.basename(file)}...")
                        temp_file = "temp_" + os.path.basename(file)
                        try:
                            blob = bucket.blob(file)
                            blob.download_to_filename(temp_file)
                            columns.update(get_columns_from_avro(temp_file))
                            os.remove(temp_file)
                        except Exception as e:
                            logging.error(
                                "Error during processing of %s: %s", file, str(e)
                            )
                            os.remove(temp_file)

            case "parquet":
                if files:
                    for file in files:
                        logging.info(f"Processing {os.path.basename(file)}...")
                        temp_file = "temp_" + os.path.basename(file)
                        try:
                            blob = bucket.blob(file)
                            blob.download_to_filename(temp_file)
                            columns.update(get_columns_from_parquet(temp_file))
                            os.remove(temp_file)
                        except Exception as e:
                            logging.error(
                                "Error during processing of %s: %s", file, str(e)
                            )
                            os.remove(temp_file)

            case _:
                logging.warning(f"No {self.file_format} files found in {input_path}")

        # Clean and process column names
        columns = list(columns)
        for i in range(len(columns)):
            columns[i] = re.sub(r"[^A-Za-z0-9_.]", "_", columns[i])
            columns[i] = re.sub(r"_{2,}", "_", columns[i]).strip("_")
        columns = set(columns)

        parent_columns = {col.split(".")[0] for col in columns if "." in col}
        columns = [col for col in columns if col not in parent_columns]

        return [columns]


class CombineColumnNames(beam.CombineFn):
    def create_accumulator(self):
        return set()

    def add_input(self, accumulator, input):
        return accumulator.union(input)

    def merge_accumulators(self, accumulators):
        combined = set()
        for acc in accumulators:
            combined = combined.union(acc)
        return combined

    def extract_output(self, accumulator):
        return sorted(accumulator)


def check_for_new_columns(source_columns, master_columns):
    logging.info(f"********** CHECKING NEW COLUMNS **********")
    logging.info(f"master_schema: {master_columns}")
    new_columns = set(source_columns) - set(master_columns)
    if new_columns:
        new_columns = list(new_columns)
        logging.info(f"Total New Columns: {len(new_columns)}")
        logging.info(f"New Column Names: {new_columns}")
        return source_columns, new_columns
    else:
        logging.info("There are no new columns.")
        return None


def check_column_exists(
    project_id: str, dataset_id: str, table_id: str, column_name: str
) -> bool:
    """
    Check if a column exists in a BigQuery table.
    Args:
        project_id (str): GCP project ID.
        dataset_id (str): BigQuery dataset ID.
        table_id (str): BigQuery table ID.
        column_name (str): The column name to check (nested columns are passed as "parent.child.sub_child").

    Returns:
        bool: True if column exists, False otherwise.
    """
    client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    table = client.get_table(table_ref)

    column_parts = column_name.split(".")

    def check_in_schema(schema_fields, parts):
        """Recursively check if the column exists in the schema."""
        for field in schema_fields:
            if field.name == parts[0]:
                if len(parts) > 1:
                    if field.field_type == "RECORD" and field.fields:
                        return check_in_schema(field.fields, parts[1:])
                    else:
                        return False
                else:
                    return True
        return False

    return check_in_schema(table.schema, column_parts)


def add_nested_column(new_schema, column_hierarchy):
    parent_field = None
    column_name = column_hierarchy[0]

    for field in new_schema:
        if field.name == column_name:
            parent_field = field
            break

    if parent_field and len(column_hierarchy) > 1:
        # If it exist, recursively add the nested fields
        updated_nested_fields = list(parent_field.fields)
        updated_nested_fields = add_nested_column(
            updated_nested_fields, column_hierarchy[1:]
        )

        updated_parent_field = bigquery.SchemaField(
            parent_field.name,
            "RECORD",
            mode=parent_field.mode,
            fields=updated_nested_fields,
        )

        for i, field in enumerate(new_schema):
            if field.name == column_name:
                new_schema[i] = updated_parent_field
                break
    else:
        logging.warning(f"Column '{column_name}' not found. Creating...")
        # If the parent field does not exist, create it and recursively add the nested fields
        if len(column_hierarchy) > 1:
            nested_field = add_nested_column([], column_hierarchy[1:])
            new_parent_field = bigquery.SchemaField(
                column_name, "RECORD", mode="REPEATED", fields=nested_field
            )
            new_schema.append(new_parent_field)
        else:
            # Add a new field at the current level
            new_schema.append(bigquery.SchemaField(column_name, "STRING"))

    return new_schema


def add_new_columns_to_table(new_columns, full_table_id):
    logging.info(f"********** ADDING NEW COLUMNS TO BQ TABLE **********")
    logging.info(f"Table: {full_table_id}")
    project_id, dataset_id, table_id = full_table_id.split(".")

    try:
        client = bigquery.Client()
        table_ref = client.dataset(dataset_id).table(table_id)
        table = client.get_table(table_ref)

        new_schema = list(table.schema)
        added_columns = []

        for column_name in new_columns:
            column_hierarchy = column_name.split(".")

            if check_column_exists(project_id, dataset_id, table_id, column_name):
                logging.warning(
                    f"Column '{column_name}' already exists in {table_id}. Skipping..."
                )
            else:
                new_schema = add_nested_column(new_schema, column_hierarchy)
                logging.info(f"Adding column '{column_name}'...")
                added_columns.append(column_name)

        if not added_columns:
            logging.warning("No new columns were added.")
            return

        table.schema = new_schema
        table = client.update_table(table, ["schema"])

        if len(table.schema) == len(new_schema):
            for name in added_columns:
                logging.info(f"New column '{name}' added to {table_id}.")
        else:
            logging.error("Failed to add the new columns.")

    except NotFound:
        logging.error(f"Table '{project_id}.{dataset_id}.{table_id}' not found.")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {str(e)}")


def insert_into_crawler_job_table(new_columns: list, table_name: str):
    client = bigquery.Client()
    table_id = f"{google_options.project}.crawler_job.audit_table"
    new_columns_str = ", ".join(new_columns)
    row_to_insert = [
        {
            "table_name": table_name,
            "new_columns_added": new_columns_str,
            "created_dt": dt.date().isoformat(),
        }
    ]
    errors = client.insert_rows_json(table_id, row_to_insert)

    if errors == []:
        logging.info(f"Crawler job record inserted for {table_name}")
    else:
        logging.info(
            f"Encountered errors while inserting crawler job records for {table_name}: {errors}"
        )


def write_to_gcs(columns, schema_file_path):
    client = storage.Client()
    bucket_name, prefix = parse_gcs_path(schema_file_path)
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(prefix)

    logging.info(f"********** COLUMN NAMES **********")
    logging.info(f"Total columns: {len(columns)}")
    logging.info(f"Column Names: {columns}")
    with blob.open("w") as file:
        for column in columns:
            file.write(f"{column}\n")
    logging.info(f"Column names saved to '{schema_file_path}'")


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--schema_file",
        dest="schema_file",
        required=True,
    )
    parser.add_argument(
        "--input_path",
        dest="input_path",
        required=True,
    )
    parser.add_argument(
        "--full_table_id",
        dest="full_table_id",
        required=True,
    )
    parser.add_argument(
        "--file_format",
        dest="file_format",
        required=True,
    )
    return parser.parse_known_args()


def run_pipeline():
    known_args, pipeline_args = parse_arguments()
    pipeline_options = PipelineOptions(pipeline_args)

    global google_options, dt
    google_options = pipeline_options.view_as(GoogleCloudOptions)
    dt = datetime.now().astimezone(pytz.timezone("Asia/Kolkata"))

    schema_file_path = known_args.schema_file

    master_schema = get_master_schema(schema_file_path)

    pipeline = beam.Pipeline(options=pipeline_options)

    input_data = pipeline | "Generat Input Path" >> beam.Create([known_args.input_path])
    columns = (
        input_data
        | "Generate Column Names"
        >> beam.ParDo(GenerateColumnNames(known_args.file_format))
        | "Combine Column Names" >> beam.CombineGlobally(CombineColumnNames())
    )

    check_new_columns = (
        columns
        | "Check New Columns"
        >> beam.Map(
            lambda source_columns: check_for_new_columns(source_columns, master_schema)
        )
        | "Remove None" >> beam.Filter(lambda x: x is not None)
    )

    check_new_columns | "Update Crawler Job Table" >> beam.Map(
        lambda new_cols: insert_into_crawler_job_table(
            new_cols[1], known_args.full_table_id
        )
    )

    check_new_columns | "Update BQ Table" >> beam.Map(
        lambda new_cols: add_new_columns_to_table(new_cols[1], known_args.full_table_id)
    )

    check_new_columns | "Write to GCS" >> beam.Map(
        lambda src_cols: write_to_gcs(src_cols[0], schema_file_path)
    )

    pipeline.run().wait_until_finish()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s - %(message)s",
        handlers=[logging.StreamHandler()],
    )
    logger = logging.getLogger()

    run_pipeline()

    logging.info("************")
    logging.info("Pipeline execution completed successfully!")
