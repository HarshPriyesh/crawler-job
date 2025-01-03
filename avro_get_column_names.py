import fastavro


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

    try:
        with open(file_path, "rb") as file:
            reader = fastavro.reader(file)
            schema = reader.writer_schema
            if not schema:
                raise ValueError("No schema found in the Avro file.")
            columns = extract_column_names(schema)

    except fastavro._reader.common.SchemaResolutionError as e:
        print("Schema resolution error:", e)
    except fastavro._reader.reader.ReaderError as e:
        print("Reader error:", e)
    except Exception as e:
        print("An error occurred while reading the Avro file:", e)

    return columns


if __name__ == "__main__":
    avro_file_path = avro_file_path = (
        r"C:\Users\HarshPriyesh\Documents\GitHub\crawler-job\avro-file-generator\account.avro"
    )
    columns = get_columns_from_avro(avro_file_path)
    print("Columns in the Avro file:", columns)
