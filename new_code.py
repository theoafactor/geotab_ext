import boto3
import logging
import pandas as pd
import json
from io import StringIO, BytesIO
from datetime import datetime, time
import mygeotab
from dateutil.relativedelta import relativedelta

# Initialize AWS clients
dynamodb = boto3.resource("dynamodb")
secrets_manager = boto3.client("secretsmanager")
s3_client = boto3.client("s3")

# Configurations
logger = logging.getLogger()
logger.setLevel(logging.INFO)
dynamodb_table_name = "geotab_job_control_master"

secret_name = "geotab_secrets"

# Entities which support pagination
paginable_entities = ["Audit", "DebugData", "Device", "Diagnostic", "DriverChange", "DutyStatusLog", "ExceptionEvent", "FaultData", "FuelTaxDetail", "LogRecord", "Route", "ShipmentLog", "StatusData", "TextMessage", "Trip", "Zone"]

# Dict for sorting
type_to_sortkey = {
    "Audit": "version",
    "DebugData": "date",
    "Device": "name",
    "Diagnostic": "version",
    "DriverChange": "date",
    "DutyStatusLog": "version",
    "ExceptionEvent": "version",
    "FaultData": "date",
    "FuelTaxDetail": "date",
    "LogRecord": "date",
    "Route": "name",
    "ShipmentLog": "version",
    "StatusData": "version",
    "TextMessage": "id",
    "Trip": "stop",
    "Zone": "name"
}

# Fetch secrets from Secrets Manager
def get_secrets():
    try:
        secret_value = secrets_manager.get_secret_value(SecretId=secret_name)
        return json.loads(secret_value["SecretString"])
    except Exception as e:
        print(f"Error retrieving secrets: {e}")
        raise e


# Initialize MyGeotab API client
def initialize_api_client(username, password, server, database):
    try:
        global client
        client = mygeotab.API(username=username, password=password, server=server, database=database)
        client.authenticate()
        print("API client authenticated.")
        return client
    except mygeotab.AuthenticationException as ex:
        print(f"Authentication failed: {ex}")
        raise

# Fetch data from MyGeotab API
def fetch_data(type_name, max_records, load_type, offset=None, last_id=None, last_processed_timestamp=None):
    try:
        print(f"Fetching data for type: {type_name}")
        # Ensure from_date is serialized correctly
        if last_processed_timestamp and isinstance(last_processed_timestamp, str):
            from_date = last_processed_timestamp
        elif last_processed_timestamp and isinstance(last_processed_timestamp, datetime):
            from_date = last_processed_timestamp.isoformat()
        else:
            from_date = None
        
        print('fromDate: ', from_date)
        print('max_records: ', int(max_records))
        print('lastId: ', last_id)
        print('offset: ', offset)
        
        last_id = last_id if last_id else None
        offset = offset if offset else None

        if type_name in paginable_entities:
            response = client.call(
                "Get",
                typeName=type_name,
                resultsLimit=int(max_records),
                search={"fromDate": from_date},
                sort={"sortBy": type_to_sortkey[type_name], "sortDirection": "asc", "offset": offset, "lastId": last_id}
            )
        else:
            response = client.call(
                "Get",
                typeName=type_name,
                search={"fromDate": from_date}
            )
        return response
    except mygeotab.MyGeotabException as error:
        print(f"Error fetching data from API: {error}")
        raise


# Store data in S3 and append if file exists
def store_data_in_s3(data, table_name, bucket_name, folder_name):
    if not data:
        print(f"No data to store for table {table_name}.")
        return

    # Convert data to DataFrame
    df = pd.json_normalize(data, sep="_")

    # Define file path in S3
    file_path = f"{folder_name}{table_name}/{table_name}.csv"

    try:
        # Check if the file already exists in S3
        response = s3_client.get_object(Bucket=bucket_name, Key=file_path)
        existing_df = pd.read_csv(response['Body'])
        # Append new data to existing DataFrame
        df = pd.concat([existing_df, df], ignore_index=True)
        print(f"Data appended to existing file for {table_name}.")
    except s3_client.exceptions.NoSuchKey:
        print(f"No existing file found for {table_name}. Creating a new file.")

    # Write updated DataFrame to CSV in memory
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    # Upload back to S3
    s3_client.put_object(Body=csv_buffer.getvalue(), Bucket=bucket_name, Key=file_path)
    print(f"Data uploaded to S3 bucket {bucket_name}, file {file_path}.")
    return file_path

# Generate a header file based on the uploaded data
def generate_header_file(table_name, bucket_name, folder_name, geotab_dir):
    try:
        # Define file paths
        data_file_path = f"{folder_name}{table_name}/{table_name}.csv"
        header_file_path = f"{geotab_dir}full/header/{table_name}.csv"

        # Fetch the uploaded CSV file from S3
        response = s3_client.get_object(Bucket=bucket_name, Key=data_file_path)
        csv_content = response['Body'].read().decode('utf-8')

        # Extract the header
        header_line = csv_content.split("\n")[0].strip()

        # Write the header to a new file
        output = BytesIO()
        output.write((header_line + "\n").encode('utf-8'))
        output.seek(0)

        # Upload the header file to S3
        s3_client.put_object(
            Body=output.getvalue(),
            Bucket=bucket_name,
            Key=header_file_path,
            ContentType="text/csv"
        )
        print(f"Header file uploaded to S3: {header_file_path}")
    except Exception as e:
        print(f"Error generating header file for {table_name}: {e}")
        raise

# Update metadata in DynamoDB
def update_metadata(table_name, last_id=None, last_timestamp=None, status="completed", response_data=None):
    print(f"Updating CompletionTimestamp for table {table_name}: {last_timestamp}")
    table = dynamodb.Table(dynamodb_table_name)
    update_expression = "SET latest_job_status = :status, CompletionTimestamp = :timestamp"
    expression_values = {
        ":status": status,
        ":timestamp": datetime.now().isoformat()
    }

    if last_id:
        update_expression += ", LastProcessedId = :last_id"
        expression_values[":last_id"] = last_id
    if last_timestamp:
        update_expression += ", LastProcessedTimestamp = :last_timestamp"
        expression_values[":last_timestamp"] = last_timestamp
    print('update expression: ', update_expression)
    print('expression_values: ', expression_values)
    print('table name: ', table_name)
    print('dynamodb_table_name: ', dynamodb_table_name)

   # Add response data if available
    if response_data:
        try:
            response_data_serializable = preprocess_response_data(response_data)
            response_data_str = json.dumps(response_data_serializable)
            if len(response_data_str) > 400000:  # DynamoDB item limit is 400KB
                print("Warning: Response data is too large to store in DynamoDB.")
            else:
                update_expression += ", ResponseData = :response_data"
                expression_values[":response_data"] = response_data_str
        except Exception as e:
            print(f"Error serializing response data: {e}")

    table.update_item(
        Key={"table_name": table_name},
        UpdateExpression=update_expression,
        ExpressionAttributeValues=expression_values,
    )
    print(f"Updated metadata for {table_name} in DynamoDB.")


## lambda handler 
def lambda_handler(event, context):
    try:
        # Fetch secrets
        secrets = get_secrets()
        username = secrets["api_user"]
        password = secrets["api_password"]
        server = secrets["api_server"]
        database = secrets["api_database"]
        bucket_name = secrets["destination_S3_bucket"]
        folder_name = secrets["destination_folder"]
        geotab_dir = secrets["api_dir"]

        # Initialize API client
        client = initialize_api_client(username, password, server, database)

        # Fetch table configurations from DynamoDB
        tables = dynamodb.Table(dynamodb_table_name).scan()["Items"]

        # # Process only tables marked as active (status = "A")
        active_tables = [table for table in tables if table.get("status") == "A"]

        print("Printing active tables: ...")
        #print(active_tables)

        for table in active_tables: 
            table_name = table["table_name"]
            type_name = table["type_name"]
            max_records_per_call = table.get("MaxRecordsPerCall", " ")
            start_time = table.get("StartTime")
            last_processed_timestamp = table.get("CompletionTimestamp")
            from_date = start_time or last_processed_timestamp

            # Dynamically determine from_date
            if last_processed_timestamp:
                from_date = last_processed_timestamp
            elif start_time:
                from_date = start_time
            else:
                from_date = None

            last_processed_id = table.get("LastProcessedId")
            load_type = "incremental" if last_processed_timestamp else "full"

            #print(f"Processing {table_name} ({type_name}) with {load_type} load...")

            # print("maximum records per call: ", max_records_per_call)


            if max_records_per_call == 0: 
                print("maximum records per call: ", max_records_per_call)
                # there is no pagination needed here, since the max records is 0 
                data = fetch_data(type_name, max_records_per_call, load_type, last_processed_timestamp=from_date)
                #print("Getting the data fetched: ", data)
                if data: 
                    print("THere is data, proceeding to store to s3 bucket ...")
                    store_data_in_s3(data, table_name, bucket_name, folder_name)

                    print("Updating the metadata ...")
                    update_metadata(table_name=table_name, last_timestamp=datetime.now().isoformat(), response_data=data[:10])
                continue
            else: 
                ## the macimum records is more than 0, attempt to paginate the records
                offset = None
                last_id = None
                count = 0
                while True:
                    try:
                        # Logging the iteration details
                        print(f"Iteration {count} ...")
                        print("Fetching data with parameters:")
                        print(f"Client: {client}")
                        print(f"type_name: {type_name}")
                        print(f"max_records_per_call: {max_records_per_call}")
                        print(f"load_type: {load_type}")
                        print(f"offset: {offset}")
                        print(f"last_id: {last_id}")
                        print(f"from_date: {from_date}")

                        # Fetch data
                        data = fetch_data(type_name, max_records_per_call, load_type, offset, last_id, from_date)
                        print(f"Data fetched, length: {len(data)}")

                        if not data:
                            print(f"No more data to fetch for {table_name}. Exiting loop.")
                            break

                        # Store data in S3
                        print(f"Storing data for {table_name} to S3 ...")
                        store_data_in_s3(data, table_name, bucket_name, folder_name)

                        # Update pagination parameters
                        if type_name in paginable_entities:
                            last_id = data[-1].get("id", last_id)
                            offset = data[-1].get(type_to_sortkey.get(type_name, ""), offset)

                            # Break if fewer records than the max_records_per_call are returned
                            if len(data) < max_records_per_call:
                                print("Fewer records than max_records_per_call received. Pagination complete.")
                                break
                        else:
                            print(f"Entity {type_name} does not support pagination. Exiting loop.")
                            break

                        # Increment the counter
                        count += 1

                    except Exception as e:
                        print(f"Error during processing iteration {count}: {e}")
                        raise

                    print(f"bucket name: {bucket_name}")
                    print(f"table_name {table_name}")
                    print(f"folder_name {folder_name}")
                    print(f"geotab_dir: {geotab_dir}")


                    #Generate header file after all data has been uploaded
                    generate_header_file(table_name, bucket_name, folder_name, geotab_dir)

                    # Update DynamoDB metadata with the response data
                    update_metadata(
                        table_name=table_name,
                        last_id=last_id,  # Update the last ID after successful processing
                        last_timestamp=datetime.now().isoformat(),
                        response_data=data[:10]  # Store only a subset to avoid large sizes
                    )

        else:
            print(f"No new data for {table_name}.")
            update_metadata(table_name, status="no new data")

    except Exception as e:
        print(f"Error in Lambda function: {e}")
        raise