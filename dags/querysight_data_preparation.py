from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

from datetime import datetime, timedelta
from clickhouse_driver import Client

import sqlparse
import pandas as pd
import json

from hyperskill_data.helpers.constants import CLICKHOUSE_HOST


def create_connection():
    client = Client(
        host=CLICKHOUSE_HOST,
        user=Variable.get("clickhouse_airflow_user"),
        password=Variable.get("clickhouse_airflow_password"),
    )
    return client

def extract_tables(parsed):
    tables = []
    for statement in parsed:
        for token in statement.tokens:
            if token.ttype is sqlparse.tokens.Keyword and token.value.upper() in ('FROM', 'JOIN'):
                next_token = statement.token_next(statement.token_index(token))
                if isinstance(next_token, sqlparse.sql.Identifier):
                    tables.append(next_token.get_real_name())
                elif isinstance(next_token, sqlparse.sql.IdentifierList):
                    for identifier in next_token.get_identifiers():
                        tables.append(identifier.get_real_name())
    return list(set(tables))

def extract_columns(parsed):
    columns = []
    for statement in parsed:
        if statement.get_type() == 'SELECT':
            select_seen = False
            for token in statement.tokens:
                if token.ttype is sqlparse.tokens.DML and token.value.upper() == 'SELECT':
                    select_seen = True
                elif select_seen:
                    if isinstance(token, sqlparse.sql.IdentifierList):
                        for identifier in token.get_identifiers():
                            if isinstance(identifier, sqlparse.sql.Identifier):
                                columns.append(identifier.get_name())
                            elif identifier.ttype is sqlparse.tokens.Wildcard:
                                columns.append(identifier.value)
                    elif isinstance(token, sqlparse.sql.Identifier):
                        columns.append(token.get_name())
                    elif token.ttype is sqlparse.tokens.Wildcard:
                        columns.append(token.value)
                    elif token.ttype is sqlparse.tokens.Keyword and token.value.upper() in ('FROM',):
                        break
    return list(set(columns))

def extract_functions(parsed):
    functions = []
    for statement in parsed:
        for token in statement.tokens:
            if isinstance(token, sqlparse.sql.Function):
                functions.append(token.get_name())
            elif token.is_group:
                functions.extend(extract_functions([token]))
    return list(set(functions))

def preprocess_query_logs(**kwargs):
    client = create_connection()
    query = """
    SELECT
        query_id,
        query,
        event_time,
        user,
        current_database AS database,
        query_duration_ms,
        read_rows AS rows_read,
        result_rows AS rows_sent,
        length(query) AS query_length,
        exception_code AS error_code,
        exception AS error_message,
        memory_usage,
        read_bytes + written_bytes AS disk_io_time
    FROM system.query_log
    WHERE type = 'QueryFinish'
      AND event_time >= now() - INTERVAL 7 DAY
      AND length(query) > 10
      AND user != 'default';
    """
    # Execute the query and get data and column types
    result = client.execute(query, with_column_types=True)
    data = result[0]
    columns_with_types = result[1]
    # Extract column names
    column_names = [col[0] for col in columns_with_types]
    # Create a DataFrame
    df = pd.DataFrame(data, columns=column_names)

    # Check if DataFrame is empty
    if df.empty:
        print("No data retrieved from query.")
        return

    # Proceed with processing the DataFrame
    processed_data = []
    for index, row in df.iterrows():
        parsed = sqlparse.parse(row['query'])

        tables_used = extract_tables(parsed)
        columns_used = extract_columns(parsed)
        functions_used = extract_functions(parsed)

        processed_row = {
            'query_id': row['query_id'],
            'query': row['query'],
            'event_time': row['event_time'],
            'user': row['user'],
            'database': row['database'],
            'query_duration_ms': row['query_duration_ms'],
            'rows_read': row['rows_read'],
            'rows_sent': row['rows_sent'],
            'query_length': row['query_length'],
            'table_count': len(tables_used),
            'tables_used': tables_used,
            'columns_used': columns_used,
            'functions_used': functions_used,
            'error_code': row['error_code'],
            'error_message': row['error_message'],
            'memory_usage': row['memory_usage'],
            'disk_io_time': row['disk_io_time']
        }
        processed_data.append(processed_row)

    processed_df = pd.DataFrame(processed_data)

    # Convert lists to JSON strings
    processed_df['tables_used'] = processed_df['tables_used'].apply(json.dumps)
    processed_df['columns_used'] = processed_df['columns_used'].apply(json.dumps)
    processed_df['functions_used'] = processed_df['functions_used'].apply(json.dumps)

    # Prepare data for insertion
    records = processed_df.to_dict('records')
    insert_columns = list(processed_df.columns)
    insert_query = f"INSERT INTO roman_ianvarev.processed_query_logs ({', '.join(insert_columns)}) VALUES"

    # Insert data
    client.execute(insert_query, [tuple(record.values()) for record in records])

    print("Advanced preprocessing completed successfully!")

default_args = {
    'owner': 'Roman Ianvarev',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 1),
    'email': ["roman.ianvarev@hyperskill.org"],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'advanced_query_log_preprocessing',
    default_args=default_args,
    description='DAG for preprocessing query logs for LLM analysis',
    schedule_interval='@once',
)

preprocess_task = PythonOperator(
    task_id='advanced_preprocess_query_logs',
    python_callable=preprocess_query_logs,
    dag=dag,
)

preprocess_task
