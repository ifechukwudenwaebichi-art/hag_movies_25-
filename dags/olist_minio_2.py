from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook
from airflow.utils.dates import days_ago
from datetime import timedelta
import pandas as pd
import warnings
import boto3
import io
warnings.filterwarnings('ignore')

# Default arguments
default_args = {
    "owner": "David_eVANGEL",
    "start_date": days_ago(0),
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "depends_on_past": False,
}

# DAG definition
dag = DAG(
    dag_id='olist_retail_etl_1.0',
    default_args=default_args,
    description='ETL pipeline for Olist retail data from Minio to PostgreSQL',
    schedule_interval='0 10 * * *',
    catchup=False,
    tags=['etl', 'retail', 'postgresql', 'pandas', 'minio']
)

MINIO_CONN_ID = 'hag_minio'
POSTGRES_CONN_ID = 'hag_postgres'
BUCKET_NAME = 'hagclasdemo'
FOLDER_NAME = 'OLIST'

# Simple helper to read from MinIO
def get_s3_client():
    """Get boto3 S3 client for MinIO"""
    connection = BaseHook.get_connection(MINIO_CONN_ID)
    import json
    extra = json.loads(connection.extra) if connection.extra else {}
    endpoint = extra.get('endpoint_url', 'http://minio:9000')
    
    return boto3.client(
        's3',
        endpoint_url=endpoint,
        aws_access_key_id=connection.login,
        aws_secret_access_key=connection.password,
        verify=False
    )

# First step
create_schema = PostgresOperator(
    task_id='create_schema',
    postgres_conn_id=POSTGRES_CONN_ID,
    sql = f"CREATE SCHEMA IF NOT EXISTS {FOLDER_NAME};",
    dag=dag,
)

# Load functions
def load_customer_table_from_minio():
    s3_client = get_s3_client()
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    
    obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=f"{FOLDER_NAME}/olist_customers_dataset.csv")
    df = pd.read_csv(io.BytesIO(obj['Body'].read()))
    
    pg_hook.run(f"DROP TABLE IF EXISTS {FOLDER_NAME}.customers CASCADE;")
    pg_hook.run(f"""
        CREATE TABLE {FOLDER_NAME}.customers (
            customer_id TEXT PRIMARY KEY,
            customer_unique_id TEXT,
            customer_zip_code_prefix INTEGER,
            customer_city TEXT,
            customer_state TEXT
        );
    """)
    
    df = df.where(pd.notnull(df), None)
    pg_hook.insert_rows(
        table=f"{FOLDER_NAME}.customers",
        rows=df.values.tolist(),
        target_fields=df.columns.tolist()
    )
    print(f"Loaded {len(df)} customers")

def load_seller_table_from_minio():
    s3_client = get_s3_client()
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    
    obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=f"{FOLDER_NAME}/olist_sellers_dataset.csv")
    df = pd.read_csv(io.BytesIO(obj['Body'].read()))
    
    pg_hook.run(f"DROP TABLE IF EXISTS {FOLDER_NAME}.sellers CASCADE;")
    pg_hook.run(f"""
        CREATE TABLE {FOLDER_NAME}.sellers (
            seller_id TEXT PRIMARY KEY,
            seller_zip_code_prefix INTEGER,
            seller_city TEXT,
            seller_state TEXT
        );
    """)
    
    df = df.where(pd.notnull(df), None)
    pg_hook.insert_rows(
        table=f"{FOLDER_NAME}.sellers",
        rows=df.values.tolist(),
        target_fields=df.columns.tolist()
    )
    print(f"Loaded {len(df)} sellers")

def load_product_table_from_minio():
    s3_client = get_s3_client()
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    
    obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=f"{FOLDER_NAME}/olist_products_dataset.csv")
    df = pd.read_csv(io.BytesIO(obj['Body'].read()))
    
    pg_hook.run(f"DROP TABLE IF EXISTS {FOLDER_NAME}.products CASCADE;")
    pg_hook.run(f"""
        CREATE TABLE {FOLDER_NAME}.products (
            product_id TEXT PRIMARY KEY,
            product_category_name TEXT,
            product_name_lenght TEXT,
            product_description_lenght TEXT,
            product_photos_qty TEXT,
            product_weight_g TEXT,
            product_length_cm TEXT,
            product_height_cm TEXT,
            product_width_cm TEXT
        );
    """)
    
    df = df.where(pd.notnull(df), None)
    pg_hook.insert_rows(
        table=f"{FOLDER_NAME}.products",
        rows=df.values.tolist(),
        target_fields=df.columns.tolist()
    )
    print(f"Loaded {len(df)} products")

def load_order_table_from_minio():
    s3_client = get_s3_client()
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    
    obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=f"{FOLDER_NAME}/olist_orders_dataset.csv")
    df = pd.read_csv(io.BytesIO(obj['Body'].read()))
    
    pg_hook.run(f"DROP TABLE IF EXISTS {FOLDER_NAME}.orders CASCADE;")
    pg_hook.run(f"""
        CREATE TABLE {FOLDER_NAME}.orders (
            order_id TEXT PRIMARY KEY,
            customer_id TEXT,
            order_status TEXT,
            order_purchase_timestamp TIMESTAMP,
            order_approved_at TIMESTAMP,
            order_delivered_carrier_date TIMESTAMP,
            order_delivered_customer_date TIMESTAMP,
            order_estimated_delivery_date TIMESTAMP
        );
    """)
    
    df = df.where(pd.notnull(df), None)
    pg_hook.insert_rows(
        table=f"{FOLDER_NAME}.orders",
        rows=df.values.tolist(),
        target_fields=df.columns.tolist()
    )
    print(f"Loaded {len(df)} orders")

def load_order_items_table_from_minio():
    s3_client = get_s3_client()
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    
    obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=f"{FOLDER_NAME}/olist_order_items_dataset.csv")
    df = pd.read_csv(io.BytesIO(obj['Body'].read()))
    
    pg_hook.run(f"DROP TABLE IF EXISTS {FOLDER_NAME}.order_items CASCADE;")
    pg_hook.run(f"""
        CREATE TABLE {FOLDER_NAME}.order_items (
            order_id TEXT,
            order_item_id INTEGER,
            product_id TEXT,
            seller_id TEXT,
            shipping_limit_date TIMESTAMP,
            price NUMERIC,
            freight_value NUMERIC,
            PRIMARY KEY (order_id, order_item_id)
        );
    """)
    
    df = df.where(pd.notnull(df), None)
    pg_hook.insert_rows(
        table=f"{FOLDER_NAME}.order_items",
        rows=df.values.tolist(),
        target_fields=df.columns.tolist()
    )
    print(f"Loaded {len(df)} order items")

def load_payments_table_from_minio():
    s3_client = get_s3_client()
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    
    obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=f"{FOLDER_NAME}/olist_order_payments_dataset.csv")
    df = pd.read_csv(io.BytesIO(obj['Body'].read()))
    
    pg_hook.run(f"DROP TABLE IF EXISTS {FOLDER_NAME}.payments CASCADE;")
    pg_hook.run(f"""
        CREATE TABLE {FOLDER_NAME}.payments (
            order_id TEXT,
            payment_sequential INTEGER,
            payment_type TEXT,
            payment_installments INTEGER,
            payment_value NUMERIC,
            PRIMARY KEY (order_id, payment_sequential)
        );
    """)
    
    df = df.where(pd.notnull(df), None)
    pg_hook.insert_rows(
        table=f"{FOLDER_NAME}.payments",
        rows=df.values.tolist(),
        target_fields=df.columns.tolist()
    )
    print(f"Loaded {len(df)} payments")

def load_reviews_table_from_minio():
    s3_client = get_s3_client()
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    
    obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=f"{FOLDER_NAME}/olist_order_reviews_dataset.csv")
    df = pd.read_csv(io.BytesIO(obj['Body'].read()))
    
    # Drop duplicates in the source data
    df = df.drop_duplicates(subset=['review_id'], keep='first')
    
    pg_hook.run(f"DROP TABLE IF EXISTS {FOLDER_NAME}.reviews CASCADE;")
    pg_hook.run(f"""
        CREATE TABLE {FOLDER_NAME}.reviews (
            review_id TEXT PRIMARY KEY,
            order_id TEXT,
            review_score INTEGER,
            review_comment_title TEXT,
            review_comment_message TEXT,
            review_creation_date TIMESTAMP,
            review_answer_timestamp TIMESTAMP
        );
    """)
    
    df = df.where(pd.notnull(df), None)
    pg_hook.insert_rows(
        table=f"{FOLDER_NAME}.reviews",
        rows=df.values.tolist(),
        target_fields=df.columns.tolist()
    )
    print(f"Loaded {len(df)} reviews")

def load_geolocation_table_from_minio():
    s3_client = get_s3_client()
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    
    obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=f"{FOLDER_NAME}/olist_geolocation_dataset.csv")
    df = pd.read_csv(io.BytesIO(obj['Body'].read()))
    
    pg_hook.run(f"DROP TABLE IF EXISTS {FOLDER_NAME}.geolocation CASCADE;")
    pg_hook.run(f"""
        CREATE TABLE {FOLDER_NAME}.geolocation (
            geolocation_zip_code_prefix INTEGER,
            geolocation_lat NUMERIC,
            geolocation_lng NUMERIC,
            geolocation_city TEXT,
            geolocation_state TEXT
        );
    """)
    
    df = df.where(pd.notnull(df), None)
    pg_hook.insert_rows(
        table=f"{FOLDER_NAME}.geolocation",
        rows=df.values.tolist(),
        target_fields=df.columns.tolist()
    )
    print(f"Loaded {len(df)} geolocations")

def load_category_name_translation_table_from_minio():
    s3_client = get_s3_client()
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    
    obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=f"{FOLDER_NAME}/product_category_name_translation.csv")
    df = pd.read_csv(io.BytesIO(obj['Body'].read()))
    
    pg_hook.run(f"DROP TABLE IF EXISTS {FOLDER_NAME}.category_name_translation CASCADE;")
    pg_hook.run(f"""
        CREATE TABLE {FOLDER_NAME}.category_name_translation (
            product_category_name TEXT,
            product_category_name_english TEXT
        );
    """)
    
    df = df.where(pd.notnull(df), None)
    pg_hook.insert_rows(
        table=f"{FOLDER_NAME}.category_name_translation",
        rows=df.values.tolist(),
        target_fields=df.columns.tolist()
    )
    print(f"Loaded {len(df)} category translations")

# Create tasks
load_customer_table = PythonOperator(
    task_id='load_customer_table',
    python_callable=load_customer_table_from_minio,
    dag=dag,
)

load_seller_table = PythonOperator(
    task_id='load_seller_table',
    python_callable=load_seller_table_from_minio,
    dag=dag,
)

load_product_table = PythonOperator(
    task_id='load_product_table',
    python_callable=load_product_table_from_minio,
    dag=dag,
)

load_order_table = PythonOperator(
    task_id='load_order_table',
    python_callable=load_order_table_from_minio,
    dag=dag,
)

load_order_items_table = PythonOperator(
    task_id='load_order_items_table',
    python_callable=load_order_items_table_from_minio,
    dag=dag,
)

load_payments_table = PythonOperator(
    task_id='load_payments_table',
    python_callable=load_payments_table_from_minio,
    dag=dag,
)

load_reviews_table = PythonOperator(
    task_id='load_reviews_table',
    python_callable=load_reviews_table_from_minio,
    dag=dag,
)

load_geolocation_table = PythonOperator(
    task_id='load_geolocation_table',
    python_callable=load_geolocation_table_from_minio,
    dag=dag,
)

load_category_name_translation_table = PythonOperator(
    task_id='load_category_name_translation_table',
    python_callable=load_category_name_translation_table_from_minio,
    dag=dag,
)

# Simple dependencies
create_schema >> load_customer_table >> load_order_table
create_schema >> load_seller_table >> load_order_items_table
create_schema >> load_product_table >> load_order_items_table
create_schema >> load_payments_table
create_schema >> load_reviews_table
create_schema >> load_geolocation_table
create_schema >> load_category_name_translation_table
