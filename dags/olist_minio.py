from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.hooks.s3 import S3Hook
from airflow.utils.dates import days_ago
from datetime import timedelta
import pandas as pd
import warnings
import os
import io
warnings.filterwarnings('ignore')

# Default arguments
default_args = {
    "owner": "David_Evangel",
    "start_date": days_ago(0),
    "email": "random@gmail.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "depends_on_past": False,
    }

# DAG definition
dag = DAG(
    dag_id='olist_retail_etl',
    default_args=default_args,
    description='ETL pipeline for Olist retail data from Minio to PostgreSQL',
    schedule_interval='0 10 * * *',  # Daily at 10 AM
    catchup=False,
    tags=['etl', 'retail', 'postgresql', 'pandas', 'minio', 's3']
)

MINIO_CONN_ID = 'hag_minio'
POSTGRES_CONN_ID = 'hag_postgres'
BUCKET_NAME = 'hagclasdemo'
FOLDER_NAME = 'OLIST'

#first step
create_schema = PostgresOperator(
    task_id='create_schema',
    postgres_conn_id=POSTGRES_CONN_ID,
    sql=f"""
    CREATE SCHEMA IF NOT EXISTS {FOLDER_NAME};
    """,
    dag=dag,
)

#second step

# Function to load each table from Minio
def load_customer_table_from_minio():
    """Load customer data from Minio and insert into PostgreSQL.
    This is a dimensional table.
    """
    s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    file_content = s3_hook.read_key(
        key=f"{FOLDER_NAME}/olist_customers_dataset.csv",
        bucket_name=BUCKET_NAME
    )
    df = pd.read_csv(io.StringIO(file_content))
    print("customers table found and read from Minio.")
    
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {FOLDER_NAME}.customers (
        customer_id TEXT PRIMARY KEY,
        customer_unique_id TEXT,
        customer_zip_code_prefix INTEGER,
        customer_city TEXT,
        customer_state TEXT
    );
    """

    # execute create table
    pg_hook.run(create_table_sql)
    print("customers table created in PostgreSQL if not exists.")

    #replace Nan with None
    df = df.where(pd.notnull(df), None) 
    
    # Insert data into PostgreSQL
    pg_hook.insert_rows(
        table=f"{FOLDER_NAME}.customers",
        rows=df.values.tolist(),
        target_fields=df.columns.tolist(),
        replace=True
    )
    print("customers data inserted into PostgreSQL.")

def load_seller_table_from_minio():

    """Load seller data from Minio and insert into PostgreSQL.
    This is a dimensional table."""
    s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    file_content = s3_hook.read_key(
        key=f"{FOLDER_NAME}/olist_sellers_dataset.csv",
        bucket_name=BUCKET_NAME
    )
    df = pd.read_csv(io.StringIO(file_content))
    print("sellers table found and read from Minio.")
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {FOLDER_NAME}.sellers (
        seller_id TEXT PRIMARY KEY,
        seller_zip_code_prefix INTEGER,
        seller_city TEXT,
        seller_state TEXT
    );
    """

    # execute create table
    pg_hook.run(create_table_sql)
    print("sellers table created in PostgreSQL if not exists.")

    #replace Nan with None
    df = df.where(pd.notnull(df), None) 
    
    # Insert data into PostgreSQL
    pg_hook.insert_rows(
        table=f"{FOLDER_NAME}.sellers",
        rows=df.values.tolist(),
        target_fields=df.columns.tolist(),
        replace=True
    )
    print("sellers data inserted into PostgreSQL.")

def load_product_table_from_minio():
    """Load product data from Minio and insert into PostgreSQL.
    This is a dimensional table.
    """
    s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    file_content = s3_hook.read_key(
        key=f"{FOLDER_NAME}/olist_products_dataset.csv",
        bucket_name=BUCKET_NAME
    )
    df = pd.read_csv(io.StringIO(file_content))
    print("products table found and read from Minio.")
    
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {FOLDER_NAME}.products (
        product_id TEXT PRIMARY KEY,
        product_category_name TEXT,
        product_name_lenght INT,
        product_description_lenght INTEGER,
        product_photos_qty INTEGER,
        product_weight_g NUMERIC,
        product_length_cm NUMERIC,
        product_height_cm NUMERIC,
        product_width_cm NUMERIC
    );
    """

    # execute create table
    pg_hook.run(create_table_sql)
    print("products table created in PostgreSQL if not exists.")

    #replace Nan with None
    df = df.where(pd.notnull(df), None) 
    
    # Insert data into PostgreSQL
    pg_hook.insert_rows(
        table=f"{FOLDER_NAME}.products",
        rows=df.values.tolist(),
        target_fields=df.columns.tolist(),
        replace=True
    )
    print("products data inserted into PostgreSQL.")

def load_order_table_from_minio():
    """Load order data from Minio and insert into PostgreSQL.
    This is a fact table.
    """
    s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    file_content = s3_hook.read_key(
        key=f"{FOLDER_NAME}/olist_orders_dataset.csv",
        bucket_name=BUCKET_NAME
    )
    df = pd.read_csv(io.StringIO(file_content))
    print("orders table found and read from Minio.")
    
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {FOLDER_NAME}.orders (
        order_id TEXT PRIMARY KEY,
        customer_id TEXT,
        order_status TEXT,
        order_purchase_timestamp TIMESTAMP,
        order_approved_at TIMESTAMP,
        order_delivered_carrier_date TIMESTAMP,
        order_delivered_customer_date TIMESTAMP,
        order_estimated_delivery_date TIMESTAMP
    );
    """

    # execute create table
    pg_hook.run(create_table_sql)
    print("orders table created in PostgreSQL if not exists.")

    #replace Nan with None
    df = df.where(pd.notnull(df), None) 
    
    # Insert data into PostgreSQL
    pg_hook.insert_rows(
        table=f"{FOLDER_NAME}.orders",
        rows=df.values.tolist(),
        target_fields=df.columns.tolist(),
        replace=True
    )
    print("orders data inserted into PostgreSQL.")

def load_order_items_table_from_minio():
    """Load order items data from Minio and insert into PostgreSQL.
    This is a fact table."""
    s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    file_content = s3_hook.read_key(
        key=f"{FOLDER_NAME}/olist_order_items_dataset.csv",
        bucket_name=BUCKET_NAME
    )
    df = pd.read_csv(io.StringIO(file_content))
    print("order_items table found and read from Minio.")
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {FOLDER_NAME}.order_items (
        order_id TEXT,
        order_item_id INTEGER,
        product_id TEXT,
        seller_id TEXT,
        shipping_limit_date TIMESTAMP,
        price NUMERIC,
        freight_value NUMERIC,
        PRIMARY KEY (order_id, order_item_id)
    );
    """

    # execute create table
    pg_hook.run(create_table_sql)
    print("order_items table created in PostgreSQL if not exists.")

    #replace Nan with None
    df = df.where(pd.notnull(df), None) 
    
    # Insert data into PostgreSQL
    pg_hook.insert_rows(
        table=f"{FOLDER_NAME}.order_items",
        rows=df.values.tolist(),
        target_fields=df.columns.tolist(),
        replace=True
    )
    print("order_items data inserted into PostgreSQL.")

def load_payments_table_from_minio():
    """Load payments data from Minio and insert into PostgreSQL.
    This is a fact table."""
    s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    file_content = s3_hook.read_key(
        key=f"{FOLDER_NAME}/olist_order_payments_dataset.csv",
        bucket_name=BUCKET_NAME
    )
    df = pd.read_csv(io.StringIO(file_content))
    print("payments table found and read from Minio.")
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {FOLDER_NAME}.payments (
        order_id TEXT,
        payment_sequential INTEGER,
        payment_type TEXT,
        payment_installments INTEGER,
        payment_value NUMERIC,
        PRIMARY KEY (order_id, payment_sequential)
    );
    """

    # execute create table
    pg_hook.run(create_table_sql)
    print("payments table created in PostgreSQL if not exists.")

    #replace Nan with None
    df = df.where(pd.notnull(df), None) 
    
    # Insert data into PostgreSQL
    pg_hook.insert_rows(
        table=f"{FOLDER_NAME}.payments",
        rows=df.values.tolist(),
        target_fields=df.columns.tolist(),
        replace=True
    )
    print("payments data inserted into PostgreSQL.")

def load_reviews_table_from_minio():
    """Load reviews data from Minio and insert into PostgreSQL.
    This is a fact table."""
    s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    file_content = s3_hook.read_key(
        key=f"{FOLDER_NAME}/olist_order_reviews_dataset.csv",
        bucket_name=BUCKET_NAME
    )
    df = pd.read_csv(io.StringIO(file_content))
    print("reviews table found and read from Minio.")
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {FOLDER_NAME}.reviews (
        review_id TEXT PRIMARY KEY,
        order_id TEXT,
        review_score INTEGER,
        review_comment_title TEXT,
        review_comment_message TEXT,
        review_creation_date TIMESTAMP,
        review_answer_timestamp TIMESTAMP
    );
    """

    # execute create table
    pg_hook.run(create_table_sql)
    print("reviews table created in PostgreSQL if not exists.")

    #replace Nan with None
    df = df.where(pd.notnull(df), None) 
    
    # Insert data into PostgreSQL
    pg_hook.insert_rows(
        table=f"{FOLDER_NAME}.reviews",
        rows=df.values.tolist(),
        target_fields=df.columns.tolist(),
        replace=True
    )
    print("reviews data inserted into PostgreSQL.")

def load_geolocation_table_from_minio():
    """Load geolocation data from Minio and insert into PostgreSQL.
    This is a dimensional table."""
    s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    file_content = s3_hook.read_key(
        key=f"{FOLDER_NAME}/olist_geolocation_dataset.csv",
        bucket_name=BUCKET_NAME
    )
    df = pd.read_csv(io.StringIO(file_content))
    print("geolocation table found and read from Minio.")
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {FOLDER_NAME}.geolocation (
        geolocation_zip_code_prefix INTEGER,
        geolocation_lat NUMERIC,
        geolocation_lng NUMERIC,
        geolocation_city TEXT,
        geolocation_state TEXT
    );
    """

    # execute create table
    pg_hook.run(create_table_sql)
    print("geolocation table created in PostgreSQL if not exists.")

    #replace Nan with None
    df = df.where(pd.notnull(df), None) 
    
    # Insert data into PostgreSQL
    pg_hook.insert_rows(
        table=f"{FOLDER_NAME}.geolocation",
        rows=df.values.tolist(),
        target_fields=df.columns.tolist(),
        replace=True
    )
    print("geolocation data inserted into PostgreSQL.")

def load_category_name_translation_table_from_minio():
    """Load category name translation data from Minio and insert into PostgreSQL.
    This is a dimensional table."""
    s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    file_content = s3_hook.read_key(
        key=f"{FOLDER_NAME}/product_category_name_translation.csv",
        bucket_name=BUCKET_NAME
    )
    df = pd.read_csv(io.StringIO(file_content))
    print("category_name_translation table found and read from Minio.")
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {FOLDER_NAME}.category_name_translation (
        product_category_name TEXT,
        product_category_name_english TEXT
    );
    """

    # execute create table
    pg_hook.run(create_table_sql)
    print("category_name_translation table created in PostgreSQL if not exists.")

    #replace Nan with None
    df = df.where(pd.notnull(df), None) 
    
    # Insert data into PostgreSQL
    pg_hook.insert_rows(
        table=f"{FOLDER_NAME}.category_name_translation",
        rows=df.values.tolist(),
        target_fields=df.columns.tolist(),
        replace=True
    )
    print("category_name_translation data inserted into PostgreSQL.")

# create tasks for loading dimension tables
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

# load fact tables
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

# Define task dependencies
create_schema >> [
    load_customer_table,
    load_seller_table,
    load_product_table
] >> [
    load_order_table,
    load_order_items_table,
    load_payments_table,
    load_reviews_table,
    load_geolocation_table,
    load_category_name_translation_table
]
