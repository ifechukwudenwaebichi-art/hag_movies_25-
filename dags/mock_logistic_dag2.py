from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from datetime import timedelta
import pandas as pd
import numpy as np
import warnings
import os
warnings.filterwarnings('ignore')

# Default arguments
default_args = {
    "owner": "FarsH",
    "start_date": days_ago(0),
    "email": "random@gmail.com",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    }

# DAG definition
dag = DAG(
    dag_id='logistics_data_etl_pipeline_v4',
    default_args=default_args,
    description='ETL pipeline for logistics data from CSV to PostgreSQL',
    schedule_interval='0 10 * * *',  # Daily at 10 AM
    catchup=False,
    tags=['etl', 'logistics', 'postgresql', 'pandas']
)

# File path
INPUT_FILE_PATH = '/opt/airflow/input_data/mock_logistics.csv'

def extract_and_clean_logistics_data(**context):
    """Extract logistics data from CSV and perform initial cleaning"""
    print("EXTRACT & CLEAN PHASE")
    
    # Read the CSV file
    df = pd.read_csv(INPUT_FILE_PATH, encoding='latin1')
    print(f"Loaded {len(df)} records from {INPUT_FILE_PATH}")
    
    # Create a copy for cleaning
    df_clean = df.copy()
    
    # DATE CLEANING
    print("Cleaning date formats...")
    df_clean['shipment_date'] = pd.to_datetime(df_clean['shipment_date'], format='%m/%d/%Y', errors='coerce')
    df_clean['delivery_date'] = pd.to_datetime(df_clean['delivery_date'], format='%m/%d/%Y', errors='coerce')
    
    # Remove rows with invalid dates
    initial_count = len(df_clean)
    df_clean = df_clean.dropna(subset=['shipment_date', 'delivery_date'])
    print(f"Removed {initial_count - len(df_clean)} records with invalid dates")
    
    # Fix logical date issues (delivery before shipment)
    invalid_dates = df_clean['delivery_date'] < df_clean['shipment_date']
    print(f"Found {invalid_dates.sum()} records with delivery date before shipment date - fixing...")
    df_clean.loc[invalid_dates, ['shipment_date', 'delivery_date']] = df_clean.loc[invalid_dates, ['delivery_date', 'shipment_date']].values
    
    # HANDLE MISSING VALUES
    print("Handling missing values...")
    print("Missing values before cleaning:")
    missing_vals = df_clean.isnull().sum()
    print(missing_vals[missing_vals > 0])
    
    df_clean['recipient_city'] = df_clean['recipient_city'].fillna('Unknown')
    df_clean['recipient_postal_code'] = df_clean['recipient_postal_code'].fillna('00000')
    
    # STANDARDIZE TEXT FIELDS
    print("Standardizing text data...")
    df_clean['origin_country'] = df_clean['origin_country'].str.title()
    df_clean['destination_country'] = df_clean['destination_country'].str.title()
    df_clean['shipping_company'] = df_clean['shipping_company'].str.title()
    df_clean['delivery_status'] = df_clean['delivery_status'].str.lower()
    
    # DATA TYPE CONVERSION with proper error handling
    print("Converting data types...")
    df_clean['package_weight'] = pd.to_numeric(df_clean['package_weight'], errors='coerce')
    df_clean['estimated_delivery_time'] = pd.to_numeric(df_clean['estimated_delivery_time'], errors='coerce')
    
    # Handle potential integer overflow for shipment_id
    df_clean['shipment_id'] = pd.to_numeric(df_clean['shipment_id'], errors='coerce')
    # Cap shipment_id to prevent integer overflow in PostgreSQL
    df_clean['shipment_id'] = df_clean['shipment_id'].clip(upper=2147483647)  # PostgreSQL INTEGER max value
    
    # BASIC VALIDATION - Remove invalid records
    initial_count = len(df_clean)
    df_clean = df_clean[
        (df_clean['package_weight'] > 0) & 
        (df_clean['package_weight'].notna()) &
        (df_clean['estimated_delivery_time'].notna()) &
        (df_clean['estimated_delivery_time'] > 0)
    ]
    removed_count = initial_count - len(df_clean)
    print(f"Removed {removed_count} invalid records")
    
    # Save cleaned data to temporary location for next task
    df_clean.to_csv('/tmp/cleaned_logistics.csv', index=False)
    print(f"Cleaned data saved: {len(df_clean)} records")
    
    return f"Cleaned {len(df_clean)} logistics records"

def add_calculated_fields(**context):
    """Add calculated fields and business metrics"""
    print("TRANSFORM PHASE: Adding Calculated Fields")
    
    # Load cleaned data with proper date parsing
    try:
        df = pd.read_csv('/tmp/cleaned_logistics.csv')
        
        # Ensure dates are properly parsed as datetime objects
        df['shipment_date'] = pd.to_datetime(df['shipment_date'], errors='coerce')
        df['delivery_date'] = pd.to_datetime(df['delivery_date'], errors='coerce')
        
        # Remove any rows where date conversion failed
        initial_count = len(df)
        df = df.dropna(subset=['shipment_date', 'delivery_date'])
        if len(df) < initial_count:
            print(f"Removed {initial_count - len(df)} records with invalid date formats")
        
        print(f"Date columns data types: shipment_date={df['shipment_date'].dtype}, delivery_date={df['delivery_date'].dtype}")
        
    except Exception as e:
        print(f"Error loading data: {e}")
        raise
    
    # Calculate shipping duration with error handling
    try:
        df['shipping_duration_days'] = (df['delivery_date'] - df['shipment_date']).dt.days
        # Ensure shipping duration is reasonable (cap at 365 days)
        df['shipping_duration_days'] = df['shipping_duration_days'].clip(lower=0, upper=365)
        print(f"Calculated shipping duration for {len(df)} records")
    except Exception as e:
        print(f"Error calculating shipping duration: {e}")
        raise
    
    # Calculate cost per kg (assuming shipping_cost contains numeric values)
    try:
        # For demo purposes, we'll create a mock numeric shipping cost
        np.random.seed(42)  # For reproducible results
        df['mock_shipping_cost'] = np.random.uniform(10, 100, len(df))
        df['cost_per_kg'] = (df['mock_shipping_cost'] / df['package_weight']).round(2)
        
        # Handle any inf or nan values
        df['cost_per_kg'] = df['cost_per_kg'].replace([np.inf, -np.inf], np.nan)
        df['cost_per_kg'] = df['cost_per_kg'].fillna(0)
        
    except Exception as e:
        print(f"Error calculating cost per kg: {e}")
        raise
    
    # Determine if shipment is expedited (faster than estimated)
    try:
        df['is_expedited'] = df['shipping_duration_days'] < df['estimated_delivery_time']
    except Exception as e:
        print(f"Error calculating expedited status: {e}")
        df['is_expedited'] = False
    
    # Add quarter and year for analysis
    try:
        df['shipment_year'] = df['shipment_date'].dt.year
        df['shipment_quarter'] = df['shipment_date'].dt.quarter
        df['shipment_month'] = df['shipment_date'].dt.month
        
        # Ensure these are regular integers, not numpy int64 which can cause issues
        df['shipment_year'] = df['shipment_year'].astype('int32')
        df['shipment_quarter'] = df['shipment_quarter'].astype('int32')
        df['shipment_month'] = df['shipment_month'].astype('int32')
        
    except Exception as e:
        print(f"Error calculating date components: {e}")
        raise
    
    # Package size category
    try:
        df['weight_category'] = pd.cut(
            df['package_weight'], 
            bins=[0, 10, 30, 50, np.inf], 
            labels=['Light', 'Medium', 'Heavy', 'Extra Heavy']
        )
        df['weight_category'] = df['weight_category'].astype(str)
    except Exception as e:
        print(f"Error creating weight categories: {e}")
        df['weight_category'] = 'Unknown'
    
    print(f"Added calculated fields to {len(df)} records")
    
    # Save enhanced data
    df.to_csv('/tmp/enhanced_logistics.csv', index=False)
    
    return f"Added calculated fields to {len(df)} records"

def load_to_postgres(**context):
    """Load processed data to PostgreSQL"""
    print("LOAD PHASE: Loading to PostgreSQL")
    
    # Read the enhanced data
    df = pd.read_csv('/tmp/enhanced_logistics.csv')
    
    # Convert date columns back to datetime for proper database insertion
    df['shipment_date'] = pd.to_datetime(df['shipment_date'])
    df['delivery_date'] = pd.to_datetime(df['delivery_date'])
    
    # Ensure all numeric columns are properly typed
    numeric_columns = ['package_weight', 'shipping_duration_days', 'mock_shipping_cost', 
                      'cost_per_kg', 'shipment_year', 'shipment_quarter', 'shipment_month']
    
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Get PostgreSQL connection
    postgres_hook = PostgresHook(postgres_conn_id='hag_postgres')
    engine = postgres_hook.get_sqlalchemy_engine()
    
    # Load data to PostgreSQL
    try:
        df.to_sql(
            name='logistics_shipments_v2',
            con=engine,
            schema='shipping_airflow',
            if_exists='replace',  # Use 'append' for production
            index=False,
            method='multi',
            chunksize=1000  # Process in chunks to avoid memory issues
        )
        print(f"Successfully loaded {len(df)} records to shipping_airflow.logistics_shipments_v2")
    except Exception as e:
        print(f"Error loading to database: {e}")
        raise
    
    # Cleanup temporary files
    temp_files = ['/tmp/cleaned_logistics.csv', '/tmp/enhanced_logistics.csv']
    for temp_file in temp_files:
        if os.path.exists(temp_file):
            os.remove(temp_file)
    
    print("Temporary files cleaned up")
    return f"Successfully loaded {len(df)} records to PostgreSQL"

# Create schema task
create_schema_task = PostgresOperator(
    task_id='create_schema',
    postgres_conn_id='hag_postgres',
    sql="""
    CREATE SCHEMA IF NOT EXISTS shipping_airflow;
    """,
    dag=dag
)

# Create table task
create_table_task = PostgresOperator(
    task_id='create_logistics_table',
    postgres_conn_id='hag_postgres',
    sql="""
    CREATE TABLE IF NOT EXISTS shipping_airflow.logistics_shipments_v2 (
        shipment_id INTEGER,
        tracking_number VARCHAR(50),
        shipment_date DATE,
        delivery_date DATE,
        origin_country VARCHAR(100),
        destination_country VARCHAR(100),
        shipping_company VARCHAR(100),
        shipping_method VARCHAR(20),
        package_weight DECIMAL(10,2),
        package_dimensions VARCHAR(20),
        shipping_cost VARCHAR(50),
        customs_value VARCHAR(50),
        delivery_status VARCHAR(20),
        recipient_name VARCHAR(200),
        recipient_address TEXT,
        recipient_city VARCHAR(100),
        recipient_postal_code VARCHAR(20),
        recipient_phone VARCHAR(20),
        delivery_signature BOOLEAN,
        estimated_delivery_time INTEGER,
        shipping_duration_days INTEGER,
        mock_shipping_cost DECIMAL(10,2),
        cost_per_kg DECIMAL(10,2),
        is_expedited BOOLEAN,
        shipment_year INTEGER,
        shipment_quarter INTEGER,
        shipment_month INTEGER,
        weight_category VARCHAR(20),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """,
    dag=dag
)

# Python tasks
extract_clean_task = PythonOperator(
    task_id='extract_and_clean',
    python_callable=extract_and_clean_logistics_data,
    dag=dag
)

add_fields_task = PythonOperator(
    task_id='add_calculated_fields',
    python_callable=add_calculated_fields,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_to_postgres',
    python_callable=load_to_postgres,
    dag=dag
)

# Task dependencies
create_schema_task >> create_table_task >> extract_clean_task >> add_fields_task >> load_task