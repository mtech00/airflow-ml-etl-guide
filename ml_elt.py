from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import pandas as pd
import os
import logging
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder
import joblib

# Constants for data directories
BASE_DIR = os.getcwd()  # Get current working directory
RAW_DATA_DIR = os.path.join(BASE_DIR, "raw_data")
CLEAN_DATA_DIR = os.path.join(BASE_DIR, "clean_data")
MERGED_DATA_DIR = os.path.join(BASE_DIR, "merged_data")
FEATURE_DATA_DIR = os.path.join(BASE_DIR, "feature_data")
SPLIT_DATA_DIR = os.path.join(BASE_DIR, "split_data")

# Ensure directories exist; create them if they don't
for directory in [RAW_DATA_DIR, CLEAN_DATA_DIR, MERGED_DATA_DIR, FEATURE_DATA_DIR, SPLIT_DATA_DIR]:
    os.makedirs(directory, exist_ok=True)
    
# Set up logger
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow', # Label indicating who "owns" not affecting functionality 
    'depends_on_past': False, # Even if the DAG runs daily, task task_a on April 4 won’t run unless April 3’s task_a succeeded.
    'start_date': datetime(2025, 4, 4), # 	DAG starts running from this date, but not on this date if we specified daily etc time 
    'retries': 1, # If the task fails, it will retry 1 time.
    'retry_delay': timedelta(minutes=2), # Wait 2 minutes before retrying a failed task.
    'email_on_failure': True, # Send an email alert if a task fails.
    'email_on_retry': True, # Send an email alert when a task is retried after failure.
    'email': ['test@example.com'], # The recipient list for failure/retry emails.
}


def get_date_stamp(logical_date):
    """Convert date to UTC time zone and YYYYMMDD format."""
    return logical_date.in_timezone('UTC').format("YYYYMMDD")

def check_file_exists(file_path):
    """Check if file with specified date stamp exists"""
    return os.path.exists(file_path)

def extract_ratings(**kwargs):
    """Extract ratings data from source"""
    logical_date = kwargs["logical_date"]
    date_stamp = get_date_stamp(logical_date)
    output_file = f"{RAW_DATA_DIR}/ratings_{date_stamp}.csv"
    
    # If file already exists, skip re-downloading
    if check_file_exists(output_file):
        logger.info(f"File {output_file} already exists. Skipping extraction.")
        kwargs['ti'].xcom_push(key='ratings_file', value=output_file)
        return output_file
    
    try:
        logger.info("Extracting ratings data")
        ratings = pd.read_csv("https://datasets.imdbws.com/title.ratings.tsv.gz", sep="\t", compression="gzip", low_memory=False)
        ratings.to_csv(output_file, index=False)
        logger.info(f"Ratings data saved to {output_file}")
        kwargs['ti'].xcom_push(key='ratings_file', value=output_file)
        return output_file
    except Exception as e:
        logger.error(f"Error extracting ratings data: {e}")
        raise

def extract_titles(**kwargs):
    """Extract titles data from source"""
    logical_date = kwargs["logical_date"]    
    date_stamp = get_date_stamp(logical_date)
    output_file = f"{RAW_DATA_DIR}/titles_{date_stamp}.csv"
    
    # If file already exists, skip re-downloading
    if check_file_exists(output_file):
        logger.info(f"File {output_file} already exists. Skipping extraction.")
        kwargs['ti'].xcom_push(key='titles_file', value=output_file)
        return output_file
    
    try:
        logger.info("Extracting titles data")
        titles = pd.read_csv("https://datasets.imdbws.com/title.basics.tsv.gz", sep="\t", compression="gzip", low_memory=False)
        titles.to_csv(output_file, index=False)
        logger.info(f"Titles data saved to {output_file}")
        kwargs['ti'].xcom_push(key='titles_file', value=output_file)
        return output_file
    except Exception as e:
        logger.error(f"Error extracting titles data: {e}")
        raise

def clean_titles(**kwargs):
    """Clean titles data"""
    ti = kwargs['ti']
    titles_file = ti.xcom_pull(task_ids='extract_titles')
    logical_date = kwargs["logical_date"]
    date_stamp = get_date_stamp(logical_date)
    output_file = f"{CLEAN_DATA_DIR}/titles_clean_{date_stamp}.csv"
    
    try:
        logger.info(f"Cleaning titles data from {titles_file}")
        titles = pd.read_csv(titles_file)
        
        # Filter only movies and select required columns for modeling
        titles = titles[titles['titleType'] == 'movie'][['tconst', 'startYear', 'runtimeMinutes', 'genres']]
        
        # Replace NULL placeholder with NaN and drop rows with missing values
        titles = titles.replace("\\N", pd.NA).dropna()
        
        # Convert data types
        titles['startYear'] = pd.to_numeric(titles['startYear']).astype(int)
        titles['runtimeMinutes'] = pd.to_numeric(titles['runtimeMinutes']).astype(int)
        
        titles.to_csv(output_file, index=False)
        logger.info(f"Clean titles data saved to {output_file}")
        ti.xcom_push(key='clean_titles_file', value=output_file)
        return output_file
    except Exception as e:
        logger.error(f"Error cleaning titles data: {e}")
        raise

def clean_ratings(**kwargs):
    """Clean ratings data"""
    ti = kwargs['ti']
    ratings_file = ti.xcom_pull(task_ids='extract_ratings')
    logical_date = kwargs["logical_date"]
    date_stamp = get_date_stamp(logical_date)
    output_file = f"{CLEAN_DATA_DIR}/ratings_clean_{date_stamp}.csv"
    
    try:
        logger.info(f"Cleaning ratings data from {ratings_file}")
        ratings = pd.read_csv(ratings_file)
        
        # Drop rows with missing values
        ratings = ratings.dropna()
        
        ratings.to_csv(output_file, index=False)
        logger.info(f"Clean ratings data saved to {output_file}")
        ti.xcom_push(key='clean_ratings_file', value=output_file)
        return output_file
    except Exception as e:
        logger.error(f"Error cleaning ratings data: {e}")
        raise

def merge_data(**kwargs):
    """Merge titles and ratings data"""
    ti = kwargs['ti']
    clean_titles_file = ti.xcom_pull(key='clean_titles_file', task_ids='transform_group.clean_titles')
    clean_ratings_file = ti.xcom_pull(key='clean_ratings_file', task_ids='transform_group.clean_ratings')
    logical_date = kwargs["logical_date"]
    date_stamp = get_date_stamp(logical_date)
    output_file = f"{MERGED_DATA_DIR}/merged_data_{date_stamp}.csv"
    
    try:
        logger.info(f"Merging data from {clean_titles_file} and {clean_ratings_file}")
        titles = pd.read_csv(clean_titles_file)
        ratings = pd.read_csv(clean_ratings_file)
        
        # Merge the datasets on tconst
        combined = pd.merge(titles, ratings, on='tconst', how='inner')
        
        # Drop tconst column (not needed and may cause data leakage)
        combined = combined.drop(columns=['tconst'])
        
        combined.to_csv(output_file, index=False)
        logger.info(f"Merged data saved to {output_file}")
        ti.xcom_push(key='merged_file', value=output_file)
        return output_file
    except Exception as e:
        logger.error(f"Error merging data: {e}")
        raise

def create_features(**kwargs):
    """Create features with one-hot encoding"""
    ti = kwargs['ti']
    merged_file = ti.xcom_pull(key='merged_file', task_ids='transform_group.merge_data')
    logical_date = kwargs["logical_date"]
    date_stamp = get_date_stamp(logical_date)
    output_file = f"{FEATURE_DATA_DIR}/features_{date_stamp}.csv"
    
    try:
        logger.info(f"Creating features from {merged_file}")
        combined = pd.read_csv(merged_file)
        
        # Prepare genres column for OneHotEncoding
        genres = combined['genres'].str.split(',', expand=True).stack().reset_index(level=1, drop=True)
        genres.name = 'genre'
        
        genres_df = pd.DataFrame(genres)
        
        # Use OneHotEncoder for genre encoding
        encoder = OneHotEncoder(sparse_output=False)  
        genre_encoded = encoder.fit_transform(genres_df[['genre']])
        
        # Create DataFrame with encoded genres
        genre_features = pd.DataFrame(
            genre_encoded,
            columns=encoder.get_feature_names_out(['genre']),
            index=genres_df.index
        )
        
        # Aggregate genre features by index (which corresponds to the original DataFrame indices)
        genre_features = genre_features.groupby(level=0).max()
        
        # Combine with original data and drop the original genres column
        result = pd.concat([combined.drop(columns=['genres']), genre_features], axis=1)
        
        joblib.dump(encoder, f"{FEATURE_DATA_DIR}/encoder_{date_stamp}.joblib")
        result.to_csv(output_file, index=False)
        
        logger.info(f"Features created and saved to {output_file}")
        ti.xcom_push(key='features_file', value=output_file)
        return output_file
    except Exception as e:
        logger.error(f"Error creating features: {e}")
        raise

def split_data(**kwargs):
    """Split data into train, validation, and test sets"""
    ti = kwargs['ti']
    features_file = ti.xcom_pull(key='features_file', task_ids='transform_group.create_features')
    logical_date = kwargs["logical_date"]
    date_stamp = get_date_stamp(logical_date)
    
    try:
        logger.info(f"Splitting data from {features_file}")
        data = pd.read_csv(features_file)
        
        # Split features and target
        X = data.drop(columns=['averageRating'])
        y = data['averageRating']
        
        # Split into train, validation, and test sets
        X_temp, X_test, y_temp, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        X_train, X_val, y_train, y_val = train_test_split(X_temp, y_temp, test_size=0.25, random_state=42)
        
        # Save splits
        X_train_file = f"{SPLIT_DATA_DIR}/X_train_{date_stamp}.csv"
        X_val_file = f"{SPLIT_DATA_DIR}/X_val_{date_stamp}.csv"
        X_test_file = f"{SPLIT_DATA_DIR}/X_test_{date_stamp}.csv"
        y_train_file = f"{SPLIT_DATA_DIR}/y_train_{date_stamp}.csv"
        y_val_file = f"{SPLIT_DATA_DIR}/y_val_{date_stamp}.csv"
        y_test_file = f"{SPLIT_DATA_DIR}/y_test_{date_stamp}.csv"
        
        X_train.to_csv(X_train_file, index=False)
        X_val.to_csv(X_val_file, index=False)
        X_test.to_csv(X_test_file, index=False)
        y_train.to_frame().to_csv(y_train_file, index=False)
        y_val.to_frame().to_csv(y_val_file, index=False)
        y_test.to_frame().to_csv(y_test_file, index=False)
        
        logger.info(f"Data split and saved to {SPLIT_DATA_DIR}")
        
        # Push paths to XCom
        ti.xcom_push(key='X_train_file', value=X_train_file)
        ti.xcom_push(key='X_val_file', value=X_val_file)
        ti.xcom_push(key='X_test_file', value=X_test_file)
        ti.xcom_push(key='y_train_file', value=y_train_file)
        ti.xcom_push(key='y_val_file', value=y_val_file)
        ti.xcom_push(key='y_test_file', value=y_test_file)

        logger.info("Data preprocessing and splitting complete.")
            
        return {
            'X_train_file': X_train_file,
            'X_val_file': X_val_file,
            'X_test_file': X_test_file,
            'y_train_file': y_train_file,
            'y_val_file': y_val_file,
            'y_test_file': y_test_file
        }
    except Exception as e:
        logger.error(f"Error splitting data: {e}")
        raise

with DAG(
    dag_id='ML_data_pipeline1', #  Unique name for DAG — shown in the Airflow UI must be unique for scheduler 
    default_args=default_args, # If we define additional arguments later, we can inherit these defaults applied to all tasks in the DAG.
    description='ETL pipeline for IMDB dataset', #  Short description of the DAG (shown in UI tooltips).
    schedule_interval=None, # This DAG will only run manually. (No automatic triggering) it is only testing we can @daily etc.
    catchup=False, # Prevents running DAGs for past dates between start_date and today. Only runs the latest execution. 
    max_active_runs=1, # Only one DAG run can be in progress at a time. Prevents overlapping runs.
    tags=['ml', 'imdb'], # Labels for filtering/grouping
    concurrency=16, #  Maximum number of tasks from this DAG
    doc_md= # Optional markdown documentation. Appears in the DAG UI "Docs" tab.
    """
    # IMDB Movie Rating Prediction Pipeline
        
        This DAG performs the following steps:
        1. Extract data from IMDB sources
        2. Clean and preprocess the data
        3. Merge datasets
        4. Create features
        5. One-hot encode categorical variables
        6. Split into train, validation, and test sets
    Owner: Learn Platform Data Team  
    Frequency: Daily
    """,
) as dag: #  context manager that registers all the tasks inside as part of the dag.

    # Start operator
    start = EmptyOperator(task_id='start')
    
    # Extract tasks
    extract_ratings_task = PythonOperator(
        task_id='extract_ratings',
        python_callable=extract_ratings,
        provide_context=True
    )
    
    extract_titles_task = PythonOperator(
        task_id='extract_titles',
        python_callable=extract_titles,
        provide_context=True
    )
    
    # File sensors for extract tasks
    ratings_file_sensor = FileSensor(
        task_id='ratings_file_sensor', # File path to watch (waits for the specified file to appear)
        filepath=os.path.join(RAW_DATA_DIR, f"ratings_{{{{ ds_nodash }}}}.csv"), # Airflow macro with date stamp
        poke_interval=30, # Check every 30 seconds
        timeout=300,  # Wait up to 300 seconds (5 minutes).  If file doesn't appear → task fails
        mode='poke' #  checks in the same process (useful for short wait times) 'reschedule' for long waits to free up worker slot
    )
    
    titles_file_sensor = FileSensor(
        task_id='titles_file_sensor', 
        filepath=os.path.join(RAW_DATA_DIR, f"titles_{{{{ ds_nodash }}}}.csv"),
        poke_interval=30,
        timeout=300,
        mode='poke'
    )
    
    # Transform task group
    with TaskGroup(group_id='transform_group') as transform_group:
        clean_titles_task = PythonOperator(
            task_id='clean_titles',
            python_callable=clean_titles,
            provide_context=True
        )
        
        clean_ratings_task = PythonOperator(
            task_id='clean_ratings',
            python_callable=clean_ratings,
            provide_context=True
        )
        
        merge_task = PythonOperator(
            task_id='merge_data',
            python_callable=merge_data,
            provide_context=True
        )
        
        features_task = PythonOperator(
            task_id='create_features',
            python_callable=create_features,
            provide_context=True
        )
        
        split_task = PythonOperator(
            task_id='split_data',
            python_callable=split_data,
            provide_context=True
        )
        
        # Define dependencies within the transform group
        [clean_titles_task, clean_ratings_task] >> merge_task >> features_task >> split_task

    # End operator
    end = EmptyOperator(task_id='end')
    
    # Define the main DAG dependencies
    start >> [extract_ratings_task, extract_titles_task] # runs in parallel
    extract_ratings_task >> ratings_file_sensor # After extracting ratings, wait for the ratings file to be fully available on disk. 
    extract_titles_task >> titles_file_sensor
    [ratings_file_sensor, titles_file_sensor] >> transform_group >> end # # Once both sensors succeed, move to transform_group 
