import os
from airflow import DAG
from airflow.decorators import task
from datetime import datetime
import requests
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 11, 29),
}

session_key = "9158"

with DAG(
    dag_id='f1_data_pipeline',
    default_args=default_args,
    schedule='@daily',
    catchup=False
) as dag:

    @task()
    def extract_data():
        """Extract F1 data from F1 API."""
        endpoint = f'https://api.openf1.org/v1/pit?session_key={session_key}'
        response = requests.get(endpoint)
        
        if response.status_code == 200:
            data = response.json()
            print(f"Extracted {len(data)} records from F1 API")
            return data
        else:
            raise Exception(f"Failed to fetch F1 data: {response.status_code}")

    @task()
    def transform_data(f1_data_list):
        """Transform raw F1 API JSON into rows for loading."""
        transformed_data = []
        
        for f1_data in f1_data_list:
            transformed_data.append({
                'date': f1_data.get('date'),
                'session_key': f1_data.get('session_key'),
                'meeting_key': f1_data.get('meeting_key'),
                'driver_number': f1_data.get('driver_number'),
                'pit_duration': f1_data.get('pit_duration'),
                'lap_number': f1_data.get('lap_number')
            })
        
        print(f"✅ Transformed {len(transformed_data)} records")
        return transformed_data

    @task()
    def load_data_into_database(transformed_data_list):
        """Load transformed data into PostgreSQL."""
        from sqlalchemy import create_engine, text
        from sqlalchemy.pool import NullPool

        DATABASE_URL = os.environ.get('DATABASE_URL')
        if not DATABASE_URL:
            raise RuntimeError('DATABASE_URL not set in environment. Add it to a .env file or export it.')

        engine = create_engine(
            DATABASE_URL,
            poolclass=NullPool,
            connect_args={
                'connect_timeout': 30,
                'keepalives': 1,
                'keepalives_idle': 30,
                'keepalives_interval': 10,
                'keepalives_count': 5,
                'options': '-c client_encoding=utf8'
            }
        )
        
        try:
            with engine.connect() as conn:
                # Create table
                conn.execute(text("""
                CREATE TABLE IF NOT EXISTS f1_data (
                    id SERIAL PRIMARY KEY,
                    date TIMESTAMP,
                    session_key INT,
                    meeting_key INT,
                    driver_number INT,
                    pit_duration FLOAT,
                    lap_number INT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """))
                conn.commit()
                
                print("✅ Table created/verified")
                
                # Insert data row by row
                insert_count = 0
                for data in transformed_data_list:
                    conn.execute(
                        text("""
                        INSERT INTO f1_data (date, session_key, meeting_key, driver_number, pit_duration, lap_number)
                        VALUES (:date, :session_key, :meeting_key, :driver_number, :pit_duration, :lap_number)
                        """),
                        {
                            'date': data.get('date'),
                            'session_key': data.get('session_key'),
                            'meeting_key': data.get('meeting_key'),
                            'driver_number': data.get('driver_number'),
                            'pit_duration': data.get('pit_duration'),
                            'lap_number': data.get('lap_number')
                        }
                    )
                    insert_count += 1
                
                conn.commit()
                print(f"✅ Successfully inserted {insert_count} records into f1_data table")
                
        except Exception as e:
            print(f"❌ Error: {str(e)}")
            raise
        finally:
            engine.dispose()

    # Define task dependencies
    f1_data = extract_data()
    transformed_data = transform_data(f1_data)
    load_data = load_data_into_database(transformed_data)