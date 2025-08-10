import pandas as pd
from sqlalchemy import create_engine

def extract(csv_file):
    """Extract data from CSV file"""
    print(f"Extracting data from {csv_file}")
    return pd.read_csv(csv_file)

def transform(df):
    """Transform the data"""
    print("Transforming data")
    
    time_cols = ['time', 'sunrise', 'sunset']
    for col in time_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col])
    
    numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns
    df[numeric_cols] = df[numeric_cols].fillna(0)
    
    string_cols = df.select_dtypes(include=['object']).columns
    df[string_cols] = df[string_cols].astype(str)
    
    return df

def load(df, db_connection_string, table_name):
    """Load data into PostgreSQL"""
    print(f"Loading data into {table_name}")
    
    engine = create_engine(db_connection_string)
    
    df.to_sql(
        name=table_name,
        con=engine,
        if_exists='append', 
        index=False,
        method='multi', 
        chunksize=1000  
    )
    
    print("Data loaded successfully")

def etl_pipeline(csv_file, db_connection_string, table_name='weather_data'):
    """Run the complete ETL pipeline"""
    try:
        df = extract(csv_file)
        
        df = transform(df)
        
        load(df, db_connection_string, table_name)
        
        print("ETL process completed successfully")
    except Exception as e:
        print(f"Error in ETL process: {e}")

if __name__ == "__main__":
    CSV_FILE = 'data.csv'
    DB_CONFIG = {
        'host': 'localhost',
        'port': '5432',
        'dbname': 'lk_weather_db',
        'user': 'postgres',
        'password': 'postgres'
    }
    
    DB_CONNECTION_STRING = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
    etl_pipeline(CSV_FILE, DB_CONNECTION_STRING)