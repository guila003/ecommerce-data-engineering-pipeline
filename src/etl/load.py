import pandas as pd
from sqlalchemy import create_engine
from src.config.db_config import DB_CONFIG
import logging
import os

#Config  logs
if not os.path.exists("logs"):
    os.makedirs("logs")

logging.basicConfig(
    filename="logs/etl.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

#connexion a postgreSQL
def get_engine():
    try:
        engine = create_engine(
            f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
            )
        logging.info("Connexion to the database established successfully.")
        return engine
    except Exception as e:
        logging.error(f"Error connecting to the database: {e}")
        raise

def load_csv_postgres(csv_path, table_name):
    try:
        logging.info(f"Starting data {csv_path} vers {table_name}")
        df = pd.read_csv(csv_path)
        engine = get_engine()

        df.to_sql(
            table_name,
            engine,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=10000
        )
        logging.info(f"Data loaded successfully into {table_name}")
    except Exception as e:
        logging.error(f"Error loading data into {table_name}: {e}")
        raise

if __name__ == "__main__":
    BASE_PATH  = "data/raw/"

    files = {
        "olist_customers_dataset.csv" : "customers",
        "olist_geolocation_dataset.csv": "geolocation",
        "olist_order_items_dataset.csv": "order_items",
        "olist_order_payments_dataset.csv": "order_payments",
        "olist_order_reviews_dataset.csv": "order_reviews",
        "olist_orders_dataset.csv": "orders",
        "olist_products_dataset.csv": "products",
        "olist_sellers_dataset.csv": "sellers"
    }

    for file,table in files.items():
        path = os.path.join(BASE_PATH, file)
        load_csv_postgres(path, table)