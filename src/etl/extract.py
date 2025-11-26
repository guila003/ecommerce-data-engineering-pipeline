import pandas as pd
import logging 
import os

logging.basicConfig(
    filename = "logs/etl.log",
    level = logging.INFO,
    format = "%(asctime)s - %(levelname)s - %(message)s"
)

def extract_csv_to_df(csv_path:str) -> pd.DataFrame:
    """
    Extract data from a CSV file and load it into a pandas DataFrame.

    Args:
        csv_path (str): The file path to the CSV file.

    Returns:
        pd.DataFrame: DataFrame containing the data from the CSV file.
    """
    try:  
        df = pd.read_csv(csv_path)
        return df
    except Exception as e:
        logging.error(f"Error reading {csv_path}: {e}")
        raise

if __name__ == "__main__":
    BASE_PATH  = "data/raw/"

    files = [
        "olist_customers_dataset.csv",
        "olist_geolocation_dataset.csv",
        "olist_order_items_dataset.csv",
        "olist_order_payments_dataset.csv",
        "olist_order_reviews_dataset.csv",
        "olist_orders_dataset.csv",
        "olist_products_dataset.csv",
        "olist_sellers_dataset.csv"
    ]

    for file in files:
        path = os.path.join(BASE_PATH, file)
        df = extract_csv_to_df(path)
        logging.info(f"Extracted {len(df)} records from {file}")
        