from sqlalchemy import create_engine
from datetime import datetime
import requests
import os
from dotenv import load_dotenv

load_dotenv()
DB_URL = os.getenv("DB_URL")
engine = create_engine(DB_URL)
DATA_INICIO = datetime(2024, 5, 16)

def buscar_ml_fee(order_id: str, access_token: str):
    url = f"https://api.mercadolibre.com/orders/{order_id}?access_token={access_token}"
    try:
        resp = requests.get(url, timeout=10)
        if resp.ok:
            full_order = resp.json()
            payments = full_order.get("payments", [])
            if payments:
                fee = payments[0].get("marketplace_fee")
                return (order_id, fee)
    except Exception as e:
        print(f"[erro buscar_ml_fee] {order_id}: {e}")
    return (order_id, None)

