import os
import requests
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from models import Base, Sale
from dotenv import load_dotenv
from tqdm import tqdm  # opcional para barra de progresso

# 1. Carrega variáveis de ambiente (.env)
load_dotenv()
DB_URL = os.getenv("DB_URL")

# 2. Configura conexão com o banco
engine = create_engine(DB_URL)
Session = sessionmaker(bind=engine)

# 3. Função principal para atualizar os SKUs
def atualizar_skus_antigos():
    session = Session()
    try:
        # Pega todas as vendas que ainda não têm SKU
        vendas_sem_sku = session.query(Sale).filter(
            Sale.seller_sku.is_(None),
            Sale.item_id.isnot(None)
        ).all()

        print(f"🔍 {len(vendas_sem_sku)} vendas sem SKU encontradas...")

        for venda in tqdm(vendas_sem_sku, desc="Atualizando SKUs"):
            try:
                resp = requests.get(f"https://api.mercadolibre.com/items/{venda.item_id}")
                if resp.ok:
                    item_data = resp.json()
                    sku = item_data.get("seller_sku") or item_data.get("seller_custom_field")
                    if sku:
                        venda.seller_sku = sku
            except Exception as e:
                print(f"⚠️ Erro ao buscar SKU para {venda.item_id}: {e}")

        session.commit()
        print("✅ SKUs atualizados com sucesso!")

    except Exception as e:
        session.rollback()
        print(f"❌ Erro geral: {e}")

    finally:
        session.close()

# 4. Executa
if __name__ == "__main__":
    atualizar_skus_antigos()
