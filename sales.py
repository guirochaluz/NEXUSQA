import os
import sys
import requests
from dateutil import parser
from db import SessionLocal
from models import Sale
from sqlalchemy import func, text
from typing import Optional, Tuple, List
from dotenv import load_dotenv
from dateutil.tz import tzutc
from requests.exceptions import HTTPError

# Carrega variáveis de ambiente
load_dotenv()
BACKEND_URL = os.getenv("BACKEND_URL")

API_BASE = "https://api.mercadolibre.com/orders/search"
FULL_PAGE_SIZE = 50

def get_full_sales(ml_user_id: str, access_token: str) -> int:
    from datetime import datetime, timedelta
    from dateutil.relativedelta import relativedelta
    from sales import _order_to_sale
    from dateutil.tz import tzutc
    from models import Sale
    from sqlalchemy import func, text
    from db import SessionLocal
    import requests

    db = SessionLocal()
    total_saved = 0

    try:
        data_min = db.query(func.min(Sale.date_closed)).filter(Sale.ml_user_id == int(ml_user_id)).scalar()
        data_max = db.query(func.max(Sale.date_closed)).filter(Sale.ml_user_id == int(ml_user_id)).scalar()

        if not data_min or not data_max:
            data_max = datetime.utcnow()
            data_min = data_max - relativedelta(years=1)

        if data_min.tzinfo is None:
            data_min = data_min.replace(tzinfo=tzutc())
        if data_max.tzinfo is None:
            data_max = data_max.replace(tzinfo=tzutc())

        current_start = data_min.replace(day=1)
        while current_start <= data_max:
            current_end = (current_start + relativedelta(months=1)) - timedelta(seconds=1)
            offset = 0
            while True:
                params = {
                    "seller": ml_user_id,
                    "offset": offset,
                    "limit": FULL_PAGE_SIZE,
                    "sort": "date_asc",
                    "order.date_closed.from": current_start.isoformat(),
                    "order.date_closed.to": current_end.isoformat()
                }
                headers = {"Authorization": f"Bearer {access_token}"}
                resp = requests.get(API_BASE, params=params, headers=headers)
                resp.raise_for_status()
                orders = resp.json().get("results", [])
                if not orders:
                    break

                for order in orders:
                    order_id = str(order["id"])
                    try:
                        full_resp = requests.get(f"https://api.mercadolibre.com/orders/{order_id}?access_token={access_token}")
                        if not full_resp.ok:
                            print(f"⚠️ Falha ao buscar ordem completa {order_id}: {full_resp.status_code}")
                            continue

                        full_order = full_resp.json()
                        nova_venda = _order_to_sale(full_order, ml_user_id, access_token, db)
                        print(f"✅ FULL - ordem {order_id} processada | ml_fee: {nova_venda.ml_fee}")

                        existing_sale = db.query(Sale).filter_by(order_id=order_id).first()
                        if not existing_sale:
                            db.add(nova_venda)
                            total_saved += 1
                        else:
                            for attr, value in nova_venda.__dict__.items():
                                if attr != "_sa_instance_state":
                                    setattr(existing_sale, attr, value)
                            total_saved += 1  # considera também atualizações

                    except Exception as e:
                        print(f"❌ Erro ao processar venda {order_id}: {e}")

                db.commit()
                if len(orders) < FULL_PAGE_SIZE:
                    break
                offset += FULL_PAGE_SIZE

            current_start += relativedelta(months=1)

    except Exception as e:
        db.rollback()
        raise RuntimeError(f"Erro ao importar vendas por intervalo: {e}")

    finally:
        db.close()

    return total_saved


def get_incremental_sales(ml_user_id: str, access_token: str) -> int:
    db = SessionLocal()
    total_saved = 0

    try:
        # 🔁 Tenta renovar token no início
        try:
            r = requests.post(f"{BACKEND_URL}/auth/refresh", json={"user_id": ml_user_id})
            r.raise_for_status()
            new_token = db.execute(
                text("SELECT access_token FROM user_tokens WHERE ml_user_id = :uid"),
                {"uid": ml_user_id}
            ).scalar()
            if new_token:
                access_token = new_token
        except Exception as e:
            print(f"⚠️ Falha no refresh inicial de token ({ml_user_id}): {e}")

        last_db_date = db.query(func.max(Sale.date_closed)).filter(Sale.ml_user_id == int(ml_user_id)).scalar()
        if last_db_date is None:
            return get_full_sales(ml_user_id, access_token)

        if last_db_date.tzinfo is None:
            last_db_date = last_db_date.replace(tzinfo=tzutc())

        params = {
            "seller": ml_user_id,
            "limit": FULL_PAGE_SIZE,
            "sort": "date_desc",
            "order.date_closed.from": last_db_date.isoformat(),
        }
        headers = {"Authorization": f"Bearer {access_token}"}

        try:
            resp = requests.get(API_BASE, params=params, headers=headers)
            resp.raise_for_status()
        except HTTPError as http_err:
            if resp.status_code == 401:
                print(f"🔄 Token expirado para {ml_user_id}, renovando e retry...")
                r2 = requests.post(f"{BACKEND_URL}/auth/refresh", json={"user_id": ml_user_id})
                r2.raise_for_status()
                new_token = db.execute(
                    text("SELECT access_token FROM user_tokens WHERE ml_user_id = :uid"),
                    {"uid": ml_user_id}
                ).scalar()
                if not new_token:
                    raise RuntimeError("Falha ao obter novo access_token após refresh")
                access_token = new_token
                headers = {"Authorization": f"Bearer {access_token}"}
                resp = requests.get(API_BASE, params=params, headers=headers)
                resp.raise_for_status()
            else:
                raise

        orders = resp.json().get("results", [])
        if not orders:
            return 0

        for o in orders:
            oid = str(o["id"])
            existing_sale = db.query(Sale).filter_by(order_id=oid).first()

            # 🔍 Busca a versão completa da ordem SEMPRE
            full_resp = requests.get(f"https://api.mercadolibre.com/orders/{oid}?access_token={access_token}")
            if not full_resp.ok:
                print(f"⚠️ Falha ao buscar ordem completa {oid}: {full_resp.status_code}")
                continue

            full_order = full_resp.json()
            nova_venda = _order_to_sale(full_order, ml_user_id, access_token, db)
            print(f"✅ Incremental - ordem {oid} processada | ml_fee: {nova_venda.ml_fee}")

            if not existing_sale:
                db.add(nova_venda)
                total_saved += 1
            else:
                # 🧠 Atualiza todos os campos
                for attr, value in nova_venda.__dict__.items():
                    if attr != "_sa_instance_state":
                        setattr(existing_sale, attr, value)

        db.commit()

    except Exception as e:
        db.rollback()
        raise RuntimeError(f"Erro no incremental: {e}")

    finally:
        db.close()

    return total_saved


def _order_to_sale(order: dict, ml_user_id: str, access_token: str, db: Optional[SessionLocal] = None) -> Sale:
    from models import Sale
    from db import SessionLocal
    from sqlalchemy import text
    from dateutil import parser
    import requests

    internal_session = False
    if db is None:
        db = SessionLocal()
        internal_session = True

    try:
        order_id = order.get("id")

        # 🔄 Sempre força buscar dados completos da ordem
        try:
            resp = requests.get(
                f"https://api.mercadolibre.com/orders/{order_id}?access_token={access_token}"
            )
            resp.raise_for_status()
            order = resp.json()
            print(f"✅ Order {order_id} complementada com dados completos")
        except Exception as e:
            print(f"⚠️ Erro ao complementar order {order_id}: {e}")

        # 🔍 Busca payments direto, caso ainda esteja ausente
        payments = order.get("payments")
        if not payments:
            try:
                pay_resp = requests.get(
                    f"https://api.mercadolibre.com/orders/{order_id}/payments?access_token={access_token}"
                )
                pay_resp.raise_for_status()
                payments = pay_resp.json()
                if isinstance(payments, list) and payments:
                    print(f"✅ Payments recuperados separadamente para {order_id}")
                    order["payments"] = payments
                else:
                    print(f"⚠️ Nenhum payment encontrado em fallback para {order_id}")
            except Exception as e:
                print(f"❌ Erro ao buscar payments separadamente: {e}")

        buyer = order.get("buyer", {}) or {}
        item = (order.get("order_items") or [{}])[0]
        item_inf = item.get("item", {}) or {}
        ship = order.get("shipping") or {}

        seller_sku = item_inf.get("seller_sku")
        quantity_sku = None
        custo_unitario = None
        level1 = None
        level2 = None

        if seller_sku:
            sku_info = db.execute(text("""
                SELECT quantity, custo_unitario, level1, level2
                FROM sku
                WHERE sku = :sku
                ORDER BY date_created DESC
                LIMIT 1
            """), {"sku": seller_sku}).fetchone()

            if sku_info:
                quantity_sku, custo_unitario, level1, level2 = sku_info

        # 🔎 marketplace_fee direto de payments
        payment_info = (order.get("payments") or [{}])[0]
        payment_id = payment_info.get("id")
        marketplace_fee = payment_info.get("marketplace_fee")

        print(f"📦 Finalizando order {order_id} | ml_fee: {marketplace_fee}")

        return Sale(
            order_id         = str(order.get("id")),
            ml_user_id       = int(ml_user_id),
            buyer_id         = buyer.get("id"),
            buyer_nickname   = buyer.get("nickname"),
            total_amount     = order.get("total_amount"),
            status           = order.get("status"),
            date_closed      = parser.isoparse(order.get("date_closed")),
            item_id          = item_inf.get("id"),
            item_title       = item_inf.get("title"),
            quantity         = item.get("quantity"),
            unit_price       = item.get("unit_price"),
            shipping_id      = ship.get("id"),
            seller_sku       = seller_sku,

            # Dados complementares do SKU
            quantity_sku     = quantity_sku,
            custo_unitario   = custo_unitario,
            level1           = level1,
            level2           = level2,

            # Taxa real do marketplace
            ml_fee           = marketplace_fee,
            payment_id       = payment_id,
        )

    finally:
        if internal_session:
            db.close()

def revisar_status_historico(ml_user_id: str, access_token: str, return_changes: bool = False) -> Tuple[int, List[Tuple[str, str, str]]]:
    from datetime import datetime, timedelta
    from dateutil.relativedelta import relativedelta
    from dateutil.tz import tzutc
    from db import SessionLocal
    from models import Sale
    from sales import _order_to_sale
    import requests
    from sqlalchemy import func

    print(f"🔁 Iniciando revisão para usuário: {ml_user_id}")

    db = SessionLocal()
    atualizadas = 0
    alteracoes = []

    try:
        data_min = db.query(func.min(Sale.date_closed)).filter(Sale.ml_user_id == int(ml_user_id)).scalar()
        data_max = db.query(func.max(Sale.date_closed)).filter(Sale.ml_user_id == int(ml_user_id)).scalar()

        if not data_min or not data_max:
            print("⚠️ Nenhuma venda encontrada para revisar.")
            return atualizadas, alteracoes

        if data_min.tzinfo is None:
            data_min = data_min.replace(tzinfo=tzutc())
        if data_max.tzinfo is None:
            data_max = data_max.replace(tzinfo=tzutc())

        current_start = data_min.replace(day=1)
        while current_start <= data_max:
            current_end = (current_start + relativedelta(months=1)) - timedelta(seconds=1)
            print(f"📅 Revisando de {current_start.date()} até {current_end.date()}")
            offset = 0

            while offset < 10000:
                params = {
                    "seller": ml_user_id,
                    "offset": offset,
                    "limit": 50,
                    "sort": "date_desc",
                    "order.date_closed.from": current_start.isoformat(),
                    "order.date_closed.to": current_end.isoformat()
                }
                headers = {"Authorization": f"Bearer {access_token}"}

                resp = requests.get("https://api.mercadolibre.com/orders/search", headers=headers, params=params)
                if not resp.ok:
                    print(f"❌ Falha ao buscar lista de orders (offset {offset}): {resp.status_code}")
                    break

                orders = resp.json().get("results", [])
                if not orders:
                    print("⛔ Nenhuma venda nesse intervalo.")
                    break

                for order in orders:
                    oid = str(order["id"])
                    existing_sale = db.query(Sale).filter_by(order_id=oid).first()

                    if existing_sale:
                        old_status = existing_sale.status

                        # 🔍 Buscar dados completos da ordem
                        full_resp = requests.get(f"https://api.mercadolibre.com/orders/{oid}?access_token={access_token}")
                        if full_resp.ok:
                            full_order = full_resp.json()
                            nova_venda = _order_to_sale(full_order, ml_user_id, access_token, db)
                            print(f"🔄 Atualizando ordem {oid} | status: {old_status} → {nova_venda.status} | ml_fee: {nova_venda.ml_fee}")

                            for attr, value in nova_venda.__dict__.items():
                                if attr != "_sa_instance_state":
                                    setattr(existing_sale, attr, value)

                            if return_changes and old_status != nova_venda.status:
                                alteracoes.append((oid, old_status, nova_venda.status))
                            atualizadas += 1
                        else:
                            print(f"⚠️ Falha ao buscar ordem completa {oid}: {full_resp.status_code}")

                db.commit()

                if len(orders) < 50:
                    break
                offset += 50

            current_start += relativedelta(months=1)

    except Exception as e:
        db.rollback()
        raise RuntimeError(f"❌ Erro ao revisar histórico: {e}")

    finally:
        db.close()

    print(f"✅ Revisão finalizada: {atualizadas} ordens atualizadas.")
    return (atualizadas, alteracoes) if return_changes else (atualizadas, [])



def padronizar_status_sales(engine):
    """
    Atualiza a tabela sales:
    - Converte 'paid' (qualquer variação de maiúscula/minúscula) para 'Pago'
    - Todos os outros status viram 'Cancelado'
    """
    with engine.begin() as conn:
        # Primeiro, converte 'paid' em 'Pago'
        conn.execute(text("""
            UPDATE sales
            SET status = 'Pago'
            WHERE LOWER(status) = 'paid'
        """))

        # Depois, define como 'Cancelado' tudo que NÃO for 'Pago'
        conn.execute(text("""
            UPDATE sales
            SET status = 'Cancelado'
            WHERE status != 'Pago'
        """))

def sync_all_accounts() -> int:
    db = SessionLocal()
    total = 0
    try:
        rows = db.execute(text("SELECT ml_user_id, access_token FROM user_tokens")).fetchall()
        for ml_user_id, access_token in rows:
            try:
                total += get_incremental_sales(str(ml_user_id), access_token)
            except Exception as e:
                print(f"❌ Erro ao sincronizar usuário {ml_user_id}: {e}")
    finally:
        db.close()

    print(f"📂️ Sincronização completa. Total de vendas importadas: {total}")
    return total

