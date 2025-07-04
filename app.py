import os
import warnings

# 1) Suprime todos os DeprecationWarning do Python
os.environ["PYTHONWARNINGS"] = "ignore::DeprecationWarning"
warnings.filterwarnings("ignore", category=DeprecationWarning)

# 2) (Opcional) Suprime warnings internos do Streamlit
import logging
logging.getLogger("streamlit").setLevel(logging.ERROR)


from dotenv import load_dotenv
import locale

# 1) Carrega .env antes de tudo
load_dotenv()
COOKIE_SECRET = os.getenv("COOKIE_SECRET")
BACKEND_URL    = os.getenv("BACKEND_URL")
FRONTEND_URL   = os.getenv("FRONTEND_URL")
DB_URL         = os.getenv("DB_URL")
ML_CLIENT_ID   = os.getenv("ML_CLIENT_ID")

# 2) Agora sim importe o Streamlit e configure a página _antes_ de qualquer outra chamada st.*
import streamlit as st
st.set_page_config(
    page_title="NEXUS Group",
    page_icon="favicon.png",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# 3) Depois de set_page_config, importe tudo o mais que precisar
from sales import sync_all_accounts, get_full_sales, revisar_banco_de_dados, get_incremental_sales, traduzir_status
from streamlit_cookies_manager import EncryptedCookieManager
import pandas as pd
import plotly.express as px
import requests
from sqlalchemy import create_engine, text
from streamlit_option_menu import option_menu
from typing import Optional
from wordcloud import WordCloud
import altair as alt
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans
from textblob import TextBlob
import io
from datetime import datetime, timedelta
from utils import engine, DATA_INICIO, buscar_ml_fee
import time





# 4) Configuração de locale
try:
    locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')
    LOCALE_OK = True
except locale.Error:
    LOCALE_OK = False

def format_currency(valor: float) -> str:
    # ...
    ...

# 5) Validações iniciais de ambiente
if not COOKIE_SECRET:
    st.error("⚠️ Defina COOKIE_SECRET no seu .env")
    st.stop()

if not all([BACKEND_URL, FRONTEND_URL, DB_URL, ML_CLIENT_ID]):
    st.error("❌ Defina BACKEND_URL, FRONTEND_URL, DB_URL e ML_CLIENT_ID em seu .env")
    st.stop()

# 6) Gerenciador de cookies e autenticação
cookies = EncryptedCookieManager(prefix="nexus/", password=COOKIE_SECRET)
if not cookies.ready():
    st.stop()

if "authenticated" not in st.session_state:
    st.session_state["authenticated"] = False

if cookies.get("access_token"):
    st.session_state["authenticated"] = True
    st.session_state["access_token"] = cookies["access_token"]

# ----------------- CSS Customizado -----------------
st.markdown("""
<style>
  html, body, [data-testid="stAppViewContainer"] {
    overflow: hidden !important;
    height: 100vh !important;
  }
  ::-webkit-scrollbar { display: none; }
  [data-testid="stSidebar"] {
    background-color: #161b22;
    overflow: hidden !important;
    height: 100vh !important;
  }
  [data-testid="stAppViewContainer"] {
    background-color: #0e1117;
    color: #fff;
  }
  .sidebar-title {
    font-size: 18px;
    font-weight: bold;
    color: #ffffff;
    margin-bottom: 10px;
  }
  .menu-button {
    width: 100%;
    padding: 8px;
    margin-bottom: 5px;
    background-color: #1d2b36;
    color: #fff;
    border: none;
    border-radius: 5px;
    text-align: left;
    cursor: pointer;
  }
  .menu-button:hover {
    background-color: #263445;
  }
</style>
""", unsafe_allow_html=True)



# ----------------- OAuth Callback -----------------
def ml_callback():
    """Trata o callback OAuth — envia o code ao backend, salva tokens e redireciona."""
    code = st.query_params.get("code", [None])[0]
    if not code:
        st.error("⚠️ Código de autorização não encontrado.")
        return
    st.success("✅ Código recebido. Processando autenticação...")
    resp = requests.post(f"{BACKEND_URL}/auth/callback", json={"code": code})
    if resp.ok:
        data = resp.json()                   # {"user_id": "...", ...}
        salvar_tokens_no_banco(data)
        st.cache_data.clear()             # limpa cache para puxar vendas novas
        st.experimental_set_query_params(account=data["user_id"])
        st.session_state["conta"] = data["user_id"]
        st.success("✅ Conta ML autenticada com sucesso!")
        st.rerun()
    else:
        st.error(f"❌ Falha na autenticação: {resp.text}")

# ----------------- Salvando Tokens -----------------
def salvar_tokens_no_banco(data: dict):
    try:
        with engine.connect() as conn:
            query = text("""
                INSERT INTO user_tokens (ml_user_id, access_token, refresh_token, expires_at)
                VALUES (:user_id, :access_token, :refresh_token, NOW() + interval '6 hours')
                ON CONFLICT (ml_user_id) DO UPDATE
                  SET access_token = EXCLUDED.access_token,
                      refresh_token = EXCLUDED.refresh_token,
                      expires_at   = NOW() + interval '6 hours';
            """)
            conn.execute(query, {
                "user_id":       data["user_id"],
                "access_token":  data["access_token"],
                "refresh_token": data["refresh_token"],
            })
    except Exception as e:
        st.error(f"❌ Erro ao salvar tokens no banco: {e}")

# ----------------- Carregamento de Vendas -----------------
@st.cache_data(ttl=300)
def carregar_vendas(conta_id: Optional[str] = None) -> pd.DataFrame:
    if conta_id:
        sql = text("""
            SELECT s.order_id,
                   s.date_adjusted,
                   s.item_id,
                   s.item_title,
                   s.status,
                   s.quantity,
                   s.unit_price,
                   s.total_amount,
                   s.ml_user_id,
                   s.buyer_nickname,
                   s.seller_sku,
                   s.custo_unitario,
                   s.quantity_sku,
                   s.ml_fee,
                   s.level1,
                   s.level2,
                   s.ads,
                   s.payment_id,
                   s.shipment_status,
                   s.shipment_substatus,
                   s.shipment_last_updated,
                   s.shipment_first_printed,
                   s.shipment_mode,
                   s.shipment_logistic_type,
                   s.shipment_list_cost,
                   s.shipment_delivery_type,
                   s.shipment_delivery_limit,
                   s.shipment_delivery_final,
                   s.shipment_receiver_name,
                   s.shipment_delivery_sla,
                   u.nickname
              FROM sales s
              LEFT JOIN user_tokens u ON s.ml_user_id = u.ml_user_id
             WHERE s.ml_user_id = :uid
        """)
        df = pd.read_sql(sql, engine, params={"uid": conta_id})
    else:
        sql = text("""
            SELECT s.order_id,
                   s.date_adjusted,
                   s.item_id,
                   s.item_title,
                   s.status,
                   s.quantity,
                   s.unit_price,
                   s.total_amount,
                   s.ml_user_id,
                   s.buyer_nickname,
                   s.seller_sku,
                   s.custo_unitario,
                   s.quantity_sku,
                   s.ml_fee,
                   s.level1,
                   s.level2,
                   s.ads,
                   s.payment_id,
                   s.shipment_status,
                   s.shipment_substatus,
                   s.shipment_last_updated,
                   s.shipment_first_printed,
                   s.shipment_mode,
                   s.shipment_logistic_type,
                   s.shipment_list_cost,
                   s.shipment_delivery_type,
                   s.shipment_delivery_limit,
                   s.shipment_delivery_final,
                   s.shipment_receiver_name,
                   s.shipment_delivery_sla,
                   u.nickname
              FROM sales s
              LEFT JOIN user_tokens u ON s.ml_user_id = u.ml_user_id
        """)
        df = pd.read_sql(sql, engine)

    return df

# ----------------- Componentes de Interface -----------------
def render_add_account_button():
    # agora com ML_CLIENT_ID e redirect_uri completos
    login_url = (
      f"{BACKEND_URL}/ml-login"
      f"?client_id={ML_CLIENT_ID}"
      f"&redirect_uri={FRONTEND_URL}/?nexus_auth=success"
    )
    st.markdown(f"""
      <a href="{login_url}" target="_blank">
        <button style="
          background-color:#4CAF50;
          color:white;
          border:none;
          padding:10px;
          border-radius:5px;
          margin-bottom:10px;
        ">
          ➕ Adicionar Conta Mercado Livre
        </button>
      </a>
    """, unsafe_allow_html=True)

from streamlit_option_menu import option_menu

def render_sidebar():
    with st.sidebar:
        # Menu de navegação sem título
        selected = option_menu(
            menu_title=None,
            options=[
                "Dashboard",
                "Contas Cadastradas",
                "Relatórios",
                "Expedição",
                "Gestão de SKU",
                "Gestão de Despesas",
                "Painel de Metas",
                "Gestão de Anúncios",
                "Configurações"  # 🔧 Nova tela adicionada
            ],
            icons=[
                "house",
                "person-up",
                "file-earmark-text",
                "collection-fill",
                "box-seam",
                "currency-dollar",
                "bar-chart-line",
                "bullseye",
                "gear"  # ícone para Configurações
            ],
            menu_icon="list",
            default_index=[
                "Dashboard",
                "Contas Cadastradas",
                "Relatórios",
                "Expedição",
                "Gestão de SKU",
                "Gestão de Despesas",
                "Painel de Metas",
                "Gestão de Anúncios",
                "Configurações"
            ].index(st.session_state.get("page", "Dashboard")),
            orientation="vertical",
            styles={
                "container": {
                    "padding": "0",
                    "background-color": "#161b22"
                },
                "icon": {
                    "color": "#2ecc71",
                    "font-size": "18px"
                },
                "nav-link": {
                    "font-size": "16px",
                    "text-align": "left",
                    "margin": "4px 0",
                    "color": "#fff",
                    "background-color": "transparent"
                },
                "nav-link:hover": {
                    "background-color": "#27ae60"
                },
                "nav-link-selected": {
                    "background-color": "#2ecc71",
                    "color": "white"
                },
            },
        )

    st.session_state["page"] = selected
    return selected
# ----------------- Telas -----------------
import io  # no topo do seu script

def format_currency(value):
    """Formata valores para o padrão brasileiro."""
    return f"R$ {value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def mostrar_dashboard():
    import time

    # --- sincroniza as vendas automaticamente apenas 1x ao carregar ---
    if "vendas_sincronizadas" not in st.session_state:
        with st.spinner("🔄 Sincronizando vendas..."):
            count = sync_all_accounts()
            st.cache_data.clear()
        placeholder = st.empty()
        with placeholder:
            st.success(f"{count} vendas novas sincronizadas com sucesso!")
            time.sleep(3)
        placeholder.empty()
        st.session_state["vendas_sincronizadas"] = True

    # --- carrega todos os dados ---
    df_full = carregar_vendas(None)
    if df_full.empty:
        st.warning("Nenhuma venda cadastrada.")
        return
        
    # ✅ TRADUZ STATUS AQUI
    from sales import traduzir_status
    df_full["status"] = df_full["status"].map(traduzir_status)

    # --- CSS para compactar inputs e remover espaços ---
    st.markdown(
        """
        <style>
        .stSelectbox > div, .stDateInput > div {
            padding-top: 0rem;
            padding-bottom: 0rem;
        }
        .stMultiSelect {
            max-height: 40px;
            overflow-y: auto;
        }
        .block-container {
            padding-top: 0rem;
        }
        .stMarkdown h1 { display: none; }
        </style>
        """,
        unsafe_allow_html=True,
    )

    # --- Filtro de contas fixo com checkboxes lado a lado + botão selecionar todos ---
    contas_df = pd.read_sql(text("SELECT nickname FROM user_tokens ORDER BY nickname"), engine)
    contas_lst = contas_df["nickname"].astype(str).tolist()
    
    st.markdown("**🧾 Contas Mercado Livre:**")


    # Estado para controlar se todas estão selecionadas
    if "todas_contas_marcadas" not in st.session_state:
        st.session_state["todas_contas_marcadas"] = True
    
    
    # Renderiza os checkboxes em colunas
    colunas_contas = st.columns(8)
    selecionadas = []
    
    for i, conta in enumerate(contas_lst):
        key = f"conta_{conta}"
        if key not in st.session_state:
            st.session_state[key] = st.session_state["todas_contas_marcadas"]
        if colunas_contas[i % 8].checkbox(conta, key=key):
            selecionadas.append(conta)
    
    # Aplica filtro
    if selecionadas:
        df_full = df_full[df_full["nickname"].isin(selecionadas)]


    # --- Linha única de filtros: Rápido | De | Até | Status ---
    col1, col2, col3, col4 = st.columns([1.5, 1.2, 1.2, 1.5])

    with col1:
        filtro_rapido = st.selectbox(
            "Filtrar Período",
            [
                "Período Personalizado",
                "Hoje",
                "Ontem",
                "Últimos 7 Dias",
                "Este Mês",
                "Últimos 30 Dias",
                "Este Ano"
            ],
            index=1,
            key="filtro_quick"
        )

    hoje = pd.Timestamp.now().date()
    data_min = df_full["date_adjusted"].dt.date.min()
    data_max = df_full["date_adjusted"].dt.date.max()
    
    if filtro_rapido == "Hoje":
        de = ate = min(hoje, data_max)
    elif filtro_rapido == "Ontem":
        de = ate = hoje - pd.Timedelta(days=1)
    elif filtro_rapido == "Últimos 7 Dias":
        de, ate = hoje - pd.Timedelta(days=7), hoje
    elif filtro_rapido == "Últimos 30 Dias":
        de, ate = hoje - pd.Timedelta(days=30), hoje
    elif filtro_rapido == "Este Mês":
        de, ate = hoje.replace(day=1), hoje
    elif filtro_rapido == "Este Ano":
        de, ate = hoje.replace(month=1, day=1), hoje
    else:
        de, ate = data_min, data_max
    
    custom = (filtro_rapido == "Período Personalizado")
    
    with col2:
        de = st.date_input("De", value=de, min_value=data_min, max_value=data_max, disabled=not custom, key="de_q")
    
    with col3:
        ate = st.date_input("Até", value=ate, min_value=data_min, max_value=data_max, disabled=not custom, key="ate_q")
    
    with col4:
        status_options = df_full["status"].dropna().unique().tolist()
        status_opcoes = ["Todos"] + status_options
        index_padrao = status_opcoes.index("Pago") if "Pago" in status_opcoes else 0
        status_selecionado = st.selectbox("Status", status_opcoes, index=index_padrao)
    
    # Aplica filtros finais
    df = df_full[
        (df_full["date_adjusted"].dt.date >= de) &
        (df_full["date_adjusted"].dt.date <= ate)
    ]
    if status_selecionado != "Todos":
        df = df[df["status"] == status_selecionado]

    
    # --- Filtros Avançados com checkbox dentro de Expander ---
    with st.expander("🔍 Filtros Avançados", expanded=False):
        # Atualiza as opções com base nos dados filtrados até aqui
        level1_opcoes = sorted(df["level1"].dropna().unique().tolist())
        st.markdown("**📂 Hierarquia 1**")
        col_l1 = st.columns(4)
        level1_selecionados = []
        for i, op in enumerate(level1_opcoes):
            if col_l1[i % 4].checkbox(op, key=f"level1_{op}"):
                level1_selecionados.append(op)
        if level1_selecionados:
            df = df[df["level1"].isin(level1_selecionados)]
    
        # Atualiza Level2 após Level1 aplicado
        level2_opcoes = sorted(df["level2"].dropna().unique().tolist())
        st.markdown("**📁 Hierarquia 2**")
        col_l2 = st.columns(4)
        level2_selecionados = []
        for i, op in enumerate(level2_opcoes):
            if col_l2[i % 4].checkbox(op, key=f"level2_{op}"):
                level2_selecionados.append(op)
        if level2_selecionados:
            df = df[df["level2"].isin(level2_selecionados)]
    
    # Verifica se há dados após os filtros
    if df.empty:
        st.warning("Nenhuma venda encontrada para os filtros selecionados.")
        st.stop()

    
    # Estilo customizado (CSS)
    st.markdown("""
        <style>
            .kpi-title {
                font-size: 15px;
                font-weight: 600;
                color: #000000;
                margin-bottom: 4px;
            }
            .kpi-value {
                font-size: 22px;
                font-weight: bold;
                color: #000000;
                line-height: 1.2;
                word-break: break-word;
            }
            .kpi-card {
                background-color: #ffffff;
                border-radius: 12px;
                padding: 16px 20px;
                margin: 5px;
                box-shadow: 0 2px 8px rgba(0,0,0,0.06);
                border-left: 5px solid #27ae60;
            }
        </style>
    """, unsafe_allow_html=True)
    
    # Função para renderizar KPI card em coluna
    def kpi_card(col, title, value):
        col.markdown(f"""
            <div class="kpi-card">
                <div class="kpi-title">{title}</div>
                <div class="kpi-value">{value}</div>
            </div>
        """, unsafe_allow_html=True)
    
    # Cálculos (ajustado)
    total_vendas        = len(df)
    total_valor         = df["total_amount"].sum()
    total_itens         = (df["quantity_sku"] * df["quantity"]).sum()
    ticket_venda        = total_valor / total_vendas if total_vendas else 0
    ticket_unidade      = total_valor / total_itens if total_itens else 0
    frete               = total_valor * 0.10
    taxa_mktplace       = df["ml_fee"].fillna(0).sum()
    cmv                 = ((df["quantity_sku"] * df["quantity"]) * df["custo_unitario"].fillna(0)).sum()
    margem_operacional  = total_valor - frete - taxa_mktplace - cmv
    sem_sku             = df["quantity_sku"].isnull().sum()

    
    pct = lambda val: f"<span style='font-size: 70%; color: #666; display: inline-block; margin-left: 6px;'>({val / total_valor * 100:.1f}%)</span>" if total_valor else "<span style='font-size: 70%'>(0%)</span>"

    
    # Bloco 1: Indicadores Financeiros
    st.markdown("### 💼 Indicadores Financeiros")
    row1 = st.columns(5)
    kpi_card(row1[0], "💰 Faturamento", format_currency(total_valor))
    kpi_card(row1[1], "🚚 Frete Estimado", f"{format_currency(frete)} {pct(frete)}")
    kpi_card(row1[2], "📉 Taxa Marketplace", f"{format_currency(taxa_mktplace)} {pct(taxa_mktplace)}")
    kpi_card(row1[3], "📦 CMV", f"{format_currency(cmv)} {pct(cmv)}")
    kpi_card(row1[4], "💵 Margem Operacional", f"{format_currency(margem_operacional)} {pct(margem_operacional)}")
    
    # Bloco 2: Indicadores de Vendas
    st.markdown("### 📊 Indicadores de Vendas")
    row2 = st.columns(5)
    kpi_card(row2[0], "🧾 Vendas Realizadas", str(total_vendas))
    kpi_card(row2[1], "📦 Unidades Vendidas", str(int(total_itens)))
    kpi_card(row2[2], "🎯 Tkt Médio p/ Venda", format_currency(ticket_venda))
    kpi_card(row2[3], "🎯 Tkt Médio p/ Unid.", format_currency(ticket_unidade))
    kpi_card(row2[4], "❌ SKU Incompleto", str(sem_sku))
    
    import plotly.express as px

    # =================== Gráfico de Linha + Barra de Proporção ===================
    st.markdown("### 💵 Total Vendido por Período")
    
    # 🔘 Seletor de período + agrupamento + métrica lado a lado
    colsel1, colsel2, colsel3 = st.columns([1.2, 1.2, 1.6])

    
    with colsel1:
        st.markdown("**📆 Período**")
        tipo_visualizacao = st.radio(
            label="",
            options=["Diário", "Semanal", "Quinzenal", "Mensal"],
            horizontal=True,
            key="periodo"
        )
    
    with colsel2:
        st.markdown("**👥 Agrupamento**")
        modo_agregacao = st.radio(
            label="",
            options=["Por Conta", "Total Geral"],
            horizontal=True,
            key="modo_agregacao"
        )

    with colsel3:
        st.markdown("**📏 Métrica da Barra**")
        metrica_barra = st.radio(
            "Métrica",
            ["Faturamento", "Qtd. Vendas", "Qtd. Unidades"],
            horizontal=True,
            key="metrica_barra"
        )


    
    df_plot = df.copy()
    
    # Define bucket de datas
    if de == ate:
        df_plot["date_bucket"] = df_plot["date_adjusted"].dt.floor("h")
        periodo_label = "Hora"
    else:
        if tipo_visualizacao == "Diário":
            df_plot["date_bucket"] = df_plot["date_adjusted"].dt.date
            periodo_label = "Dia"
        elif tipo_visualizacao == "Semanal":
            df_plot["date_bucket"] = df_plot["date_adjusted"].dt.to_period("W").apply(lambda p: p.start_time.date())
            periodo_label = "Semana"
        elif tipo_visualizacao == "Quinzenal":
            df_plot["quinzena"] = df_plot["date_adjusted"].apply(
                lambda d: f"{d.year}-Q{(d.month-1)*2//30 + 1}-{1 if d.day <= 15 else 2}"
            )
            df_plot["date_bucket"] = df_plot["quinzena"]
            periodo_label = "Quinzena"
        else:
            df_plot["date_bucket"] = df_plot["date_adjusted"].dt.to_period("M").astype(str)
            periodo_label = "Mês"
    
    # Agrupamento e definição de cores
    if modo_agregacao == "Por Conta":
        vendas_por_data = (
            df_plot.groupby(["date_bucket", "nickname"])["total_amount"]
            .sum()
            .reset_index(name="Valor Total")
        )
        color_dim = "nickname"
    
        total_por_conta = (
            df_plot.groupby("nickname")["total_amount"]
            .sum()
            .reset_index(name="total")
            .sort_values("total", ascending=False)
        )
    
        color_palette = px.colors.sequential.Agsunset
        nicknames = total_por_conta["nickname"].tolist()
        color_map = {nick: color_palette[i % len(color_palette)] for i, nick in enumerate(nicknames)}
    
    else:
        vendas_por_data = (
            df_plot.groupby("date_bucket")["total_amount"]
            .sum()
            .reset_index(name="Valor Total")
        )
        color_dim = None
        color_map = None  # Não será usado
        total_por_conta = None
    
    # 🔢 Gráfico(s)
    if modo_agregacao == "Por Conta":
        col1, col2 = st.columns([4, 1])
    else:
        col1 = st.container()
        col2 = None
    
    # 📈 Gráfico de Linha
    with col1:
        fig = px.line(
            vendas_por_data,
            x="date_bucket",
            y="Valor Total",
            color=color_dim,
            labels={"date_bucket": periodo_label, "Valor Total": "Valor Total", "nickname": "Conta"},
            color_discrete_map=color_map,
        )
        fig.update_traces(mode="lines+markers", marker=dict(size=5))
        fig.update_layout(
            margin=dict(t=20, b=20, l=40, r=10),
            showlegend=True
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # 📊 Gráfico de barra proporcional (somente se Por Conta)
    if modo_agregacao == "Por Conta" and not total_por_conta.empty:

    
        if metrica_barra == "Faturamento":
            base = (
                df_plot.groupby("nickname")["total_amount"]
                .sum()
                .reset_index(name="valor")
            )
        elif metrica_barra == "Qtd. Vendas":
            base = (
                df_plot.groupby("nickname")
                .size()
                .reset_index(name="valor")
            )
        else:  # Qtd. Unidades
            base = (
                df_plot.groupby("nickname")
                .apply(lambda x: (x["quantity_sku"] * x["quantity"]).sum())
                .reset_index(name="valor")
            )
    
        base = base.sort_values("valor", ascending=False)
        base["percentual"] = base["valor"] / base["valor"].sum()
    
        # 🏷️ Texto das barras
        def formatar_valor(v):
            if metrica_barra == "Faturamento":
                return f"R$ {v:,.0f}".replace(",", "v").replace(".", ",").replace("v", ".")
            elif metrica_barra == "Qtd. Vendas":
                return f"{int(v)} vendas"
            else:
                return f"{int(v)} unid."
    
        base["texto"] = base.apply(
            lambda row: f"{row['percentual']:.0%} ({formatar_valor(row['valor'])})", axis=1
        )
        base["grupo"] = "Contas"
    
        fig_bar = px.bar(
            base,
            x="grupo",
            y="percentual",
            color="nickname",
            text="texto",
            color_discrete_map=color_map,
        )
    
        fig_bar.update_layout(
            yaxis=dict(title=None, tickformat=".0%", range=[0, 1]),
            xaxis=dict(title=None),
            showlegend=False,
            margin=dict(t=20, b=20, l=10, r=10),
            height=400
        )
    
        fig_bar.update_traces(
            textposition="inside",
            insidetextanchor="middle",
            textfont=dict(color="white", size=12)
        )
    
        with col2:
            st.plotly_chart(fig_bar, use_container_width=True)




    # === Gráfico de barras: Média por dia da semana ===
    st.markdown('<div class="section-title">📅 Vendas por Dia da Semana</div>', unsafe_allow_html=True)
    
    # Nome dos dias na ordem certa
    dias = ["Segunda", "Terça", "Quarta", "Quinta", "Sexta", "Sábado", "Domingo"]
    
    # Extrai dia da semana em português
    df["dia_semana"] = df["date_adjusted"].dt.day_name().map({
        "Monday": "Segunda", "Tuesday": "Terça", "Wednesday": "Quarta",
        "Thursday": "Quinta", "Friday": "Sexta", "Saturday": "Sábado", "Sunday": "Domingo"
    })
    
    # Extrai a data (sem hora)
    df["data"] = df["date_adjusted"].dt.date
    
    # Soma o total vendido por dia (independente da hora)
    total_por_data = df.groupby(["dia_semana", "data"])["total_amount"].sum().reset_index()
    
    # Agora calcula a média por dia da semana
    media_por_dia = total_por_data.groupby("dia_semana")["total_amount"].mean().reindex(dias).reset_index()
    
    # Plota o gráfico de barras
    fig_bar = px.bar(
        media_por_dia,
        x="dia_semana",
        y="total_amount",
        text_auto=".2s",
        labels={"dia_semana": "Dia da Semana", "total_amount": "Média Vendida (R$)"},
        color_discrete_sequence=["#27ae60"]
    )
    
    st.plotly_chart(fig_bar, use_container_width=True, theme="streamlit")




    # =================== Gráfico de Linha - Faturamento Acumulado por Hora ===================
    st.markdown("### ⏰ Faturamento Acumulado por Hora do Dia (Média)")
    
    # Extrai hora e data
    df["hora"] = df["date_adjusted"].dt.hour
    df["data"] = df["date_adjusted"].dt.date
    
    # Soma o total vendido por hora e por dia
    vendas_por_dia_e_hora = df.groupby(["data", "hora"])["total_amount"].sum().reset_index()
    
    # Garante que todas as horas estejam presentes para todos os dias
    todos_dias = vendas_por_dia_e_hora["data"].unique()
    todas_horas = list(range(0, 24))
    malha_completa = pd.MultiIndex.from_product([todos_dias, todas_horas], names=["data", "hora"])
    vendas_completa = vendas_por_dia_e_hora.set_index(["data", "hora"]).reindex(malha_completa, fill_value=0).reset_index()
    
    # Acumula por hora dentro de cada dia
    vendas_completa["acumulado_dia"] = vendas_completa.groupby("data")["total_amount"].cumsum()
    
    # Agora calcula a média acumulada por hora (entre os dias)
    media_acumulada_por_hora = (
        vendas_completa
        .groupby("hora")["acumulado_dia"]
        .mean()
        .reset_index(name="Valor Médio Acumulado")
    )
    
    # Verifica se é filtro de hoje
    hoje = pd.Timestamp.now(tz="America/Sao_Paulo").date()
    filtro_hoje = (de == ate) and (de == hoje)
    
    if filtro_hoje:
        hora_atual = pd.Timestamp.now(tz="America/Sao_Paulo").hour
        df_hoje = df[df["data"] == hoje]
        vendas_hoje_por_hora = (
            df_hoje.groupby("hora")["total_amount"].sum().reindex(range(24), fill_value=0)
            .cumsum()
            .reset_index(name="Valor Médio Acumulado")
            .rename(columns={"index": "hora"})
        )
        # Traz o ponto até hora atual
        ponto_extra = pd.DataFrame([{
            "hora": hora_atual,
            "Valor Médio Acumulado": vendas_hoje_por_hora.loc[hora_atual, "Valor Médio Acumulado"]
        }])
        media_acumulada_por_hora = pd.concat([media_acumulada_por_hora, ponto_extra]).groupby("hora").last().reset_index()
    
    else:
        # Para histórico, adiciona o ponto final às 23h com média total diária
        media_final = df.groupby("data")["total_amount"].sum().mean()
        ponto_final = pd.DataFrame([{
            "hora": 23,
            "Valor Médio Acumulado": media_final
        }])
        media_acumulada_por_hora = pd.concat([media_acumulada_por_hora, ponto_final]).groupby("hora").last().reset_index()
    
    # Plota o gráfico
    fig_hora = px.line(
        media_acumulada_por_hora,
        x="hora",
        y="Valor Médio Acumulado",
        title="⏰ Faturamento Acumulado por Hora (Média por Dia)",
        labels={
            "hora": "Hora do Dia",
            "Valor Médio Acumulado": "Valor Acumulado (R$)"
        },
        color_discrete_sequence=["#27ae60"],
        markers=True
    )
    fig_hora.update_layout(xaxis=dict(dtick=1))
    
    st.plotly_chart(fig_hora, use_container_width=True)


def mostrar_contas_cadastradas():
    st.markdown(
        """
        <style>
        .block-container {
            padding-top: 0rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
    st.header("🏷️ Contas Cadastradas")
    render_add_account_button()

    df = pd.read_sql(text("SELECT ml_user_id, nickname, access_token, refresh_token FROM user_tokens ORDER BY nickname"), engine)

    if df.empty:
        st.warning("Nenhuma conta cadastrada.")
        return

    # --- Botões globais ---
    col_a, col_b = st.columns(2)
    
    with col_a:
        if st.button("🔄 Atualizar Vendas Recentes (Todas)", use_container_width=True):
            with st.spinner("🔄 Executando atualizações incrementais..."):
                for row in df.itertuples(index=False):
                    ml_user_id = str(row.ml_user_id)
                    access_token = row.access_token
                    nickname = row.nickname

                    st.subheader(f"🔗 Conta: {nickname}")
                    novas = get_incremental_sales(ml_user_id, access_token)
                    st.success(f"✅ {novas} novas vendas ou alterações recentes importadas.")

    with col_b:
        if st.button("♻️ Reprocessar Histórico Completo", use_container_width=True):
            with st.spinner("♻️ Atualizando histórico de todas as vendas..."):
                total = len(df)
                progresso = st.progress(0, text="🔁 Iniciando reprocessamento...")
                
                for i, row in enumerate(df.itertuples(index=False)):
                    ml_user_id = str(row.ml_user_id)
                    access_token = row.access_token
                    nickname = row.nickname
                
                    st.write(f"▶️ Processando conta {nickname}...")
                    atualizadas, _ = revisar_banco_de_dados(ml_user_id, access_token, return_changes=False)
                    st.info(f"♻️ {atualizadas} vendas atualizadas para a conta {nickname}.")
                    st.write(f"✅ Conta {nickname} finalizada.\n---")
                    
                    progresso.progress((i + 1) / total, text=f"⏳ {i + 1}/{total} contas processadas...")
                    time.sleep(0.1)
                
                st.success("✅ Reprocessamento completo!")
                progresso.empty()



                st.success("✅ Todos os status foram padronizados com sucesso.")
                    


    # --- Seção por conta individual ---
    for row in df.itertuples(index=False):
        with st.expander(f"🔗 Conta ML: {row.nickname}"):
            ml_user_id = str(row.ml_user_id)
            access_token = row.access_token
            refresh_token = row.refresh_token

            st.write(f"**User ID:** `{ml_user_id}`")
            st.write(f"**Access Token:** `{access_token}`")
            st.write(f"**Refresh Token:** `{refresh_token}`")

            col1, col2, col3 = st.columns(3)

            # Renovar Token
            with col1:
                if st.button("🔄 Renovar Token", key=f"renew_{ml_user_id}"):
                    try:
                        resp = requests.post(f"{BACKEND_URL}/auth/refresh", json={"user_id": ml_user_id})
                        if resp.ok:
                            data = resp.json()
                            salvar_tokens_no_banco(data)
                            st.success("✅ Token atualizado com sucesso!")
                        else:
                            st.error(f"❌ Erro ao atualizar o token: {resp.text}")
                    except Exception as e:
                        st.error(f"❌ Erro ao conectar com o servidor: {e}")

            # Processar Status (somente da conta)
            with col2:
                if st.button("♻️ Processar Status", key=f"status_{ml_user_id}"):
                    with st.spinner("♻️ Atualizando status das vendas..."):
                        atualizadas, _ = revisar_banco_de_dados(ml_user_id, access_token, return_changes=False)
                        st.info(f"♻️ {atualizadas} vendas com status alterados.")


def mostrar_anuncios():
    st.markdown(
        """
        <style>
        .block-container {
            padding-top: 0rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.header("🎯 Análise de Anúncios")
    df = carregar_vendas()

    if df.empty:
        st.warning("Nenhum dado para exibir.")
        return

    df['date_adjusted'] = pd.to_datetime(df['date_adjusted'])

    # ========== FILTROS ==========
    data_ini = st.date_input("De:",  value=df['date_adjusted'].min().date())
    data_fim = st.date_input("Até:", value=df['date_adjusted'].max().date())

    df_filt = df.loc[
    (df['date_adjusted'].dt.date >= data_ini) &
    (df['date_adjusted'].dt.date <= data_fim)
    ]

    if df_filt.empty:
        st.warning("Sem registros para os filtros escolhidos.")
        return

    title_col = 'item_title'
    faturamento_col = 'total_amount'

    # 1️⃣ Nuvem de Palavras
    st.subheader("1️⃣ 🔍 Nuvem de Palavras dos Títulos")
    text = " ".join(df_filt[title_col])
    wc = WordCloud(width=600, height=300, background_color="white").generate(text)
    c1, c2, c3 = st.columns([1, 2, 1])
    with c2:
        st.image(wc.to_array(), use_column_width=True)

    # 2️⃣ Top 10 Títulos por Faturamento
    st.subheader("2️⃣ 🌟 Top 10 Títulos por Faturamento")
    top10_df = (
        df_filt
        .groupby(title_col)[faturamento_col]
        .sum()
        .reset_index()
        .sort_values(by=faturamento_col, ascending=False)
        .head(10)
    )
    fig_top10 = px.bar(
        top10_df,
        x=title_col,
        y=faturamento_col,
        text_auto='.2s',
        labels={title_col: "Título", faturamento_col: "Faturamento (R$)"},
        color_discrete_sequence=["#1abc9c"]
    )
    st.plotly_chart(fig_top10, use_container_width=True)

    # 4️⃣ Faturamento por Palavra
    st.subheader("3️⃣ 🧠 Palavras que mais faturam nos Títulos")
    from collections import Counter
    word_faturamento = Counter()
    for _, row in df_filt.iterrows():
        palavras = str(row[title_col]).lower().split()
        for p in palavras:
            word_faturamento[p] += row[faturamento_col]

    df_words = pd.DataFrame(word_faturamento.items(), columns=['palavra', 'faturamento'])
    df_words = df_words.sort_values(by='faturamento', ascending=False).head(15)
    fig_words = px.bar(
        df_words,
        x='palavra',
        y='faturamento',
        text_auto='.2s',
        labels={'palavra': 'Palavra no Título', 'faturamento': 'Faturamento (R$)'},
        color_discrete_sequence=["#f39c12"]
    )
    st.plotly_chart(fig_words, use_container_width=True)

    # 5️⃣ Faturamento por Comprimento de Título
    st.subheader("4️⃣ 📏 Faturamento por Comprimento de Título (nº de palavras)")
    df['title_len'] = df[title_col].str.split().apply(len)
    df_len_fat = (
        df
        .groupby('title_len')[faturamento_col]
        .sum()
        .reset_index()
        .sort_values('title_len')
    )
    fig_len = px.bar(
        df_len_fat,
        x='title_len',
        y=faturamento_col,
        labels={'title_len': 'Nº de Palavras no Título', 'total_amount': 'Faturamento (R$)'},
        text_auto='.2s',
        color_discrete_sequence=["#9b59b6"]
    )
    st.plotly_chart(fig_len, use_container_width=True)

    # 6️⃣ Títulos com 0 vendas no período filtrado
    st.subheader("5️⃣ 🚨 Títulos sem Vendas no Período")
    df_sem_venda = (
        df_filt[df_filt['quantity'] == 0]
        .groupby(['item_id', 'item_title'])
        .agg(total_amount=('total_amount', 'sum'), quantidade=('quantity', 'sum'))
        .reset_index()
    )
    df_sem_venda['link'] = df_sem_venda['item_id'].apply(
        lambda x: f"https://www.mercadolivre.com.br/anuncio/{x}"
    )
    df_sem_venda['link'] = df_sem_venda['link'].apply(
        lambda url: f"[🔗 Ver Anúncio]({url})"
    )
    df_sem_venda['total_amount'] = df_sem_venda['total_amount'].apply(
        lambda x: f"R$ {x:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    )
    df_sem_venda['quantidade'] = df_sem_venda['quantidade'].astype(int)
    st.dataframe(df_sem_venda, use_container_width=True)

    # 7️⃣ Faturamento por item_id com link
    st.subheader("6️⃣ 📊 Faturamento por MLB (item_id, Título e Link)")

    df_mlb = (
        df_filt
        .groupby(['item_id', 'item_title'])[faturamento_col]
        .sum()
        .reset_index()
        .sort_values(by=faturamento_col, ascending=False)
    )
    df_mlb['link'] = df_mlb['item_id'].apply(
        lambda x: f"https://www.mercadolivre.com.br/anuncio/{x}"
    )
    df_mlb_display = df_mlb.copy()
    df_mlb_display['total_amount'] = df_mlb_display['total_amount'].apply(
        lambda x: f"R$ {x:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    )
    df_mlb_display['link'] = df_mlb_display['link'].apply(
        lambda url: f"[🔗 Ver Anúncio]({url})"
    )
    st.dataframe(df_mlb_display, use_container_width=True)

    # Exportação CSV (sem formatação)
    csv = df_mlb.to_csv(index=False).encode('utf-8')
    st.download_button(
        label="⬇️ Exportar CSV",
        data=csv,
        file_name="faturamento_por_mlb.csv",
        mime="text/csv"
    )

def mostrar_relatorios():
    # Remove o espaçamento superior
    st.markdown(
        """
        <style>
        .block-container {
            padding-top: 0rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.header("📋 Relatórios de Vendas")

    df = carregar_vendas()

    if df.empty:
        st.warning("Nenhum dado encontrado.")
        return

    df['date_adjusted'] = pd.to_datetime(df['date_adjusted'])

    # === Filtro Rápido ===
    col1, col2, col3 = st.columns(3)
    hoje = datetime.now().date()
    ultimos_7 = hoje - timedelta(days=6)
    ultimos_30 = hoje - timedelta(days=29)

    filtro_rapido = col1.selectbox("📅 Período rápido:", ["Hoje", "Últimos 7 dias", "Últimos 30 dias", "Personalizado"])

    if filtro_rapido == "Hoje":
        data_ini = data_fim = hoje
    elif filtro_rapido == "Últimos 7 dias":
        data_ini, data_fim = ultimos_7, hoje
    elif filtro_rapido == "Últimos 30 dias":
        data_ini, data_fim = ultimos_30, hoje
    else:
        data_ini = col2.date_input("De:", value=df['date_adjusted'].min().date())
        data_fim = col3.date_input("Até:", value=df['date_adjusted'].max().date())

    df_filt = df.loc[
        (df['date_adjusted'].dt.date >= data_ini) & 
        (df['date_adjusted'].dt.date <= data_fim)
    ]

    if df_filt.empty:
        st.warning("Nenhuma venda no período selecionado.")
        return

    # === Preparar dados para exibição ===
    df_filt = df_filt.copy()
    df_filt['Quantidade'] = df_filt['quantity'] * df_filt['quantity_sku'].fillna(1)

    df_filt['Preço Unitário'] = df_filt['unit_price'].apply(
        lambda x: f"R$ {x:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    )
    df_filt['Total da Venda'] = df_filt['total_amount'].apply(
        lambda x: f"R$ {x:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    )
    df_filt['Data da Venda'] = df_filt['date_adjusted'].dt.strftime('%d/%m/%Y')

    df_filt['link'] = df_filt['item_id'].apply(
        lambda x: f"[🔗 Ver Anúncio](https://www.mercadolivre.com.br/anuncio/{x})"
    )

    # Ordenar por data desc
    df_filt = df_filt.sort_values("date_adjusted", ascending=False)

    # Reorganizar e renomear colunas
    df_exibir = df_filt.rename(columns={
        "order_id": "ID da Venda",
        "item_id": "MLB",
        "item_title": "Título do Anúncio"
    })[[
        "ID da Venda",
        "Data da Venda",
        "MLB",
        "Título do Anúncio",
        "Quantidade",
        "Preço Unitário",
        "Total da Venda",
        "link"
    ]]

    st.dataframe(df_exibir, use_container_width=True)

    # Exportação CSV (valores crus, sem formatação)
    df_exportar = df_filt[[
        "order_id", "date_adjusted", "item_id", "item_title",
        "quantity", "quantity_sku", "unit_price", "total_amount"
    ]]
    df_exportar["quantidade_total"] = df_exportar["quantity"] * df_exportar["quantity_sku"].fillna(1)

    csv = df_exportar.to_csv(index=False).encode('utf-8')
    st.download_button(
        label="⬇️ Exportar CSV das Vendas",
        data=csv,
        file_name="relatorio_vendas.csv",
        mime="text/csv"
    )



def mostrar_gestao_sku():
    st.markdown("""
        <style>
        .block-container {
            padding-top: 0rem;
        }
        </style>
    """, unsafe_allow_html=True)

    st.header("📦 Gestão de SKU")

    if st.button("🔄 Recarregar Dados"):
        st.session_state["atualizar_gestao_sku"] = True

    # === Consulta de SKUs únicos ===
    if st.session_state.get("atualizar_gestao_sku", False) or "df_gestao_sku" not in st.session_state:
        df = pd.read_sql(text("""
            SELECT
                seller_sku,
                MAX(level1) AS level1,
                MAX(level2) AS level2,
                MAX(custo_unitario) AS custo_unitario,
                MAX(quantity_sku) AS quantity_sku,
                COUNT(DISTINCT item_id) AS qtde_vendas
            FROM sales
            WHERE seller_sku IS NOT NULL
            GROUP BY seller_sku
        """), engine)
        st.session_state["df_gestao_sku"] = df
        st.session_state["atualizar_gestao_sku"] = False
    else:
        df = st.session_state["df_gestao_sku"]

    # === Métricas ===
    with engine.begin() as conn:
        vendas_sem_sku = conn.execute(text("SELECT COUNT(*) FROM sales WHERE seller_sku IS NULL")).scalar()
        mlbs_sem_sku = conn.execute(text("SELECT COUNT(DISTINCT item_id) FROM sales WHERE seller_sku IS NULL")).scalar()
        sku_incompleto = conn.execute(text("""
            SELECT COUNT(DISTINCT seller_sku)
            FROM sales
            WHERE seller_sku IS NOT NULL AND (
                level1 IS NULL OR level2 IS NULL OR custo_unitario IS NULL OR quantity_sku IS NULL
            )
        """)).scalar()

    col1, col2, col3 = st.columns(3)
    col1.metric("🚫 Vendas sem SKU", vendas_sem_sku)
    col2.metric("📦 MLBs sem SKU", mlbs_sem_sku)
    col3.metric("⚠️ SKUs com Cadastro Incompleto", sku_incompleto)

    st.markdown("---")
    st.markdown("### 🔍 Filtros de Diagnóstico")

    # === Filtros ===
    colf1, colf2, colf3, colf4, colf5 = st.columns([1.2, 1.2, 1.2, 1.2, 2])
    op_sku     = colf1.selectbox("Seller SKU", ["Todos", "Nulo", "Não Nulo"])
    op_level1  = colf2.selectbox("Hierarquia 1", ["Todos", "Nulo", "Não Nulo"])
    op_level2  = colf3.selectbox("Hierarquia ", ["Todos", "Nulo", "Não Nulo"])
    op_preco   = colf4.selectbox("Preço Unitário", ["Todos", "Nulo", "Não Nulo"])
    filtro_txt = colf5.text_input("🔎 Pesquisa (SKU, Hierarquias)")

    # === Aplicar filtros ===
    if op_sku == "Nulo":
        df = df[df["seller_sku"].isna()]
    elif op_sku == "Não Nulo":
        df = df[df["seller_sku"].notna()]
    if op_level1 == "Nulo":
        df = df[df["level1"].isna()]
    elif op_level1 == "Não Nulo":
        df = df[df["level1"].notna()]
    if op_level2 == "Nulo":
        df = df[df["level2"].isna()]
    elif op_level2 == "Não Nulo":
        df = df[df["level2"].notna()]
    if op_preco == "Nulo":
        df = df[df["custo_unitario"].isna()]
    elif op_preco == "Não Nulo":
        df = df[df["custo_unitario"].notna()]
    if filtro_txt:
        filtro_txt = filtro_txt.lower()
        df = df[df.apply(lambda row: filtro_txt in str(row["seller_sku"]).lower()
                         or filtro_txt in str(row["level1"]).lower()
                         or filtro_txt in str(row["level2"]).lower(), axis=1)]

    # === Tabela editável ===
    st.markdown("### 📝 Editar Cadastro de SKUs")

    colunas_editaveis = ["level1", "level2", "custo_unitario", "quantity_sku"]

    df_editado = st.data_editor(
        df,
        use_container_width=True,
        disabled=[col for col in df.columns if col not in colunas_editaveis],
        num_rows="dynamic",
        key="editor_sku"
    )

    # === Salvar alterações ===
    if st.button("💾 Salvar Alterações"):
        try:
            with engine.begin() as conn:
                for _, row in df_editado.iterrows():
                    conn.execute(text("""
                        INSERT INTO sku (sku, level1, level2, custo_unitario, quantity, date_created)
                        VALUES (:sku, :level1, :level2, :custo, :quantidade, NOW())
                        ON CONFLICT (sku) DO UPDATE
                        SET
                            level1 = EXCLUDED.level1,
                            level2 = EXCLUDED.level2,
                            custo_unitario = EXCLUDED.custo_unitario,
                            quantity = EXCLUDED.quantity
                    """), {
                        "sku": row["seller_sku"],
                        "level1": row["level1"],
                        "level2": row["level2"],
                        "custo": row["custo_unitario"],
                        "quantidade": row["quantity_sku"]
                    })

                conn.execute(text("""
                    UPDATE sales s
                    SET
                        level1 = sku.level1,
                        level2 = sku.level2,
                        custo_unitario = sku.custo_unitario,
                        quantity_sku = sku.quantity
                    FROM (
                        SELECT DISTINCT ON (sku) * FROM sku
                        ORDER BY sku, date_created DESC
                    ) sku
                    WHERE s.seller_sku = sku.sku
                """))

            st.success("✅ Alterações salvas com sucesso!")
            st.session_state["atualizar_gestao_sku"] = True
            st.rerun()

        except Exception as e:
            st.error(f"❌ Erro ao salvar alterações: {e}")



    # 5️⃣ Atualização da base SKU via planilha
    st.markdown("---")
    st.markdown("### 📥 Atualizar Base de SKUs via Planilha")

    modelo = pd.DataFrame(columns=["seller_sku", "level1", "level2", "custo_unitario", "quantity"])
    buffer = io.BytesIO()
    modelo.to_excel(buffer, index=False, engine="openpyxl")
    st.download_button(
        label="⬇️ Baixar Modelo Excel de SKUs",
        data=buffer.getvalue(),
        file_name="modelo_sku.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

    arquivo = st.file_uploader("Selecione um arquivo Excel (.xlsx)", type=["xlsx"])
    if arquivo is not None:
        df_novo = pd.read_excel(arquivo)
        colunas_esperadas = {"seller_sku", "level1", "level2", "custo_unitario", "quantity"}
        if not colunas_esperadas.issubset(df_novo.columns):
            st.error("❌ A planilha deve conter: seller_sku, level1, level2, custo_unitario, quantity.")
        else:
            if st.button("✅ Processar Planilha e Atualizar"):
                try:
                    df_novo["quantity"] = df_novo["quantity"].fillna(0).astype(int)
                    df_novo["custo_unitario"] = df_novo["custo_unitario"].fillna(0).astype(float)
                    df_novo["seller_sku"] = df_novo["seller_sku"].astype(str).str.strip()
                    df_novo["level1"] = df_novo["level1"].astype(str).str.strip()
                    df_novo["level2"] = df_novo["level2"].astype(str).str.strip()

                    with engine.begin() as conn:
                        for _, row in df_novo.iterrows():
                            row_dict = row.to_dict()
                            result = conn.execute(text("""
                                SELECT 1 FROM sku
                                WHERE sku = :seller_sku
                                  AND TRIM(level1) = :level1
                                  AND TRIM(level2) = :level2
                                  AND ROUND(CAST(custo_unitario AS numeric), 2) = ROUND(CAST(:custo_unitario AS numeric), 2)
                                  AND quantity = :quantity
                                LIMIT 1
                            """), row_dict).fetchone()

                            if result is None:
                                conn.execute(text("""
                                    INSERT INTO sku (sku, level1, level2, custo_unitario, quantity, date_created)
                                    VALUES (:seller_sku, :level1, :level2, :custo_unitario, :quantity, NOW())
                                """), row_dict)

                        # Atualizar tabela de vendas
                        conn.execute(text("""
                            UPDATE sales s
                            SET
                                level1 = sku.level1,
                                level2 = sku.level2,
                                custo_unitario = sku.custo_unitario,
                                quantity_sku = sku.quantity
                            FROM (
                                SELECT DISTINCT ON (sku) *
                                FROM sku
                                ORDER BY sku, date_created DESC
                            ) sku
                            WHERE s.seller_sku = sku.sku
                        """))

                    # Recarregar métricas e dados
                    st.session_state["atualizar_gestao_sku"] = True
                    st.success("✅ Planilha importada, vendas atualizadas, métricas e tabela recarregadas!")
                    st.rerun()

                except Exception as e:
                    st.error(f"❌ Erro ao processar: {e}")

def mostrar_expedicao_logistica(df: pd.DataFrame):
    import streamlit as st
    import plotly.express as px
    import pandas as pd
    from io import BytesIO
    import base64
    from datetime import datetime
    from reportlab.platypus import (
        SimpleDocTemplate, Paragraph, Spacer, Image, Table, TableStyle, PageBreak
    )
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.enums import TA_CENTER
    from reportlab.lib import colors
    import pytz
    from sales import traduzir_status

    # Estilo
    st.markdown(  
        """
        <style>
        .block-container { padding-top: 0rem; }
        </style>
        """, unsafe_allow_html=True
    )
    st.header("🚚 Expedição e Logística")

    if df.empty:
        st.warning("Nenhum dado encontrado.")
        return

    # === Mapeamentos e cálculos iniciais ===
    def mapear_tipo(valor):
        match valor:
            case 'fulfillment': return 'FULL'
            case 'self_service': return 'FLEX'
            case 'drop_off': return 'Correios'
            case 'xd_drop_off': return 'Agência'
            case 'cross_docking': return 'Coleta'
            case 'me2': return 'Envio Padrão'
            case _: return 'outros'

    df["Tipo de Envio"] = df["shipment_logistic_type"].apply(mapear_tipo)

    # Garantir que 'shipment_delivery_sla' esteja em datetime
    if "shipment_delivery_sla" in df.columns and not pd.api.types.is_datetime64_any_dtype(df["shipment_delivery_sla"]):
        df["shipment_delivery_sla"] = pd.to_datetime(df["shipment_delivery_sla"], errors="coerce")

    # Cálculo de quantidade
    if "quantity" in df.columns and "quantity_sku" in df.columns:
        df["quantidade"] = df["quantity"] * df["quantity_sku"]
    else:
        st.error("Colunas 'quantity' e/ou 'quantity_sku' não encontradas.")
        st.stop()

    # Data da venda
    if "date_adjusted" not in df.columns:
        st.error("Coluna 'date_adjusted' não encontrada.")
        st.stop()
    df["data_venda"] = pd.to_datetime(df["date_adjusted"]).dt.date

    # Conversão para fuso de SP
    def _to_sp_date(x):
        if pd.isna(x):
            return pd.NaT
        ts = pd.to_datetime(x, utc=True)
        return ts.tz_convert("America/Sao_Paulo").date()

    if "shipment_delivery_sla" in df.columns:
        df["shipment_delivery_sla"] = pd.to_datetime(df["shipment_delivery_sla"], utc=True, errors="coerce")
        df["data_limite"] = df["shipment_delivery_sla"].apply(
            lambda x: x.tz_convert("America/Sao_Paulo").date() if pd.notnull(x) else pd.NaT
        )
    else:
        df["data_limite"] = pd.NaT

    hoje = pd.Timestamp.now().date()
    data_min_venda = df["data_venda"].dropna().min()
    data_max_venda = df["data_venda"].dropna().max()

    data_min_limite = df["data_limite"].dropna().min()
    data_max_limite = df["data_limite"].dropna().max()
    if pd.isna(data_min_limite):
        data_min_limite = hoje
    if pd.isna(data_max_limite) or data_max_limite < data_min_limite:
        data_max_limite = data_min_limite + pd.Timedelta(days=7)

    # === UNIFICADO 1: Datas (Venda + Expedição) + Filtro de Período (só Expedição) ===
    
    # --- Linha 1: Período + Despacho Limite ---
    col1, col2, col3 = st.columns([1.5, 1.2, 1.2])
    
    with col1:
        periodo = st.selectbox(
            "Filtrar Período de Expedição",
            [
                "Período Personalizado",
                "Hoje",
                "Amanhã",
                "Ontem",
                "Próximos 7 Dias",
                "Este Mês",
                "Próximos 30 Dias",
                "Este Ano"
            ],
            index=1,
            key="filtro_expedicao_periodo"
        )
    
    # Define intervalo padrão com base no filtro
    hoje = pd.Timestamp.now().date()
    if periodo == "Hoje":
        de_limite_default = ate_limite_default = min(hoje, data_max_limite)
    elif periodo == "Amanhã":
        de_limite_default = ate_limite_default = hoje + pd.Timedelta(days=1)
    elif periodo == "Ontem":
        de_limite_default = ate_limite_default = hoje - pd.Timedelta(days=1)
    elif periodo == "Próximos 7 Dias":
        de_limite_default, ate_limite_default = hoje, hoje + pd.Timedelta(days=6)
    elif periodo == "Próximos 30 Dias":
        de_limite_default, ate_limite_default = hoje, hoje + pd.Timedelta(days=29)
    elif periodo == "Este Mês":
        de_limite_default, ate_limite_default = hoje.replace(day=1), hoje
    elif periodo == "Este Ano":
        de_limite_default, ate_limite_default = hoje.replace(month=1, day=1), hoje
    else:
        de_limite_default, ate_limite_default = data_min_limite, data_max_limite
    
    modo_personalizado = (periodo == "Período Personalizado")
    
    with col2:
        de_limite = st.date_input(
            "Despacho Limite de:",
            value=de_limite_default,
            min_value=data_min_limite,
            max_value=data_max_limite,
            disabled=not modo_personalizado
        )
    with col3:
        ate_limite = st.date_input(
            "Despacho Limite até:",
            value=ate_limite_default,
            min_value=data_min_limite,
            max_value=data_max_limite,
            disabled=not modo_personalizado
        )
    
    if not modo_personalizado:
        de_limite = de_limite_default
        ate_limite = ate_limite_default
    
    # --- Linha 2: Venda de / até ---
    col_v1, col_v2 = st.columns(2)
    
    with col_v1:
        de_venda = st.date_input(
            "Venda de:",
            value=data_min_venda,
            min_value=data_min_venda,
            max_value=data_max_venda,
            key="data_venda_de"
        )
    with col_v2:
        ate_venda = st.date_input(
            "Venda até:",
            value=data_max_venda,
            min_value=data_min_venda,
            max_value=data_max_venda,
            key="data_venda_ate"
        )

    # --- Aplicar filtro por data de venda e expedição no DataFrame base ---
    df = df[
        (df["data_venda"] >= de_venda) & (df["data_venda"] <= ate_venda) &
        (df["data_limite"].isna() |
         ((df["data_limite"] >= de_limite) & (df["data_limite"] <= ate_limite)))
    ]

    df_filtrado = df.copy()
    
    # --- Linha 3: Conta, Status, Status Envio, Tipo de Envio ---
    col6, col7, col8, col11 = st.columns(4)
    
    with col6:
        contas = df["nickname"].dropna().unique().tolist()
        conta = st.selectbox("Conta", ["Todos"] + sorted(contas))
    
    with col7:
        status_traduzido = sorted(df["status"].dropna().unique().tolist())
        status_ops = ["Todos"] + status_traduzido
        index_padrao = status_ops.index("Pago") if "Pago" in status_ops else 0
        status = st.selectbox("Status", status_ops, index=index_padrao)
    
    with col8:
        status_data_envio = st.selectbox(
            "Status Envio",
            ["Todos", "Com Data de Envio", "Sem Data de Envio"],
            index=1
        )
    
    with col11:
        tipo_envio = st.selectbox(
            "Tipo de Envio",
            ["Todos"] + sorted(df["Tipo de Envio"].dropna().unique().tolist())
        )

    # --- Aplicar filtros restantes ---

    if conta != "Todos":
        df_filtrado = df_filtrado[df_filtrado["nickname"] == conta]
    if status != "Todos":
        df_filtrado = df_filtrado[df_filtrado["status"] == status]
    if status_data_envio == "Com Data de Envio":
        df_filtrado = df_filtrado[df_filtrado["data_limite"].notna()]
    elif status_data_envio == "Sem Data de Envio":
        df_filtrado = df_filtrado[df_filtrado["data_limite"].isna()]
    if tipo_envio != "Todos":
        df_filtrado = df_filtrado[df_filtrado["Tipo de Envio"] == tipo_envio]
    
    
    # Aqui entra o bloco com os filtros de hierarquia
    with st.expander("🔍 Filtros Avançados por Hierarquia", expanded=False):
        # Hierarquia 1
        level1_opcoes = sorted(df_filtrado["level1"].dropna().unique().tolist())
        st.markdown("**📂 Hierarquia 1**")
        col_l1 = st.columns(4)
        level1_selecionados = []
        for i, op in enumerate(level1_opcoes):
            if col_l1[i % 4].checkbox(op, key=f"filtros_level1_{op}"):
                level1_selecionados.append(op)
        if level1_selecionados:
            df_filtrado = df_filtrado[df_filtrado["level1"].isin(level1_selecionados)]
    
        # Hierarquia 2
        level2_opcoes = sorted(df_filtrado["level2"].dropna().unique().tolist())
        st.markdown("**📁 Hierarquia 2**")
        col_l2 = st.columns(4)
        level2_selecionados = []
        for i, op in enumerate(level2_opcoes):
            if col_l2[i % 4].checkbox(op, key=f"filtros_level2_{op}"):
                level2_selecionados.append(op)
        if level2_selecionados:
            df_filtrado = df_filtrado[df_filtrado["level2"].isin(level2_selecionados)]


    # Verificação final
    if df_filtrado.empty:
        st.warning("Nenhum dado encontrado com os filtros aplicados.")
        return


    df_filtrado = df_filtrado.copy()
    df_filtrado["Canal de Venda"] = "MERCADO LIVRE"
    
    df_filtrado["Data Limite do Envio"] = df_filtrado["data_limite"].apply(
        lambda d: d.strftime("%d/%m/%Y") if pd.notna(d) else "—"
    )


    tabela = df_filtrado[[
        "order_id",                  
        "shipment_receiver_name",    
        "nickname",                  
        "Tipo de Envio",            
        "quantidade",              
        "level1",                    
        "Data Limite do Envio"     
    ]].rename(columns={
        "order_id": "ID VENDA",
        "shipment_receiver_name": "NOME CLIENTE",
        "nickname": "CONTA",
        "Tipo de Envio": "TIPO DE ENVIO",
        "quantidade": "QUANTIDADE",
        "level1": "PRODUTO [HIERARQUIA 1]",
        "Data Limite do Envio": "DATA DE ENVIO"
    })

    
    # Ordenar pela quantidade em ordem decrescente
    tabela = tabela.sort_values(by="QUANTIDADE", ascending=False)

    st.markdown("### 📋 Tabela de Expedição por Venda")
    st.dataframe(tabela, use_container_width=True, height=500)

    df_grouped = df_filtrado.groupby("level1", as_index=False).agg({"quantidade": "sum"})
    df_grouped = df_grouped.rename(columns={"level1": "Hierarquia 1", "quantidade": "Quantidade"})
    
    # Ordenar do maior para o menor
    df_grouped = df_grouped.sort_values(by="Quantidade", ascending=False)
    
    fig_bar = px.bar(
        df_grouped,
        x="Hierarquia 1",
        y="Quantidade",
        text="Quantidade",  # Adiciona o rótulo
        barmode="group",
        height=400,
        color_discrete_sequence=["#2ECC71"]
    )
    
    # Ajustar posição dos rótulos (em cima)
    fig_bar.update_traces(textposition="outside")
    
    # Ajustar layout para não cortar os rótulos
    fig_bar.update_layout(uniformtext_minsize=8, uniformtext_mode='hide', margin=dict(t=40, b=40))
    
    st.plotly_chart(fig_bar, use_container_width=True)


    # === TABELAS LADO A LADO ===
    st.markdown("### 📊 Resumo por Agrupamento")
    
    col_r1, col_r2, col_r3 = st.columns(3)

    
    # Tabela 1: Hierarquia 1
    with col_r1:
        df_h1 = df_filtrado.groupby("level1", as_index=False)["quantidade"].sum().rename(columns={
            "level1": "Hierarquia 1", "quantidade": "Quantidade"
        })
        st.dataframe(df_h1, use_container_width=True, hide_index=True)
    
    # Tabela 2: Hierarquia 2
    with col_r2:
        df_h2 = df_filtrado.groupby("level2", as_index=False)["quantidade"].sum().rename(columns={
            "level2": "Hierarquia 2", "quantidade": "Quantidade"
        })
        st.dataframe(df_h2, use_container_width=True, hide_index=True)
    
    # Tabela 3: Tipo de Envio
    with col_r3:
        df_tipo = df_filtrado.groupby("Tipo de Envio", as_index=False)["quantidade"].sum().rename(columns={
            "Tipo de Envio": "Tipo de Envio", "quantidade": "Quantidade"
        })
        st.dataframe(df_tipo, use_container_width=True, hide_index=True)


    def gerar_relatorio_pdf(
        tabela_df, df_h1, df_h2, df_tipo,
        periodo_venda, periodo_expedicao
    ):
        buffer = BytesIO()
        doc = SimpleDocTemplate(buffer, pagesize=A4,
                                leftMargin=20, rightMargin=20,
                                topMargin=20, bottomMargin=20)
        styles = getSampleStyleSheet()
        title_style = ParagraphStyle(
            name="CenteredTitle",
            parent=styles["Title"],
            alignment=TA_CENTER,
            fontSize=16
        )
        normal = styles["Normal"]
        elems = []

        # cabeçalho
        try:
            logo = Image("favicon.png", width=50, height=50)
        except:
            logo = Paragraph("", normal)
        titulo = Paragraph("Relatório de Expedição e Logística", title_style)
        header = Table([[logo, titulo]], colWidths=[60, 460])
        header.setStyle(TableStyle([
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ("ALIGN",  (1, 0), (1, 0), "CENTER"),
            ("LEFTPADDING",  (0,0), (-1,-1), 0),
            ("RIGHTPADDING", (0,0), (-1,-1), 0),
        ]))
        elems.append(header)
        elems.append(Spacer(1, 8))

        # períodos
        txt = (
            f"<b>Venda:</b> {periodo_venda[0].strftime('%d/%m/%Y')} ↔ {periodo_venda[1].strftime('%d/%m/%Y')}<br/>"
            f"<b>Expedição:</b> {periodo_expedicao[0].strftime('%d/%m/%Y')} ↔ {periodo_expedicao[1].strftime('%d/%m/%Y')}"
        )
        elems.append(Paragraph(txt, normal))
        elems.append(Spacer(1, 12))

        # tabela principal
        main = tabela_df.copy()
        main["QUANTIDADE"] = main["QUANTIDADE"].astype(int)
        data = [main.columns.tolist()] + main.values.tolist()
        tab = Table(data, repeatRows=1, splitByRow=1)
        tab.setStyle(TableStyle([
            ("BACKGROUND",   (0, 0), (-1, 0), colors.lightgrey),
            ("TEXTCOLOR",    (0, 0), (-1, 0), colors.black),
            ("ALIGN",        (0, 0), (-1, -1), "CENTER"),
            ("FONTSIZE",     (0, 0), (-1, -1), 6),
            ("BOTTOMPADDING",(0, 0), (-1, 0), 6),
            ("GRID",         (0, 0), (-1, -1), 0.25, colors.grey),
        ]))
        elems.append(tab)
        elems.append(PageBreak())

        # resumos
        def resume(df, title):
            d = df.copy()
            d["Quantidade"] = d["Quantidade"].astype(int)
            t = Table([d.columns.tolist()] + d.values.tolist(), repeatRows=1)
            t.setStyle(TableStyle([
                ("BACKGROUND", (0,0), (-1,0), colors.lightgrey),
                ("ALIGN",      (0,0), (-1,-1), "CENTER"),
                ("FONTSIZE",   (0,0), (-1,-1), 6),
                ("GRID",       (0,0), (-1,-1), 0.25, colors.grey),
            ]))
            return [Paragraph(title, styles["Heading3"]), Spacer(1, 4), t]

        e1 = resume(df_h1, "Hierarquia 1")
        e2 = resume(df_h2, "Hierarquia 2")
        e3 = resume(df_tipo, "Tipo de Envio")

        # --- Ajuste dinâmico de largura para não sobrepor ---
        page_w, _ = A4
        usable_w = page_w - doc.leftMargin - doc.rightMargin
        col_w = usable_w / 3

        sum_table = Table(
            [[e1, e2, e3]],
            colWidths=[col_w, col_w, col_w]
        )
        sum_table.setStyle(TableStyle([
            ("VALIGN",      (0, 0), (-1, -1), "TOP"),
            ("LEFTPADDING", (0, 0), (-1, -1), 4),
            ("RIGHTPADDING",(0, 0), (-1, -1), 4),
        ]))
        elems.append(sum_table)

        doc.build(elems)

        # PDF link
        pdf_b64 = base64.b64encode(buffer.getvalue()).decode()
        href_pdf = (
            f'<a style="margin-right:20px;" '
            f'href="data:application/pdf;base64,{pdf_b64}" '
            f'download="relatorio_expedicao.pdf">📄 Baixar PDF</a>'
        )

        # XLSX link
        xlsx_buf = BytesIO()
        with pd.ExcelWriter(xlsx_buf, engine="openpyxl") as w:
            main.to_excel(w, index=False, sheet_name="Dados")
            df_h1.to_excel(w, index=False, sheet_name="Hierarquia_1")
            df_h2.to_excel(w, index=False, sheet_name="Hierarquia_2")
            df_tipo.to_excel(w, index=False, sheet_name="Tipo_Envio")
        xlsx_b64 = base64.b64encode(xlsx_buf.getvalue()).decode()
        href_xlsx = (
            f'<a href="data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64,{xlsx_b64}" '
            f'download="relatorio_expedicao.xlsx">⬇️ Baixar Excel</a>'
        )

        return href_pdf + href_xlsx

    # chamada única dos downloads
    periodo_venda     = (de_venda, ate_venda)
    periodo_expedicao = (de_limite, ate_limite)
    botoes = gerar_relatorio_pdf(
        tabela, df_h1, df_h2, df_tipo,
        periodo_venda, periodo_expedicao
    )
    st.markdown(botoes, unsafe_allow_html=True)

def mostrar_gestao_despesas():
    st.markdown(
        """
        <style>
        .block-container {
            padding-top: 0rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
    st.header("💰 Gestão de Despesas")
    st.info("Em breve...")

def mostrar_painel_metas():
    st.markdown(
        """
        <style>
        .block-container {
            padding-top: 0rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
    st.header("🎯 Painel de Metas")
    st.info("Em breve...")
    

# ----------------- Fluxo Principal -----------------
if "code" in st.query_params:
    ml_callback()

df_vendas = carregar_vendas()

pagina = render_sidebar()
if pagina == "Dashboard":
    mostrar_dashboard()
elif pagina == "Contas Cadastradas":
    mostrar_contas_cadastradas()
elif pagina == "Relatórios":
    mostrar_relatorios()
elif pagina == "Expedição":
    mostrar_expedicao_logistica(df_vendas)
elif pagina == "Gestão de SKU":
    mostrar_gestao_sku()
elif pagina == "Gestão de Despesas":
    mostrar_gestao_despesas()
elif pagina == "Painel de Metas":
    mostrar_painel_metas()
elif pagina == "Gestão de Anúncios":
    mostrar_anuncios()
elif pagina == "Configurações":
    mostrar_configuracoes()
