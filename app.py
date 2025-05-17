import os
import streamlit as st
import pandas as pd
import plotly.express as px
import requests
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import locale
from streamlit_option_menu import option_menu
from typing import Optional
from sales import sync_all_accounts

# Tenta configurar locale pt_BR; guarda se deu certo
try:
    locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')
    LOCALE_OK = True
except locale.Error:
    LOCALE_OK = False

def format_currency(valor: float) -> str:
    """
    Formata um float como BRL:
    - Usa locale se LOCALE_OK for True;
    - Senão, faz um fallback manual 'R$ 1.234,56'.
    """
    if LOCALE_OK:
        try:
            return locale.currency(valor, grouping=True)
        except Exception:
            pass
    # Fallback manual:
    inteiro, frac = f"{valor:,.2f}".split('.')
    inteiro = inteiro.replace(',', '.')
    return f"R$ {inteiro},{frac}"

# ----------------- Configuração da Página -----------------
st.set_page_config(
    page_title="Sistema de Gestão - NEXUS QA",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ----------------- Autenticação -----------------
if "authenticated" not in st.session_state:
    st.session_state["authenticated"] = False

params = st.query_params
# login automático via ?nexus_auth=success
if params.get("nexus_auth", [None])[0] == "success":
    st.session_state["authenticated"] = False
    sync_all_accounts()
    st.cache_data.clear()
    st.experimental_set_query_params()

if not st.session_state["authenticated"]:
    st.title("Sistema de Gestão - Grupo Nexus")
    username = st.text_input("Usuário")
    password = st.text_input("Senha", type="password")
    if st.button("Entrar"):
        if username == "." and password == ".":
            st.session_state["authenticated"] = True
            sync_all_accounts()
            st.cache_data.clear()
            st.rerun()
        else:
            st.error("Credenciais inválidas")
    st.stop()

# ----------------- Título -----------------
st.title("Dashboard")

# ----------------- Variáveis de Ambiente -----------------
load_dotenv()
BACKEND_URL  = os.getenv("BACKEND_URL")
FRONTEND_URL = os.getenv("FRONTEND_URL")
DB_URL       = os.getenv("DB_URL")
ML_CLIENT_ID = os.getenv("ML_CLIENT_ID")

if not all([BACKEND_URL, FRONTEND_URL, DB_URL, ML_CLIENT_ID]):
    st.error("❌ Defina BACKEND_URL, FRONTEND_URL, DB_URL e ML_CLIENT_ID em seu .env")
    st.stop()

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

# ----------------- Banco de Dados -----------------
engine = create_engine(
    DB_URL,
    pool_size=5,
    max_overflow=10,
    pool_timeout=30
)

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
        # Verifica se é um nickname e faz a conversão
        ml_user_id = pd.read_sql(text("SELECT ml_user_id FROM user_tokens WHERE nickname = :nickname"), 
                                 engine, params={"nickname": conta_id})

        if ml_user_id.empty:
            st.error(f"Nickname '{conta_id}' não encontrado no banco de dados.")
            return pd.DataFrame()

        # Converte para tipo nativo Python (int)
        ml_user_id = int(ml_user_id.iloc[0]["ml_user_id"])

        sql = text("""
            SELECT s.order_id,
                   s.date_created,
                   s.item_title,
                   s.status,
                   s.quantity,
                   s.total_amount,
                   u.nickname
              FROM sales s
              LEFT JOIN user_tokens u ON s.ml_user_id = u.ml_user_id
             WHERE s.ml_user_id = :uid
        """)
        df = pd.read_sql(sql, engine, params={"uid": ml_user_id})
    else:
        sql = text("""
            SELECT s.order_id,
                   s.date_created,
                   s.item_title,
                   s.status,
                   s.quantity,
                   s.total_amount,
                   u.nickname
              FROM sales s
              LEFT JOIN user_tokens u ON s.ml_user_id = u.ml_user_id
        """)
        df = pd.read_sql(sql, engine)

    # converte de UTC para Horário de Brasília e descarta info de tz
    df["date_created"] = (
        pd.to_datetime(df["date_created"], utc=True)
          .dt.tz_convert("America/Sao_Paulo")
          .dt.tz_localize(None)
    )
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
                "Expedição e Logística",
                "Gestão de SKU",
                "Gestão de Despesas",
                "Painel de Metas"
            ],
            icons=[
                "house",
                "collection",
                "file-earmark-text",
                "truck",
                "box-seam",
                "currency-dollar",
                "bar-chart-line"
            ],
            menu_icon="list",
            default_index=[
                "Dashboard",
                "Contas Cadastradas",
                "Relatórios",
                "Expedição e Logística",
                "Gestão de SKU",
                "Gestão de Despesas",
                "Painel de Metas"
            ].index(st.session_state.get("page", "Dashboard")),
            orientation="vertical",
            styles={
                "container": {
                    "padding": "0",
                    "background-color": "#161b22"
                },
                "icon": {
                    "color": "#2ecc71",      # ícones em verde
                    "font-size": "18px"
                },
                "nav-link": {
                    "font-size": "16px",
                    "text-align": "left",
                    "margin": "4px 0",
                    "color": "#fff",          # texto branco
                    "background-color": "transparent"
                },
                "nav-link:hover": {
                    "background-color": "#27ae60"  # hover verde escuro
                },
                "nav-link-selected": {
                    "background-color": "#2ecc71", # seleção em verde claro
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
    st.markdown(
        '''
        <style>
        .stSelectbox label div[data-testid="stMarkdownContainer"] > div > span {
            color: #32CD32 !important;
        }
        .stDateInput label div[data-testid="stMarkdownContainer"] > div > span {
            color: #32CD32 !important;
        }
        </style>
        ''',
        unsafe_allow_html=True
    )

    st.header("📊 Dashboard de Vendas")

    # Botão para sincronização incremental
    if st.button("🔄 Sincronizar Vendas"):
        count = sync_all_accounts()
        st.cache_data.clear()
        st.success(f"{count} vendas novas sincronizadas com sucesso!")
        st.rerun()

    # 0) Carrega dados brutos
    df_full = carregar_vendas(None)
    if df_full.empty:
        st.warning("Nenhuma venda cadastrada.")
        return

    # 1) Layout dos filtros
    col1, col2, col3 = st.columns([2, 2, 2])
    
    # Selectbox de Conta
    contas_df  = pd.read_sql(text("SELECT nickname FROM user_tokens ORDER BY nickname"), engine)
    contas_lst = contas_df["nickname"].astype(str).tolist()
    escolha    = col1.selectbox("🔹 Conta", ["Todas as contas"] + contas_lst)
    conta_id   = None if escolha == "Todas as contas" else escolha

    # Selectbox de Filtro Rápido
    filtro_rapido = col2.selectbox(
        "🔹 Filtro Rápido",
        ["Período Personalizado", "Hoje", "Últimos 7 Dias", "Este Mês", "Últimos 30 Dias"]
    )

    # 2) Ajuste Dinâmico dos Campos de Data
    data_min = df_full["date_created"].dt.date.min()
    data_max = df_full["date_created"].dt.date.max()
    hoje = pd.Timestamp.now().date()
    
    if filtro_rapido == "Hoje":
        de, ate = hoje, hoje
    elif filtro_rapido == "Últimos 7 Dias":
        de, ate = hoje - pd.Timedelta(days=7), hoje
    elif filtro_rapido == "Este Mês":
        de, ate = hoje.replace(day=1), hoje
    elif filtro_rapido == "Últimos 30 Dias":
        de, ate = hoje - pd.Timedelta(days=30), hoje
    else:
        col2, col3 = st.columns([1, 1])
        de = col2.date_input("🔹 De", value=data_min, min_value=data_min, max_value=data_max)
        ate = col3.date_input("🔹 Até", value=data_max, min_value=data_min, max_value=data_max)

    # 3) Aplica filtros
    try:
        df = carregar_vendas(conta_id)
    except Exception as e:
        st.error(f"Erro ao carregar vendas: {e}")
        df = pd.DataFrame(columns=["date_created", "total_amount", "quantity"])

    if df.empty:
        st.warning("Nenhuma venda encontrada para os filtros selecionados.")
        return

    # Aplica o filtro de data
    df = df[(df["date_created"].dt.date >= de) & (df["date_created"].dt.date <= ate)]

    # =================== Ajuste de Timezone ===================
    df["date_created"] = df["date_created"].dt.tz_localize("UTC")
    df["date_created"] = df["date_created"].dt.tz_convert("America/Sao_Paulo")

    # 4) Métricas
    total_vendas = len(df)
    total_valor  = df["total_amount"].sum()
    total_itens  = df["quantity"].sum()
    ticket_medio = total_valor / total_vendas if total_vendas else 0

    # Exibição das métricas
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("🧾 Vendas Realizadas", total_vendas)
    c2.metric("💰 Receita Total", format_currency(total_valor))
    c3.metric("📦 Itens Vendidos", int(total_itens))
    c4.metric("🎯 Ticket Médio", format_currency(ticket_medio))

    # =================== Gráfico de Linha e Pizza ===================
    st.markdown("### 💵 Total Vendido por Data e Faturamento por Conta")
    col1, col2 = st.columns([4, 1])

    with col1:
        tipo_visualizacao = st.radio("Visualização do Gráfico", ["Diária", "Mensal"], horizontal=True)
        modo_linha       = st.radio("Modo de Linha", ["Por Conta", "Total Acumulado"], horizontal=True)

        # Define período
        base = df.copy()
        if tipo_visualizacao == "Diária":
            base['periodo'] = base['date_created'].dt.date
        else:
            base['periodo'] = base['date_created'].dt.to_period('M').astype(str)

        # Gera figura
        if modo_linha == "Por Conta":
            vendas_linha = (
                base.groupby(['periodo','nickname'])['total_amount']
                    .sum().reset_index(name='Valor Total')
            )
            titulo = f"💵 Total Vendido por {'Dia' if tipo_visualizacao=='Diária' else 'Mês'} (Por Conta)"
            fig = px.line(
                vendas_linha, x='periodo', y='Valor Total', color='nickname',
                title=titulo, labels={'periodo':'Data','Valor Total':'Valor','nickname':'Conta'},
                color_discrete_sequence=px.colors.sequential.Greens
            )
        else:
            vendas_total = (
                base.groupby('periodo')['total_amount']
                    .sum().reset_index(name='Valor Total')
            )
            vendas_total['Valor Total'] = vendas_total['Valor Total'].cumsum()
            titulo = f"💵 Total Acumulado por {'Dia' if tipo_visualizacao=='Diária' else 'Mês'}"
            fig = px.line(
                vendas_total, x='periodo', y='Valor Total',
                title=titulo, labels={'periodo':'Data','Valor Total':'Acumulado'},
                color_discrete_sequence=px.colors.sequential.Greens
            )
        fig.update_traces(showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.markdown("### 🥧 Faturamento por Conta")
        vendas_por_nickname = df.groupby('nickname')['total_amount'].sum().reset_index()
        fig_pie = px.pie(
            vendas_por_nickname, values='total_amount', names='nickname',
            title='📊 Faturamento por Conta', color_discrete_sequence=px.colors.sequential.Greens
        )
        st.plotly_chart(fig_pie, use_container_width=True)

    # =================== Gráfico de Barras - Dias da Semana ===================
    st.markdown("### 📅 Média Vendida por Dia da Semana")
    dias = ['Segunda-feira','Terça-feira','Quarta-feira','Quinta-feira','Sexta-feira','Sábado','Domingo']
    df['dia_semana'] = df['date_created'].dt.day_name().map({
        'Monday':'Segunda-feira','Tuesday':'Terça-feira','Wednesday':'Quarta-feira',
        'Thursday':'Quinta-feira','Friday':'Sexta-feira','Saturday':'Sábado','Sunday':'Domingo'
    })
    vendas_dia = df.groupby(['dia_semana', df['date_created'].dt.date])['total_amount'].sum().reset_index()
    media_dia = vendas_dia.groupby('dia_semana')['total_amount'].mean().reindex(dias).reset_index(name='Valor Médio')
    fig_bar = px.bar(
        media_dia, x='dia_semana', y='Valor Médio', text_auto='.2s',
        labels={'dia_semana':'Dia','Valor Médio':'Valor Médio'},
        title='📅 Média vendida por dia da semana', color_discrete_sequence=px.colors.sequential.Greens
    )
    st.plotly_chart(fig_bar, use_container_width=True)

    # =================== Gráfico de Linha - Faturamento por Hora ===================
    st.markdown("### ⏰ Média Acumulada por Hora do Dia")
    df['hora'] = df['date_created'].dt.hour
    faturamento_hora = df.groupby('hora')['total_amount'].mean().cumsum().reset_index(name='Valor Médio Acumulado')
    fig_hour = px.line(
        faturamento_hora, x='hora', y='Valor Médio Acumulado',
        labels={'hora':'Hora','Valor Médio Acumulado':'Valor Acumulado'},
        title='⏰ Média acumulada de faturamento por hora',
        color_discrete_sequence=px.colors.sequential.Greens
    )
    st.plotly_chart(fig_hour, use_container_width=True)


def mostrar_contas_cadastradas():
    st.header("🏷️ Contas Cadastradas")
    
    # Botão para Adicionar Nova Conta
    render_add_account_button()

    # Carregar as contas cadastradas
    df = pd.read_sql(text("SELECT ml_user_id, nickname, access_token, refresh_token FROM user_tokens ORDER BY nickname"), engine)
    
    if df.empty:
        st.warning("Nenhuma conta cadastrada.")
        return

    # Loop para criar expansores para cada conta
    for row in df.itertuples(index=False):
        with st.expander(f"🔗 Conta ML: {row.nickname}"):
            st.write(f"**User ID:** {row.ml_user_id}")
            st.write(f"**Access Token:** `{row.access_token}`")
            st.write(f"**Refresh Token:** `{row.refresh_token}`")
            
            # Botão para renovar o token
            if st.button("🔄 Renovar Token", key=f"renew_{row.ml_user_id}"):
                try:
                    resp = requests.post(f"{BACKEND_URL}/auth/refresh", json={"user_id": row.ml_user_id})
                    if resp.ok:
                        data = resp.json()
                        salvar_tokens_no_banco(data)
                        st.success("✅ Token atualizado com sucesso!")
                    else:
                        st.error(f"❌ Erro ao atualizar o token: {resp.text}")
                except Exception as e:
                    st.error(f"❌ Erro ao conectar com o servidor: {e}")

def mostrar_relatorios():
    st.header("📋 Relatórios de Vendas")
    df = carregar_vendas()
    if df.empty:
        st.warning("Nenhum dado para exibir.")
        return
    data_ini = st.date_input("De:",  value=df["date_created"].min())
    data_fim = st.date_input("Até:", value=df["date_created"].max())
    status  = st.multiselect("Status:", options=df["status"].unique(), default=df["status"].unique())
    df_filt = df.loc[
        (df["date_created"].dt.date >= data_ini) &
        (df["date_created"].dt.date <= data_fim) &
        (df["status"].isin(status))
    ]
    if df_filt.empty:
        st.warning("Sem registros para os filtros escolhidos.")
    else:
        st.dataframe(df_filt)

# Funções para cada página
def mostrar_expedicao_logistica():
    st.header("🚚 Expedição e Logística")
    st.info("Em breve...")

def mostrar_relatorios():
    st.header("📑 Relatórios")
    st.info("Em breve...")

def mostrar_gestao_sku():
    st.header("📦 Gestão de SKU")
    st.info("Em breve...")

def mostrar_gestao_despesas():
    st.header("💰 Gestão de Despesas")
    st.info("Em breve...")

def mostrar_painel_metas():
    st.header("🎯 Painel de Metas")
    st.info("Em breve...")

# ----------------- Fluxo Principal -----------------
if "code" in st.query_params:
    ml_callback()

pagina = render_sidebar()
if pagina == "Dashboard":
    mostrar_dashboard()
elif pagina == "Contas Cadastradas":
    mostrar_contas_cadastradas()
elif pagina == "Relatórios":
    mostrar_relatorios()
elif pagina == "Expedição e Logística":
    mostrar_expedicao_logistica()
elif pagina == "Gestão de SKU":
    mostrar_gestao_sku()
elif pagina == "Gestão de Despesas":
    mostrar_gestao_despesas()
elif pagina == "Painel de Metas":
    mostrar_painel_metas()
