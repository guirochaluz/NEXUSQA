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

import os
import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from datetime import datetime
from babel.numbers import format_currency as babel_format

# Carrega variáveis de ambiente
env_path = os.getenv('DOTENV_PATH', None)
if env_path:
    load_dotenv(env_path)
else:
    load_dotenv()

# Configuração da conexão com o banco de dados
DATABASE_URL = os.getenv("DATABASE_URL")
engine = create_engine(DATABASE_URL)

# Configurações de estilo
COLORS = ['#2ecc71', '#27ae60', '#16a085', '#1abc9c']

# Formatação monetária
def format_currency(value: float) -> str:
    try:
        return babel_format(value, 'BRL', locale='pt_BR')
    except Exception:
        return f"R$ {value:,.2f}"

# Carrega vendas, opcional por nickname
def carregar_vendas(nickname=None) -> pd.DataFrame:
    query = (
        "SELECT s.date_created, s.total_amount, s.quantity, u.nickname "
        "FROM sales s JOIN user_tokens u ON s.ml_user_id = u.ml_user_id"
    )
    params = {}
    if nickname and nickname != "Todas as contas":
        query += " WHERE u.nickname = :nick"
        params['nick'] = nickname
    df = pd.read_sql(text(query), engine, params=params, parse_dates=['date_created'])
    return df

# Stub para sincronização incremental
def sync_all_accounts() -> int:
    # TODO: implementar integração real com ML
    return 0

def mostrar_dashboard():
    # Estilos customizados
    st.markdown(
        '''
        <style>
        .stSelectbox label div[data-testid="stMarkdownContainer"] > div > span { color: #32CD32 !important; }
        .stDateInput label div[data-testid="stMarkdownContainer"] > div > span { color: #32CD32 !important; }
        </style>
        ''',
        unsafe_allow_html=True
    )

    # Carrega dados sem filtros para configurar datas
    df_full = carregar_vendas(None)
    if df_full.empty:
        st.warning("Nenhuma venda cadastrada.")
        return
    data_min = df_full['date_created'].dt.date.min()
    data_max = df_full['date_created'].dt.date.max()
    hoje = datetime.now().date()

    # Container sticky para filtros e botão
    st.markdown(
        '<div style="position:sticky; top:0; background:white; padding:10px; z-index:100">',
        unsafe_allow_html=True
    )
    col_acc, col_fast, col_de, col_ate, col_btn = st.columns([2, 2, 2, 2, 1])

    # Filtros
    with col_acc:
        escolha = st.selectbox("Conta", ["Todas as contas"] + df_full['nickname'].unique().tolist())
    with col_fast:
        filtro = st.selectbox(
            "Período", ["Hoje", "Últimos 7 Dias", "Este Mês", "Últimos 30 Dias", "Personalizado"]
        )
    # Datas customizadas
    if filtro == "Hoje":
        de = ate = hoje
    elif filtro == "Últimos 7 Dias":
        de, ate = hoje - pd.Timedelta(days=7), hoje
    elif filtro == "Este Mês":
        de, ate = hoje.replace(day=1), hoje
    elif filtro == "Últimos 30 Dias":
        de, ate = hoje - pd.Timedelta(days=30), hoje
    else:
        de = col_de.date_input("De", value=data_min, min_value=data_min, max_value=data_max)
        ate = col_ate.date_input("Até", value=data_max, min_value=data_min, max_value=data_max)

    # Botão sincronizar no canto direito
    with col_btn:
        if st.button("🔄"):
            count = sync_all_accounts()
            st.cache_data.clear()
            st.success(f"{count} vendas sincronizadas")
            st.experimental_rerun()
    st.markdown('</div>', unsafe_allow_html=True)

    # Aplica filtros e timezone
    try:
        df = carregar_vendas(escolha)
    except Exception as e:
        st.error(f"Erro: {e}")
        return
    df = df[(df['date_created'].dt.date >= de) & (df['date_created'].dt.date <= ate)]
    df['date_created'] = (df['date_created'].dt.tz_localize('UTC').dt.tz_convert('America/Sao_Paulo'))
    if df.empty:
        st.warning("Nenhuma venda no período selecionado.")
        return

    # Métricas
    total_vendas = len(df)
    total_valor = df['total_amount'].sum()
    total_itens = df['quantity'].sum()
    ticket_medio = total_valor / total_vendas if total_vendas else 0
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Vendas", total_vendas)
    m2.metric("Receita", format_currency(total_valor))
    m3.metric("Itens", total_itens)
    m4.metric("Ticket Médio", format_currency(ticket_medio))

    # Gráfico de linha - diário/mensal e modo de linha
    col1, col2 = st.columns([4,1])
    with col1:
        tipo = st.radio("Visão", ["Diária", "Mensal"], horizontal=True)
        modo = st.radio("Linha", ["Por Conta", "Total Geral"], horizontal=True)
        base = df.copy()
        if tipo == "Diária":
            base['periodo'] = base['date_created'].dt.date
        else:
            base['periodo'] = base['date_created'].dt.to_period('M').astype(str)
        if modo == "Por Conta":
            grp = base.groupby(['periodo','nickname'])['total_amount'].sum().reset_index()
            fig_line = px.line(grp, x='periodo', y='total_amount', color='nickname',
                               labels={'periodo':'Data','total_amount':'Valor','nickname':'Conta'},
                               color_discrete_sequence=COLORS)
            fig_line.update_traces(showlegend=False)
        else:
            grp = base.groupby('periodo')['total_amount'].sum().reset_index()
            fig_line = px.line(grp, x='periodo', y='total_amount',
                               labels={'periodo':'Data','total_amount':'Total'},
                               color_discrete_sequence=[COLORS[0]])
        st.plotly_chart(fig_line, use_container_width=True)
    # Gráfico de pizza - controla legendas
    with col2:
        grp_pie = df.groupby('nickname')['total_amount'].sum().reset_index()
        fig_pie = px.pie(grp_pie, names='nickname', values='total_amount',
                         color_discrete_sequence=COLORS)
        st.plotly_chart(fig_pie, use_container_width=True)

    # Gráfico de barras - dia da semana
    dias = ['Segunda','Terça','Quarta','Quinta','Sexta','Sábado','Domingo']
    df['dia'] = df['date_created'].dt.day_name().map({
        'Monday':'Segunda','Tuesday':'Terça','Wednesday':'Quarta',
        'Thursday':'Quinta','Friday':'Sexta','Saturday':'Sábado','Sunday':'Domingo'
    })
    grp_bar = df.groupby(['dia', df['date_created'].dt.date])['total_amount'].sum().reset_index()
    avg_bar = grp_bar.groupby('dia')['total_amount'].mean().reindex(dias).reset_index()
    fig_bar = px.bar(avg_bar, x='dia', y='total_amount', text_auto=True,
                     labels={'dia':'Dia','total_amount':'Média'},
                     color_discrete_sequence=COLORS)
    st.plotly_chart(fig_bar, use_container_width=True)

    # Gráfico de linha - hora do dia
    df['hora'] = df['date_created'].dt.hour
    grp_hour = df.groupby('hora')['total_amount'].mean().reset_index()
    fig_hour = px.line(grp_hour, x='hora', y='total_amount',
                       labels={'hora':'Hora','total_amount':'Média'},
                       color_discrete_sequence=[COLORS[1]])
    st.plotly_chart(fig_hour, use_container_width=True)

if __name__ == '__main__':
    mostrar_dashboard()


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
