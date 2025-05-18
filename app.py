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
    page_title="Sistema de Gestão - NEXUS",
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
    st.session_state["authenticated"] = True
    sync_all_accounts()
    st.cache_data.clear()
    st.experimental_set_query_params()

if not st.session_state["authenticated"]:
    st.title("Sistema de Gestão - Grupo Nexus")
    username = st.text_input("Usuário")
    password = st.text_input("Senha", type="password")
    if st.button("Entrar"):
        if username == "GRUPONEXUS" and password == "NEXU$2025":
            st.session_state["authenticated"] = True
            sync_all_accounts()
            st.cache_data.clear()
            st.rerun()
        else:
            st.error("Credenciais inválidas")
    st.stop()

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
    import streamlit as st
    import pandas as pd
    from sqlalchemy import text
    from datetime import datetime
    import pytz
    import plotly.express as px

    # fuso SP
    tz_sp = pytz.timezone("America/Sao_Paulo")

    # --- CSS para compactar e colorir labels ---
    st.markdown(
        """
        <style>
        /* labels verdes */
        .stSelectbox label span,
        .stDateInput label span,
        .stMultiSelect label span,
        .stRadio label span {
            color: #32CD32 !important;
        }
        /* reduzir margens/paddings */
        .stMultiSelect, .stSelectbox, .stRadio, .stDateInput, .stButton {
            margin-right: 4px;
            padding: 2px 4px;
        }
        /* badge contas */
        .badge-contas {
          background-color: #27ae60;
          color: white;
          padding: 4px 8px;
          border-radius: 12px;
          font-size: 0.9rem;
        }
        </style>
        """,
        unsafe_allow_html=True
    )

    # --- carrega dados e botão de sync ---
    df_full = carregar_vendas(None)
    if df_full.empty:
        st.warning("Nenhuma venda cadastrada.")
        return

    # define colunas do cabeçalho
    (
        sync_col,
        contas_col,
        modo_col,
        gran_col,
        search_col,
        quick_col,
        de_col,
        ate_col,
        clear_col
    ) = st.columns([0.4, 1.4, 1, 1, 2, 1.2, 1, 1, 0.4])

    # 2) Sync + hora da última execução
    if sync_col.button("🔄", help="Sincronizar Vendas"):
        count = sync_all_accounts()
        st.cache_data.clear()
        st.session_state["last_sync"] = datetime.now(tz_sp)
        st.success(f"{count} vendas novas sincronizadas!")
    last = st.session_state.get("last_sync")
    if last:
        sync_col.caption(last.strftime("Últ. sync: %d/%m %H:%M"))

    # 5) Badge de Contas + expander com multiselect
    contas_df  = pd.read_sql(text("SELECT nickname FROM user_tokens ORDER BY nickname"), engine)
    contas_lst = contas_df["nickname"].astype(str).tolist()
    st.session_state.setdefault("contas_ms", contas_lst.copy())
    sel = st.session_state["contas_ms"]
    badge_label = f'<span class="badge-contas">{"Todas as contas" if len(sel)==len(contas_lst) else f"{len(sel)} contas"}</span>'
    with contas_col.expander(badge_label, expanded=False):
        st.markdown("")  # forçar conteúdo acima
        st.multiselect("", options=contas_lst, default=sel, key="contas_ms")
    # filtra
    df_full = df_full[df_full["nickname"].isin(sel)]

    # 3) Agregação: Por Conta vs Total Geral
    modo_agregacao = modo_col.radio(
        "", ["👤 Por Conta", "🔢 Total Geral"],
        horizontal=True, key="modo_agregacao"
    )

    # 10) Granularidade
    granularidade = gran_col.radio(
        "", ["⏱️ Por Hora", "📅 Diária", "🗓️ Semanal", "🗓️ Mensal"],
        horizontal=True, key="granularidade"
    )

    # 4) Busca rápida
    search_text = search_col.text_input(
        "🔍", "",
        placeholder="Código ou cliente...",
        help="Busca rápida",
        key="search_txt"
    )
    if search_text:
        df_full = df_full[
            df_full["external_reference"].str.contains(search_text, case=False, na=False) |
            df_full["nickname"].str.contains(search_text, case=False, na=False)
        ]

    # 1) Quick-filter (Hoje, Ontem, etc)
    quick_col.selectbox(
        "", 
        ["Hoje", "Ontem", "Últimos 7 Dias", "Este Mês", "Últimos 30 Dias", "Período Personalizado"],
        key="filtro_quick"
    )

    # 2) Date inputs De/Até
    data_min = df_full["date_created"].dt.date.min()
    data_max = df_full["date_created"].dt.date.max()
    hoje     = datetime.now(tz_sp).date()
    filtro   = st.session_state["filtro_quick"]

    if filtro == "Hoje":
        de = ate = hoje
    elif filtro == "Ontem":
        de = ate = hoje - pd.Timedelta(days=1)
    elif filtro == "Últimos 7 Dias":
        de, ate = hoje - pd.Timedelta(days=7), hoje
    elif filtro == "Este Mês":
        de, ate = hoje.replace(day=1), hoje
    elif filtro == "Últimos 30 Dias":
        de, ate = hoje - pd.Timedelta(days=30), hoje
    else:
        de, ate = data_min, data_max

    custom = (filtro == "Período Personalizado")
    de = de_col.date_input("", value=de, min_value=data_min, max_value=data_max,
                          disabled=not custom, key="de_q")
    ate = ate_col.date_input("", value=ate, min_value=data_min, max_value=data_max,
                             disabled=not custom, key="ate_q")

    # 2) Limpar filtros
    if clear_col.button("✖", help="Limpar Filtros"):
        # redefinir tudo
        st.session_state["contas_ms"]      = contas_lst.copy()
        st.session_state["modo_agregacao"] = "👤 Por Conta"
        st.session_state["granularidade"]  = "📅 Diária"
        st.session_state["search_txt"]     = ""
        st.session_state["filtro_quick"]   = "Hoje"
        st.session_state["de_q"]           = data_min
        st.session_state["ate_q"]          = data_max
        st.experimental_rerun()

    # --- A partir daqui, aplique timezone, métricas e gráfico ---
    # (… seu código existente …)

    # --- 9) Mini-sparkline de tendência ---
    spark_col = st.columns([1,1,1,1,2])[4]
    trend = (
        df_full
        .groupby(df_full["date_created"].dt.date)["total_amount"]
        .sum()
    )
    spark_col.line_chart(trend, use_container_width=True, height=80)
 
    # 2) Prepara eixo X e agrupamentos
    if tipo_visualizacao == "Diária":
        eixo_x = "date_created"
        df_plot = df.copy()
        df_plot["date_created"] = df_plot["date_created"].dt.date
    
        if modo_agregacao == "Por Conta":
            vendas_por_data = (
                df_plot
                .groupby(["date_created", "nickname"])["total_amount"]
                .sum()
                .reset_index(name="Valor Total")
            )
            titulo = "💵 Total Vendido por Dia (Linha por Nickname)"
            color_dim = "nickname"
            color_seq = px.colors.sequential.Agsunset
    
        else:  # Total Geral
            vendas_por_data = (
                df_plot
                .groupby("date_created")["total_amount"]
                .sum()
                .reset_index(name="Valor Total")
            )
            titulo = "💵 Total Vendido por Dia (Soma Total)"
            color_dim = None
            color_seq = ["#27ae60"]
    
    elif tipo_visualizacao == "Mensal":
        eixo_x = "date_created"
        df_plot = df.copy()
        df_plot["date_created"] = df_plot["date_created"].dt.to_period("M").astype(str)
    
        if modo_agregacao == "Por Conta":
            vendas_por_data = (
                df_plot
                .groupby(["date_created", "nickname"])["total_amount"]
                .sum()
                .reset_index(name="Valor Total")
            )
            titulo = "💵 Total Vendido por Mês (Linha por Nickname)"
            color_dim = "nickname"
            color_seq = px.colors.sequential.Agsunset
    
        else:  # Total Geral
            vendas_por_data = (
                df_plot
                .groupby("date_created")["total_amount"]
                .sum()
                .reset_index(name="Valor Total")
            )
            titulo = "💵 Total Vendido por Mês (Soma Total)"
            color_dim = None
            color_seq = ["#27ae60"]
    
    # 3) Desenha o gráfico
    fig = px.line(
        vendas_por_data,
        x=eixo_x,
        y="Valor Total",
        color=color_dim,
        title=titulo,
        labels={
            "Valor Total": "Valor Total",
            "date_created": "Data",
            "nickname": "Conta"
        },
        color_discrete_sequence=color_seq
    )
    
    fig.update_traces(
        mode='lines+markers',
        marker=dict(size=5),
        texttemplate='%{y:,.2f}',
        textposition='top center'
    )
    
    st.plotly_chart(fig, use_container_width=True)

    # === Gráfico de barras: Média por dia da semana ===
    st.markdown('<div class="section-title">📅 Vendas por Dia da Semana</div>', unsafe_allow_html=True)
    dias = ["Segunda","Terça","Quarta","Quinta","Sexta","Sábado","Domingo"]
    df["dia"] = df["date_created"].dt.day_name().map({
        "Monday":"Segunda","Tuesday":"Terça","Wednesday":"Quarta",
        "Thursday":"Quinta","Friday":"Sexta","Saturday":"Sábado","Sunday":"Domingo"
    })
    gb = df.groupby(["dia", df["date_created"].dt.date])["total_amount"].sum().reset_index()
    ab = gb.groupby("dia")["total_amount"].mean().reindex(dias).reset_index()
    fig_bar = px.bar(
        ab, x="dia", y="total_amount", text_auto=".2s",
        labels={"dia":"Dia","total_amount":"Média"},
        color_discrete_sequence=["#27ae60"]
    )
    st.plotly_chart(fig_bar, use_container_width=True, theme="streamlit")

    # =================== Gráfico de Linha - Faturamento Acumulado por Hora ===================
    st.markdown("### ⏰ Faturamento Acumulado por Hora do Dia (Média)")
    
    # Extrai hora e calcula média acumulada
    df["hora"] = df["date_created"].dt.hour
    faturamento_por_hora = (
        df.groupby("hora")["total_amount"]
          .mean()
          .cumsum()
          .reset_index(name="Valor Médio Acumulado")
    )
    
    # Plota
    fig_hora = px.line(
        faturamento_por_hora,
        x="hora",
        y="Valor Médio Acumulado",
        title="⏰ Média de Faturamento Acumulado por Hora",
        labels={
            "hora": "Hora do Dia",
            "Valor Médio Acumulado": "Valor Médio Acumulado"
        },
        color_discrete_sequence=["#27ae60"],
        markers=True
    )
    st.plotly_chart(fig_hora, use_container_width=True)


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
