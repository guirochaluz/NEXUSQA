import os
import streamlit as st
import pandas as pd
import plotly.express as px
import requests
import locale
from typing import Optional
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from streamlit_option_menu import option_menu
from sales import sync_all_accounts  # sua implementação real

# ----------------- Locale para moeda -----------------
try:
    locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')
    LOCALE_OK = True
except locale.Error:
    LOCALE_OK = False

def format_currency(valor: float) -> str:
    """Formata float em R$ 1.234,56 usando locale ou fallback manual."""
    if LOCALE_OK:
        try:
            return locale.currency(valor, grouping=True)
        except Exception:
            pass
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

# ----------------- Autenticação Simples -----------------
if "authenticated" not in st.session_state:
    st.session_state["authenticated"] = False

params = st.query_params
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

# ----------------- Variáveis de Ambiente -----------------
load_dotenv()
BACKEND_URL   = os.getenv("BACKEND_URL")
FRONTEND_URL  = os.getenv("FRONTEND_URL")
DB_URL        = os.getenv("DB_URL")
ML_CLIENT_ID  = os.getenv("ML_CLIENT_ID")

if not all([BACKEND_URL, FRONTEND_URL, DB_URL, ML_CLIENT_ID]):
    st.error("❌ Defina BACKEND_URL, FRONTEND_URL, DB_URL e ML_CLIENT_ID no .env")
    st.stop()

# ----------------- CSS Global -----------------
st.markdown("""
<style>
  html, body, [data-testid="stAppViewContainer"] {
    overflow: hidden !important; height: 100vh !important;
  }
  ::-webkit-scrollbar { display: none; }
  [data-testid="stSidebar"] {
    background-color: #161b22; overflow: hidden; height: 100vh;
  }
  [data-testid="stAppViewContainer"] {
    background-color: #0e1117; color: #fff;
  }
  .sidebar-title { font-size:18px; font-weight:bold; color:#fff; margin-bottom:10px; }
  .menu-button { width:100%; padding:8px; margin-bottom:5px;
                 background-color:#1d2b36; color:#fff; border:none; border-radius:5px; text-align:left; }
  .menu-button:hover { background-color:#263445; }
</style>
""", unsafe_allow_html=True)

# ----------------- Conexão com Banco -----------------
engine = create_engine(DB_URL, pool_size=5, max_overflow=10, pool_timeout=30)

# ----------------- OAuth Callback -----------------
def ml_callback():
    code = st.query_params.get("code", [None])[0]
    if not code:
        st.error("⚠️ Código de autorização não encontrado.")
        return
    st.success("✅ Código recebido. Autenticando...")
    resp = requests.post(f"{BACKEND_URL}/auth/callback", json={"code": code})
    if resp.ok:
        data = resp.json()
        salvar_tokens_no_banco(data)
        st.cache_data.clear()
        st.experimental_set_query_params(account=data["user_id"])
        st.session_state["conta"] = data["user_id"]
        st.success("✅ Conta autenticada!")
        st.rerun()
    else:
        st.error(f"❌ Falha na autenticação: {resp.text}")

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
        st.error(f"❌ Erro ao salvar tokens: {e}")

# ----------------- Carregamento de Vendas -----------------
@st.cache_data(ttl=300)
def carregar_vendas(nickname: Optional[str] = None) -> pd.DataFrame:
    base_q = (
        "SELECT s.order_id, s.date_created, s.item_title, s.status, "
        "s.quantity, s.total_amount, u.nickname "
        "FROM sales s LEFT JOIN user_tokens u ON s.ml_user_id = u.ml_user_id"
    )
    params = {}
    if nickname and nickname != "Todas as contas":
        base_q += " WHERE u.nickname = :nick"
        params["nick"] = nickname
    df = pd.read_sql(text(base_q), engine, params=params, parse_dates=["date_created"])
    df["date_created"] = (
        pd.to_datetime(df["date_created"], utc=True)
          .dt.tz_convert("America/Sao_Paulo")
          .dt.tz_localize(None)
    )
    return df

# ----------------- Botão Adicionar Conta -----------------
def render_add_account_button():
    login_url = (
      f"{BACKEND_URL}/ml-login"
      f"?client_id={ML_CLIENT_ID}"
      f"&redirect_uri={FRONTEND_URL}/?nexus_auth=success"
    )
    st.markdown(f"""
      <a href="{login_url}" target="_blank">
        <button style="
          background-color:#4CAF50;color:white;border:none;
          padding:10px;border-radius:5px;margin-bottom:10px;">
          ➕ Adicionar Conta ML
        </button>
      </a>
    """, unsafe_allow_html=True)

# ----------------- Sidebar -----------------
def render_sidebar():
    with st.sidebar:
        selected = option_menu(
            menu_title=None,
            options=["Dashboard","Contas Cadastradas","Relatórios",
                     "Expedição","SKU","Despesas","Metas"],
            icons=["house","collection","file-earmark-text","truck",
                   "box-seam","currency-dollar","bar-chart-line"],
            default_index=0,
            orientation="vertical",
            styles={
                "icon": {"color":"#2ecc71","font-size":"18px"},
                "nav-link": {"font-size":"16px","color":"#fff"},
                "nav-link-selected":{"background-color":"#2ecc71","color":"#fff"}
            },
        )
    st.session_state["page"] = selected
    return selected

# ================= Páginas =================

def mostrar_dashboard():
    # filtros sticky
    df_full = carregar_vendas(None)
    if df_full.empty:
        st.warning("Nenhuma venda cadastrada.")
        return
    dat_min, dat_max = df_full["date_created"].dt.date.min(), df_full["date_created"].dt.date.max()
    today = pd.Timestamp.now().date()

    st.markdown(
      '<div style="position:sticky; top:0; background:#0e1117; padding:10px; z-index:999;">', 
      unsafe_allow_html=True
    )
    c1,c2,c3,c4,c5 = st.columns([2,2,2,2,1])
    with c1:
        nick = st.selectbox("Conta", ["Todas as contas"] + df_full["nickname"].unique().tolist())
    with c2:
        per = st.selectbox("Período", ["Hoje","Últimos 7 Dias","Este Mês","Últimos 30 Dias","Personalizado"])
    if per=="Hoje":
        de=ate=today
    elif per=="Últimos 7 Dias":
        de,ate = today-pd.Timedelta(7), today
    elif per=="Este Mês":
        de,ate = today.replace(day=1), today
    elif per=="Últimos 30 Dias":
        de,ate = today-pd.Timedelta(30), today
    else:
        de= c3.date_input("De", value=dat_min, min_value=dat_min, max_value=dat_max)
        ate=c4.date_input("Até",value=dat_max, min_value=dat_min, max_value=dat_max)
    with c5:
        if st.button("🔄"):
            cnt=sync_all_accounts()
            st.cache_data.clear()
            st.success(f"{cnt} vendas sincronizadas")
            st.rerun()
    st.markdown('</div>', unsafe_allow_html=True)

    df = carregar_vendas(nick)
    df = df[(df["date_created"].dt.date>=de)&(df["date_created"].dt.date<=ate)]
    if df.empty:
        st.warning("Sem vendas no período.")
        return

    # métricas
    v,t,i= len(df), df["total_amount"].sum(), df["quantity"].sum()
    tm = t/v if v else 0
    m1,m2,m3,m4 = st.columns(4)
    m1.metric("Vendas",v); m2.metric("Receita",format_currency(t))
    m3.metric("Itens",i); m4.metric("Ticket Médio",format_currency(tm))

    # linha + pizza
    colL,colP = st.columns([4,1])
    with colL:
        view = st.radio("Visão",["Diária","Mensal"],horizontal=True)
        mode = st.radio("Linha",["Por Conta","Total Geral"],horizontal=True)
        tmp = df.copy()
        tmp["periodo"] = tmp["date_created"].dt.date if view=="Diária" else tmp["date_created"].dt.to_period("M").astype(str)
        if mode=="Por Conta":
            g = tmp.groupby(["periodo","nickname"])["total_amount"].sum().reset_index()
            figL = px.line(g, x="periodo",y="total_amount",color="nickname",
                           color_discrete_sequence=px.colors.sequential.Greens)
            figL.update_traces(showlegend=False)
        else:
            g = tmp.groupby("periodo")["total_amount"].sum().reset_index()
            figL = px.line(g, x="periodo",y="total_amount",
                           color_discrete_sequence=[px.colors.sequential.Greens[0]])
        st.plotly_chart(figL, use_container_width=True)
    with colP:
        gp = df.groupby("nickname")["total_amount"].sum().reset_index()
        figP = px.pie(gp,names="nickname",values="total_amount",
                      color_discrete_sequence=px.colors.sequential.Greens)
        st.plotly_chart(figP, use_container_width=True)

    # barras dia da semana
    dias = ["Segunda","Terça","Quarta","Quinta","Sexta","Sábado","Domingo"]
    df["dia"] = df["date_created"].dt.day_name().map({
        "Monday":"Segunda","Tuesday":"Terça","Wednesday":"Quarta",
        "Thursday":"Quinta","Friday":"Sexta","Saturday":"Sábado","Sunday":"Domingo"
    })
    gb = df.groupby(["dia", df["date_created"].dt.date])["total_amount"].sum().reset_index()
    ab = gb.groupby("dia")["total_amount"].mean().reindex(dias).reset_index()
    figB = px.bar(ab,x="dia",y="total_amount",text_auto=True,
                  color_discrete_sequence=px.colors.sequential.Greens)
    st.plotly_chart(figB, use_container_width=True)

    # linha hora
    df["hora"] = df["date_created"].dt.hour
    gh = df.groupby("hora")["total_amount"].mean().reset_index()
    figH = px.line(gh,x="hora",y="total_amount",
                   color_discrete_sequence=[px.colors.sequential.Greens[1]])
    st.plotly_chart(figH, use_container_width=True)

def mostrar_contas_cadastradas():
    st.title("🏷️ Contas Cadastradas")
    render_add_account_button()
    df = pd.read_sql(text("SELECT ml_user_id,nickname,access_token,refresh_token FROM user_tokens ORDER BY nickname"),engine)
    if df.empty:
        st.warning("Nenhuma conta cadastrada.")
        return
    for r in df.itertuples():
        with st.expander(f"{r.nickname} ({r.ml_user_id})"):
            st.write(f"TOKEN: `{r.access_token}`")
            st.write(f"REFRESH: `{r.refresh_token}`")
            if st.button("🔄 Renovar", key=r.ml_user_id):
                resp = requests.post(f"{BACKEND_URL}/auth/refresh",json={"user_id":r.ml_user_id})
                if resp.ok:
                    salvar_tokens_no_banco(resp.json())
                    st.success("Token atualizado!")
                else:
                    st.error("Falha ao atualizar token.")

def mostrar_relatorios():
    st.title("📋 Relatórios de Vendas")
    df = carregar_vendas()
    if df.empty:
        st.warning("Sem dados.")
        return
    d0 = st.date_input("De",df["date_created"].min())
    d1 = st.date_input("Até",df["date_created"].max())
    st.dataframe(df[(df["date_created"].dt.date>=d0)&(df["date_created"].dt.date<=d1)])

def mostrar_expedicao():
    st.title("🚚 Expedição e Logística"); st.info("Em breve...")

def mostrar_sku():
    st.title("📦 Gestão de SKU"); st.info("Em breve...")

def mostrar_despesas():
    st.title("💰 Gestão de Despesas"); st.info("Em breve...")

def mostrar_metas():
    st.title("🎯 Painel de Metas"); st.info("Em breve...")

# ----------------- Main -----------------
if "code" in st.query_params:
    ml_callback()

pag = render_sidebar()
if pag == "Dashboard":
    mostrar_dashboard()
elif pag == "Contas Cadastradas":
    mostrar_contas_cadastradas()
elif pag == "Relatórios":
    mostrar_relatorios()
elif pag == "Expedição":
    mostrar_expedicao()
elif pag == "SKU":
    mostrar_sku()
elif pag == "Despesas":
    mostrar_despesas()
elif pag == "Metas":
    mostrar_metas()
