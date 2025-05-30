#!/bin/bash

# Inicia o backend (FastAPI) na porta 8000
uvicorn api:app --host 0.0.0.0 --port 8000 &

# Inicia o frontend (Streamlit) na porta 8501
streamlit run app.py --server.port 8501 --server.address=0.0.0.0
