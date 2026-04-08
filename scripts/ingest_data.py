import duckdb
import pandas as pd
import os

# 1. Configuração de caminhos
# Procuramos o CSV na pasta data/raw que criamos
csv_path = 'data/raw/Procurement KPI Analysis Dataset.csv'
db_path = 'procurement.duckdb'

print("--- Iniciando a ingestão de dados ---")

# 2. Conectar ao DuckDB (ele cria o arquivo se não existir)
con = duckdb.connect(db_path)

# 3. Ler o CSV e carregar para o DuckDB
# Usamos a função nativa do DuckDB que é extremamente veloz
try:
    con.execute(f"""
        CREATE OR REPLACE TABLE raw_procurement AS 
        SELECT * FROM read_csv_auto('{csv_path}')
    """)
    
    # Verificar se os dados foram carregados
    result = con.execute("SELECT COUNT(*) FROM raw_procurement").fetchone()
    print(f"✅ Sucesso! {result[0]} linhas carregadas na tabela 'raw_procurement'.")

except Exception as e:
    print(f"❌ Erro ao carregar os dados: {e}")

finally:
    con.close()

print("--- Processo finalizado ---")