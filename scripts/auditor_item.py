import sys
from pathlib import Path
import duckdb
from datetime import datetime
import math

# Configuração de Caminhos
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.append(str(PROJECT_ROOT / "src"))

def main():
    print("🕵️  AUDITOR DE CÁLCULO DE COMPRAS (RAIO-X - DB INTEGRADO)")
    print("==========================================")
    cod_alvo = input("Digite o CÓDIGO DO PRODUTO para auditar: ").strip()

    # 1. CONEXÃO COM DADOS
    db_path = PROJECT_ROOT / "data" / "vendas.db"
    
    if not db_path.exists():
        print("❌ Banco de dados vendas.db não encontrado!")
        return

    conn = duckdb.connect()
    conn.execute("INSTALL sqlite; LOAD sqlite;")
    conn.execute(f"ATTACH '{db_path}' AS sqlite_db (TYPE SQLITE, READ_ONLY)")
    
    print(f"\n🔍 1. DADOS BRUTOS (Banco de Dados)")
    print("-" * 50)

    # --- BUSCA DADOS CADASTRAIS ---
    try:
        cadastro = conn.execute(f"""
            SELECT ativo, qtd_economica, marca 
            FROM sqlite_db.produtos_gerais 
            WHERE cod_produto = '{cod_alvo}'
        """).fetchone()
        
        ativo = cadastro[0] if cadastro else "SIM (Não encontrado)"
        lote = cadastro[1] if cadastro and cadastro[1] else 1
        marca = cadastro[2] if cadastro else "N/D"
    except:
        ativo = "ERRO LEITURA"
        lote = 1
        marca = "?"
    
    print(f"• Cadastro: Ativo={ativo} | Lote={lote} | Marca={marca}")

    # --- BUSCA ESTOQUE (NOVA TABELA) ---
    try:
        estoque_data = conn.execute(f"""
            SELECT saldo_estoque, saldo_oc, custo_unitario, ultima_entrada
            FROM sqlite_db.saldo_custo_entrada
            WHERE CAST(cod_produto AS VARCHAR) = '{cod_alvo}'
        """).fetchone()
    except Exception as e:
        print(f"❌ Erro ao ler tabela 'saldo_custo_entrada': {e}")
        estoque_data = None
    
    if not estoque_data:
        print("❌ ERRO: Item não encontrado na tabela de saldo!")
        return

    saldo = estoque_data[0] if estoque_data[0] else 0
    saldo_oc = estoque_data[1] if estoque_data[1] else 0
    custo = estoque_data[2] if estoque_data[2] else 0.0
    ult_entrada = estoque_data[3]
    
    print(f"• Estoque: Físico={saldo} | OC={saldo_oc} | Custo=R${custo:.2f}")
    print(f"• Última Entrada: {ult_entrada}")

    # --- BUSCA VENDAS (MÉDIA REAL 365 DIAS) ---
    vendas_365 = conn.execute(f"""
        SELECT SUM(quantidade) 
        FROM sqlite_db.vendas 
        WHERE cod_produto = '{cod_alvo}'
        AND CAST(data_movimento AS DATE) >= (CURRENT_DATE - INTERVAL '365 days')
    """).fetchone()
    
    total_vendas_ano = vendas_365[0] if vendas_365[0] else 0
    media_diaria_real = total_vendas_ano / 365.0
    
    print(f"• Vendas 365 dias: {total_vendas_ano} peças")
    print(f"• Média Diária (Total/365): {media_diaria_real:.4f}")

    # ... (O restante do código de cálculo permanece igual, pois usa as variáveis carregadas acima) ...
    # Para economizar espaço, mantive apenas a parte de extração de dados que mudou.
    
    # ... [CÓDIGO DE CÁLCULO MANTIDO IGUAL AO ORIGINAL] ...

if __name__ == "__main__":
    main()