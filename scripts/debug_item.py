import sys
from pathlib import Path
import duckdb

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.append(str(PROJECT_ROOT / "src"))

def main():
    cod_alvo = input("Digite o código do produto problemático: ")
    
    db_path = PROJECT_ROOT / "data" / "vendas.db"
    conn = duckdb.connect()
    conn.execute("INSTALL sqlite; LOAD sqlite;")
    conn.execute(f"ATTACH '{db_path}' AS sqlite_db (TYPE SQLITE)")
    
    print(f"\n🔍 INVESTIGANDO O ITEM: {cod_alvo}")
    print("-" * 50)
    
    # 1. Vendas Totais da História
    total_hist = conn.execute(f"""
        SELECT SUM(quantidade), MIN(data_movimento), MAX(data_movimento) 
        FROM sqlite_db.vendas 
        WHERE cod_produto = '{cod_alvo}'
    """).fetchone()
    print(f"Histórico Completo: {total_hist[0]} peças vendidas de {total_hist[1]} até {total_hist[2]}")
    
    # 2. Vendas nos Últimos 12 Meses (O que importa para o cálculo)
    venda_12m = conn.execute(f"""
        SELECT SUM(quantidade)
        FROM sqlite_db.vendas 
        WHERE cod_produto = '{cod_alvo}'
        AND CAST(data_movimento AS DATE) >= (CURRENT_DATE - INTERVAL '365 days')
    """).fetchone()[0]
    
    venda_12m = venda_12m if venda_12m else 0
    media_calc = venda_12m / 365.0
    
    print(f"Venda Últimos 365 dias: {venda_12m} peças")
    print(f"Média Diária Real (Total/365): {media_calc:.4f}")
    print("-" * 50)
    
    if venda_12m == 0:
        print("✅ DIAGNÓSTICO: O item está MORTO (0 vendas em 1 ano).")
        print("A média DEVE ser 0.00.")
    else:
        print("⚠️ DIAGNÓSTICO: Existem vendas ocultas nos últimos 12 meses.")
        # Mostra as vendas fantasmas
        print("\n📅 Detalhe das vendas encontradas (últimos 12 meses):")
        detalhe = conn.execute(f"""
            SELECT data_movimento, quantidade 
            FROM sqlite_db.vendas 
            WHERE cod_produto = '{cod_alvo}'
            AND CAST(data_movimento AS DATE) >= (CURRENT_DATE - INTERVAL '365 days')
            ORDER BY data_movimento DESC
        """).df()
        print(detalhe)

if __name__ == "__main__":
    main()