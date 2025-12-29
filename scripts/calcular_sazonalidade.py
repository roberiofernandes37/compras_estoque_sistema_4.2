import sys
from pathlib import Path
import duckdb

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.append(str(PROJECT_ROOT / "src"))

def main():
    print("🌊 Calculando Índices de Sazonalidade...")
    
    # Caminhos dos arquivos
    sqlite_path = PROJECT_ROOT / "data" / "vendas.db"
    
    # Usaremos um arquivo DuckDB persistente para salvar os índices
    # Assim o outro script consegue ler depois
    duck_path = PROJECT_ROOT / "data" / "analytics.duckdb"
    
    # Conecta (ou cria) o banco analítico
    conn = duckdb.connect(str(duck_path))
    
    try:
        # 1. Instala suporte a SQLite (caso não tenha)
        conn.execute("INSTALL sqlite; LOAD sqlite;")
        
        # 2. Anexa o banco de vendas (O PULO DO GATO QUE FALTAVA)
        # Agora o DuckDB enxerga o 'sqlite_db'
        print(f"🔌 Conectando ao histórico: {sqlite_path}")
        conn.execute(f"ATTACH '{sqlite_path}' AS sqlite_db (TYPE SQLITE)")
        
        # 3. Cria tabela de índices
        print("📊 Processando estatísticas mensais...")
        conn.execute("""
            CREATE OR REPLACE TABLE indices_sazonais AS
            WITH vendas_mensais AS (
                SELECT 
                    EXTRACT(MONTH FROM CAST(data_movimento AS DATE)) as mes,
                    SUM(quantidade) as qtd_total
                FROM sqlite_db.vendas
                WHERE CAST(data_movimento AS DATE) >= (CURRENT_DATE - INTERVAL '24 months')
                GROUP BY 1
            ),
            media_global AS (
                SELECT AVG(qtd_total) as media_ano FROM vendas_mensais
            )
            SELECT 
                mes,
                qtd_total / media_global.media_ano as indice_sazonal
            FROM vendas_mensais, media_global
            ORDER BY mes;
        """)
        
        print("✅ Índices calculados e salvos em 'analytics.duckdb':")
        print(conn.execute("SELECT * FROM indices_sazonais").df())
        
    except Exception as e:
        print(f"❌ Erro: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()