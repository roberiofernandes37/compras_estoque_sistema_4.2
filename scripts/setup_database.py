import duckdb
from pathlib import Path
import sys

# Setup de caminhos
ROOT_DIR = Path(__file__).parent.parent
DATA_DIR = ROOT_DIR / "data"
DB_PATH = DATA_DIR / "vendas.db"

def setup_database():
    print("🚀 Iniciando criação do Banco de Dados de Vendas...")
    
    # 1. Encontrar o arquivo de vendas (CSV)
    # Procura arquivos que contenham "Movimento" ou "Vendas" no nome
    sales_files = list(DATA_DIR.glob("*ovimento*.csv")) + list(DATA_DIR.glob("*endas*.csv"))
    
    if not sales_files:
        print("❌ Nenhum arquivo de vendas encontrado na pasta 'data/'!")
        print("   Por favor, coloque o arquivo CSV de vendas (ex: 'Movimento.csv') lá.")
        return
    
    csv_path = sales_files[0]
    print(f"📂 Arquivo de origem detectado: {csv_path.name}")

    # 2. Conectar ao DuckDB (em memória para processamento rápido)
    con = duckdb.connect()
    
    try:
        # 3. Ler o CSV e tratar colunas
        # Baseado no PDF, mapeamos os nomes originais para o padrão do sistema
        print("⏳ Lendo CSV e convertendo dados...")
        
        # Cria uma view temporária lendo o CSV
        con.execute(f"""
            CREATE VIEW raw_vendas AS 
            SELECT * FROM read_csv_auto('{str(csv_path)}', normalize_names=True)
        """)
        
        # Verifica quais colunas existem para garantir o mapeamento
        columns = [c[0] for c in con.execute("DESCRIBE raw_vendas").fetchall()]
        print(f"   Colunas detectadas: {columns}")
        
        # Query de transformação (Adapte os nomes 'cod_produto', 'data', etc se necessário)
        # O DuckDB normalize_names remove acentos e espaços (ex: "Cód. Produto" vira "cod_produto")
        query = """
            SELECT 
                CAST(cod_produto AS VARCHAR) as cod_produto,
                CAST(data AS DATE) as data_movimento,
                CAST(qtde AS INTEGER) as quantidade,
                CAST(total AS DECIMAL(10,2)) as valor_total,
                CAST(cod_clifor AS INTEGER) as cod_cliente,
                uf as uf_cliente
            FROM raw_vendas
            WHERE data IS NOT NULL
        """
        
        # 4. Salvar no SQLite
        print(f"💾 Salvando em {DB_PATH.name}...")
        
        # Remove banco antigo se existir para recriar do zero
        if DB_PATH.exists():
            DB_PATH.unlink()
            
        con.execute(f"ATTACH '{str(DB_PATH)}' AS sqlite_db (TYPE SQLITE)")
        con.execute(f"CREATE TABLE sqlite_db.vendas AS {query}")
        
        # Validação
        count = con.execute("SELECT COUNT(*) FROM sqlite_db.vendas").fetchone()[0]
        print(f"\n✅ Sucesso! {count:,} registros de vendas importados.")
        
        # Mostra prévia
        print("\n📊 Amostra dos dados gravados:")
        print(con.execute("SELECT * FROM sqlite_db.vendas LIMIT 5").df())
        
    except Exception as e:
        print(f"\n❌ Erro durante a importação: {e}")
        print("Dica: Verifique se os nomes das colunas no CSV batem com a query.")
        
    finally:
        con.close()

if __name__ == "__main__":
    setup_database()