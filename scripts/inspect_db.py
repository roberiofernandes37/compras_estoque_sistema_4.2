import duckdb
from pathlib import Path

# Caminhos
BASE_DIR = Path(__file__).parent.parent
DB_PATH = BASE_DIR / "data" / "vendas.db"

def inspect():
    print(f"🔍 Inspecionando banco de dados: {DB_PATH}")
    
    if not DB_PATH.exists():
        print("❌ ERRO: O arquivo 'vendas.db' não foi encontrado na pasta 'data/'")
        return

    try:
        con = duckdb.connect(":memory:")
        con.execute(f"ATTACH '{str(DB_PATH)}' AS sqlite_db (TYPE SQLITE, READ_ONLY)")
        
        # Verifica tabela
        print("\n📋 Estrutura da tabela 'vendas':")
        print(f"{'Nome da Coluna':<25} | {'Tipo de Dado'}")
        print("-" * 45)
        
        # CORREÇÃO: Usamos fetchall() (listas nativas) em vez de .df()
        columns = con.execute("DESCRIBE sqlite_db.vendas").fetchall()
        
        for col in columns:
            name = col[0]
            dtype = col[1]
            print(f"{name:<25} | {dtype}")

        print("\n📊 Amostra de dados (Via Polars):")
        # Aqui usamos .pl() porque o Polars já está instalado e testado
        print(con.execute("SELECT * FROM sqlite_db.vendas LIMIT 3").pl())

    except Exception as e:
        print(f"❌ Erro ao ler o banco: {e}")

if __name__ == "__main__":
    inspect()
