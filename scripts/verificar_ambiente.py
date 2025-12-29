import sys
from pathlib import Path

# Adiciona o src ao path para poder importar os módulos
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root / "src"))

import structlog
from compras_sistema.core.config import ConfigManager
from compras_sistema.data_engine.duckdb_manager import DuckDBManager

# Configurar logger simples para o teste
structlog.configure(
    processors=[structlog.processors.JSONRenderer()],
)

def main():
    print("🚀 Iniciando verificação do ambiente...\n")
    
    # 1. Teste de Configuração
    try:
        config_mgr = ConfigManager()
        config_mgr.load_configs(project_root / "config")
        print("✅ Configurações carregadas com sucesso!")
    except Exception as e:
        print(f"❌ Erro ao carregar configurações: {e}")
        return

    # 2. Teste de Banco de Dados
    sqlite_path = project_root / "data" / "vendas.db" 
    
    if not sqlite_path.exists():
        print(f"⚠️  Banco de dados não encontrado em: {sqlite_path}")
    
    try:
        db = DuckDBManager()
        db.initialize(sqlite_path)
        
        with db.get_connection() as conn:
            # Verifica se a nova tabela existe
            print("⏳ Verificando tabela 'saldo_custo_entrada'...")
            try:
                res = conn.execute("SELECT * FROM sqlite_db.saldo_custo_entrada LIMIT 3").pl()
                print("\n✅ Tabela de Estoque encontrada no Banco de Dados!")
                print("\n📊 Prévia dos Dados:")
                print(res)
            except Exception as e:
                print(f"\n❌ Tabela 'saldo_custo_entrada' NÃO encontrada no banco! Erro: {e}")
                
    except Exception as e:
        print(f"❌ Erro no DuckDB: {e}")

    print("\n🏁 Verificação concluída.")

if __name__ == "__main__":
    main()