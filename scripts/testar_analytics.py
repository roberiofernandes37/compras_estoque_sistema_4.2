import sys
from pathlib import Path
import polars as pl

# Setup de Caminhos
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.append(str(PROJECT_ROOT / "src"))

from compras_sistema.data_engine.duckdb_manager import DuckDBManager
from compras_sistema.data_engine.analytics_service import AnalyticsService

def main():
    print("--- 📊 TESTE DO SERVIÇO DE ANALYTICS (CORRIGIDO) ---")
    
    # 1. Conexão
    db_path = PROJECT_ROOT / "data" / "vendas.db"
    print(f"📂 Conectando ao banco: {db_path}")
    
    db = DuckDBManager()
    db.initialize(db_path)
    
    # --- DIAGNÓSTICO DO BANCO DE DADOS ---
    print("\n🔍 Verificando tabelas existentes no DuckDB:")
    with db.get_connection() as conn:
        tabelas = conn.execute("SHOW TABLES").fetchall()
        lista_tabelas = [t[0] for t in tabelas]
        print(f"   Tabelas encontradas: {lista_tabelas}")
        
        if "historico_snapshots" not in lista_tabelas:
            print("   ⚠️ AVISO CRÍTICO: Tabela 'historico_snapshots' NÃO EXISTE.")
            print("   -> Solução: Rode 'python scripts/gerar_relatorio_final.py' novamente para criar a tabela.")
        else:
            qtd = conn.execute("SELECT COUNT(*) FROM historico_snapshots").fetchone()[0]
            print(f"   ✅ Tabela 'historico_snapshots' existe com {qtd} registros.")

    service = AnalyticsService(db)
    
    # 2. Teste de KPIs Atuais
    print("\n1. Buscando KPIs Atuais...")
    kpis = service.get_kpis_atuais()
    print(f"   Resultado: {kpis}")
    
    # 3. Teste de Tendência (Gráfico)
    print("\n2. Buscando Tendência de Cobertura (Histórico)...")
    
    # CORREÇÃO AQUI: O parâmetro correto é 'dias_historico', não 'dias'
    df_tendencia = service.get_tendencia_cobertura(dias_historico=30)
    
    if df_tendencia.is_empty():
        print("⚠️ DataFrame vazio! O serviço rodou, mas não achou dados no período.")
    else:
        print(f"✅ Sucesso! Retornou {len(df_tendencia)} linhas.")
        print(df_tendencia)
        
        print("\n--- Prévia dos Dados para o Gráfico ---")
        for row in df_tendencia.iter_rows(named=True):
            print(f"Data: {row['data']} | Curva: {row['curva_abc']} | Cobertura: {row['cobertura_meses']} meses")

if __name__ == "__main__":
    main()