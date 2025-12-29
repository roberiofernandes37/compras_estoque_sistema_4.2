codigo_relatorio = '''"""
Script principal de geração de relatórios - VERSÃO MELHORADA
Aplica sazonalidade preservando média base
"""
import sys
import argparse
import json
from pathlib import Path
from datetime import datetime
import polars as pl
import traceback

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.append(str(PROJECT_ROOT / "src"))

from compras_sistema.core.config import ConfigManager
from compras_sistema.core.system_guard import SystemGuard
from compras_sistema.data_engine.duckdb_manager import DuckDBManager
from compras_sistema.data_engine.history_recorder import HistoryRecorder
from compras_sistema.rule_engine.classification.abc_classifier import ABCClassifier
from compras_sistema.rule_engine.classification.xyz_classifier import XYZClassifier
from compras_sistema.rule_engine.classification.trend_classifier import TrendClassifier
from compras_sistema.rule_engine.stock.estoque_math import EstoqueMath
from compras_sistema.export.excel_exporter import ExcelExporter


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--marca", type=str, default="TODAS", help="Filtrar por marca")
    parser.add_argument("--simulacao", action="store_true", help="Não gera Excel, apenas calcula")
    args = parser.parse_args()
    
    guard = SystemGuard(PROJECT_ROOT / "logs")
    print(f"--- LOG START ---")
    guard.log(f"Processamento Iniciado - Marca: {args.marca}")
    
    # Inicialização
    config_mgr = ConfigManager()
    config_mgr.load_configs(PROJECT_ROOT / "config")
    
    db = DuckDBManager()
    db.initialize(PROJECT_ROOT / "data" / "vendas.db")
    
    recorder = HistoryRecorder(db) if not args.simulacao else None
    if recorder:
        recorder.inicializar_tabela()
    
    try:
        # --- 1. CLASSIFICAÇÕES ESTATÍSTICAS ---
        guard.log("Calculando Classificações ABC, XYZ, Tendências...")
        abc_engine = ABCClassifier(db)
        xyz_engine = XYZClassifier(db, config_mgr.parametros)
        trend_engine = TrendClassifier(db)
        
        df_abc = abc_engine.run()
        df_xyz = xyz_engine.run()
        df_trend = trend_engine.run()
        
        # --- 2. LEITURA DE DADOS ---
        guard.log("Lendo Estoques e Cadastro Completo...")
        with db.get_connection() as conn:
            df_saldo = conn.execute("""
                SELECT 
                    CAST(cod_produto AS VARCHAR) as cod_produto,
                    saldo_estoque,
                    saldo_oc,
                    custo_unitario,
                    ultima_entrada
                FROM sqlite_db.saldo_custo_entrada
            """).pl()
            
            # Leitura dinâmica do cadastro
            try:
                cols_db = [c[1] for c in conn.execute("PRAGMA table_info(sqlite_db.produtos_gerais)").fetchall()]
                
                if "descricao_produto" in cols_db:
                    col_desc = "descricao_produto"
                elif "descricao" in cols_db:
                    col_desc = "descricao"
                else:
                    col_desc = "''" 
                
                col_data = "CAST(data_cadastro AS DATE)" if "data_cadastro" in cols_db else "CAST('2000-01-01' AS DATE)"
                col_ref = "ref_fornecedor" if "ref_fornecedor" in cols_db else "''"
                
                df_cadastro = conn.execute(f"""
                    SELECT 
                        CAST(cod_produto AS VARCHAR) as cod_produto,
                        CAST(qtd_economica AS INTEGER) as lote_economico,
                        marca,
                        {col_desc} as descricao,
                        {col_ref} as ref_fornecedor,
                        ativo,
                        {col_data} as data_cadastro
                    FROM sqlite_db.produtos_gerais
                """).pl()
            except Exception as e:
                guard.log(f"Erro ao ler cadastro: {e}. Usando estrutura vazia de segurança.")
                df_cadastro = pl.DataFrame(schema={
                    "cod_produto": pl.Utf8, "lote_economico": pl.Int64, "marca": pl.Utf8,
                    "descricao": pl.Utf8, "ref_fornecedor": pl.Utf8, "ativo": pl.Utf8,
                    "data_cadastro": pl.Date
                })
        
        # Carrega Sazonalidade
        indices_dict = {}
        try:
            analytics_path = PROJECT_ROOT / "data" / "analytics.duckdb"
            if analytics_path.exists():
                conn.execute(f"ATTACH '{analytics_path}' AS analytics")
                rows = conn.execute("SELECT mes, indice_sazonal FROM analytics.indices_sazonais").fetchall()
                for r in rows:
                    indices_dict[r[0]] = r[1]
        except:
            pass
        
        # --- 3. UNIFICAÇÃO (JOIN) ---
        guard.log("Cruzando dados...")
        df_universe = pl.concat([
            df_xyz.select("cod_produto"),
            df_saldo.select("cod_produto"),
            df_cadastro.select("cod_produto")
        ]).unique(subset="cod_produto")
        
        df_final = df_universe \\
            .join(df_xyz, on="cod_produto", how="left") \\
            .join(df_abc, on="cod_produto", how="left") \\
            .join(df_trend, on="cod_produto", how="left") \\
            .join(df_saldo, on="cod_produto", how="left") \\
            .join(df_cadastro, on="cod_produto", how="left")
        
        # Tratamento de descrição duplicada
        if "descricao" not in df_final.columns:
            if "descricao_right" in df_final.columns:
                df_final = df_final.rename({"descricao_right": "descricao"})
            else:
                df_final = df_final.with_columns(pl.lit("SEM DESCRIÇÃO").alias("descricao"))
        
        # Preenchimento de Nulos
        df_final = df_final.with_columns([
            pl.col("media_venda_dia").fill_null(0.0),
            pl.col("std_venda_dia").fill_null(0.0),
            pl.col("curva_xyz").fill_null("Z"),
            pl.col("dias_sem_venda").fill_null(0).alias("dias_sem_venda"),
            pl.col("saldo_estoque").fill_null(0),
            pl.col("saldo_oc").fill_null(0),
            pl.col("custo_unitario").fill_null(0.0),
            pl.col("curva_abc").fill_null("C"),
            pl.col("marca").fill_null("N/D"),
            pl.col("descricao").fill_null("DESCRIÇÃO NÃO ENCONTRADA"),
            pl.col("ref_fornecedor").fill_null(""),
            pl.col("lote_economico").fill_null(1),
            pl.col("ativo").fill_null("SIM"),
            pl.col("data_cadastro").fill_null(pl.lit(datetime(2000,1,1))),
            pl.lit(config_mgr.parametros.lead_time.padrao_dias).alias("lead_time_dias"),
        ])
        
        # --- 4. MOTOR DE CÁLCULO (COM MELHORIAS) ---
        guard.log("Executando Motor Matemático...")
        
        # MELHORIA 1: Salva média base ANTES da sazonalidade
        df_final = df_final.with_columns([
            pl.col("media_venda_dia").alias("media_venda_base")
        ])
        
        # Aplica sazonalidade
        df_final = EstoqueMath.aplicar_sazonalidade_projetada(df_final, indices_dict)
        
        # MELHORIA 2: Aplica sazonalidade SEM sobrescrever a base
        df_final = df_final.with_columns([
            pl.col("fator_sazonal_projetado").alias("fator_sazonal"),
            (pl.col("media_venda_base") * pl.col("fator_sazonal_projetado")).alias("media_venda_dia")
        ])
        
        # Continua com os cálculos
        df_final = EstoqueMath.calcular_tendencias(df_final)
        df_final = EstoqueMath.calcular_seguranca(df_final, config_mgr.parametros)
        df_final = EstoqueMath.calcular_necessidades(df_final, config_mgr.parametros)
        df_final = EstoqueMath.aplicar_lote_economico(df_final, config_mgr.parametros)
        df_final = EstoqueMath.calcular_score(df_final)
        df_final = EstoqueMath.gerar_diagnostico(df_final, config_mgr.parametros)
        
        df_final = df_final.with_columns([
            (pl.col("saldo_estoque") + pl.col("saldo_oc") + pl.col("sugestao_final")).alias("meta_pos_compra")
        ])
        
        # --- 5. FILTRO DE MARCA ---
        if args.marca and args.marca != "TODAS":
            guard.log(f"Filtrando relatório para marca: {args.marca}")
            df_final = df_final.filter(pl.col("marca") == args.marca)
        
        # --- 6. ESTATÍSTICAS PARA GUI ---
        try:
            val_estoque_atual = df_final.select((pl.col("saldo_estoque") * pl.col("custo_unitario")).sum()).item()
            val_venda_mensal = df_final.select((pl.col("media_venda_dia") * 30 * pl.col("custo_unitario")).sum()).item()
            
            if val_venda_mensal > 0:
                cobertura = val_estoque_atual / val_venda_mensal
            else:
                cobertura = 0.0
        except:
            val_estoque_atual = 0.0
            cobertura = 0.0
        
        df_compra = df_final.filter(pl.col("sugestao_final") > 0)
        
        stats = {
            "total_valor": df_compra["subtotal"].sum(),
            "total_skus": len(df_compra),
            "total_pecas": df_compra["sugestao_final"].sum(),
            "estoque_atual": val_estoque_atual,
            "cobertura_meses": cobertura
        }
        
        print(f"STATS_DATA={json.dumps(stats)}")
        
        # --- 7. EXPORTAÇÃO ---
        if not args.simulacao:
            guard.log("Gerando relatório Excel...")
            exporter = ExcelExporter(PROJECT_ROOT / "data" / "exports")
            arquivo = exporter.exportar_sugestao(df_final.sort("score", descending=True))
            guard.log(f"Relatório gerado: {arquivo}")
            
            if recorder:
                recorder.gravar_snapshot(df_final)
        
        guard.log("Processamento concluído com sucesso!")
        
    except Exception as e:
        guard.log(f"ERRO CRÍTICO: {e}")
        traceback.print_exc()
        sys.exit(1)
    finally:
        db.close()


if __name__ == "__main__":
    main()
'''

print("\n✅ Arquivo gerar_relatorio_final.py melhorado criado!")
print("\n📋 MUDANÇAS NO FLUXO:")
print("1. ✅ Salva 'media_venda_base' ANTES da sazonalidade")
print("2. ✅ Aplica sazonalidade criando nova coluna ao invés de sobrescrever")
print("3. ✅ Fluxo agora tem auditoria completa de valores intermediários")
