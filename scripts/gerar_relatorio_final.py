import sys
import argparse
import json
from pathlib import Path
from datetime import datetime
import polars as pl
import traceback
from pandera.errors import SchemaError

# ==============================================================================
# 0. SETUP DE AMBIENTE E CAMINHOS
# ==============================================================================
# Adiciona o diretório 'src' ao path para importar os módulos do sistema
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.append(str(PROJECT_ROOT / "src"))

# Imports dos Módulos do Sistema (Core e Engines)
from compras_sistema.core.config import ConfigManager
from compras_sistema.core.system_guard import SystemGuard
from compras_sistema.core.reporter import ExecutionReporter
from compras_sistema.data_engine.duckdb_manager import DuckDBManager
from compras_sistema.data_engine.history_recorder import HistoryRecorder

# Imports das Regras de Negócio (Classificadores e Matemática)
from compras_sistema.rule_engine.classification.abc_classifier import ABCClassifier
from compras_sistema.rule_engine.classification.xyz_classifier import XYZClassifier
from compras_sistema.rule_engine.classification.trend_classifier import TrendClassifier
from compras_sistema.rule_engine.stock.estoque_math import EstoqueMath
from compras_sistema.export.excel_exporter import ExcelExporter
from compras_sistema.utils.sanitizer import sanear_dados_dataframe

# Tenta importar o Validador (Pandera), mas não quebra se faltar
try:
    from compras_sistema.rule_engine.validators.input_schema import InputCalcSchema
except ImportError:
    InputCalcSchema = None

def main():
    # --- Configuração de Argumentos via Linha de Comando ---
    parser = argparse.ArgumentParser()
    parser.add_argument("--marca", type=str, default="TODAS", help="Filtrar processamento por marca")
    parser.add_argument("--simulacao", action="store_true", help="Modo Simulação: Não gera Excel, apenas calcula")
    args = parser.parse_args()
    
    # --- Inicialização de Logs e Guardiões ---
    guard = SystemGuard(PROJECT_ROOT / "logs")
    print(f"--- LOG START ---") # Marcador visual para o Launcher
    guard.log(f"🚀 Processamento Iniciado - Filtro Marca: {args.marca}")
    
    # Reporter: Responsável por enviar dados JSON para o Dashboard
    reporter = ExecutionReporter(PROJECT_ROOT / "data")
    reporter.limpar_stats_anteriores()
    
    # Carregamento de Configurações (YAML)
    config_mgr = ConfigManager()
    config_mgr.load_configs(PROJECT_ROOT / "config")
    
    # Inicialização do Banco de Dados (Com Health Check)
    db = DuckDBManager()
    db.initialize(PROJECT_ROOT / "data" / "vendas.db")
    
    # Inicialização do Gravador de Histórico (apenas se não for simulação)
    recorder = HistoryRecorder(db) if not args.simulacao else None
    if recorder:
        recorder.inicializar_tabela()
    
    try:
        # ==============================================================================
        # 1. MOTOR DE CLASSIFICAÇÃO (ABC, XYZ, TENDÊNCIAS)
        # ==============================================================================
        guard.log("📊 Calculando Classificações Estatísticas (ABC, XYZ, Trends)...")
        
        abc_engine = ABCClassifier(db)
        xyz_engine = XYZClassifier(db, config_mgr.parametros)
        trend_engine = TrendClassifier(db)
        
        df_abc = abc_engine.run()
        df_xyz = xyz_engine.run()
        df_trend = trend_engine.run()
        
        # ==============================================================================
        # 2. LEITURA DE DADOS (SNAPSHOT DO ERP)
        # ==============================================================================
        guard.log("💾 Lendo Estoques e Cadastro Completo do Banco de Dados...")
        
        with db.get_connection() as conn:
            # 2.1 Leitura de Saldos e Custos
            df_saldo = conn.execute("""
                SELECT 
                    CAST(cod_produto AS VARCHAR) as cod_produto,
                    saldo_estoque,
                    saldo_oc,
                    custo_unitario,
                    ultima_entrada
                FROM sqlite_db.saldo_custo_entrada
            """).pl()
            
            # 2.2 Leitura Dinâmica do Cadastro de Produtos
            # Verifica quais colunas existem para evitar erros se o banco mudar
            try:
                cols_db = [c[1] for c in conn.execute("PRAGMA table_info(sqlite_db.produtos_gerais)").fetchall()]
                
                # Mapeamento seguro de colunas
                col_desc = "descricao_produto" if "descricao_produto" in cols_db else ("descricao" if "descricao" in cols_db else "''")
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
                guard.log(f"⚠️ Erro parcial ao ler cadastro: {e}. Usando estrutura de fallback.")
                df_cadastro = pl.DataFrame(schema={
                    "cod_produto": pl.Utf8, "lote_economico": pl.Int64, "marca": pl.Utf8,
                    "descricao": pl.Utf8, "ref_fornecedor": pl.Utf8, "ativo": pl.Utf8,
                    "data_cadastro": pl.Date
                })
        
        # 2.3 Carregamento de Sazonalidade (Analytics)
        indices_dict = {}
        try:
            analytics_path = PROJECT_ROOT / "data" / "analytics.duckdb"
            if analytics_path.exists():
                with db.get_connection() as conn:
                    conn.execute(f"ATTACH '{analytics_path}' AS analytics")
                    rows = conn.execute("SELECT mes, indice_sazonal FROM analytics.indices_sazonais").fetchall()
                    conn.execute("DETACH analytics")
                    for r in rows:
                        indices_dict[r[0]] = r[1]
        except Exception:
            pass # Sazonalidade é opcional, segue sem erro crítico se falhar

        # ==============================================================================
        # 3. UNIFICAÇÃO DOS DADOS (O "BIG JOIN")
        # ==============================================================================
        guard.log("🔗 Cruzando tabelas (Join)...")
        
        # Cria um universo com todos os códigos de produto encontrados em qualquer tabela
        df_universe = pl.concat([
            df_xyz.select("cod_produto"),
            df_saldo.select("cod_produto"),
            df_cadastro.select("cod_produto")
        ]).unique(subset="cod_produto")
        
        # Realiza os Left Joins para montar a tabela mestre
        df_final = (df_universe
            .join(df_xyz, on="cod_produto", how="left")
            .join(df_abc, on="cod_produto", how="left")
            .join(df_trend, on="cod_produto", how="left")
            .join(df_saldo, on="cod_produto", how="left")
            .join(df_cadastro, on="cod_produto", how="left"))
        
        # Garante que temos descrição
        if "descricao" not in df_final.columns:
            if "descricao_right" in df_final.columns:
                df_final = df_final.rename({"descricao_right": "descricao"})
            else:
                df_final = df_final.with_columns(pl.lit("SEM DESCRIÇÃO").alias("descricao"))
        
        # ==============================================================================
        # 4. TRATAMENTO E HIGIENIZAÇÃO DE DADOS
        # ==============================================================================
        
        # Recupera Lead Time do Config
        lead_time_padrao = config_mgr.parametros.lead_time.padrao_dias
        if isinstance(lead_time_padrao, dict):
            lead_time_padrao = lead_time_padrao.get('padrao_dias', 10)
            
        # 4.1 Preenchimento de Nulos (FillNA) - Bloco Expandido para Clareza
        df_final = df_final.with_columns([
            # Métricas de Venda
            pl.col("media_venda_dia").fill_null(0.0),
            pl.col("std_venda_dia").fill_null(0.0),
            pl.col("dias_sem_venda").fill_null(0).alias("dias_sem_venda"),
            
            # Dados Financeiros/Logísticos
            pl.col("saldo_estoque").fill_null(0),
            pl.col("saldo_oc").fill_null(0),
            pl.col("custo_unitario").fill_null(0.0),
            
            # Classificações
            pl.col("curva_abc").fill_null("C"),
            pl.col("curva_xyz").fill_null("Z"),
            
            # Cadastro
            pl.col("marca").fill_null("N/D"),
            pl.col("descricao").fill_null("DESCRIÇÃO NÃO ENCONTRADA"),
            pl.col("ref_fornecedor").fill_null(""),
            pl.col("lote_economico").fill_null(1).map_elements(lambda x: max(1, x), return_dtype=pl.Int64),
            pl.col("ativo").fill_null("SIM"),
            pl.col("data_cadastro").fill_null(pl.lit(datetime(2000,1,1)).cast(pl.Date)),
            
            # Parâmetro Global
            pl.lit(lead_time_padrao).alias("lead_time_dias"),
        ])

        # 4.2 Detecção de Anomalias (Cria alertas visuais no Excel)
        df_final = df_final.with_columns([
            pl.when(pl.col("saldo_estoque") < 0)
            .then(pl.lit("ESTOQUE NEGATIVO"))
            .when(pl.col("saldo_oc") < 0)
            .then(pl.lit("OC NEGATIVA (ERRO ERP)"))
            .otherwise(None)
            .alias("alerta_dados")
        ])

        # 4.3 Validação Estrutural (Pandera) - Opcional mas Recomendado
        if InputCalcSchema:
            guard.log("🛡️ Validando integridade estrutural dos dados...")
            try:
                df_final = InputCalcSchema.validate(df_final)
            except SchemaError as e:
                guard.log(f"❌ ERRO DE VALIDAÇÃO: {e.schema.name if e.schema else 'Global'}")
                sys.exit(1)

        # 4.4 Sanitização Final de Negócios (Remove caracteres estranhos, espaços, etc)
        guard.log("🧹 Aplicando Sanitização de Negócios...")
        df_final = sanear_dados_dataframe(df_final)
        
        # ==============================================================================
        # 5. MOTOR MATEMÁTICO (CÁLCULO DE SUGESTÃO)
        # ==============================================================================
        guard.log("🧮 Executando Motor Matemático de Reposição...")
        
        # 5.1 Prepara Sazonalidade
        df_final = df_final.with_columns([pl.col("media_venda_dia").alias("media_venda_base")])
        df_final = EstoqueMath.aplicar_sazonalidade_projetada(df_final, indices_dict)
        df_final = df_final.with_columns([
            pl.col("fator_sazonal_projetado").alias("fator_sazonal"),
            (pl.col("media_venda_base") * pl.col("fator_sazonal_projetado")).alias("media_venda_dia")
        ])
        
        # 5.2 Cria DataFrame Matemático (Temporário)
        # Removemos OC negativa apenas para o cálculo, para não distorcer a conta.
        # No Excel final, o valor original negativo aparecerá com alerta.
        df_math = df_final.with_columns([pl.col("saldo_oc").clip(lower_bound=0)])
        
        # 5.3 Pipeline de Cálculo
        df_math = EstoqueMath.calcular_tendencias(df_math)
        df_math = EstoqueMath.calcular_seguranca(df_math, config_mgr.parametros)
        df_math = EstoqueMath.calcular_necessidades(df_math, config_mgr.parametros)
        df_math = EstoqueMath.aplicar_lote_economico(df_math, config_mgr.parametros)
        df_math = EstoqueMath.calcular_score(df_math)
        df_math = EstoqueMath.gerar_diagnostico(df_math, config_mgr.parametros)
        
        # 5.4 Mesclagem dos Resultados
        cols_calculadas = [
            "tendencia_vendas", "tendencia_clientes", "perfil_cliente", 
            "estoque_seguranca", "fator_z",                             
            "ponto_suprimento", "estoque_meta", "sugestao_bruta",       
            "media_calculo", "dias_vida",                               
            "lotes_cheios", "sugestao_final", "subtotal",               
            "score",                                                    
            "validacao_giro", "motivo_bloqueio",                        
            "calculado_mas_bloqueado", "status_diagnostico", 
            "cobertura_virtual_meses", "sugestao_calculada"
        ]
        
        df_final = df_final.with_columns(df_math.select(cols_calculadas))
        
        # 5.5 KPI Final de Posição
        df_final = df_final.with_columns([
            (pl.col("saldo_estoque") + pl.col("saldo_oc") + pl.col("sugestao_final")).alias("meta_pos_compra")
        ])
        
        # ==============================================================================
        # 6. PÓS-PROCESSAMENTO, ESTATÍSTICAS E EXPORTAÇÃO
        # ==============================================================================
        
        # 6.1 Aplicação de Filtro de Marca
        if args.marca and args.marca != "TODAS":
            guard.log(f"🔎 Filtrando relatório para marca: {args.marca}")
            df_final = df_final.filter(pl.col("marca") == args.marca)
        
        # -------------------------------------------------------------------------
        # 6.2 CÁLCULO DE TOTAIS GERAIS (Necessário para Porcentagens)
        # -------------------------------------------------------------------------
        df_final = df_final.with_columns([
            (pl.col("saldo_estoque") * pl.col("custo_unitario")).fill_null(0).alias("vlr_estoque_total"),
            pl.col("subtotal").fill_null(0).alias("vlr_compra_total")
        ])

        val_estoque_atual = df_final["vlr_estoque_total"].sum()
        total_skus_geral = len(df_final)

        # -------------------------------------------------------------------------
        # 6.3 LÓGICA DE RISCO: ESTOQUE OBSOLETO (COM 5 INDICADORES)
        # -------------------------------------------------------------------------
        dias_novo_param = config_mgr.parametros.produto.get('dias_lancamento', 180)
        
        # Filtro: Antigo AND Tem Saldo AND (Não Vende há 1 ano OR Bug de venda 0 dias mas antigo)
        df_obsoleto = df_final.filter(
            (pl.col("dias_vida") > dias_novo_param) &
            (pl.col("saldo_estoque") > 0) &
            (
                (pl.col("dias_sem_venda") > 364) | 
                ((pl.col("dias_sem_venda") == 0) & (pl.col("dias_vida") > 364))
            )
        )
        
        # Métricas Absolutas de Obsoleto
        obs_valor = df_obsoleto["vlr_estoque_total"].sum()
        obs_skus = len(df_obsoleto)
        obs_pecas = df_obsoleto["saldo_estoque"].sum()

        # Métricas Relativas (%)
        pct_obs_valor = (obs_valor / val_estoque_atual) if val_estoque_atual > 0 else 0.0
        pct_obs_skus = (obs_skus / total_skus_geral) if total_skus_geral > 0 else 0.0

        guard.log(f"🔎 Risco: {obs_skus} SKUs obsoletos. Valor: R$ {obs_valor:.2f} ({pct_obs_valor*100:.1f}% do estoque)")

        # -------------------------------------------------------------------------
        # 6.4 ESTATÍSTICAS AVANÇADAS (ABC + Dashboard)
        # -------------------------------------------------------------------------
        
        # Agregação ABC
        df_abc_summary = df_final.group_by("curva_abc").agg([
            pl.col("vlr_estoque_total").sum(),
            pl.col("vlr_compra_total").sum()
        ]).sort("curva_abc")
        
        abc_data = {}
        for row in df_abc_summary.iter_rows(named=True):
            curva = row['curva_abc'] if row['curva_abc'] else 'N/D'
            abc_data[curva] = {"estoque": row['vlr_estoque_total'], "compra": row['vlr_compra_total']}

        # Stats Gerais Finais
        val_compra_total = df_final["vlr_compra_total"].sum()
        
        try:
            val_venda_mensal = df_final.select((pl.col("media_venda_dia") * 30 * pl.col("custo_unitario")).sum()).item()
            cobertura = (val_estoque_atual / val_venda_mensal) if val_venda_mensal > 0 else 0.0
        except:
            cobertura = 0.0
        
        df_compra = df_final.filter(pl.col("sugestao_final") > 0)
        
        # PAYLOAD COMPLETO PARA O DASHBOARD (JSON)
        stats_payload = {
            # Gerais
            "total_valor": val_compra_total,
            "total_skus": len(df_compra),
            "total_pecas": df_compra["sugestao_final"].sum(),
            "estoque_atual": val_estoque_atual,
            "cobertura_meses": cobertura,
            "abc_breakdown": abc_data,
            
            # Detalhamento de Risco (Novos Campos)
            "obs_valor": obs_valor,
            "obs_pct_valor": pct_obs_valor,
            "obs_skus": obs_skus,
            "obs_pct_skus": pct_obs_skus,
            "obs_pecas": obs_pecas
        }
        
        # Envia para o Frontend via arquivo seguro
        reporter.salvar_stats(stats_payload)
        guard.log(f"✅ Estatísticas calculadas e enviadas ao Dashboard.")
        
        # 6.5 Exportação Excel e Histórico
        if not args.simulacao:
            guard.log("📑 Gerando relatório Excel detalhado...")
            exporter = ExcelExporter(PROJECT_ROOT / "data" / "exports")
            
            # Ordenação inteligente: Primeiro os problemas (Alertas), depois os Melhores (Score)
            df_final = df_final.sort(["alerta_dados", "score"], descending=[True, True])
            
            arquivo = exporter.exportar_sugestao(df_final)
            guard.log(f"✅ Relatório disponível em: {arquivo}")
            
            if recorder:
                guard.log("🕰️ Gravando snapshot no Histórico...")
                contexto = {
                    "marca": args.marca,
                    "usuario": "Usuario_Padrao",
                    "stats": stats_payload,
                    "config": config_mgr.parametros.model_dump()
                }
                recorder.gravar_snapshot(df_final, contexto)
        
        guard.log("🏁 Processamento concluído com sucesso!")
        
    except Exception as e:
        guard.log(f"❌ ERRO CRÍTICO DURANTE EXECUÇÃO: {e}")
        traceback.print_exc()
        sys.exit(1)
    finally:
        db.close()

if __name__ == "__main__":
    main()