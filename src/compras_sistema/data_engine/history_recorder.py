import polars as pl
import duckdb
from datetime import datetime
from pathlib import Path
import json
import structlog
from typing import Dict, Any

logger = structlog.get_logger(__name__)

class HistoryRecorder:
    """
    Grava o histórico de execuções para auditoria e análise de tendências.
    Padrão Arquitetural: Header-Detail (Mestre-Detalhe).
    """
    
    def __init__(self, db_manager):
        # Usamos um arquivo separado para não pesar o banco transacional principal
        self.history_db_path = Path("data/analytics.duckdb")
        self.db_manager = db_manager
        # Garante que a pasta data existe
        self.history_db_path.parent.mkdir(parents=True, exist_ok=True)

    def inicializar_tabela(self):
        """Cria o esquema relacional (Header e Detalhes) se não existir."""
        try:
            with duckdb.connect(str(self.history_db_path)) as conn:
                # 1. Sequência para gerar IDs únicos de execução
                conn.execute("CREATE SEQUENCE IF NOT EXISTS seq_execucao_id")

                # 2. Tabela HEADER (Metadados da Execução)
                # Guarda QUEM rodou, QUANDO, com quais PARÂMETROS e TOTAIS.
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS historico_execucoes (
                        id_execucao INTEGER PRIMARY KEY DEFAULT nextval('seq_execucao_id'),
                        data_registro TIMESTAMP,
                        marca_filtro VARCHAR,
                        usuario VARCHAR,
                        total_sugestao_valor DOUBLE,
                        total_itens_comprar INTEGER,
                        config_snapshot JSON  -- O "Segredo": salva como estava o YAML no momento
                    )
                """)

                # 3. Tabela DETALHES (Os Produtos em si)
                # Guarda o estado de cada item naquele momento.
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS historico_detalhes (
                        id_execucao INTEGER, -- Chave Estrangeira (Virtual)
                        cod_produto VARCHAR,
                        descricao VARCHAR,
                        curva_abc VARCHAR,
                        curva_xyz VARCHAR,
                        saldo_estoque INTEGER,
                        saldo_oc INTEGER,
                        media_venda_dia DOUBLE,
                        custo_unitario DOUBLE,
                        sugestao_final INTEGER,
                        fator_z DOUBLE,
                        motivo_bloqueio VARCHAR
                    )
                """)
                
                logger.info("schema_historico_verificado", path=str(self.history_db_path))
                
        except Exception as e:
            logger.error("erro_inicializar_historico", error=str(e))

    def gravar_snapshot(self, df_final: pl.DataFrame, context_data: Dict[str, Any]):
        """
        Grava uma execução completa (Header + Detalhes).
        
        Args:
            df_final: DataFrame com os produtos calculados.
            context_data: Dicionário contendo metadados (marca, config, stats).
        """
        try:
            logger.info("iniciando_gravacao_historico")
            
            # Extração de Contexto
            marca = context_data.get('marca', 'TODAS')
            usuario = context_data.get('usuario', 'SYSTEM')
            stats = context_data.get('stats', {})
            config_dict = context_data.get('config', {}) # Configuração completa em dict

            # Prepara JSON de config (converte objetos Pydantic se necessário)
            if hasattr(config_dict, 'model_dump_json'):
                config_json = config_dict.model_dump_json()
            else:
                config_json = json.dumps(config_dict, default=str)

            with duckdb.connect(str(self.history_db_path)) as conn:
                # --- PASSO 1: INSERIR HEADER E PEGAR ID ---
                # Usamos RETURNING id_execucao para saber qual ID foi gerado
                query_header = """
                    INSERT INTO historico_execucoes (
                        data_registro, marca_filtro, usuario, 
                        total_sugestao_valor, total_itens_comprar, config_snapshot
                    ) VALUES (
                        current_timestamp, ?, ?, ?, ?, ?
                    ) RETURNING id_execucao
                """
                
                # Executa e pega o ID gerado
                id_execucao = conn.execute(query_header, [
                    marca, 
                    usuario, 
                    stats.get('total_valor', 0.0),
                    stats.get('total_skus', 0),
                    config_json
                ]).fetchone()[0]
                
                logger.info("historico_header_criado", id=id_execucao)

                # --- PASSO 2: PREPARAR DATAFRAME DE DETALHES ---
                # Adiciona o ID da execução em todas as linhas
                df_detalhes = df_final.with_columns(
                    pl.lit(id_execucao).alias("id_execucao")
                )

                # Seleciona e renomeia colunas para bater com a tabela SQL
                # Garante que campos opcionais existam
                cols_necessarias = [
                    "id_execucao", "cod_produto", "descricao", 
                    "curva_abc", "curva_xyz", 
                    "saldo_estoque", "saldo_oc", "media_venda_dia", "custo_unitario", 
                    "sugestao_final", "fator_z", "motivo_bloqueio"
                ]
                
                # Preenche colunas faltantes com null/default para não quebrar o insert
                for col in cols_necessarias:
                    if col not in df_detalhes.columns:
                        df_detalhes = df_detalhes.with_columns(pl.lit(None).alias(col))

                # Ordena colunas para inserção
                df_insert = df_detalhes.select(cols_necessarias)

                # --- PASSO 3: INSERT BULK (Alta Performance) ---
                # O DuckDB permite inserir direto de um DataFrame Polars
                conn.register("view_temp_insert", df_insert)
                conn.execute("INSERT INTO historico_detalhes SELECT * FROM view_temp_insert")
                conn.unregister("view_temp_insert")
                
                logger.info("historico_detalhes_gravado", linhas=len(df_insert))

        except Exception as e:
            logger.error("erro_fatal_gravacao_historico", error=str(e))
            # Não damos raise aqui para não travar a geração do Excel se o log falhar
            # Mas o erro fica registrado no structlog