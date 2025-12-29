import polars as pl
from compras_sistema.data_engine.duckdb_manager import DuckDBManager

class XYZClassifier:
    def __init__(self, db_manager: DuckDBManager, config):
        self.db = db_manager
        self.config = config

    def run(self) -> pl.DataFrame:
        # A Query continua a mesma (Corrigida para olhar apenas os últimos 365 dias)
        query = """
        WITH vendas_recentes AS (
            -- 1. Pega apenas vendas dos últimos 365 dias
            SELECT 
                cod_produto,
                CAST(data_movimento AS DATE) as data,
                SUM(quantidade) as qtd_dia
            FROM sqlite_db.vendas
            WHERE CAST(data_movimento AS DATE) >= (CURRENT_DATE - INTERVAL '365 days')
            GROUP BY 1, 2
        ),
        estatisticas AS (
            SELECT 
                cod_produto,
                -- Desvio padrão das vendas nos dias que houve venda
                STDDEV(qtd_dia) as std_venda_dia,
                
                -- MÉDIA CORRETA: Total vendido no ano / 365 dias
                -- (Isso garante que dias sem venda puxem a média para baixo)
                SUM(qtd_dia) / 365.0 as media_venda_dia,
                
                -- Coeficiente de Variação (CV)
                (STDDEV(qtd_dia) / NULLIF(AVG(qtd_dia), 0)) as cv
            FROM vendas_recentes
            GROUP BY 1
        )
        SELECT 
            CAST(cod_produto AS VARCHAR) as cod_produto,
            COALESCE(media_venda_dia, 0.0) as media_venda_dia,
            COALESCE(std_venda_dia, 0.0) as std_venda_dia,
            CASE 
                WHEN media_venda_dia <= 0 THEN 'Z' -- Se média é 0, é Z (Morto)
                WHEN cv <= 0.5 THEN 'X'  -- Muito estável
                WHEN cv <= 1.0 THEN 'Y'  -- Variável
                ELSE 'Z'                 -- Imprevisível
            END as curva_xyz
        FROM estatisticas
        """
        
        # --- CORREÇÃO AQUI ---
        # Usamos o gerenciador de contexto para abrir a conexão de forma segura
        with self.db.get_connection() as conn:
            # Executa a query e converte direto para Polars
            df = conn.execute(query).pl()
        
        return df