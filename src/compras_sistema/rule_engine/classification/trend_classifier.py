import polars as pl
from compras_sistema.data_engine.duckdb_manager import DuckDBManager

class TrendClassifier:
    def __init__(self, db_manager: DuckDBManager):
        self.db = db_manager

    def run(self) -> pl.DataFrame:
        """
        Calcula tendências de Vendas, Clientes e DIAS SEM VENDA (Ruptura Temporal).
        """
        query = """
        WITH periodos AS (
            SELECT 
                cod_produto,
                MAX(data_movimento) as ultima_venda,
                -- Vendas Recentes (90 dias) vs Ano (365 dias)
                SUM(CASE WHEN CAST(data_movimento AS DATE) >= (CURRENT_DATE - INTERVAL '90 days') THEN quantidade ELSE 0 END) as qtd_90d,
                SUM(CASE WHEN CAST(data_movimento AS DATE) >= (CURRENT_DATE - INTERVAL '365 days') THEN quantidade ELSE 0 END) as qtd_365d,
                
                -- Contagem de Clientes Únicos
                COUNT(DISTINCT CASE WHEN CAST(data_movimento AS DATE) >= (CURRENT_DATE - INTERVAL '90 days') THEN cod_clifor END) as clientes_atuais,
                COUNT(DISTINCT CASE WHEN CAST(data_movimento AS DATE) < (CURRENT_DATE - INTERVAL '90 days') 
                                     AND CAST(data_movimento AS DATE) >= (CURRENT_DATE - INTERVAL '180 days') THEN cod_clifor END) as clientes_anteriores
            FROM sqlite_db.vendas
            WHERE CAST(data_movimento AS DATE) >= (CURRENT_DATE - INTERVAL '365 days')
            GROUP BY 1
        )
        SELECT 
            CAST(cod_produto AS VARCHAR) as cod_produto,
            
            -- Cálculo de dias sem venda (usado para boost de ruptura)
            date_diff('day', CAST(ultima_venda AS DATE), CURRENT_DATE) as dias_sem_venda,

            -- Variação de Vendas (%)
            CASE 
                WHEN qtd_365d = 0 THEN 0
                ELSE ((qtd_90d * 4.0) / qtd_365d) - 1.0 
            END as var_vendas,
            
            -- Saldo de Clientes
            (clientes_atuais - clientes_anteriores) as saldo_clientes,
            clientes_atuais as qtd_clientes_ativos
        FROM periodos
        """
        
        with self.db.get_connection() as conn:
            return conn.execute(query).pl()