import polars as pl
from pathlib import Path
from ...data_engine.duckdb_manager import DuckDBManager
from ...core.config import ConfigManager
import structlog

logger = structlog.get_logger(__name__)

class ABCClassifier:
    """
    Calcula a Curva ABC Financeira.
    Refatorado (Fase 4): Lógica movida do SQL para Python (Polars) para permitir configuração.
    """
    
    def __init__(self, db_manager: DuckDBManager):
        self.db = db_manager
        # [MUDANÇA] Agora apontamos para um SQL 'burro' que só traz totais
        self.query_path = Path(__file__).parent.parent.parent / "data_engine" / "queries" / "abc_financeiro_base.sql"
        # Se o arquivo novo não existir, usamos o antigo temporariamente (fallback)
        if not self.query_path.exists():
            self.query_path = Path(__file__).parent.parent.parent / "data_engine" / "queries" / "abc_financeiro.sql"

    @staticmethod
    def calcular_abc_polars(df: pl.DataFrame, config_abc: dict) -> pl.DataFrame:
        """
        Método Estático Puro: Recebe dados brutos e aplica as regras ABC do config.
        Isso permite testar a lógica sem precisar de banco de dados.
        """
        if df.height == 0:
            return df.with_columns(pl.lit("C").alias("curva_abc"))

        # 1. Ordenar do maior para o menor (Pareto)
        df = df.sort("total_vendido", descending=True)

        # 2. Calcular Acumulados
        total_geral = df["total_vendido"].sum()
        
        # Evitar divisão por zero
        if total_geral == 0:
            return df.with_columns(pl.lit("C").alias("curva_abc"))

        df = df.with_columns([
            pl.col("total_vendido").cum_sum().alias("valor_acumulado")
        ])

        df = df.with_columns([
            (pl.col("valor_acumulado") / total_geral).alias("percentual_acumulado")
        ])

        # 3. Ler Configuração e Definir Cortes
        # Ex: A=80, B=15. 
        # Corte A = 0.80
        # Corte B = 0.80 + 0.15 = 0.95
        pct_a = config_abc.get("A", 80.0) / 100.0
        pct_b = config_abc.get("B", 15.0) / 100.0
        
        corte_a = pct_a
        corte_b = pct_a + pct_b

        # 4. Aplicar Classificação
        df = df.with_columns([
            pl.when(pl.col("percentual_acumulado") <= corte_a).then(pl.lit("A"))
            .when(pl.col("percentual_acumulado") <= corte_b).then(pl.lit("B"))
            .otherwise(pl.lit("C"))
            .alias("curva_abc")
        ])

        return df

    def run(self) -> pl.DataFrame:
        """Executa o fluxo completo: Banco -> Lógica -> Resultado"""
        logger.info("iniciando_curva_abc_v2")
        
        # 1. Carregar Configuração
        try:
            config = ConfigManager().parametros
            # Tenta pegar dict de float, se der erro converte ou usa padrao
            abc_dict = config.abc if hasattr(config, 'abc') else {'A': 80.0, 'B': 15.0, 'C': 5.0}
        except Exception as e:
            logger.warning(f"Erro ao ler config ABC ({e}). Usando padrão 80/15/5.")
            abc_dict = {'A': 80.0, 'B': 15.0, 'C': 5.0}

        # 2. Buscar Dados Brutos (Total vendido por produto)
        # Se estivermos usando o SQL antigo (que já calcula ABC), precisamos apenas das colunas de valor
        # para recalcular ou ignorar o cálculo do SQL.
        # Estratégia Segura: Ler o SQL, se vier com ABC, ignoramos e recalculamos.
        
        with open(self.query_path, 'r', encoding='utf-8') as f:
            query = f.read()

        with self.db.get_connection() as conn:
            df_bruto = conn.execute(query).pl()

        # 3. Aplicar Lógica Python
        # Se o SQL for o antigo, ele retorna 'curva_abc'. Vamos sobrescrever.
        # Garantimos que existe 'total_vendido'
        if "total_vendido" not in df_bruto.columns:
            logger.error("Coluna 'total_vendido' não encontrada no retorno do SQL.")
            return df_bruto

        df_final = self.calcular_abc_polars(df_bruto, abc_dict)
        
        logger.info("curva_abc_concluida", total_produtos=len(df_final))
        return df_final