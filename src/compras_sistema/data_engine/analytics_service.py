import polars as pl
import duckdb
from datetime import datetime

class AnalyticsService:
    def __init__(self, db_manager):
        """
        Serviço de Inteligência de Dados.
        Responsável por transformar dados brutos do DuckDB em KPIs e Tendências.
        """
        self.db_manager = db_manager

    def _obter_conexao_segura(self):
        """Garante que a conexão seja extraída corretamente do db_manager."""
        try:
            return self.db_manager.get_connection()
        except Exception as e:
            if "inicializado" in str(e).lower():
                raise RuntimeError("DuckDB não inicializado. Chame initialize() no Launcher.")
            raise e

    def get_kpis_atuais(self, marca="TODAS"):
        """
        Calcula os KPIs financeiros e operacionais do último snapshot.
        Permite filtragem dinâmica por marca.
        """
        try:
            # SQL dinâmico para filtragem por marca
            condicao_marca = "" if marca == "TODAS" else f"AND marca = '{marca}'"
            
            query = f"""
                WITH ultimo_snapshot AS (
                    SELECT MAX(data_snapshot) as data_viga FROM historico_snapshots
                )
                SELECT 
                    CAST(MAX(data_snapshot) AS TIMESTAMP) as data_referencia,
                    SUM(saldo_estoque * custo_unitario) as valor_estoque,
                    SUM(sugestao_final * custo_unitario) as investimento_pendente,
                    AVG(cobertura_meses) as cobertura_media
                FROM historico_snapshots
                WHERE data_snapshot = (SELECT data_viga FROM ultimo_snapshot)
                {condicao_marca}
            """
            
            print(f"🔍 [Analytics] Buscando KPIs atuais para marca: {marca}")
            
            with self._obter_conexao_segura() as conn:
                res = conn.execute(query).df()
            
            if res.empty or res["valor_estoque"][0] is None:
                print(f"⚠️ [Analytics] Nenhum dado encontrado para a marca: {marca}")
                return {
                    "status": "vazio",
                    "data_referencia": datetime.now(),
                    "valor_estoque": 0.0,
                    "investimento_pendente": 0.0,
                    "cobertura_media": 0.0
                }

            return {
                "status": "ok",
                "data_referencia": res["data_referencia"][0],
                "valor_estoque": float(res["valor_estoque"][0]),
                "investimento_pendente": float(res["investimento_pendente"][0]),
                "cobertura_media": float(res["cobertura_media"][0])
            }

        except Exception as e:
            print(f"❌ [Analytics] Erro crítico ao buscar KPIs: {str(e)}")
            return {"status": "erro", "erro_msg": str(e)}

    def get_tendencia_cobertura(self, marca="TODAS", dias_historico=90):
        """Busca a evolução da cobertura por Curva ABC para o gráfico."""
        try:
            condicao_marca = "" if marca == "TODAS" else f"WHERE marca = '{marca}'"
            
            query = f"""
                SELECT 
                    CAST(data_snapshot AS DATE) as data,
                    curva_abc,
                    AVG(cobertura_meses) as cobertura_meses
                FROM historico_snapshots
                {condicao_marca}
                GROUP BY 1, 2
                HAVING data >= CURRENT_DATE - INTERVAL {dias_historico} DAY
                ORDER BY 1 ASC, 2 ASC
            """
            
            print(f"📈 [Analytics] Gerando tendência de cobertura (Marca: {marca})")
            
            with self._obter_conexao_segura() as conn:
                return conn.execute(query).pl()

        except Exception as e:
            print(f"❌ [Analytics] Erro ao processar tendência: {str(e)}")
            return pl.DataFrame()