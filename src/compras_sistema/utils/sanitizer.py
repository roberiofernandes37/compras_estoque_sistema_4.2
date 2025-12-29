# Arquivo: src/compras_sistema/utils/sanitizer.py
import polars as pl
import logging

def sanear_dados_dataframe(df: pl.DataFrame) -> pl.DataFrame:
    """
    Blindagem de Dados (Refatoração Fase 1):
    Garante que números críticos para a matemática não quebrem o cálculo.
    """
    logger = logging.getLogger("Sanitizer")
    
    # Validação prévia para evitar erro se dataframe estiver vazio
    if df.height == 0:
        return df

    # 1. Lead Time: Não pode ser negativo. Se for, assume 0.
    if "lead_time_dias" in df.columns:
        qtd_negativos = df.filter(pl.col("lead_time_dias") < 0).height
        if qtd_negativos > 0:
            logger.warning(f"⚠️ BLINDAGEM: Encontrados {qtd_negativos} produtos com Lead Time negativo. Forçados para 0.")
            
        df = df.with_columns(
            pl.when(pl.col("lead_time_dias") < 0)
            .then(0)
            .otherwise(pl.col("lead_time_dias"))
            .alias("lead_time_dias")
        )
    
    # 2. Média de Venda: Nulo vira 0.0
    if "media_venda_dia" in df.columns:
        df = df.with_columns(
            pl.col("media_venda_dia").fill_null(0.0)
        )
    
    # 3. Estoque e OC: Nulos viram 0
    cols_zero = ["saldo_estoque", "saldo_oc"]
    cols_existentes = [c for c in cols_zero if c in df.columns]
    
    if cols_existentes:
        df = df.with_columns([
            pl.col(c).fill_null(0) for c in cols_existentes
        ])
    
    return df