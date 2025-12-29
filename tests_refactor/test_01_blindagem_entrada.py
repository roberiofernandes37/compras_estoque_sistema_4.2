import pytest
import polars as pl

# --- A FUNÇÃO NOVA QUE ESTAMOS CRIANDO ---
# (Futuramente ela irá para src/utils/sanitizer.py)
def sanear_dados_dataframe(df: pl.DataFrame) -> pl.DataFrame:
    """
    Blindagem de Dados (Fase 1):
    Garante que números críticos para a matemática não quebrem o cálculo.
    """
    # 1. Lead Time: Não pode ser negativo. Se for, assume 0.
    # (Proteção contra erro humano no cadastro ou hardcoding incorreto)
    if "lead_time_dias" in df.columns:
        df = df.with_columns(
            pl.when(pl.col("lead_time_dias") < 0)
            .then(0)
            .otherwise(pl.col("lead_time_dias"))
            .alias("lead_time_dias")
        )
    
    # 2. Média de Venda: Nulo vira 0.0 (Essencial)
    if "media_venda_dia" in df.columns:
        df = df.with_columns(
            pl.col("media_venda_dia").fill_null(0.0)
        )
    
    # 3. Estoque e OC: Nulos viram 0 (Essencial)
    # NOTA: Não removemos negativos do estoque (pois estoque negativo = dívida técnica/venda sem baixa),
    # mas garantimos que não seja Nulo/None.
    cols_zero = ["saldo_estoque", "saldo_oc"]
    cols_existentes = [c for c in cols_zero if c in df.columns]
    
    if cols_existentes:
        df = df.with_columns([
            pl.col(c).fill_null(0) for c in cols_existentes
        ])
    
    return df

# --- OS TESTES ---

def test_deve_corrigir_lead_time_negativo():
    # Cenário: Lead time veio negativo do banco ou config
    df_sujo = pl.DataFrame({
        "cod_produto": ["A1", "B2"],
        "lead_time_dias": [-5, 10], 
        "media_venda_dia": [1.0, 2.0],
        "saldo_estoque": [10, 20]
    })

    # Ação
    df_limpo = sanear_dados_dataframe(df_sujo)

    # Verificação
    lead_times = df_limpo["lead_time_dias"].to_list()
    assert lead_times == [0, 10], f"Erro: Lead time negativo deveria ser 0. Recebido: {lead_times}"

def test_deve_preencher_nulos_criticos():
    # Cenário: Dados nulos que quebrariam a matemática (ex: multiplicar None por 10 falha)
    df_sujo = pl.DataFrame({
        "cod_produto": ["C3"],
        "lead_time_dias": [10],
        "media_venda_dia": [None], # Perigo!
        "saldo_estoque": [None],   # Perigo!
        "saldo_oc": [None]
    })

    df_limpo = sanear_dados_dataframe(df_sujo)

    # Polars trata None numérico como null, fill_null(0) deve resolver
    assert df_limpo["media_venda_dia"].item(0) == 0.0
    assert df_limpo["saldo_estoque"].item(0) == 0
    assert df_limpo["saldo_oc"].item(0) == 0

def test_nao_deve_alterar_estoque_negativo():
    # Cenário: Estoque negativo é válido (significa furo de estoque), não devemos zerar
    df_sujo = pl.DataFrame({
        "cod_produto": ["D4"],
        "saldo_estoque": [-5]
    })
    
    df_limpo = sanear_dados_dataframe(df_sujo)
    
    assert df_limpo["saldo_estoque"].item(0) == -5