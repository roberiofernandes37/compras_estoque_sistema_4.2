import polars as pl
import pytest
from datetime import date
from compras_sistema.rule_engine.validators.input_schema import InputCalcSchema
from pandera.errors import SchemaError

def test_validador_sucesso():
    # Dados corretos
    df = pl.DataFrame({
        "cod_produto": ["A"],
        "saldo_estoque": [10],
        "saldo_oc": [0],
        "media_venda_dia": [1.5],
        "std_venda_dia": [0.1],
        "lead_time_dias": [10.0],
        "lote_economico": [12],
        "curva_abc": ["A"],
        "curva_xyz": ["X"],
        "data_cadastro": [date(2023, 1, 1)]
    })
    # Não deve levantar erro
    InputCalcSchema.validate(df)

def test_validador_falha_lote_zero():
    # Lote = 0 (Erro)
    df = pl.DataFrame({
        "cod_produto": ["A"],
        "saldo_estoque": [10],
        "saldo_oc": [0],
        "media_venda_dia": [1.5],
        "std_venda_dia": [0.1],
        "lead_time_dias": [10.0],
        "lote_economico": [0], # <--- ERRO AQUI (gt=0)
        "curva_abc": ["A"],
        "curva_xyz": ["X"],
        "data_cadastro": [date(2023, 1, 1)]
    })
    
    with pytest.raises(SchemaError):
        InputCalcSchema.validate(df)