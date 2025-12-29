# tests/unit/test_estoque_math.py
import polars as pl
from datetime import datetime, timedelta
from compras_sistema.rule_engine.stock.estoque_math import EstoqueMath

def test_calculo_estoque_seguranca(df_produto_base, config_mock):
    """Verifica se o cálculo do Z-Score para XYZ está correto."""
    df = EstoqueMath.calcular_seguranca(df_produto_base, config_mock)
    resultado = df["estoque_seguranca"].item()
    
    # 1.65 * 0.5 * sqrt(10) approx 2.61
    assert resultado > 2.0
    assert resultado < 3.0

def test_ponto_suprimento_e_meta(df_produto_base, config_mock):
    """Testa a lógica principal de reposição."""
    
    # Precisamos calcular a segurança antes, pois calcular_necessidades depende dela
    df = EstoqueMath.calcular_seguranca(df_produto_base, config_mock)
    df = EstoqueMath.calcular_necessidades(df, config_mock)
    
    row = df.row(0, named=True)
    
    # Ponto Suprimento = (2 * 10) + ES(~2.6) ≈ 22.6 -> Arredonda 23
    # Estoque Meta = (2 * 45) + ES(~2.6) ≈ 92.6 -> Arredonda 93
    assert row["ponto_suprimento"] >= 22
    assert row["estoque_meta"] >= 92

def test_boost_anti_ruptura(df_produto_base, config_mock):
    """Valida a regra de aumentar a média se o item A/B estiver zerado."""
    # Cenário: Estoque 0, Curva A, Item Antigo
    df_ruptura = df_produto_base.with_columns([
        pl.lit(0).alias("saldo_estoque"),
        pl.lit("A").alias("curva_abc"),
        pl.lit(31).alias("dias_sem_venda"), # > 30 dias parado
        pl.lit(0.0).alias("estoque_seguranca") # <--- CORREÇÃO: Coluna necessária para o cálculo
    ])
    
    df = EstoqueMath.calcular_necessidades(df_ruptura, config_mock)
    
    media_original = 2.0
    media_calculada = df["media_calculo"].item()
    
    # Regra: Se > 30 dias sem venda, boost de 20%
    assert media_calculada == media_original * 1.20

def test_item_novo_sem_movimento(df_produto_base, config_mock):
    """Valida a lógica de diagnóstico para item recém cadastrado."""
    # Item com 10 dias de vida (menor que config 60)
    df_novo = df_produto_base.with_columns([
        pl.lit(datetime.now() - timedelta(days=10)).alias("data_cadastro"),
        pl.lit(0).alias("media_venda_dia"),
        pl.lit(0).alias("saldo_estoque"),
        pl.lit(0).alias("saldo_oc"),
        pl.lit(100).alias("score") # <--- CORREÇÃO: Coluna necessária para recalculo de score
    ])
    
    # Gera sugestão forçada para testar bloqueio/liberação
    df_novo = df_novo.with_columns(pl.lit(100).alias("sugestao_final"))
    
    df = EstoqueMath.gerar_diagnostico(df_novo, config_mock)
    diag = df["validacao_giro"].item()
    
    # Deve identificar como item novo
    assert "ITEM NOVO" in diag 
    # Item novo deve sugerir compra mínima (1 lote)
    assert df["sugestao_final"].item() == df["lote_economico"].item()
    # Score deve ser boostado para 9999
    assert df["score"].item() == 9999

def test_lote_economico(df_produto_base, config_mock):
    """Testa arredondamento para lotes."""
    # Sugestão Bruta: 15
    # Lote: 10
    # Deve virar 20 (2 lotes)
    
    df = df_produto_base.with_columns([
        pl.lit(15).alias("sugestao_bruta"),
        pl.lit(10).alias("lote_economico")
    ])
    
    df = EstoqueMath.aplicar_lote_economico(df, config_mock)
    assert df["sugestao_final"].item() == 20