import pytest
import polars as pl
import sys
from pathlib import Path

# Setup de Importação
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(PROJECT_ROOT / "src"))

try:
    from compras_sistema.rule_engine.classification.abc_classifier import ABCClassifier
except ImportError:
    from src.compras_sistema.rule_engine.classification.abc_classifier import ABCClassifier

def test_classificacao_abc_pareto_perfeito():
    """
    Testa a lógica ABC pura usando Polars.
    Cenário: Venda Total = 1000
    - Produto 1: 800 (80%) -> Deve ser A
    - Produto 2: 150 (15%) -> Acumulado 95% -> Deve ser B
    - Produto 3: 50  (5%)  -> Acumulado 100% -> Deve ser C
    """
    
    # 1. Setup
    df_vendas = pl.DataFrame({
        "cod_produto": ["P1", "P2", "P3"],
        "total_vendido": [800.0, 150.0, 50.0]
    })

    # 2. Configuração Simulada (Lida do YAML)
    config_abc = {
        "A": 80.0,
        "B": 15.0,
        "C": 5.0
    }

    # 3. Act
    df_resultado = ABCClassifier.calcular_abc_polars(df_vendas, config_abc)

    # 4. Assert
    cat_p1 = df_resultado.filter(pl.col("cod_produto") == "P1")["curva_abc"].item()
    assert cat_p1 == "A", f"P1 deveria ser A, mas foi {cat_p1}"

    cat_p2 = df_resultado.filter(pl.col("cod_produto") == "P2")["curva_abc"].item()
    assert cat_p2 == "B", f"P2 deveria ser B, mas foi {cat_p2}"

    cat_p3 = df_resultado.filter(pl.col("cod_produto") == "P3")["curva_abc"].item()
    assert cat_p3 == "C", f"P3 deveria ser C, mas foi {cat_p3}"

def test_classificacao_abc_configuracao_personalizada():
    """
    Teste de Mutação:
    Alteramos a regra para ser super rigorosa: Classe A é apenas 50% do faturamento.
    
    Cenário:
    - Produto X vendeu 600 de um total de 1000 (60%).
    
    Na regra Padrão (A=80%): 60% < 80% -> Seria 'A'.
    Na regra Mutante (A=50%): 60% > 50% -> Cai para 'B'.
    """
    
    df_vendas = pl.DataFrame({
        "cod_produto": ["PROD_X", "PROD_RESTO"],
        "total_vendido": [600.0, 400.0] # Total 1000
    })

    # Configuração Mutante (Rigorosa)
    config_mutante = {
        "A": 50.0, # Só os top 50% são A
        "B": 40.0,
        "C": 10.0
    }

    # Executa
    df_resultado = ABCClassifier.calcular_abc_polars(df_vendas, config_mutante)

    # Verifica
    cat_x = df_resultado.filter(pl.col("cod_produto") == "PROD_X")["curva_abc"].item()
    
    # Como o acumulado do PROD_X é 0.60 (60%), e o corte A é 0.50 (50%),
    # ele deve cair para a próxima faixa (B).
    assert cat_x == "B", f"Falha na configuração dinâmica. Esperado B, recebeu {cat_x}"