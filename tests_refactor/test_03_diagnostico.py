import pytest
import polars as pl
import sys
from pathlib import Path

# Setup de Importação
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(PROJECT_ROOT / "src"))

try:
    from compras_sistema.rule_engine.stock.estoque_math import EstoqueMath
except ImportError:
    from src.compras_sistema.rule_engine.stock.estoque_math import EstoqueMath

# --- TESTE 1: Regra Padrão (Excesso) ---
def test_diagnostico_excesso_estoque():
    """
    Testa se o sistema detecta Excesso de Estoque.
    """
    df_input = pl.DataFrame({
        "cod_produto": ["PROD_OK", "PROD_EXCESSO"],
        "saldo_estoque": [100, 1000],
        "saldo_oc": [0, 0],
        "media_venda_dia": [1.0, 1.0], # Venda mensal = 30
        "dias_vida": [500, 500],
        "ativo": ["SIM", "SIM"],
        "sugestao_final": [0, 0],
        "score": [100, 100],
        "lote_economico": [10, 10],
        "custo_unitario": [10.0, 10.0] 
    })

    # Mock de Configuração Padrão
    config_mock = {
        "produto": {"dias_lancamento": 180},
        "giro": {"limite_meses_cobertura": 6, "minimo_venda_dia": 0.05}
    }

    df_result = EstoqueMath.gerar_diagnostico(df_input, config_mock)

    diag_ok = df_result.filter(pl.col("cod_produto") == "PROD_OK")["validacao_giro"].item()
    assert "ALERTA" not in diag_ok

    diag_excesso = df_result.filter(pl.col("cod_produto") == "PROD_EXCESSO")["validacao_giro"].item()
    assert "ALERTA: Excesso" in diag_excesso

# --- TESTE 2: Regra Padrão (Venda Baixa) ---
def test_diagnostico_venda_irrisoria():
    """
    Testa se detecta venda muito baixa (< 0.05/dia).
    """
    df_input = pl.DataFrame({
        "cod_produto": ["PROD_LENTO"],
        "saldo_estoque": [0],
        "saldo_oc": [0],
        "media_venda_dia": [0.01], 
        "dias_vida": [500],
        "ativo": ["SIM"],
        "sugestao_final": [10], 
        "score": [100],
        "lote_economico": [12],
        "custo_unitario": [5.50]
    })
    
    config_mock = {
        "produto": {"dias_lancamento": 180},
        "giro": {"limite_meses_cobertura": 6, "minimo_venda_dia": 0.05}
    }

    df_result = EstoqueMath.gerar_diagnostico(df_input, config_mock)
    
    diag = df_result["validacao_giro"].item()
    assert "ALERTA: Sem Venda Recente" in diag

# --- TESTE 3: Regra Configurável (A PROVA REAL) ---
def test_diagnostico_obedece_configuracao_personalizada():
    """
    Teste de Mutação:
    Alteramos o limite de cobertura para 1 mês.
    Um produto com 3 meses de estoque (que seria OK antes) agora deve dar ALERTA.
    """
    df_input = pl.DataFrame({
        "cod_produto": ["PROD_MUTANTE"],
        "saldo_estoque": [90],
        "saldo_oc": [0],
        "media_venda_dia": [1.0], # Venda mês = 30. Cobertura = 3 meses.
        "dias_vida": [500],
        "ativo": ["SIM"],
        "sugestao_final": [0],
        "score": [100],
        "lote_economico": [10],
        "custo_unitario": [10.0]
    })

    # Config Rigorosa: Limite de cobertura é apenas 1.0 mês
    config_rigorosa = {
        "produto": {"dias_lancamento": 180},
        "giro": {
            "limite_meses_cobertura": 1.0, # MUDANÇA CRÍTICA
            "minimo_venda_dia": 0.05
        }
    }

    df_result = EstoqueMath.gerar_diagnostico(df_input, config_rigorosa)
    
    diag = df_result["validacao_giro"].item()
    
    # Se passar aqui, o seu sistema é oficialmente Config-Driven
    assert "ALERTA: Excesso > 1.0m" in diag