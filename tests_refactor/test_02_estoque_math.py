import pytest
import polars as pl
import sys
from pathlib import Path

# Adiciona o diretório src ao path para garantir importação correta
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(PROJECT_ROOT / "src"))

try:
    # Tenta importar do jeito que funcionou no seu ambiente
    from compras_sistema.rule_engine.stock.estoque_math import EstoqueMath
except ImportError:
    # Fallback caso o path precise ser explícito
    from src.compras_sistema.rule_engine.stock.estoque_math import EstoqueMath

def test_calculo_seguranca_valores_padrao():
    """
    Testa se o cálculo de Estoque de Segurança respeita a fórmula padrão
    quando não há configuração específica (Fallback).
    ES = FatorZ * StdDev * Raiz(LeadTime)
    """
    
    # 1. Setup: Dados controlados
    df_input = pl.DataFrame({
        "cod_produto": ["PROD_X", "PROD_Y", "PROD_Z"],
        "curva_xyz": ["X", "Y", "Z"],
        "std_venda_dia": [1.0, 1.0, 1.0],
        "lead_time_dias": [1, 1, 1] 
    })

    # Config Mock (Vazio -> Força o fallback para 1.65, 1.28, 0.84)
    config_mock = {} 

    # 2. Act
    df_result = EstoqueMath.calcular_seguranca(df_input, config_mock)

    # 3. Assert
    res_x = df_result.filter(pl.col("curva_xyz") == "X")["estoque_seguranca"].item()
    assert res_x == 1.65, f"Erro no fator X. Esperado 1.65, obtido {res_x}"

    res_y = df_result.filter(pl.col("curva_xyz") == "Y")["estoque_seguranca"].item()
    assert res_y == 1.28, f"Erro no fator Y. Esperado 1.28, obtido {res_y}"

    res_z = df_result.filter(pl.col("curva_xyz") == "Z")["estoque_seguranca"].item()
    assert res_z == 0.84, f"Erro no fator Z. Esperado 0.84, obtido {res_z}"

def test_calculo_seguranca_com_lead_time_variavel():
    """
    Testa a fórmula completa: ES = 1.65 * 2.0 * Sqrt(4)
    Esperado: 1.65 * 2 * 2 = 6.6
    """
    df_input = pl.DataFrame({
        "cod_produto": ["TESTE_COMPLEXO"],
        "curva_xyz": ["X"],        
        "std_venda_dia": [2.0],    
        "lead_time_dias": [4]      # Raiz de 4 é 2
    })
    
    df_result = EstoqueMath.calcular_seguranca(df_input, {})
    
    val = df_result["estoque_seguranca"].item()
    assert val == pytest.approx(6.6, 0.01)

def test_calculo_seguranca_lendo_configuracao_personalizada():
    """
    Teste de Mutação:
    Injetamos uma configuração com valores 'estranhos' (ex: X=10.0)
    para garantir que o sistema obedece ao config e não ao hardcode.
    """
    df_input = pl.DataFrame({
        "cod_produto": ["PROD_TESTE"],
        "curva_xyz": ["X"],
        "std_venda_dia": [1.0],
        "lead_time_dias": [1] 
    })

    # Simula o objeto de configuração (Dict) com valores alterados
    config_custom = {
        "estoque": {
            "fator_z": {
                "X": 10.0, # Valor exagerado para prova de conceito
                "Y": 5.0,
                "Z": 1.0
            }
        }
    }

    # Executa
    df_result = EstoqueMath.calcular_seguranca(df_input, config_custom)
    
    # Verifica
    # Se estivesse usando hardcode, seria 1.65.
    # Como injetamos 10.0, o resultado tem de ser 10.0.
    resultado = df_result["estoque_seguranca"].item()
    
    assert resultado == 10.0, f"Falha: O sistema ignorou o config. Esperado 10.0, recebeu {resultado}"