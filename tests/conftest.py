# tests/conftest.py
import pytest
import polars as pl
from datetime import datetime, timedelta
import sys
from pathlib import Path

# Adiciona o src ao path para importar os módulos do sistema
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.append(str(PROJECT_ROOT / "src"))

@pytest.fixture
def config_mock():
    """Simula o objeto de configuração carregado do YAML."""
    class MockConfig:
        def __init__(self):
            # Simula a estrutura híbrida (Objeto/Dict) que o seu sistema usa
            self.compras = {'meses_cobertura': 1.5}
            self.produto = {'dias_lancamento': 60}
            self.lead_time = {'padrao_dias': 10}
            
            # Permite acesso via dicionário também (para compatibilidade com _ler_config)
            self._data = {
                'compras': self.compras,
                'produto': self.produto,
                'lead_time': self.lead_time
            }
        
        def __getitem__(self, item):
            return self._data[item]

    return MockConfig()

@pytest.fixture
def df_produto_base():
    """Retorna um DataFrame Polars básico para testes de cálculo."""
    return pl.DataFrame({
        "cod_produto": ["PROD-001"],
        "data_cadastro": [datetime.now() - timedelta(days=365)], # Item velho
        "saldo_estoque": [100],
        "saldo_oc": [0],
        "media_venda_dia": [2.0],
        "std_venda_dia": [0.5],
        "lead_time_dias": [10],
        "curva_abc": ["A"],
        "curva_xyz": ["X"],
        "lote_economico": [10],
        "custo_unitario": [50.0],
        "dias_sem_venda": [0],
        "ativo": ["SIM"]
    })