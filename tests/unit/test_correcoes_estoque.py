import unittest
import polars as pl
from datetime import datetime, timedelta
import sys
import os

# --- CORREÇÃO DO IMPORT ---
# Pega o caminho onde este arquivo de teste está
diretorio_teste = os.path.dirname(os.path.abspath(__file__))
# Sobe dois níveis (sai de 'unit', sai de 'tests') e aponta para 'src'
caminho_src = os.path.abspath(os.path.join(diretorio_teste, '../../src'))

# Adiciona ao sistema para o Python encontrar
if caminho_src not in sys.path:
    sys.path.append(caminho_src)

# Agora o import deve funcionar corretamente
try:
    from compras_sistema.rule_engine.stock.estoque_math import EstoqueMath
    print(f"Sucesso: EstoqueMath importado de {caminho_src}")
except ImportError as e:
    print(f"ERRO CRÍTICO DE IMPORTAÇÃO: {e}")
    print(f"O Python procurou em: {sys.path}")
    raise e
# ---------------------------

class TestCorrecoesEstoque(unittest.TestCase):
    def setUp(self):
        # Configuração Mock (Simulada) para passar nos métodos
        self.config_mock = {
            'compras': {'meses_cobertura': 1},
            'produto': {'dias_lancamento': 90}, # Produtos com mais de 90 dias não são "novos"
            'estoque': {'fator_z': {'X': 1.65, 'Y': 1.28, 'Z': 0.84}}
        }
        
        # Data base para simular "dias_vida" antigos
        self.data_antiga = datetime.now() - timedelta(days=500)

    def test_correcao_1_boost_anti_ruptura(self):
        """
        Teste da Correção de Lógica:
        Verifica se a regra de > 90 dias (1.5x) tem prioridade sobre a de > 30 dias (1.2x).
        Antes, o >30 sombreava o >90.
        """
        print("\n--- TESTE 1: Validação da Lógica Boost Anti-Ruptura ---")
        
        # Cenário: Produtos A em rutura (saldo 0) e antigos
        df_input = pl.DataFrame({
            "sku": ["ITEM_35_DIAS", "ITEM_100_DIAS"],
            "saldo_estoque": [0.0, 0.0],
            "saldo_oc": [0.0, 0.0],
            "curva_abc": ["A", "A"],
            "data_cadastro": [self.data_antiga, self.data_antiga],
            "media_venda_dia": [10.0, 10.0],
            "dias_sem_venda": [35, 100], # O ponto chave do teste
            "lead_time_dias": [10, 10],
            "estoque_seguranca": [0, 0]
        })

        # Executa o cálculo
        df_result = EstoqueMath.calcular_necessidades(df_input, self.config_mock)
        
        # Extrai os resultados
        res_35 = df_result.filter(pl.col("sku") == "ITEM_35_DIAS")["media_calculo"][0]
        res_100 = df_result.filter(pl.col("sku") == "ITEM_100_DIAS")["media_calculo"][0]
        
        print(f"Item (35 dias sem venda) -> Média Original: 10.0 | Média Calculada: {res_35}")
        print(f"Item (100 dias sem venda) -> Média Original: 10.0 | Média Calculada: {res_100}")

        # Asserções (Validações)
        # 35 dias (>30) deve ter boost de 20% -> 10 * 1.2 = 12.0
        self.assertAlmostEqual(res_35, 12.0, places=2, msg="FALHA: Regra de 30 dias não aplicada corretamente.")
        
        # 100 dias (>90) deve ter boost de 50% -> 10 * 1.5 = 15.0
        # Se a ordem estiver errada, daria 12.0 (cairia na regra de 30)
        self.assertAlmostEqual(res_100, 15.0, places=2, msg="ERRO CRÍTICO: A regra de 30 dias está a bloquear a regra de 90 dias!")
        print(">> SUCESSO: A ordem lógica dos boosts está correta.")

    def test_correcao_2_trava_zumbi(self):
        """
        Teste da Correção de Risco:
        Verifica se produtos sem venda há > 180 dias têm a sugestão ZERADA,
        mesmo que tenham média de venda alta.
        """
        print("\n--- TESTE 2: Validação da Trava Zumbi (Risco de Negócio) ---")
        
        df_input = pl.DataFrame({
            "sku": ["ITEM_ATIVO", "ITEM_ZUMBI"],
            "saldo_estoque": [0.0, 0.0], # Ambos precisam de compra teoricamente
            "saldo_oc": [0.0, 0.0],
            "curva_abc": ["A", "A"],
            "data_cadastro": [self.data_antiga, self.data_antiga],
            "media_venda_dia": [10.0, 10.0], # Média alta para ambos
            "dias_sem_venda": [10, 200], # Ativo (10 dias), Zumbi (200 dias)
            "lead_time_dias": [30, 30],
            "estoque_seguranca": [0, 0]
        })

        # Executa o cálculo
        df_result = EstoqueMath.calcular_necessidades(df_input, self.config_mock)

        # Resultados
        sugestao_ativo = df_result.filter(pl.col("sku") == "ITEM_ATIVO")["sugestao_bruta"][0]
        sugestao_zumbi = df_result.filter(pl.col("sku") == "ITEM_ZUMBI")["sugestao_bruta"][0]

        print(f"Item Ativo (10 dias parado) -> Sugestão Bruta: {sugestao_ativo}")
        print(f"Item Zumbi (200 dias parado) -> Sugestão Bruta: {sugestao_zumbi}")

        # Validação do Item Ativo (Controlo)
        # Meta aprox: 10 * 30 (cobertura) = 300. Sugestão deve ser ~300 (ou com boost)
        self.assertTrue(sugestao_ativo > 0, "O item ativo deveria ter sugestão de compra.")

        # Validação do Item Zumbi (O Teste Real)
        self.assertEqual(sugestao_zumbi, 0.0, "ERRO CRÍTICO: O sistema sugeriu comprar um produto Zumbi!")
        print(">> SUCESSO: A Trava Zumbi funcionou e bloqueou a compra.")

if __name__ == '__main__':
    unittest.main()