#!/usr/bin/env python3
"""
AUDITOR DE ITEM - VALIDAÇÃO COMPLETA DE CÁLCULOS (STANDALONE)
Sistema: Gestão de Compras e Estoque
Autor: Robério (com assistência de IA)
Data: 16/12/2025

Versão standalone - não depende de módulos externos
CORRIGIDO: Nomes de colunas conforme schema real do banco
"""

import sys
from pathlib import Path
import math
from datetime import datetime
import yaml

try:
    import duckdb
except ImportError:
    print("❌ ERRO: Biblioteca 'duckdb' não encontrada!")
    print("   Instale com: pip install duckdb")
    sys.exit(1)


class ConfigSimples:
    """Gerenciador simples de configurações"""

    def __init__(self, config_path):
        self.config_path = Path(config_path)
        self.parametros = self._carregar_config()

    def _carregar_config(self):
        """Carrega configurações do YAML"""
        if not self.config_path.exists():
            print(f"⚠️  Arquivo de config não encontrado: {self.config_path}")
            print("   Usando valores padrão...")
            return {
                'compras': {
                    'leadtime_padrao': 7,
                    'meses_cobertura': 2
                },
                'produto': {
                    'dias_lancamento': 365
                }
            }

        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
            return config
        except Exception as e:
            print(f"⚠️  Erro ao ler config: {e}")
            print("   Usando valores padrão...")
            return {
                'compras': {
                    'leadtime_padrao': 7,
                    'meses_cobertura': 2
                },
                'produto': {
                    'dias_lancamento': 365
                }
            }


class AuditorItem:
    """Auditor completo de cálculos de um item"""

    def __init__(self, db_path, config):
        self.db_path = Path(db_path)
        self.config = config
        self.conn = None
        self.resultado = {}

    def conectar(self):
        """Conecta ao banco de dados"""
        if not self.db_path.exists():
            print(f"❌ ERRO: Banco de dados não encontrado: {self.db_path}")
            return False

        try:
            self.conn = duckdb.connect(':memory:')
            self.conn.execute(f"ATTACH '{self.db_path}' AS sqlite_db (TYPE SQLITE, READ_ONLY)")
            print(f"✅ Conectado ao banco: {self.db_path.name}")
            return True
        except Exception as e:
            print(f"❌ ERRO ao conectar: {e}")
            return False

    def desconectar(self):
        """Desconecta do banco"""
        if self.conn:
            self.conn.close()
            self.conn = None

    def auditar(self, cod_produto):
        """Executa auditoria completa de um produto"""
        print("=" * 80)
        print(f"AUDITORIA COMPLETA - ITEM: {cod_produto}")
        print(f"Data/Hora: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
        print("=" * 80)
        print()

        # Etapa 1: Dados Cadastrais
        print("▶ ETAPA 1: DADOS CADASTRAIS")
        print("-" * 80)
        cadastro = self._buscar_cadastro(cod_produto)
        if not cadastro:
            print("❌ ERRO: Produto não encontrado no cadastro!")
            return False
        self._exibir_cadastro(cadastro)
        self.resultado['cadastro'] = cadastro

        # Etapa 2: Saldo e Estoque
        print("\n▶ ETAPA 2: POSIÇÃO DE ESTOQUE")
        print("-" * 80)
        estoque = self._buscar_estoque(cod_produto)
        if not estoque:
            print("❌ ERRO: Estoque não encontrado!")
            return False
        self._exibir_estoque(estoque)
        self.resultado['estoque'] = estoque

        # Etapa 3: Histórico de Vendas
        print("\n▶ ETAPA 3: ANÁLISE DE VENDAS")
        print("-" * 80)
        vendas = self._buscar_vendas(cod_produto)
        self._exibir_vendas(vendas)
        self.resultado['vendas'] = vendas

        # Etapa 4: Classificações ABC e XYZ
        print("\n▶ ETAPA 4: CLASSIFICAÇÕES")
        print("-" * 80)
        classificacao = self._buscar_classificacao(cod_produto)
        self._exibir_classificacao(classificacao)
        self.resultado['classificacao'] = classificacao

        # Etapa 5: Cálculo de Tendências
        print("\n▶ ETAPA 5: TENDÊNCIAS")
        print("-" * 80)
        tendencias = self._calcular_tendencias(vendas)
        self._exibir_tendencias(tendencias)
        self.resultado['tendencias'] = tendencias

        # Etapa 6: Cálculo de Segurança (Z-Score)
        print("\n▶ ETAPA 6: ESTOQUE DE SEGURANÇA")
        print("-" * 80)
        seguranca = self._calcular_seguranca(vendas, classificacao)
        self._exibir_seguranca(seguranca)
        self.resultado['seguranca'] = seguranca

        # Etapa 7: Ponto de Suprimento e Estoque Meta
        print("\n▶ ETAPA 7: PONTO DE SUPRIMENTO E ESTOQUE META")
        print("-" * 80)
        necessidades = self._calcular_necessidades(
            vendas, seguranca, estoque, cadastro, classificacao
        )
        self._exibir_necessidades(necessidades)
        self.resultado['necessidades'] = necessidades

        # Etapa 8: Sugestão Bruta e Final
        print("\n▶ ETAPA 8: CÁLCULO DA SUGESTÃO DE COMPRA")
        print("-" * 80)
        sugestao = self._calcular_sugestao(necessidades, estoque, cadastro)
        self._exibir_sugestao(sugestao)
        self.resultado['sugestao'] = sugestao

        # Etapa 9: Score de Prioridade
        print("\n▶ ETAPA 9: SCORE DE PRIORIZAÇÃO")
        print("-" * 80)
        score = self._calcular_score(sugestao, vendas, estoque, classificacao, tendencias)
        self._exibir_score(score)
        self.resultado['score'] = score

        # Etapa 10: Diagnóstico e Validações
        print("\n▶ ETAPA 10: DIAGNÓSTICO E BLOQUEIOS")
        print("-" * 80)
        diagnostico = self._gerar_diagnostico(
            cadastro, estoque, vendas, sugestao, necessidades
        )
        self._exibir_diagnostico(diagnostico)
        self.resultado['diagnostico'] = diagnostico

        # Etapa 11: Comparação com Sistema (opcional)
        print("\n▶ ETAPA 11: COMPARAÇÃO COM SISTEMA")
        print("-" * 80)
        comparacao = self._comparar_com_sistema(cod_produto, diagnostico)
        self.resultado['comparacao'] = comparacao

        # Etapa 12: Opinião Técnica Final
        print("\n" + "=" * 80)
        print("📊 OPINIÃO TÉCNICA - VALIDAÇÃO DA AUDITORIA")
        print("=" * 80)
        self._emitir_parecer()

        return True

    def _buscar_cadastro(self, cod_produto):
        """Busca dados cadastrais do produto"""
        try:
            # CORRIGIDO: usando cod_produto com underscore
            result = self.conn.execute(f"""
                SELECT 
                    cod_produto,
                    descricao_produto,
                    marca,
                    ref_fornecedor,
                    ativo,
                    qtd_economica,
                    data_cadastro
                FROM sqlite_db.produtos_gerais
                WHERE CAST(cod_produto AS VARCHAR) = '{cod_produto}'
            """).fetchone()

            if not result:
                return None

            data_cadastro = result[6]
            if data_cadastro:
                try:
                    dt_cad = datetime.fromisoformat(data_cadastro)
                    dias_vida = (datetime.now() - dt_cad).days
                except:
                    dias_vida = 9999
            else:
                dias_vida = 9999

            return {
                'codigo': result[0],
                'descricao': result[1],
                'marca': result[2],
                'ref_fornecedor': result[3],
                'ativo': result[4],
                'lote_economico': result[5] if result[5] else 1,
                'data_cadastro': data_cadastro,
                'dias_vida': dias_vida
            }
        except Exception as e:
            print(f"⚠️  Erro ao buscar cadastro: {e}")
            return None

    def _buscar_estoque(self, cod_produto):
        """Busca posição de estoque"""
        try:
            # CORRIGIDO: usando cod_produto com underscore
            result = self.conn.execute(f"""
                SELECT 
                    saldo_estoque,
                    saldo_oc,
                    custo_unitario,
                    ultima_entrada
                FROM sqlite_db.saldo_custo_entrada
                WHERE CAST(cod_produto AS VARCHAR) = '{cod_produto}'
            """).fetchone()

            if not result:
                return None

            return {
                'saldo_fisico': result[0] if result[0] else 0,
                'saldo_oc': result[1] if result[1] else 0,
                'custo_unitario': float(result[2]) if result[2] else 0.0,
                'ultima_entrada': result[3],
                'estoque_total': (result[0] if result[0] else 0) + (result[1] if result[1] else 0)
            }
        except Exception as e:
            print(f"⚠️  Erro ao buscar estoque: {e}")
            return None

    def _buscar_vendas(self, cod_produto):
        """Busca histórico completo de vendas"""
        try:
            # CORRIGIDO: usando cod_produto e data_movimento
            # Vendas 12 meses
            vendas_12m = self.conn.execute(f"""
                SELECT SUM(quantidade)
                FROM sqlite_db.vendas
                WHERE cod_produto = '{cod_produto}'
                AND CAST(data_movimento AS DATE) >= CURRENT_DATE - INTERVAL 365 days
            """).fetchone()[0] or 0

            # Estatísticas gerais (últimos 12 meses)
            stats = self.conn.execute(f"""
                SELECT 
                    COUNT(DISTINCT CAST(data_movimento AS DATE)) as dias_com_venda,
                    SUM(quantidade) as total_vendido,
                    MIN(data_movimento) as primeira_venda,
                    MAX(data_movimento) as ultima_venda,
                    STDDEV_POP(quantidade) as std_venda,
                    COUNT(DISTINCT cod_clifor) as total_clientes
                FROM sqlite_db.vendas
                WHERE cod_produto = '{cod_produto}'
                AND CAST(data_movimento AS DATE) >= CURRENT_DATE - INTERVAL 12 months
            """).fetchone()

            dias_com_venda = stats[0] or 0
            total_vendido = stats[1] or 0
            primeira_venda = stats[2]
            ultima_venda = stats[3]
            std_venda = float(stats[4]) if stats[4] else 0.0
            total_clientes = stats[5] or 0

            # Calcula dias desde última venda
            if ultima_venda:
                try:
                    dt_ult = datetime.fromisoformat(ultima_venda)
                    dias_sem_venda = (datetime.now() - dt_ult).days
                except:
                    dias_sem_venda = 9999
            else:
                dias_sem_venda = 9999

            # Calcula média diária baseada no intervalo
            if primeira_venda and ultima_venda:
                try:
                    dt_pri = datetime.fromisoformat(primeira_venda)
                    dt_ult = datetime.fromisoformat(ultima_venda)
                    dias_intervalo = (dt_ult - dt_pri).days + 1
                    media_dia = total_vendido / dias_intervalo if dias_intervalo > 0 else 0
                except:
                    media_dia = 0.0
            else:
                media_dia = 0.0

            # Tendência trimestral
            trim_atual = self.conn.execute(f"""
                SELECT SUM(quantidade), COUNT(DISTINCT cod_clifor)
                FROM sqlite_db.vendas
                WHERE cod_produto = '{cod_produto}'
                AND CAST(data_movimento AS DATE) >= CURRENT_DATE - INTERVAL 90 days
            """).fetchone()

            trim_anterior = self.conn.execute(f"""
                SELECT SUM(quantidade), COUNT(DISTINCT cod_clifor)
                FROM sqlite_db.vendas
                WHERE cod_produto = '{cod_produto}'
                AND CAST(data_movimento AS DATE) >= CURRENT_DATE - INTERVAL 180 days
                AND CAST(data_movimento AS DATE) < CURRENT_DATE - INTERVAL 90 days
            """).fetchone()

            qtd_trim_atual = trim_atual[0] or 0
            cli_trim_atual = trim_atual[1] or 0
            qtd_trim_anterior = trim_anterior[0] or 0
            cli_trim_anterior = trim_anterior[1] or 0

            return {
                'vendas_12m': vendas_12m,
                'dias_com_venda': dias_com_venda,
                'total_vendido': total_vendido,
                'media_dia': media_dia,
                'media_dia_real_12m': vendas_12m / 365.0,
                'std_venda_dia': std_venda,
                'primeira_venda': primeira_venda,
                'ultima_venda': ultima_venda,
                'dias_sem_venda': dias_sem_venda,
                'total_clientes': total_clientes,
                'qtd_trim_atual': qtd_trim_atual,
                'qtd_trim_anterior': qtd_trim_anterior,
                'cli_trim_atual': cli_trim_atual,
                'cli_trim_anterior': cli_trim_anterior
            }
        except Exception as e:
            print(f"⚠️  Erro ao buscar vendas: {e}")
            return {
                'vendas_12m': 0,
                'dias_com_venda': 0,
                'total_vendido': 0,
                'media_dia': 0.0,
                'media_dia_real_12m': 0.0,
                'std_venda_dia': 0.0,
                'primeira_venda': None,
                'ultima_venda': None,
                'dias_sem_venda': 9999,
                'total_clientes': 0,
                'qtd_trim_atual': 0,
                'qtd_trim_anterior': 0,
                'cli_trim_atual': 0,
                'cli_trim_anterior': 0
            }

    def _buscar_classificacao(self, cod_produto):
        """Busca classificação ABC e XYZ"""
        try:
            # CORRIGIDO: usando cod_produto
            # Busca ABC
            abc_result = self.conn.execute(f"""
                SELECT curva_abc
                FROM curva_abc_financeira
                WHERE cod_produto = '{cod_produto}'
            """).fetchone()

            abc = abc_result[0] if abc_result else 'C'

            # Busca XYZ
            xyz_result = self.conn.execute(f"""
                SELECT curva_xyz
                FROM curva_xyz_consistencia
                WHERE cod_produto = '{cod_produto}'
            """).fetchone()

            xyz = xyz_result[0] if xyz_result else 'Z'

        except Exception as e:
            print(f"⚠️  Tabelas de classificação não encontradas: {e}")
            abc = 'C'
            xyz = 'Z'

        # Lead time da config
        cfg_compras = self.config.parametros.get('compras', {})
        lead_time_padrao = cfg_compras.get('leadtime_padrao', 7)

        return {
            'curva_abc': abc,
            'curva_xyz': xyz,
            'lead_time_dias': lead_time_padrao
        }

    def _calcular_tendencias(self, vendas):
        """Calcula tendências de vendas e clientes"""
        # Variação de vendas
        if vendas['qtd_trim_anterior'] > 0:
            var_vendas = (vendas['qtd_trim_atual'] - vendas['qtd_trim_anterior']) / vendas['qtd_trim_anterior']
        else:
            var_vendas = 0.0

        # Tendência vendas
        if var_vendas >= 0.20:
            tend_vendas = "EM ALTA"
        elif var_vendas <= -0.20:
            tend_vendas = "EM QUEDA"
        else:
            tend_vendas = "ESTÁVEL"

        # Saldo clientes
        saldo_clientes = vendas['cli_trim_atual'] - vendas['cli_trim_anterior']

        # Tendência clientes
        if saldo_clientes > 0:
            tend_clientes = f"GANHO +{saldo_clientes}"
        elif saldo_clientes < 0:
            tend_clientes = f"PERDA {saldo_clientes}"
        else:
            tend_clientes = "MANTEVE"

        # Perfil cliente
        qtd_clientes = vendas['total_clientes']
        if qtd_clientes == 0:
            perfil = "Sem Venda"
        elif qtd_clientes <= 2:
            perfil = "Dedicado (1-2)"
        elif qtd_clientes <= 9:
            perfil = "Concentrado (3-9)"
        else:
            perfil = "Pulverizado (10+)"

        return {
            'var_vendas': var_vendas,
            'tendencia_vendas': tend_vendas,
            'saldo_clientes': saldo_clientes,
            'tendencia_clientes': tend_clientes,
            'perfil_cliente': perfil
        }

    def _calcular_seguranca(self, vendas, classificacao):
        """Calcula estoque de segurança"""
        # Fator Z baseado em XYZ
        xyz = classificacao['curva_xyz']
        if xyz == 'X':
            fator_z = 1.65
        elif xyz == 'Y':
            fator_z = 1.28
        else:
            fator_z = 0.84

        # Estoque de segurança
        lead_time = classificacao['lead_time_dias']
        std_dia = vendas['std_venda_dia']

        estoque_seguranca = fator_z * std_dia * math.sqrt(lead_time)

        return {
            'fator_z': fator_z,
            'std_venda_dia': std_dia,
            'lead_time': lead_time,
            'estoque_seguranca': round(estoque_seguranca, 0)
        }

    def _calcular_necessidades(self, vendas, seguranca, estoque, cadastro, classificacao):
        """Calcula ponto de suprimento e estoque meta"""
        cfg_compras = self.config.parametros.get('compras', {})
        cfg_produto = self.config.parametros.get('produto', {})

        meses_cobertura = cfg_compras.get('meses_cobertura', 2)
        dias_novo = cfg_produto.get('dias_lancamento', 365)

        media_dia = vendas['media_dia']
        lead_time = seguranca['lead_time']
        est_seguranca = seguranca['estoque_seguranca']
        dias_vida = cadastro['dias_vida']
        dias_sem_venda = vendas['dias_sem_venda']
        abc = classificacao['curva_abc']

        # LÓGICA DE AJUSTE PARA ITEM NOVO (Boost anti-ruptura)
        if (estoque['saldo_fisico'] == 0 and 
            abc in ['A', 'B'] and 
            dias_vida <= dias_novo):

            if dias_sem_venda <= 30:
                media_calculo = media_dia * 1.20
                boost_aplicado = "1.20x (Sem venda ≤ 30 dias)"
            elif dias_sem_venda <= 90:
                media_calculo = media_dia * 1.50
                boost_aplicado = "1.50x (Sem venda ≤ 90 dias)"
            else:
                media_calculo = media_dia * 2.00
                boost_aplicado = "2.00x (Sem venda > 90 dias)"
        else:
            media_calculo = media_dia
            boost_aplicado = "Nenhum (Item normal)"

        # Ponto de Suprimento
        ponto_suprimento = round(media_calculo * lead_time + est_seguranca, 0)

        # Estoque Meta
        estoque_meta = round(media_calculo * 30 * meses_cobertura + est_seguranca, 0)

        return {
            'media_calculo': media_calculo,
            'boost_aplicado': boost_aplicado,
            'ponto_suprimento': ponto_suprimento,
            'estoque_meta': estoque_meta,
            'meses_cobertura': meses_cobertura
        }

    def _calcular_sugestao(self, necessidades, estoque, cadastro):
        """Calcula sugestão de compra"""
        estoque_meta = necessidades['estoque_meta']
        saldo_fisico = estoque['saldo_fisico']
        saldo_oc = max(0, estoque['saldo_oc'])  # Sanitiza OC negativa

        # Sugestão bruta
        sugestao_bruta = estoque_meta - saldo_fisico - saldo_oc

        # Necessidade líquida
        necessidade_liquida = max(0, sugestao_bruta)

        # Arredonda para lotes econômicos
        lote_economico = cadastro['lote_economico']
        if necessidade_liquida > 0:
            lotes_cheios = math.ceil(necessidade_liquida / lote_economico)
            sugestao_final = lotes_cheios * lote_economico
        else:
            lotes_cheios = 0
            sugestao_final = 0

        # Subtotal
        subtotal = sugestao_final * estoque['custo_unitario']

        return {
            'sugestao_bruta': sugestao_bruta,
            'necessidade_liquida': necessidade_liquida,
            'lotes_cheios': lotes_cheios,
            'sugestao_final': sugestao_final,
            'subtotal': subtotal
        }

    def _calcular_score(self, sugestao, vendas, estoque, classificacao, tendencias):
        """Calcula score de priorização"""
        score = 0
        detalhes = []

        # Ruptura total
        if estoque['saldo_fisico'] <= 0:
            score += 5000
            detalhes.append("Ruptura Total: +5000")

        # Risco iminente
        if (estoque['saldo_fisico'] <= 
            vendas['media_dia'] * classificacao['lead_time_dias']):
            score += 2500
            detalhes.append("Risco Iminente: +2500")

        # Curva ABC
        abc = classificacao['curva_abc']
        if abc == 'A':
            score += 1000
            detalhes.append("Curva A: +1000")
        elif abc == 'B':
            score += 500
            detalhes.append("Curva B: +500")
        else:
            score += 100
            detalhes.append("Curva C: +100")

        # Tendência alta
        if tendencias['tendencia_vendas'] == "EM ALTA":
            score += 500
            detalhes.append("Tendência Alta: +500")

        # Giro financeiro
        giro_financeiro = int(vendas['media_dia'] * estoque['custo_unitario'])
        score += giro_financeiro
        detalhes.append(f"Giro Financeiro: +{giro_financeiro}")

        return {
            'score': score,
            'detalhes': detalhes
        }

    def _gerar_diagnostico(self, cadastro, estoque, vendas, sugestao, necessidades):
        """Gera diagnóstico e aplica bloqueios"""
        cfg_produto = self.config.parametros.get('produto', {})
        dias_novo = cfg_produto.get('dias_lancamento', 365)

        # Cobertura virtual
        estoque_total = estoque['estoque_total']
        venda_mensal = vendas['media_dia'] * 30

        if venda_mensal > 0:
            cobertura_meses = estoque_total / venda_mensal
        else:
            cobertura_meses = 99.0

        # Validação de Giro (O Juiz)
        if (estoque['saldo_fisico'] == 0 and 
            estoque['saldo_oc'] == 0 and 
            vendas['media_dia'] == 0):

            if cadastro['dias_vida'] <= dias_novo:
                validacao_giro = "SEM MOVIMENTO - ITEM NOVO (Implantação)"
            else:
                validacao_giro = "SEM MOVIMENTO (Item velho parado)"
        elif cobertura_meses > 6:
            validacao_giro = f"ALERTA: Excesso ({cobertura_meses:.1f}m)"
        elif vendas['media_dia'] < 0.05 and sugestao['sugestao_final'] > 0:
            validacao_giro = "ALERTA: Sem Venda Recente"
        else:
            validacao_giro = "COERENTE"

        # Define motivo de bloqueio
        if cadastro['ativo'] == 'NÃO':
            motivo_bloqueio = "Produto inativo no cadastro"
            bloqueado = True
        elif "ALERTA" in validacao_giro:
            motivo_bloqueio = validacao_giro
            bloqueado = True
        else:
            motivo_bloqueio = ""
            bloqueado = False

        # Sugestão calculada (antes do bloqueio)
        sugestao_calculada = sugestao['sugestao_final']

        # Aplica bloqueio na sugestão final
        if cadastro['ativo'] == 'NÃO':
            sugestao_final = 0
        elif "ALERTA" in validacao_giro:
            sugestao_final = 0
        elif validacao_giro == "SEM MOVIMENTO - ITEM NOVO (Implantação)":
            sugestao_final = cadastro['lote_economico']  # 1 lote para item novo
        else:
            sugestao_final = sugestao['sugestao_final']

        return {
            'cobertura_virtual_meses': cobertura_meses,
            'validacao_giro': validacao_giro,
            'motivo_bloqueio': motivo_bloqueio,
            'bloqueado': bloqueado,
            'sugestao_calculada': sugestao_calculada,
            'sugestao_final': sugestao_final
        }

    def _comparar_com_sistema(self, cod_produto, diagnostico):
        """Compara cálculo manual com o do sistema (se existir tabela)"""
        try:
            # Tenta buscar valor do sistema
            sistema = self.conn.execute(f"""
                SELECT sugestao_final, validacao_giro, motivo_bloqueio
                FROM relatorio_final
                WHERE cod_produto = '{cod_produto}'
            """).fetchone()

            if not sistema:
                print("ℹ️  Item não encontrado no relatório do sistema")
                return {'encontrado': False}

            sugestao_sistema = sistema[0] or 0
            validacao_sistema = sistema[1]
            motivo_sistema = sistema[2]

            # Compara
            diferenca = diagnostico['sugestao_final'] - sugestao_sistema
            percentual = (abs(diferenca) / sugestao_sistema * 100) if sugestao_sistema > 0 else 0

            if diferenca == 0:
                print("✅ CÁLCULOS VALIDADOS: Auditoria = Sistema")
                print(f"   Sugestão: {diagnostico['sugestao_final']} pçs")
            else:
                print("⚠️  DIVERGÊNCIA DETECTADA")
                print(f"   Sistema: {sugestao_sistema} pçs")
                print(f"   Auditoria: {diagnostico['sugestao_final']} pçs")
                print(f"   Diferença: {diferenca:+} pçs ({percentual:.1f}%)")

            return {
                'encontrado': True,
                'sugestao_sistema': sugestao_sistema,
                'sugestao_auditoria': diagnostico['sugestao_final'],
                'diferenca': diferenca,
                'percentual_diferenca': percentual,
                'validacao_sistema': validacao_sistema,
                'motivo_sistema': motivo_sistema,
                'match': diferenca == 0
            }

        except Exception as e:
            print(f"ℹ️  Tabela relatorio_final não encontrada ou erro: {e}")
            return {'encontrado': False}

    # =====================================================================
    # MÉTODOS DE EXIBIÇÃO
    # =====================================================================

    def _exibir_cadastro(self, cadastro):
        """Exibe dados cadastrais"""
        print(f"Código: {cadastro['codigo']}")
        print(f"Descrição: {cadastro['descricao']}")
        print(f"Marca: {cadastro['marca']}")
        print(f"Referência: {cadastro['ref_fornecedor']}")
        print(f"Status: {cadastro['ativo']}")
        print(f"Lote Econômico: {cadastro['lote_economico']}")
        print(f"Data Cadastro: {cadastro['data_cadastro']}")
        print(f"Dias de Vida: {cadastro['dias_vida']} dias")

        if cadastro['dias_vida'] <= 365:
            print(f"  ℹ️  Este é um ITEM NOVO (menos de 1 ano)")

    def _exibir_estoque(self, estoque):
        """Exibe posição de estoque"""
        print(f"Saldo Físico: {estoque['saldo_fisico']}")
        print(f"Saldo OC: {estoque['saldo_oc']}")
        print(f"Estoque Total: {estoque['estoque_total']}")
        print(f"Custo Unitário: R$ {estoque['custo_unitario']:.2f}")
        print(f"Última Entrada: {estoque['ultima_entrada']}")

        if estoque['saldo_fisico'] <= 0:
            print(f"  ⚠️  RUPTURA DE ESTOQUE!")
        if estoque['saldo_oc'] < 0:
            print(f"  ⚠️  OC NEGATIVA (erro de lançamento)")

    def _exibir_vendas(self, vendas):
        """Exibe análise de vendas"""
        print(f"Vendas últimos 12 meses: {vendas['vendas_12m']} pçs")
        print(f"Média Diária (Base 12m): {vendas['media_dia_real_12m']:.4f} pçs/dia")
        print(f"Média Diária (Intervalo): {vendas['media_dia']:.4f} pçs/dia")
        print(f"Desvio Padrão: {vendas['std_venda_dia']:.4f}")
        print(f"Dias com Venda: {vendas['dias_com_venda']}")
        print(f"Dias sem Venda: {vendas['dias_sem_venda']}")
        print(f"Total de Clientes: {vendas['total_clientes']}")
        print(f"Primeira Venda: {vendas['primeira_venda']}")
        print(f"Última Venda: {vendas['ultima_venda']}")

        if vendas['vendas_12m'] == 0:
            print(f"  ⚠️  Item SEM VENDAS nos últimos 12 meses!")

    def _exibir_classificacao(self, classificacao):
        """Exibe classificações"""
        print(f"Curva ABC: {classificacao['curva_abc']}")
        print(f"Curva XYZ: {classificacao['curva_xyz']}")
        print(f"Lead Time: {classificacao['lead_time_dias']} dias")

    def _exibir_tendencias(self, tendencias):
        """Exibe tendências"""
        print(f"Variação Vendas: {tendencias['var_vendas']:.2%}")
        print(f"Tendência Vendas: {tendencias['tendencia_vendas']}")
        print(f"Saldo Clientes: {tendencias['saldo_clientes']}")
        print(f"Tendência Clientes: {tendencias['tendencia_clientes']}")
        print(f"Perfil Cliente: {tendencias['perfil_cliente']}")

    def _exibir_seguranca(self, seguranca):
        """Exibe cálculo de segurança"""
        print(f"Curva XYZ: {self.resultado['classificacao']['curva_xyz']}")
        print(f"Fator Z: {seguranca['fator_z']}")
        print(f"Fórmula: Estoque Seg. = Z × σ × √Lead Time")
        print(f"Cálculo: {seguranca['fator_z']} × {seguranca['std_venda_dia']:.4f} × √{seguranca['lead_time']}")
        print(f"Estoque de Segurança: {seguranca['estoque_seguranca']:.0f} pçs")

    def _exibir_necessidades(self, necessidades):
        """Exibe ponto de suprimento e estoque meta"""
        print(f"Média Base: {self.resultado['vendas']['media_dia']:.4f} pçs/dia")
        print(f"Boost Aplicado: {necessidades['boost_aplicado']}")
        print(f"Média de Cálculo: {necessidades['media_calculo']:.4f} pçs/dia")
        print(f"Meses de Cobertura: {necessidades['meses_cobertura']}")
        print()
        print(f"📍 Ponto de Suprimento:")
        print(f"   Fórmula: Média × Lead Time + Estoque Segurança")
        print(f"   Cálculo: {necessidades['media_calculo']:.4f} × {self.resultado['seguranca']['lead_time']} + {self.resultado['seguranca']['estoque_seguranca']}")
        print(f"   Resultado: {necessidades['ponto_suprimento']:.0f} pçs")
        print()
        print(f"🎯 Estoque Meta:")
        print(f"   Fórmula: Média × 30 × Meses Cobertura + Estoque Segurança")
        print(f"   Cálculo: {necessidades['media_calculo']:.4f} × 30 × {necessidades['meses_cobertura']} + {self.resultado['seguranca']['estoque_seguranca']}")
        print(f"   Resultado: {necessidades['estoque_meta']:.0f} pçs")

    def _exibir_sugestao(self, sugestao):
        """Exibe cálculo da sugestão"""
        print(f"Estoque Meta: {self.resultado['necessidades']['estoque_meta']:.0f}")
        print(f"(-) Saldo Físico: {self.resultado['estoque']['saldo_fisico']}")
        print(f"(-) Saldo OC: {max(0, self.resultado['estoque']['saldo_oc'])}")
        print(f"(=) Sugestão Bruta: {sugestao['sugestao_bruta']:.0f}")
        print()
        print(f"Necessidade Líquida: {sugestao['necessidade_liquida']:.0f}")
        print(f"Lote Econômico: {self.resultado['cadastro']['lote_economico']}")
        print(f"Lotes Cheios: {sugestao['lotes_cheios']}")
        print()
        print(f"✅ SUGESTÃO FINAL (antes bloqueio): {sugestao['sugestao_final']} pçs")
        print(f"💰 Subtotal: R$ {sugestao['subtotal']:.2f}")

    def _exibir_score(self, score):
        """Exibe score de priorização"""
        print(f"Score Total: {score['score']} pontos")
        print()
        print("Composição:")
        for detalhe in score['detalhes']:
            print(f"  • {detalhe}")

    def _exibir_diagnostico(self, diagnostico):
        """Exibe diagnóstico e validações"""
        print(f"Cobertura Virtual: {diagnostico['cobertura_virtual_meses']:.1f} meses")
        print(f"Validação de Giro: {diagnostico['validacao_giro']}")
        print()

        if diagnostico['bloqueado']:
            print(f"🚫 COMPRA BLOQUEADA")
            print(f"   Motivo: {diagnostico['motivo_bloqueio']}")
            print(f"   Sugestão Calculada: {diagnostico['sugestao_calculada']} pçs")
            print(f"   Sugestão Final: {diagnostico['sugestao_final']} pçs")
        else:
            print(f"✅ COMPRA APROVADA")
            print(f"   Sugestão Final: {diagnostico['sugestao_final']} pçs")

    def _emitir_parecer(self):
        """Emite parecer técnico final"""
        cadastro = self.resultado['cadastro']
        estoque = self.resultado['estoque']
        vendas = self.resultado['vendas']
        classificacao = self.resultado['classificacao']
        necessidades = self.resultado['necessidades']
        sugestao = self.resultado['sugestao']
        diagnostico = self.resultado['diagnostico']
        tendencias = self.resultado['tendencias']

        print()
        print("-" * 80)
        print("ANÁLISE TÉCNICA:")
        print("-" * 80)

        # Análise do item
        if cadastro['ativo'] == 'NÃO':
            print("❌ RECOMENDAÇÃO: NÃO COMPRAR")
            print("   Produto está INATIVO no cadastro.")
            print("   Ação sugerida: Verificar se deve ser reativado.")
            return

        if vendas['vendas_12m'] == 0:
            if cadastro['dias_vida'] <= 365:
                print("⚠️  RECOMENDAÇÃO: AGUARDAR OU COMPRAR 1 LOTE")
                print("   Item NOVO sem vendas ainda.")
                print("   Considere comprar 1 lote para teste de mercado.")
            else:
                print("❌ RECOMENDAÇÃO: NÃO COMPRAR")
                print("   Item VELHO sem vendas há mais de 12 meses.")
                print("   Produto provavelmente descontinuado.")
            return

        if diagnostico['cobertura_virtual_meses'] > 6:
            print("⚠️  RECOMENDAÇÃO: NÃO COMPRAR (Excesso)")
            print(f"   Cobertura atual: {diagnostico['cobertura_virtual_meses']:.1f} meses")
            print("   Risco: Obsolescência e capital parado.")
            return

        if estoque['saldo_fisico'] <= 0:
            print("🚨 PRIORIDADE MÁXIMA: RUPTURA DE ESTOQUE")
            print(f"   Sugestão: {diagnostico['sugestao_final']} pçs")
            print(f"   Investimento: R$ {sugestao['subtotal']:.2f}")
            print("   Ação: Comprar URGENTE!")
            return

        if sugestao['sugestao_final'] > 0:
            print("✅ RECOMENDAÇÃO: COMPRAR")
            print(f"   Sugestão: {diagnostico['sugestao_final']} pçs")
            print(f"   Investimento: R$ {sugestao['subtotal']:.2f}")
            print(f"   Cobertura após compra: ~{necessidades['meses_cobertura']} meses")
            print(f"   Curva ABC: {classificacao['curva_abc']} | XYZ: {classificacao['curva_xyz']}")

            if tendencias['tendencia_vendas'] == "EM ALTA":
                print("   📈 Tendência de vendas em ALTA")
            elif tendencias['tendencia_vendas'] == "EM QUEDA":
                print("   📉 Atenção: Tendência de vendas em QUEDA")
        else:
            print("✅ RECOMENDAÇÃO: NÃO COMPRAR")
            print("   Estoque atual é suficiente.")
            print(f"   Cobertura: {diagnostico['cobertura_virtual_meses']:.1f} meses")

        print()
        print("=" * 80)


def main():
    """Função principal"""

    print("=" * 80)
    print("AUDITOR DE ITEM - Sistema de Compras e Estoque")
    print("Versão Standalone - sem dependências de módulos")
    print("=" * 80)
    print()

    # Caminhos padrão
    PROJECT_ROOT = Path.cwd()

    # Verifica se estamos na pasta correta
    if not (PROJECT_ROOT / "data").exists():
        print("⚠️  Pasta 'data' não encontrada no diretório atual")
        print(f"   Diretório atual: {PROJECT_ROOT}")
        resposta = input("\nDeseja informar o caminho do projeto? (s/n): ").strip().lower()
        if resposta == 's':
            caminho = input("Digite o caminho completo do projeto: ").strip()
            PROJECT_ROOT = Path(caminho)
        else:
            print("❌ Operação cancelada")
            return

    sqlite_path = PROJECT_ROOT / "data" / "vendas.db"
    config_path = PROJECT_ROOT / "config" / "config.yaml"

    # Carrega configurações
    config = ConfigSimples(config_path)

    # Cria auditor
    auditor = AuditorItem(sqlite_path, config)

    # Conecta ao banco
    if not auditor.conectar():
        return

    try:
        # Solicita código do produto
        cod_produto = input("\nDigite o CÓDIGO DO PRODUTO para auditar: ").strip()

        if not cod_produto:
            print("❌ Código não informado!")
            return

        print()

        # Executa auditoria
        sucesso = auditor.auditar(cod_produto)

        if sucesso:
            print("\n✅ Auditoria concluída com sucesso!")
        else:
            print("\n❌ Auditoria falhou!")

    except KeyboardInterrupt:
        print("\n\n⚠️  Operação cancelada pelo usuário")
    except Exception as e:
        print(f"\n❌ ERRO: {e}")
        import traceback
        traceback.print_exc()
    finally:
        auditor.desconectar()


if __name__ == "__main__":
    main()
