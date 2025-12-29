import polars as pl
import numpy as np
from datetime import datetime

class EstoqueMath:
    """Classe com métodos estáticos para cálculos de estoque (Refatorada Fases 2 e 3)"""
    
    @staticmethod
    def _ler_config(objeto_config, atributo_ou_chave):
        """Tenta ler uma configuração seja ela um Atributo (Objeto) ou Chave (Dict)."""
        try:
            return getattr(objeto_config, atributo_ou_chave)
        except AttributeError:
            # Suporte a dicionários aninhados e acesso seguro
            try:
                return objeto_config[atributo_ou_chave]
            except (KeyError, TypeError):
                raise Exception(f"Configuração '{atributo_ou_chave}' não encontrada")

    @staticmethod
    def aplicar_sazonalidade_projetada(df: pl.DataFrame, indices_dict: dict) -> pl.DataFrame:
        """Calcula o fator sazonal baseando-se na DATA DE CHEGADA da mercadoria."""
        if not indices_dict or len(indices_dict) != 12:
            return df.with_columns(pl.lit(1.0).alias("fator_sazonal_projetado"))
        
        lista_indices = [indices_dict.get(m, 1.0) for m in range(1, 13)]
        mes_atual = datetime.now().month
        
        def calcular_fator_futuro(leadtime):
            if leadtime is None:
                leadtime = 7
            
            meses_espera = leadtime / 30.0
            duracao_estoque = 1.5
            soma_indices = 0.0
            pontos_verificados = 0
            cursor = meses_espera
            fim_janela = meses_espera + duracao_estoque
            
            while cursor < fim_janela:
                mes_futuro_absoluto = mes_atual + int(cursor)
                index_lista = (mes_futuro_absoluto - 1) % 12
                soma_indices += lista_indices[index_lista]
                pontos_verificados += 1
                cursor += 0.5
            
            if pontos_verificados == 0:
                return 1.0
            
            fator = soma_indices / pontos_verificados
            return max(0.5, min(fator, 2.5))
        
        return df.with_columns([
            pl.col("lead_time_dias").map_elements(
                calcular_fator_futuro, return_dtype=pl.Float64
            ).alias("fator_sazonal_projetado")
        ])

    @staticmethod
    def calcular_tendencias(df: pl.DataFrame) -> pl.DataFrame:
        """Calcula as classificações de Tendência e Perfil de Cliente."""
        if "var_vendas" not in df.columns:
            df = df.with_columns([
                pl.lit(0.0).alias("var_vendas"),
                pl.lit(0).alias("saldo_clientes"),
                pl.lit(0).alias("qtd_clientes_ativos")
            ])
        
        return df.with_columns([
            pl.when(pl.col("var_vendas").fill_null(0.0) > 0.20).then(pl.lit("EM ALTA"))
            .when(pl.col("var_vendas").fill_null(0.0) < -0.20).then(pl.lit("EM QUEDA"))
            .otherwise(pl.lit("ESTÁVEL")).alias("tendencia_vendas"),
            
            pl.when(pl.col("saldo_clientes").fill_null(0) > 0)
            .then(pl.format("GANHO +{}", pl.col("saldo_clientes")))
            .when(pl.col("saldo_clientes").fill_null(0) < 0)
            .then(pl.format("PERDA {}", pl.col("saldo_clientes")))
            .otherwise(pl.lit("MANTEVE")).alias("tendencia_clientes"),
            
            pl.when(pl.col("qtd_clientes_ativos").fill_null(0) == 0).then(pl.lit("Sem Venda"))
            .when(pl.col("qtd_clientes_ativos").fill_null(0) <= 2).then(pl.lit("Dedicado (1-2)"))
            .when(pl.col("qtd_clientes_ativos").fill_null(0) <= 9).then(pl.lit("Concentrado (3-9)"))
            .otherwise(pl.lit("Pulverizado (10+)")).alias("perfil_cliente")
        ])

    @staticmethod
    def calcular_seguranca(df: pl.DataFrame, config) -> pl.DataFrame:
        """Calcula Estoque de Segurança (Refatorado FASE 2 - Config Dinâmica)."""
        # --- Lógica de Leitura de Configuração com Fallback ---
        try:
            cfg_estoque = EstoqueMath._ler_config(config, 'estoque')
            cfg_fatores = EstoqueMath._ler_config(cfg_estoque, 'fator_z')
            z_x = float(EstoqueMath._ler_config(cfg_fatores, 'X'))
            z_y = float(EstoqueMath._ler_config(cfg_fatores, 'Y'))
            z_z = float(EstoqueMath._ler_config(cfg_fatores, 'Z'))
        except Exception:
            z_x, z_y, z_z = 1.65, 1.28, 0.84

        def get_z_factor(xyz):
            if xyz == "X": return z_x
            if xyz == "Y": return z_y
            return z_z
        
        return df.with_columns([
            pl.col("curva_xyz").map_elements(get_z_factor, return_dtype=pl.Float64).alias("fator_z"),
            (
                pl.col("curva_xyz").map_elements(get_z_factor, return_dtype=pl.Float64) *
                pl.col("std_venda_dia") *
                pl.col("lead_time_dias").fill_null(7).sqrt()
            ).fill_null(0).alias("estoque_seguranca")
        ])

    @staticmethod
    def calcular_necessidades(df: pl.DataFrame, config) -> pl.DataFrame:
        """Calcula Ponto de Suprimento e Estoque Meta (COM TRAVA ZUMBI)."""
        cfg_compras = EstoqueMath._ler_config(config, 'compras')
        cfg_produto = EstoqueMath._ler_config(config, 'produto')
        meses_cobertura = EstoqueMath._ler_config(cfg_compras, 'meses_cobertura')
        dias_novo = EstoqueMath._ler_config(cfg_produto, 'dias_lancamento')
        
        # Cria a coluna dias_vida
        df = df.with_columns([
            (pl.lit(datetime.now()) - pl.col("data_cadastro").cast(pl.Datetime)).dt.total_days().alias("dias_vida")
        ])
        
        # Calcula a média ajustada (Boost anti-ruptura já está correto aqui)
        df = df.with_columns([
            pl.when(
                (pl.col("saldo_estoque") == 0) &
                pl.col("curva_abc").is_in(["A", "B"]) &
                (pl.col("dias_vida") > dias_novo) 
            )
            .then(
                pl.when(pl.col("dias_sem_venda") > 90).then(pl.col("media_venda_dia") * 1.50)
                .when(pl.col("dias_sem_venda") > 30).then(pl.col("media_venda_dia") * 1.20)
                .otherwise(pl.col("media_venda_dia") * 2.00)
            )
            .otherwise(pl.col("media_venda_dia"))
            .alias("media_calculo")
        ])
        
        # Calcula os alvos
        df = df.with_columns([
            (pl.col("media_calculo") * pl.col("lead_time_dias") + pl.col("estoque_seguranca")).round(0).alias("ponto_suprimento"),
            (pl.col("media_calculo") * 30 * meses_cobertura + pl.col("estoque_seguranca")).round(0).alias("estoque_meta")
        ])

        # --- ALTERAÇÃO AQUI: IMPLEMENTAÇÃO DA TRAVA ZUMBI ---
        return df.with_columns([
            pl.when(pl.col("dias_sem_venda") > 180) # Se não vende há 6 meses
            .then(0.0)                              # Mata a sugestão (Zumbi)
            .otherwise(                             # Caso contrário, segue o cálculo normal
                pl.col("estoque_meta") - pl.col("saldo_estoque") - pl.col("saldo_oc")
            ).alias("sugestao_bruta")
        ])
        
    @staticmethod
    def aplicar_lote_economico(df: pl.DataFrame, config) -> pl.DataFrame:
        """Arredonda para lotes econômicos usando Limite de Virada."""
        cfg_lote = EstoqueMath._ler_config(config, 'lote')
        limite = EstoqueMath._ler_config(cfg_lote, 'limite_virada')

        return df.with_columns([
            pl.when(pl.col("sugestao_bruta") <= 0).then(0).otherwise(pl.col("sugestao_bruta")).alias("necessidade_liquida")
        ]).with_columns([
            (pl.col("necessidade_liquida") % pl.col("lote_economico")).alias("resto")
        ]).with_columns([
            pl.when(pl.col("resto") < (pl.col("lote_economico") * limite))
            .then((pl.col("necessidade_liquida") / pl.col("lote_economico")).floor())
            .otherwise((pl.col("necessidade_liquida") / pl.col("lote_economico")).ceil())
            .alias("lotes_cheios")
        ]).with_columns([
            (pl.col("lotes_cheios") * pl.col("lote_economico")).cast(pl.Int32).alias("sugestao_final")
        ]).with_columns([
            (pl.col("sugestao_final") * pl.col("custo_unitario")).alias("subtotal")
        ])

    @staticmethod
    def calcular_score(df: pl.DataFrame) -> pl.DataFrame:
        """Calcula pontuação de prioridade."""
        return df.with_columns([
            (
                pl.when(pl.col("saldo_estoque") == 0).then(5000).otherwise(0) +
                pl.when(pl.col("saldo_estoque") < pl.col("media_venda_dia") * pl.col("lead_time_dias")).then(2500).otherwise(0) +
                pl.when(pl.col("curva_abc") == "A").then(1000).when(pl.col("curva_abc") == "B").then(500).otherwise(100) +
                pl.when(pl.col("tendencia_vendas") == "EM ALTA").then(500).otherwise(0) +
                (pl.col("media_venda_dia") * pl.col("custo_unitario")).fill_null(0)
            ).round(0).cast(pl.Int32).alias("score")
        ])

    @staticmethod
    def gerar_diagnostico(df: pl.DataFrame, config) -> pl.DataFrame:
        """Gera diagnósticos e bloqueios de segurança (Refatorado FASE 3 - Config Dinâmica)."""
        
        # --- 1. Leitura de Parâmetros (Giro e Risco) ---
        try:
            cfg_giro = EstoqueMath._ler_config(config, 'giro')
            cfg_produto = EstoqueMath._ler_config(config, 'produto')
            
            # Limites do Config
            limite_cobertura = float(EstoqueMath._ler_config(cfg_giro, 'limite_meses_cobertura'))
            min_venda_dia = float(EstoqueMath._ler_config(cfg_giro, 'minimo_venda_dia'))
            dias_novo = int(EstoqueMath._ler_config(cfg_produto, 'dias_lancamento'))
        except Exception:
            # Fallback Seguro
            limite_cobertura = 6.0
            min_venda_dia = 0.05
            dias_novo = 180

        # --- 2. Preparação de Colunas Auxiliares ---
        estoque_total = pl.col("saldo_estoque") + pl.col("saldo_oc")
        venda_mensal = pl.col("media_venda_dia") * 30
        
        if "dias_vida" not in df.columns:
            df = df.with_columns([
                (pl.lit(datetime.now()) - pl.col("data_cadastro").cast(pl.Datetime)).dt.total_days().alias("dias_vida")
            ])
        
        base_calc = pl.when(estoque_total == 0).then(0.0).otherwise(estoque_total / venda_mensal)
        calc_cobertura = pl.when(base_calc.is_infinite()).then(99.0).otherwise(base_calc).fill_nan(99.0)
        
        df = df.with_columns([calc_cobertura.alias("cobertura_virtual_meses")])
        
        # --- 3. Aplicação das Regras ---
        df = df.with_columns([
            pl.when((pl.col("saldo_estoque") == 0) & (pl.col("saldo_oc") == 0) & (pl.col("media_venda_dia") == 0))
                .then(
                    pl.when(pl.col("dias_vida") <= dias_novo)
                    .then(pl.lit("SEM MOVIMENTO - ITEM NOVO (Implantação)"))
                    .otherwise(pl.lit("SEM MOVIMENTO (Item velho parado)"))
                )
                .when(pl.col("cobertura_virtual_meses") > limite_cobertura)
                .then(pl.lit(f"ALERTA: Excesso > {limite_cobertura}m"))
                .when((pl.col("media_venda_dia") < min_venda_dia) & (pl.col("sugestao_final") > 0))
                .then(pl.lit("ALERTA: Sem Venda Recente"))
                .otherwise(pl.lit("COERENTE")).alias("validacao_giro")
        ])
        
        df = df.with_columns([pl.col("sugestao_final").alias("sugestao_calculada")])
        
        # --- 4. Bloqueios ---
        df = df.with_columns([
            pl.when(pl.col("ativo") == "NO").then(pl.lit("Produto inativo no cadastro"))
            .when(pl.col("validacao_giro").str.contains("ALERTA")).then(pl.col("validacao_giro"))
            .otherwise(pl.lit("")).alias("motivo_bloqueio"),
            
            pl.when((pl.col("sugestao_final") > 0) & ((pl.col("ativo") == "NO") | pl.col("validacao_giro").str.contains("ALERTA")))
            .then(pl.lit("SIM")).otherwise(pl.lit("NÃO")).alias("calculado_mas_bloqueado"),
            
            pl.when(pl.col("ativo") == "NO").then(0)
            .when(pl.col("validacao_giro").str.contains("ALERTA")).then(0)
            .when(pl.col("validacao_giro") == "SEM MOVIMENTO - ITEM NOVO (Implantação)")
            .then(pl.col("lote_economico"))  
            .otherwise(pl.col("sugestao_final")).alias("sugestao_final")
        ])
        
        # --- 5. Score e Status Final ---
        df = df.with_columns([
            pl.when(pl.col("sugestao_final") == 0).then(0)
            .when(pl.col("validacao_giro") == "SEM MOVIMENTO - ITEM NOVO (Implantação)").then(pl.lit(9999))
            .otherwise(pl.col("score")).alias("score")
        ])
        
        return df.with_columns([
            (pl.col("sugestao_final") * pl.col("custo_unitario")).alias("subtotal"),
            pl.when(pl.col("ativo") == "NO").then(pl.lit("INATIVO"))
            .when(pl.col("validacao_giro").str.contains("ALERTA")).then(pl.lit("BLOQUEADO"))
            .when(pl.col("validacao_giro") == "SEM MOVIMENTO - ITEM NOVO (Implantação)").then(pl.lit("IMPLANTAÇÃO"))
            .when(pl.col("saldo_estoque") == 0).then(pl.lit("RUPTURA"))
            .when(pl.col("sugestao_final") > 0).then(pl.lit("COMPRAR"))
            .when(pl.col("cobertura_virtual_meses") > 12).then(pl.lit("EXCESSO"))
            .otherwise(pl.lit("OK")).alias("status_diagnostico")
        ])