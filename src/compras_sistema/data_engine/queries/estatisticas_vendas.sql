/*
  Cálculo de Estatísticas Avançadas: Tendências e Validação de Giro (12m)
  Correção: Reintrodução da coluna dias_com_venda necessária para o XYZ.
*/

WITH vendas_tratadas AS (
    SELECT 
        cod_produto,
        TRY_CAST(data_movimento AS DATE) as data_venda,
        quantidade,
        cod_clifor,
        MAX(ref_fornecedor) OVER (PARTITION BY cod_produto) as ref_fornecedor,
        MAX(marca) OVER (PARTITION BY cod_produto) as marca
    FROM sqlite_db.vendas
    WHERE 
        -- Pega histórico longo para cálculo de Dias de Vida
        TRY_CAST(data_movimento AS DATE) >= (CURRENT_DATE - INTERVAL '48 months')
),

periodos AS (
    SELECT
        cod_produto,
        MAX(marca) as marca,
        MAX(ref_fornecedor) as ref_fornecedor,
        
        -- Períodos Curtos para Tendência (90 dias)
        SUM(CASE WHEN data_venda >= (CURRENT_DATE - INTERVAL '90 days') THEN quantidade ELSE 0 END) as qtd_trimestre_atual,
        COUNT(DISTINCT CASE WHEN data_venda >= (CURRENT_DATE - INTERVAL '90 days') THEN cod_clifor END) as clientes_trimestre_atual,
        SUM(CASE WHEN data_venda < (CURRENT_DATE - INTERVAL '90 days') AND data_venda >= (CURRENT_DATE - INTERVAL '180 days') THEN quantidade ELSE 0 END) as qtd_trimestre_anterior,
        COUNT(DISTINCT CASE WHEN data_venda < (CURRENT_DATE - INTERVAL '90 days') AND data_venda >= (CURRENT_DATE - INTERVAL '180 days') THEN cod_clifor END) as clientes_trimestre_anterior,

        -- Venda 12 Meses (Crítico para Validação de Giro)
        SUM(CASE WHEN data_venda >= (CURRENT_DATE - INTERVAL '12 months') THEN quantidade ELSE 0 END) as venda_total_12m,

        -- Dados Gerais (Baseados no parametro meses_analise)
        SUM(CASE WHEN data_venda >= (CURRENT_DATE - INTERVAL '{meses_analise} months') THEN quantidade ELSE 0 END) as total_vendido,
        
        -- A COLUNA QUE FALTAVA FOI REINSERIDA ABAIXO:
        COUNT(DISTINCT CASE WHEN data_venda >= (CURRENT_DATE - INTERVAL '{meses_analise} months') THEN data_venda END) as dias_com_venda,
        
        MAX(data_venda) as ultima_venda,
        MIN(data_venda) as primeira_venda,
        COUNT(DISTINCT cod_clifor) as total_clientes_unicos,
        STDDEV_POP(quantidade) as std_venda_dia_amostra
    FROM vendas_tratadas
    GROUP BY cod_produto
)

SELECT 
    *,
    DATE_DIFF('day', primeira_venda, ultima_venda) + 1 as dias_intervalo,
    CASE 
        WHEN (DATE_DIFF('day', primeira_venda, ultima_venda) + 1) <= 1 THEN total_vendido 
        ELSE CAST(total_vendido AS DOUBLE) / (DATE_DIFF('day', primeira_venda, ultima_venda) + 1)
    END as media_venda_dia,
    COALESCE(std_venda_dia_amostra, 0.0) as std_venda_dia
FROM periodos;
