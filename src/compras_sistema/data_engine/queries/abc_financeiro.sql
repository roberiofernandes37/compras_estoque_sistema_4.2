/*
  Cálculo da Curva ABC Financeira (Pareto)
  Regra: 
    A = Até 80% do faturamento acumulado
    B = De 80% até 95%
    C = Restante
*/

WITH vendas_por_produto AS (
    SELECT 
        cod_produto,
        SUM(valor_total) as total_vendido
    FROM sqlite_db.vendas
    WHERE 
        -- Converte texto para data e pega os últimos 12 meses
        TRY_CAST(data_movimento AS DATE) >= (CURRENT_DATE - INTERVAL '12 months')
    GROUP BY cod_produto
    HAVING total_vendido > 0
),

calculo_acumulado AS (
    SELECT 
        cod_produto,
        total_vendido,
        -- Soma acumulada ordenada do maior para o menor
        SUM(total_vendido) OVER (ORDER BY total_vendido DESC) as valor_acumulado,
        -- Valor total geral de todas as vendas
        SUM(total_vendido) OVER () as valor_total_geral
    FROM vendas_por_produto
)

SELECT 
    cod_produto,
    total_vendido,
    valor_acumulado,
    (valor_acumulado / valor_total_geral) as percentual_acumulado,
    CASE 
        WHEN (valor_acumulado / valor_total_geral) <= 0.80 THEN 'A'
        WHEN (valor_acumulado / valor_total_geral) <= 0.95 THEN 'B'
        ELSE 'C'
    END as curva_abc
FROM calculo_acumulado
ORDER BY total_vendido DESC;