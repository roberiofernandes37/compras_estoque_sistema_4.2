import sys
import os
import duckdb
import polars as pl
from datetime import datetime, timedelta
from decimal import Decimal

# --- CONFIGURAÇÃO ---
CAMINHO_BASE = "/home/roberio/Documentos/New4.2 THE BEST 18 12 2025/compras-estoque-sistema"
CAMINHO_DB = os.path.join(CAMINHO_BASE, "data/vendas.db")
CAMINHO_SRC = os.path.join(CAMINHO_BASE, "src")

sys.path.append(CAMINHO_SRC)
try:
    from compras_sistema.rule_engine.stock.estoque_math import EstoqueMath
except ImportError:
    print("❌ Erro: Não foi possível importar 'EstoqueMath'.")
    sys.exit(1)

# --- SEUS PARÂMETROS REAIS ---
CONFIG = {
    'compras': {'meses_cobertura': 3.0},  # <--- AJUSTADO PARA 3 MESES
    'produto': {'dias_lancamento': 180},
    'estoque': {'fator_z': {'X': 1.65, 'Y': 1.28, 'Z': 0.84}},
    'giro': {'limite_meses_cobertura': 6.0, 'minimo_venda_dia': 0.05},
    'lote': {'limite_virada': 0.3}
}

def to_float(val):
    if val is None: return 0.0
    if isinstance(val, Decimal): return float(val)
    return float(val)

def buscar_dados_completos(sku, lead_time_manual):
    if not os.path.exists(CAMINHO_DB):
        print(f"❌ Banco não encontrado: {CAMINHO_DB}")
        return None

    con = duckdb.connect(CAMINHO_DB)
    
    try:
        print(f"🔍 Consultando SKU '{sku}' (Últimos 12 meses)...")
        
        # Data de corte: Hoje menos 365 dias
        data_corte = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
        
        query = f"""
        WITH historico AS (
            SELECT 
                cod_produto,
                MAX(data_movimento) as ultima_venda,
                -- Soma apenas vendas após a data de corte (últimos 12 meses)
                SUM(CASE WHEN data_movimento >= '{data_corte}' THEN quantidade ELSE 0 END) as total_vendido_12m,
                COUNT(*) as num_notas
            FROM vendas 
            WHERE cod_produto = '{sku}'
            GROUP BY cod_produto
        ),
        posicao_atual AS (
            SELECT
                cod_produto,
                saldo_estoque,
                saldo_oc,
                custo_unitario
            FROM saldo_custo_entrada
            WHERE cod_produto = '{sku}'
        )
        SELECT 
            h.ultima_venda,
            h.total_vendido_12m,
            p.saldo_estoque,
            p.saldo_oc,
            p.custo_unitario
        FROM historico h
        LEFT JOIN posicao_atual p ON h.cod_produto = p.cod_produto
        """
        
        df_raw = con.execute(query).pl()
        
        if df_raw.height == 0:
            print("⚠️ Item não encontrado.")
            return None

        row = df_raw.row(0, named=True)
        hoje = datetime.now()

        # 1. Datas
        str_data_venda = row['ultima_venda']
        try:
            if "/" in str_data_venda: dt_venda = datetime.strptime(str_data_venda, "%d/%m/%Y")
            else: dt_venda = datetime.strptime(str_data_venda, "%Y-%m-%d")
        except: dt_venda = hoje
        
        dias_sem_venda = (hoje - dt_venda).days
        if dias_sem_venda < 0: dias_sem_venda = 0

        # 2. Média (Baseada em 365 dias - Regra de 12 meses)
        total_12m = to_float(row['total_vendido_12m'])
        # Se o total for 48 (como você disse), a média será 48/365 = 0.13
        media_dia = total_12m / 365.0 

        # 3. Estoque
        saldo = to_float(row['saldo_estoque'])
        oc = to_float(row['saldo_oc'])
        custo = to_float(row['custo_unitario'])
        
        print(f"   ✅ Dados Calculados (Base 12 meses):")
        print(f"      • Total Vendido (12m): {total_12m} un")
        print(f"      • Média Dia: {media_dia:.4f} un/dia")
        print(f"      • Estoque: {saldo} | OC: {oc}")
        print(f"      • Lead Time Configurado: {lead_time_manual} dias")

        return pl.DataFrame({
            "cod_produto": [sku],
            "saldo_estoque": [saldo],
            "saldo_oc": [oc],
            "media_venda_dia": [media_dia],
            "dias_sem_venda": [int(dias_sem_venda)],
            "lead_time_dias": [lead_time_manual], # Usa o parametro manual
            "std_venda_dia": [media_dia * 0.8], # Simulando variabilidade maior (típico de baixa venda)
            "curva_abc": ["C" if media_dia < 0.5 else "A"], # Ajuste dinâmico para teste
            "curva_xyz": ["Z"],
            "tendencia_vendas": ["ESTÁVEL"],
            "data_cadastro": [datetime.now()],
            "custo_unitario": [custo],
            "lote_economico": [1],
            "ativo": ["SIM"]
        })

    except Exception as e:
        print(f"❌ Erro SQL: {e}")
        return None
    finally:
        con.close()

def auditar_regras(df):
    try:
        # Pipeline Completo
        df = EstoqueMath.calcular_seguranca(df, CONFIG)
        df = EstoqueMath.calcular_necessidades(df, CONFIG)
        df = EstoqueMath.aplicar_lote_economico(df, CONFIG)
        df = EstoqueMath.calcular_score(df)
        df = EstoqueMath.gerar_diagnostico(df, CONFIG)
        
        row = df.to_dicts()[0]
        
        print("\n" + "="*50)
        print(f"🏁 RESULTADO DA AUDITORIA: {row['cod_produto']}")
        print("="*50)
        print(f"📊 Parâmetros Usados:")
        print(f"   • Cobertura Alvo: {CONFIG['compras']['meses_cobertura']} meses (90 dias)")
        print(f"   • Média Calculada: {row['media_venda_dia']:.4f}")
        
        # Memória de Cálculo
        print(f"\n🧮 MEMÓRIA DE CÁLCULO:")
        print(f"   (+) Consumo Cobertura (90d * media): {row['media_venda_dia'] * 30 * 3:.2f}")
        print(f"   (+) Consumo Lead Time ({row['lead_time_dias']}d * media): {row['media_venda_dia'] * row['lead_time_dias']:.2f}")
        print(f"   (+) Estoque Segurança: {row['estoque_seguranca']:.2f}")
        print(f"   (=) ESTOQUE META: {row['estoque_meta']}")
        print(f"   (-) Saldo Atual: {row['saldo_estoque']}")
        print(f"   (-) OC (Pedidos): {row['saldo_oc']}")
        print(f"   (=) NECESSIDADE: {row['sugestao_bruta']}")

        print(f"\n📦 SUGESTÃO FINAL: {row['sugestao_final']} unidades")
        print("-" * 50 + "\n")

    except Exception as e:
        print(f"❌ Erro: {e}")
        import traceback
        traceback.print_exc()

def main():
    print("\n🤖 AUDITOR CALIBRADO (Base 12 Meses)")
    sku = input("Digite o SKU (ex: 30129): ").strip()
    if sku:
        # Pergunta o Lead Time para garantir que bate com seu parametro
        try:
            lead = int(input(f"Confirme o Lead Time (padrão 17): ") or 17)
        except:
            lead = 17
            
        df = buscar_dados_completos(sku, lead)
        if df is not None:
            auditar_regras(df)

if __name__ == "__main__":
    main()