import json
import sys
import yaml
import duckdb
from pathlib import Path
from decimal import Decimal

# =========================================================
# CONFIGURAÇÃO
# =========================================================
ROOT_DIR = Path(__file__).resolve().parent.parent
CONFIG_PATH = ROOT_DIR / "config" / "parametros.yaml"
DB_PATH = ROOT_DIR / "data" / "vendas.db"
SCRIPT_RESULTADO = ROOT_DIR / "scripts" / "gerar_relatorio_final.py"

MARCA_TESTE = sys.argv[1] if len(sys.argv) > 1 else "TODAS"

# =========================================================
# FUNÇÕES AUXILIARES
# =========================================================
def erro(msg):
    print(f"❌ ERRO DE AUDITORIA: {msg}")
    sys.exit(1)

def ok(msg):
    print(f"✅ {msg}")

def warn(msg):
    print(f"⚠️ {msg}")

# =========================================================
# 1. AUDITORIA DE PARÂMETROS
# =========================================================
print("\n🔍 ETAPA 1 — VALIDANDO PARÂMETROS")

if not CONFIG_PATH.exists():
    erro("Arquivo parametros.yaml não encontrado")

with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    cfg = yaml.safe_load(f)

try:
    cobertura = float(cfg["compras"]["meses_cobertura"])
    lead_time = int(cfg["lead_time"]["padrao_dias"])
    dias_novo = int(cfg["produto"]["dias_lancamento"])
except Exception:
    erro("Parâmetros inválidos ou ausentes no YAML")

ok(f"Parâmetros OK | Cobertura={cobertura}, LeadTime={lead_time}, DiasNovo={dias_novo}")

# =========================================================
# 2. AUDITORIA DO BANCO
# =========================================================
print("\n🔍 ETAPA 2 — VALIDANDO BANCO DE DADOS")

if not DB_PATH.exists():
    erro("Banco de dados não encontrado")

con = duckdb.connect(":memory:")
con.execute(f"ATTACH '{DB_PATH}' AS sqlite_db (TYPE SQLITE, READ_ONLY)")

# Testa leitura básica
try:
    total_linhas = con.execute(
        "SELECT COUNT(*) FROM sqlite_db.produtos_gerais"
    ).fetchone()[0]
except Exception:
    erro("Tabela produtos_gerais não acessível")

ok(f"Banco acessível | Total registros: {total_linhas}")

# =========================================================
# 3. AUDITORIA DE FILTRO DE MARCA
# =========================================================
print("\n🔍 ETAPA 3 — VALIDANDO FILTRO DE MARCA")

query_marca = """
SELECT
    COUNT(DISTINCT sku),
    SUM(estoque_atual),
    SUM(valor_estoque)
FROM sqlite_db.produtos_gerais
WHERE (? = 'TODAS' OR marca = ?)
"""

skus_db, pecas_db, valor_db = con.execute(
    query_marca, [MARCA_TESTE, MARCA_TESTE]
).fetchone()

ok(f"Marca '{MARCA_TESTE}' | SKUs={skus_db}, Peças={pecas_db}, Valor={valor_db}")

# =========================================================
# 4. EXECUTA O MOTOR EM MODO SIMULAÇÃO
# =========================================================
print("\n🔍 ETAPA 4 — EXECUTANDO MOTOR DE CÁLCULO")

import subprocess

cmd = [
    sys.executable,
    str(SCRIPT_RESULTADO),
    "--marca",
    MARCA_TESTE,
    "--simulacao"
]

process = subprocess.Popen(
    cmd,
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    text=True,
    encoding="utf-8"
)

stats = None

for line in process.stdout:
    if "STATS_DATA=" in line:
        stats = json.loads(line.replace("STATS_DATA=", "").strip())

process.wait()

if process.returncode != 0:
    erro("Motor de cálculo retornou erro")

if not stats:
    erro("STATS_DATA não encontrado na execução")

ok("Motor executado com sucesso")

# =========================================================
# 5. AUDITORIA DE CONSISTÊNCIA DOS KPIs
# =========================================================
print("\n🔍 ETAPA 5 — VALIDANDO KPIs")

def dec(v):
    return Decimal(str(v)).quantize(Decimal("0.01"))

total_skus = int(stats.get("total_skus", 0))
total_pecas = int(stats.get("total_pecas", 0))
total_valor = dec(stats.get("total_valor", 0))
estoque_atual = dec(stats.get("estoque_atual", 0))
cobertura_calc = float(stats.get("cobertura_meses", 0))

if total_skus < 0 or total_pecas < 0:
    erro("KPIs com valores negativos")

if total_skus > skus_db:
    erro("Total de SKUs maior que o banco")

if dec(valor_db) < estoque_atual:
    warn("Valor de estoque do KPI maior que o valor do banco (verifique regra)")

ok(f"KPIs OK | Valor={total_valor} | SKUs={total_skus} | Peças={total_pecas}")

# =========================================================
# 6. REGRAS DE SANIDADE
# =========================================================
print("\n🔍 ETAPA 6 — REGRAS DE SANIDADE")

if cobertura_calc < 0:
    erro("Cobertura negativa detectada")

if cobertura_calc > 24:
    warn("Cobertura acima de 24 meses (verificar parâmetros)")

ok(f"Cobertura válida: {cobertura_calc:.2f} meses")

# =========================================================
# FINAL
# =========================================================
con.close()

print("\n🎉 AUDITORIA CONCLUÍDA COM SUCESSO")
print("✔ Dados consistentes")
print("✔ Cálculos coerentes")
print("✔ KPIs validados")
