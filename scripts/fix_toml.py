from pathlib import Path

# Conteúdo correto do pyproject.toml
content = """[project]
name = "compras-estoque-sistema"
version = "1.0.0"
description = "Sistema modular de compras e gestão de estoque"
requires-python = ">=3.11"
dependencies = [
    "duckdb>=1.1.0",
    "polars>=1.12.0",
    "pydantic>=2.9.0",
    "pyyaml>=6.0.2",
    "pandera[polars]>=0.20.4",
    "structlog>=24.4.0",
    "openpyxl>=3.1.5",
    "reportlab>=4.2.5",
    "pyinstaller>=6.11.0",
]

[project.optional-dependencies]
dev = [
    "pytest>=8.3.3",
    "pytest-cov>=6.0.0",
    "pytest-benchmark>=5.1.0",
    "hypothesis>=6.115.6",
    "ruff>=0.7.4",
    "mypy>=1.13.0",
]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.uv]
dev-dependencies = ["ruff", "mypy", "pytest"]

# Esta seção ensina o hatchling onde encontrar o código
[tool.hatch.build.targets.wheel]
packages = ["src/compras_sistema"]
"""

# Reescreve o arquivo na raiz do projeto
root_dir = Path(__file__).parent.parent
toml_path = root_dir / "pyproject.toml"

print(f"🔧 Corrigindo: {toml_path}")
with open(toml_path, "w", encoding="utf-8") as f:
    f.write(content)

print("✅ pyproject.toml corrigido com sucesso!")