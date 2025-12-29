import sys
import os
import subprocess
from pathlib import Path

def diagnostico():
    print("--- 🔍 DIAGNÓSTICO DE AMBIENTE ---")
    print(f"Caminho do Executável Python: {sys.executable}")
    print(f"Versão do Python: {sys.version}")
    print(f"Pasta Atual (CWD): {os.getcwd()}")
    
    # 1. Tenta importar pandera
    print("\n--- 📦 TESTE DE BIBLIOTECAS ---")
    try:
        import pandera
        print(f"✅ Pandera: Instalado (Versão: {pandera.__version__})")
        print(f"   Local: {pandera.__file__}")
    except ImportError:
        print("❌ Pandera: NÃO ENCONTRADO neste ambiente.")

    try:
        import matplotlib
        print(f"✅ Matplotlib: Instalado")
    except ImportError:
        print("❌ Matplotlib: NÃO ENCONTRADO.")

    # 2. Verifica estrutura do venv
    print("\n--- 📁 ESTRUTURA DE PASTAS ---")
    venv_path = Path("./venv/bin/python")
    print(f"O arquivo ./venv/bin/python existe? {'✅ Sim' if venv_path.exists() else '❌ Não'}")

    # 3. Testa como o subprocesso seria chamado
    print("\n--- 🚀 TESTE DE CHAMADA (SUBPROCESSO) ---")
    try:
        # Tenta rodar o python do venv pedindo a versão
        res = subprocess.run([str(venv_path), "--version"], capture_output=True, text=True)
        print(f"Chamada './venv/bin/python --version' retornou: {res.stdout.strip()}")
    except Exception as e:
        print(f"❌ Erro ao tentar chamar o python do venv: {e}")

if __name__ == "__main__":
    diagnostico()