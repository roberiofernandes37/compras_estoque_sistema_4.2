import os
from pathlib import Path

# Configuração
PROJECT_ROOT = Path(__file__).parent.parent
OUTPUT_FILE = PROJECT_ROOT / "contexto_completo_para_ia.txt"

# Pastas e arquivos que queremos incluir
INCLUDED_DIRS = [
    PROJECT_ROOT / "src",
    PROJECT_ROOT / "scripts",
    PROJECT_ROOT / "config",
]

# Arquivos específicos para ignorar (opcional)
IGNORE_FILES = ["__pycache__", ".DS_Store", "vendas.db", "analytics.duckdb", ".git", ".vscode"]

def generate_tree(root_dir):
    """Gera uma representação visual da árvore de arquivos."""
    tree_str = "PROJECT STRUCTURE:\n"
    for path in sorted(root_dir.rglob('*')):
        if any(part.startswith('.') or part == "__pycache__" for part in path.parts):
            continue
        if path.is_file() and path.suffix in ['.py', '.yaml', '.json', '.sql']:
            depth = len(path.relative_to(root_dir).parts)
            indent = '    ' * (depth - 1)
            tree_str += f"{indent}├── {path.name}\n"
    return tree_str + "\n" + "="*50 + "\n\n"

def main():
    print(f"📦 Empacotando projeto em: {OUTPUT_FILE}")
    
    with open(OUTPUT_FILE, "w", encoding="utf-8") as out:
        # 1. Escreve o Cabeçalho e Estrutura
        out.write("CONTEXTO DO PROJETO DE COMPRAS E ESTOQUE\n")
        out.write("Linguagem: Python 3.11\n")
        out.write("Libs principais: Polars, DuckDB, OpenPyXL\n\n")
        out.write(generate_tree(PROJECT_ROOT))
        
        # 2. Varre os arquivos e escreve o conteúdo
        for folder in INCLUDED_DIRS:
            if not folder.exists(): continue
            
            for root, dirs, files in os.walk(folder):
                # Filtra pastas ignoradas
                dirs[:] = [d for d in dirs if d not in IGNORE_FILES]
                
                for file in files:
                    if file in IGNORE_FILES or not file.endswith(('.py', '.yaml')):
                        continue
                        
                    file_path = Path(root) / file
                    rel_path = file_path.relative_to(PROJECT_ROOT)
                    
                    # Escreve o separador e o nome do arquivo
                    out.write(f"FILE: {rel_path}\n")
                    out.write("-" * 50 + "\n")
                    
                    try:
                        with open(file_path, "r", encoding="utf-8") as f:
                            content = f.read()
                            out.write(content)
                    except Exception as e:
                        out.write(f"# Erro ao ler arquivo: {e}")
                    
                    out.write("\n\n" + "="*50 + "\n\n")
    
    print("✅ Arquivo gerado com sucesso!")
    print("Agora você pode anexar 'contexto_completo_para_ia.txt' no chat da IA.")

if __name__ == "__main__":
    main()