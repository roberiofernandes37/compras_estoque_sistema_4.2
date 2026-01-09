import os
from pathlib import Path

# --- CONFIGURAÇÕES ---
PROJECT_ROOT = Path(__file__).parent
OUTPUT_FILE = "contexto_completo_do_sistema.txt"

# Pastas para ignorar (não entra nem lê nada dentro)
IGNORE_DIRS = {
    ".git", 
    ".venv", 
    "venv", 
    "env", 
    "__pycache__", 
    ".idea", 
    ".vscode", 
    "data",       # Ignora bancos de dados binários
    "logs",       # Ignora logs de execução
    "exports",    # Ignora os Excels gerados
    "build",
    "dist"
}

# Extensões permitidas (só salva arquivos deste tipo)
INCLUDE_EXTS = {
    ".py", 
    ".yaml", 
    ".yml", 
    ".sql", 
    ".json", 
    ".md", 
    ".txt", 
    ".toml", 
    ".ini"
}

# Arquivos específicos para ignorar (caso necessário)
IGNORE_FILES = {
    OUTPUT_FILE, # Não ler o próprio arquivo de saída
    "package-lock.json",
    ".DS_Store"
}

def generate_tree(path, prefix=""):
    """Gera uma string visual da estrutura de pastas."""
    tree_str = ""
    try:
        # Pega itens e ordena (pastas primeiro, depois arquivos)
        items = list(path.iterdir())
        items.sort(key=lambda x: (not x.is_dir(), x.name.lower()))
        
        # Filtra itens ignorados
        items = [i for i in items if i.name not in IGNORE_DIRS and i.name not in IGNORE_FILES]
        
        count = len(items)
        for i, item in enumerate(items):
            is_last = (i == count - 1)
            connector = "└── " if is_last else "├── "
            
            tree_str += f"{prefix}{connector}{item.name}\n"
            
            if item.is_dir():
                extension = "    " if is_last else "│   "
                tree_str += generate_tree(item, prefix + extension)
                
    except PermissionError:
        tree_str += f"{prefix}└── [Acesso Negado]\n"
        
    return tree_str

def main():
    print(f"🚀 Iniciando exportação do projeto em: {PROJECT_ROOT}")
    
    with open(OUTPUT_FILE, "w", encoding="utf-8") as out:
        # 1. Cabeçalho Principal
        out.write("="*80 + "\n")
        out.write(f"PROJETO: COMPRAS-ESTOQUE-SISTEMA\n")
        out.write(f"DATA GERACAO: {os.path.basename(str(PROJECT_ROOT))}\n")
        out.write("="*80 + "\n\n")

        # 2. Estrutura de Diretórios (Árvore)
        out.write("ESTRUTURA DE DIRETÓRIOS:\n")
        out.write("-" * 40 + "\n")
        out.write(generate_tree(PROJECT_ROOT))
        out.write("\n" + "="*80 + "\n\n")
        
        # 3. Conteúdo dos Arquivos
        file_count = 0
        
        for root, dirs, files in os.walk(PROJECT_ROOT):
            # Modifica dirs in-place para pular pastas ignoradas no walk
            dirs[:] = [d for d in dirs if d not in IGNORE_DIRS]
            
            for file in files:
                if file in IGNORE_FILES:
                    continue
                
                file_path = Path(root) / file
                
                # Verifica extensão
                if file_path.suffix.lower() not in INCLUDE_EXTS:
                    continue

                # Caminho relativo para exibição
                rel_path = file_path.relative_to(PROJECT_ROOT)
                
                try:
                    content = file_path.read_text(encoding="utf-8", errors="ignore")
                    
                    # Escreve separador e conteúdo
                    out.write(f"START FILE: {rel_path}\n")
                    out.write("-" * 80 + "\n")
                    out.write(content)
                    out.write("\n")
                    out.write("-" * 80 + "\n")
                    out.write(f"END FILE: {rel_path}\n")
                    out.write("\n\n")
                    
                    file_count += 1
                    print(f"✅ Incluído: {rel_path}")
                    
                except Exception as e:
                    print(f"❌ Erro ao ler {rel_path}: {e}")

    print(f"\n✨ Concluído! {file_count} arquivos salvos em '{OUTPUT_FILE}'.")

if __name__ == "__main__":
    main()
    # teste git