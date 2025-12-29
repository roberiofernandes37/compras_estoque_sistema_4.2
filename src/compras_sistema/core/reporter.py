import json
from pathlib import Path
from datetime import datetime
from typing import Dict, Any

class ExecutionReporter:
    """
    Responsável por salvar os resultados da execução de forma estruturada.
    Substitui o uso frágil de 'print' para comunicação com o Frontend.
    """
    
    def __init__(self, data_dir: Path):
        self.report_path = data_dir / "cache" / "last_run_stats.json"
        # Garante que a pasta existe
        self.report_path.parent.mkdir(parents=True, exist_ok=True)

    def salvar_stats(self, stats: Dict[str, Any]):
        """Salva as estatísticas finais em JSON atômico."""
        payload = {
            "timestamp": datetime.now().isoformat(),
            "status": "success",
            "data": stats
        }
        
        # Escrita segura (utf-8)
        with open(self.report_path, 'w', encoding='utf-8') as f:
            json.dump(payload, f, indent=2)
            
    def ler_ultimo_status(self) -> Dict[str, Any]:
        """Lê o último relatório gerado."""
        if not self.report_path.exists():
            return {}
            
        with open(self.report_path, 'r', encoding='utf-8') as f:
            return json.load(f)

    def limpar_stats_anteriores(self):
        """Remove dados antigos para evitar falsos positivos."""
        if self.report_path.exists():
            try:
                self.report_path.unlink()
            except Exception:
                pass