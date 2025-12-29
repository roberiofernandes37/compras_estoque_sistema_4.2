import psutil
import logging
import sys
from pathlib import Path
from datetime import datetime

class SystemGuard:
    def __init__(self, log_dir: Path):
        self.log_dir = log_dir
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.setup_logger()

    def setup_logger(self):
        filename = f"mrp_log_{datetime.now().strftime('%Y-%m-%d')}.txt"
        log_path = self.log_dir / filename
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s | %(levelname)s | %(message)s',
            handlers=[
                logging.FileHandler(log_path),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger("MRP_Guard")

    def log(self, message):
        self.logger.info(message)

    def check_memory(self, min_mb=500):
        """
        Verifica se há memória RAM disponível suficiente.
        Se houver menos que 'min_mb', avisa ou aborta.
        """
        mem = psutil.virtual_memory()
        available_mb = mem.available / (1024 * 1024)
        
        self.logger.info(f"RAM Disponível: {available_mb:.0f} MB")

        if available_mb < min_mb:
            self.logger.warning(f"⚠️ PERIGO: Memória crítica! Apenas {available_mb:.0f}MB livres.")
            self.logger.warning("⚠️ Feche o navegador (Chrome/Firefox) imediatamente.")
            # Opcional: input("Pressione Enter quando liberar memória...") 
            # No seu caso, vamos apenas logar o perigo.

    def log_performance(self, task_name, start_time):
        elapsed = (datetime.now() - start_time).total_seconds()
        self.logger.info(f"⏱️ Tarefa '{task_name}' concluída em {elapsed:.2f} segundos.")