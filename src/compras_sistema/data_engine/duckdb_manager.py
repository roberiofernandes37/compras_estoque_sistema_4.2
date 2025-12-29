import duckdb
from pathlib import Path
from contextlib import contextmanager
from threading import Lock
import structlog
import sys

logger = structlog.get_logger(__name__)

class DuckDBManager:
    """
    Gerenciador singleton de conexões DuckDB.
    Refatorado (Fase 1.3): Inclui Health Check para garantir integridade do banco.
    """
    
    def __init__(self, memory_limit: str = "2GB", threads: int = 4):
        self.memory_limit = memory_limit
        self.threads = threads
        self._conn = None
        self._lock = Lock()
        
    def initialize(self, sqlite_path: Path):
        """
        Inicializa conexão DuckDB, anexa o SQLite e VALIDA a estrutura.
        Se falhar, aborta o sistema imediatamente.
        """
        # 1. Validação Física
        if not sqlite_path.exists():
            msg = f"CRÍTICO: Banco de dados não encontrado em {sqlite_path}"
            logger.critical("db_not_found", path=str(sqlite_path))
            raise FileNotFoundError(msg)

        with self._lock:
            # Se já existe conexão, fecha para reiniciar limpo
            if self._conn is not None:
                try: self._conn.close()
                except: pass
                
            try:
                self._conn = duckdb.connect(":memory:")
                
                # Configurações de performance
                self._conn.execute(f"SET memory_limit='{self.memory_limit}'")
                self._conn.execute(f"SET threads TO {self.threads}")
                self._conn.execute("SET enable_progress_bar=true")
                
                # 2. Attach SQLite (Federação)
                logger.info("connecting_sqlite", path=str(sqlite_path))
                self._conn.execute(f"""
                    ATTACH '{str(sqlite_path)}' AS sqlite_db (TYPE SQLITE, READ_ONLY)
                """)
                
                # 3. HEALTH CHECK (A Blindagem Nova)
                self._validar_tabelas_criticas()
                
                logger.info("duckdb_initialized_successfully")

            except Exception as e:
                # Se algo der errado, matamos a conexão para não deixar um objeto "zumbi"
                if self._conn:
                    try: self._conn.close()
                    except: pass
                self._conn = None
                
                logger.critical("duckdb_init_failed", error=str(e))
                raise RuntimeError(f"Falha Crítica na Inicialização do Banco: {e}")

    def _validar_tabelas_criticas(self):
        """Verifica se as tabelas essenciais existem no banco anexado."""
        tabelas_necessarias = ["saldo_custo_entrada", "produtos_gerais"]
        
        # Pega todas as tabelas do schema sqlite_db
        # Nota: DuckDB usa information_schema
        try:
            # Lista tabelas no schema anexado
            df_tables = self._conn.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_catalog = 'sqlite_db'
            """).pl()
            
            tabelas_existentes = [t.lower() for t in df_tables["table_name"].to_list()]
            
            for necessaria in tabelas_necessarias:
                if necessaria.lower() not in tabelas_existentes:
                    raise ValueError(f"Tabela obrigatória '{necessaria}' não encontrada no banco de dados!")
                    
            logger.info("schema_validation_passed", tables=tabelas_necessarias)
            
        except Exception as e:
            # Re-lança como erro de validação claro
            raise ValueError(f"O banco de dados parece corrompido ou incompleto: {e}")

    @contextmanager
    def get_connection(self):
        """Context manager para obter conexão thread-safe."""
        with self._lock:
            if self._conn is None:
                raise RuntimeError("ERRO INTERNO: Tentativa de usar DuckDB sem inicialização (initialize() não foi chamado ou falhou).")
            yield self._conn

    def execute_query_file(self, query_file: Path) -> duckdb.DuckDBPyRelation:
        """Executa query SQL de arquivo."""
        if not query_file.exists():
            raise FileNotFoundError(f"Arquivo de query não encontrado: {query_file}")

        with open(query_file, 'r', encoding='utf-8') as f:
            query = f.read()
        
        with self.get_connection() as conn:
            return conn.execute(query)

    def close(self):
        """Fecha conexão DuckDB."""
        with self._lock:
            if self._conn:
                try:
                    self._conn.close()
                except Exception as e:
                    logger.warning("error_closing_connection", error=str(e))
                finally:
                    self._conn = None
                    logger.info("duckdb_closed")