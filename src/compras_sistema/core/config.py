from pathlib import Path
from typing import Any, Dict, Union
import yaml
from pydantic import BaseModel, Field

class XYZConfig(BaseModel):
    threshold: float
    z_score: float

class LoteConfig(BaseModel):
    minima_absoluta: int
    limite_virada: float

# --- CLASSES AUXILIARES ---
class LeadTimeConfig(BaseModel):
    padrao_dias: float
    desvio_padrao: float

class ComprasConfig(BaseModel):
    meses_cobertura: float

# ----------------------------------------------------------

class ParametrosConfig(BaseModel):
    lead_time: LeadTimeConfig 
    compras: ComprasConfig
    
    historico: Dict[str, int]
    produto: Dict[str, int]
    xyz: Dict[str, XYZConfig]
    abc: Dict[str, float]
    tolerancia_abc: Dict[str, float]
    lote: LoteConfig
    outlier: Dict[str, float]
    
    # [CORREÇÃO 1] Mudado para float para aceitar 0.05
    giro: Dict[str, float]  
    
    risco: Dict[str, Any]
    sazonalidade: Dict[str, Any]
    ruptura: Dict[str, float]

    # [CORREÇÃO 2] Adicionado para suportar a Fase 2 (Fator Z)
    # Usamos Any para permitir flexibilidade na estrutura interna
    estoque: Dict[str, Any] = Field(default_factory=dict)
    
    @classmethod
    def from_yaml(cls, path: Path) -> "ParametrosConfig":
        if not path.exists():
            raise FileNotFoundError(f"Arquivo de configuração não encontrado: {path}")
        with open(path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)
        return cls(**data)

class ConfigManager:
    """Singleton para gerenciamento de configurações."""
    
    _instance = None
    _parametros: ParametrosConfig | None = None
    _pesos_score: dict | None = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def load_configs(self, config_dir: Path):
        """Carrega todos os arquivos de configuração."""
        self._parametros = ParametrosConfig.from_yaml(
            config_dir / "parametros.yaml"
        )
        
        score_path = config_dir / "pesos_score.yaml"
        if score_path.exists():
            with open(score_path, 'r', encoding='utf-8') as f:
                self._pesos_score = yaml.safe_load(f)
        else:
            self._pesos_score = {}

    @property
    def parametros(self) -> ParametrosConfig:
        if self._parametros is None:
            raise RuntimeError("Configurações não carregadas. Chame load_configs() primeiro.")
        return self._parametros
    
    @property
    def pesos_score(self) -> dict:
        if self._pesos_score is None:
            raise RuntimeError("Pesos de score não carregados.")
        return self._pesos_score