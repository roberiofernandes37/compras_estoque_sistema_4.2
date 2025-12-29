from abc import ABC, abstractmethod
import polars as pl
from typing import Dict, Any

class BaseRule(ABC):
    """
    Classe abstrata para todas as regras de negócio.
    Garante que toda regra tenha um método 'apply'.
    """
    
    @abstractmethod
    def apply(self, df: pl.LazyFrame, config: Any) -> pl.LazyFrame:
        """
        Aplica a regra de negócio ao DataFrame.
        """
        pass