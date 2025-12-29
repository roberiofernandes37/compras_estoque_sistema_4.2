import pandera.polars as pa
import polars as pl

class InputCalcSchema(pa.DataFrameModel):
    """
    Contrato de dados obrigatório antes de entrar no Motor Matemático.
    Garante que não existem nulos onde não deve e que os tipos estão certos.
    """
    
    # Identificação
    cod_produto: str
    
    # Dados de Estoque (Não podem ser nulos)
    # coerce=True tenta converter string "10" para int 10 automaticamente
    saldo_estoque: int = pa.Field(coerce=True) 
    saldo_oc: int = pa.Field(coerce=True)
    
    # Dados de Venda (Essenciais para o cálculo)
    media_venda_dia: float = pa.Field(ge=0.0, coerce=True)
    std_venda_dia: float = pa.Field(ge=0.0, coerce=True)
    
    # Parâmetros Logísticos
    lead_time_dias: float = pa.Field(ge=0, coerce=True)
    
    # CRÍTICO: Lote 0 causa divisão por zero no script
    lote_economico: int = pa.Field(gt=0, coerce=True) 
    
    # Classificações
    curva_abc: str = pa.Field(isin=["A", "B", "C"])
    curva_xyz: str = pa.Field(isin=["X", "Y", "Z"])
    
    # Datas (Essencial para a lógica de Item Novo)
    data_cadastro: pl.Date

    class Config:
        # strict=False permite que o DataFrame tenha colunas extras (descricao, marca, etc)
        # sem dar erro. Validamos apenas as colunas essenciais listadas acima.
        strict = False