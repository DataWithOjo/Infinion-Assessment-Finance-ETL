import polars as pl

def clean_cols(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Standardizes column names to lowercase to prevent case-sensitivity issues.
    
    Args:
        lf (pl.LazyFrame): Input LazyFrame.
        
    Returns:
        pl.LazyFrame: LazyFrame with lowercase column names.
    """
    return lf.select(pl.all().name.to_lowercase())

def smart_date(col_name: str) -> pl.Expr:
    """
    Parses date columns with mixed formats (ISO, DD-MM-YYYY, etc.).
    
    Args:
        col_name (str): The name of the column to parse.
        
    Returns:
        pl.Expr: A Polars Expression executing the coalesce logic.
    """
    return pl.coalesce([
        pl.col(col_name).str.to_datetime("%Y-%m-%d %H:%M:%S%.f", strict=False),
        pl.col(col_name).str.to_datetime("%Y-%m-%d %H:%M:%S", strict=False),
        pl.col(col_name).str.to_datetime("%d-%m-%Y %H:%M:%S", strict=False),
        pl.col(col_name).str.to_datetime("%Y-%m-%d", strict=False)
    ])