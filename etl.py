import pandas as pd
import os
import glob
from log import log_decorator

#extract function that read and consolidate json 

@log_decorator
def extract_data(folder: str) -> pd.DataFrame:
    arquivos_json = glob.glob(os.path.join(folder, '*.json'))
    df_list = [pd.read_json(arquivo) for arquivo in arquivos_json]
    df_total = pd.concat(df_list, ignore_index=True)
    return df_total

#function that transform
@log_decorator
def transform_kpi_total_sales(df_total: pd.DataFrame) -> pd.DataFrame:
    df_total["Total"] = df_total["Quantidade"] * df_total["Venda"]
    return df_total

#function that load csv or parquet
@log_decorator
def load_data(df: pd.DataFrame, format_output: list):
    """
    parameter that will be either "csv" or "parquet" or both
    """
    for format in format_output:
        if format == 'csv':
            df.to_csv("dados.csv", index=False)
        if format == 'parquet':
            df.to_parquet("dados.parquet", index=False)
@log_decorator
def pipeline_calculate_kpi_sales_consolidated(folder: str, format_output: list):
    data_frame = extract_data(folder)
    kpi_sales = transform_kpi_total_sales(data_frame)
    load_data(kpi_sales,format_output)