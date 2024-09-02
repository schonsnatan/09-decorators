from etl import pipeline_calculate_kpi_sales_consolidated

source_folder = 'data'
format_ouput = ['csv','parquet']

pipeline_calculate_kpi_sales_consolidated(source_folder,format_ouput)