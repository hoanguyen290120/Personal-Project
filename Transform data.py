import os
from pyspark.sql import SparkSession

# 1. Tạo SparkSession
spark = SparkSession.builder \
    .appName("ETL") \
    .master("local[*]") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.extraClassPath", "E:\\System\\Spark\\spark-3.5.5-bin-hadoop3\\jars\\mssql-jdbc-12.10.0.jre11.jar") \
    .getOrCreate()

# 2. Đọc tất cả các file CSV trong 4 thư mục con
base_path = r"C:\Users\Admin\PycharmProjects\PythonProject1\Data\archive"
subfolders = ["Application", "Purchasing", "Sales", "Warehouse"]
all_dataframes = {}

for folder in subfolders:
    folder_path = os.path.join(base_path, folder)
    for file in os.listdir(folder_path):
        if file.endswith(".csv"):
            file_path = os.path.join(folder_path, file)
            # Tách schema và table từ tên file
            schema_table = file.replace(".csv", "").split(".") 
            schema = schema_table[0]  # Phần trước dấu "."
            table = schema_table[1] if len(schema_table) > 1 else schema_table[0]  
            full_table_name = f"{schema}.{table}"
            df = spark.read.option("header", True).option("delimiter", ";").csv(file_path)
            all_dataframes[full_table_name] = df
            print(f" Đã đọc file: {file_path} -> sẽ ghi vào bảng: {full_table_name}")

# 3. Cấu hình kết nối SQL Server
database = "WideWorldImporters_dbt"
user = "********"
password = "*********"
driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
url = (
    f"jdbc:sqlserver://localhost:1433;databaseName={database};"
    "encrypt=true;trustServerCertificate=true;"
)

jdbc_properties = {
    "user": user,
    "password": password,
    "driver": driver
}

# 4. Ghi từng DataFrame vào SQL Server theo schema.table
for full_table_name, df in all_dataframes.items():
    print(f"Đang ghi bảng: {full_table_name}")
    df.write.mode("overwrite").jdbc(url=url, table=full_table_name, properties=jdbc_properties)
    print(f"Đã ghi xong bảng: {full_table_name}")
