import pandas as pd
import os

print("Iniciando ETL E-commerce")


#################################
# PASO 1 - EXTRACT (Cargar datos)
#################################


DATA_PATH = "data"  # Carpeta donde están los CSV

# Verifico archivos
files = os.listdir(DATA_PATH)
csv_files = [f for f in files if f.endswith(".csv")]

print(f"\n Archivos encontrados: {len(csv_files)}")
for f in sorted(csv_files):
    print(f"  - {f}")

# Cargar CSVs principales
df_orders = pd.read_csv(os.path.join(DATA_PATH, "ecommerce_orders.csv"))
df_order_items = pd.read_csv(os.path.join(DATA_PATH, "ecommerce_order_items.csv"))
df_customers = pd.read_csv(os.path.join(DATA_PATH, "ecommerce_customers.csv"))
df_products = pd.read_csv(os.path.join(DATA_PATH, "ecommerce_products.csv"))


################################
# PASO 2 - CARGA Y EXPLORACIÓN
################################


print("\n Resumen:")
print(f"Orders: {len(df_orders)} filas, {len(df_orders.columns)} columnas")
print(f"Order Items: {len(df_order_items)} filas")
print(f"Customers: {len(df_customers)} filas")
print(f"Products: {len(df_products)} filas")

print("\n Primeras filas de orders:")
print(df_orders.head())

print("\n Info de orders:")
print(df_orders.info())



######################################
# PASO 3 - TRANSFORM: Manejo de nulos
######################################




print("\n🔎 Nulos por columna en orders (antes):")
print(df_orders.isnull().sum())

# Campos críticos : no pueden ser nulos
df_orders_clean = df_orders.dropna(
    subset=["order_id", "customer_id", "order_date", "total_amount"]
)

# Campos opcionales : se rellenan
df_orders_clean["promotion_id"] = df_orders_clean["promotion_id"].fillna(0)
df_orders_clean["notes"] = df_orders_clean["notes"].fillna("")

print("\n Nulos por columna (después):")
print(df_orders_clean.isnull().sum())

print(f"\n Filas antes: {len(df_orders)}")
print(f" Filas después: {len(df_orders_clean)}")





################################
# TRANSFORM: Duplicados
################################






print("\n Duplicados exactos:")
print(df_orders_clean.duplicated().sum())

print(" Duplicados por order_id:")
print(df_orders_clean.duplicated(subset=["order_id"]).sum())

# Eliminar duplicados por order_id (si existieran)
df_orders_clean = df_orders_clean.drop_duplicates(
    subset=["order_id"], keep="last"
)




#####################################
# PASO 6 - TRANSFORM: Tipos de datos
#####################################





print("\n Tipos de datos ANTES:")
print(df_orders_clean.dtypes)

# Convertir fecha
df_orders_clean["order_date"] = pd.to_datetime(
    df_orders_clean["order_date"], errors="coerce"
)

# Verificar tipos luego de conversión
print("\n Tipos de datos DESPUÉS:")
print(df_orders_clean.dtypes)

print("\n Nulos después de conversión de tipos:")
print(df_orders_clean.isnull().sum())

# HASTA AHORA:
#- Se verificó la existencia de archivos en la carpeta `data/`
#- Se manejaron valores nulos según criticidad del campo
#- No se encontraron duplicados por `order_id`
#- Se convirtió `order_date` a datetime






#########################################################
# PASO 7 -  TRANSFORM - Respondé preguntas de negocio
#########################################################




# Tenemos estos DataFrames:
#df_orders_clean → órdenes limpias (1 fila = 1 orden)
#df_order_items → productos dentro de cada orden (1 fila = 1 producto vendido)


print("\n PASO 7 - PREGUNTAS DE NEGOCIO")

# PREGUNTA 1 : Top 5 clientes que más gastaron

ventas_cliente = (
    df_orders_clean
    .groupby("customer_id")
    .agg(
        total_gastado=("total_amount", "sum"),
        cantidad_ordenes=("order_id", "count")
    )
    .sort_values("total_gastado", ascending=False)
)

print("\n Top 5 clientes por gasto:")
print(ventas_cliente.head(5))


# PREGUNTA 2 : Producto más vendido (cantidad)

productos_vendidos = (
    df_order_items
    .groupby("product_id")["quantity"]
    .sum()
    .sort_values(ascending=False)
)

producto_mas_vendido_id = productos_vendidos.idxmax()
producto_mas_vendido_qty = productos_vendidos.max()

print(
    f"\n Producto más vendido: "
    f"ID {producto_mas_vendido_id} "
    f"con {producto_mas_vendido_qty} unidades"
)



# PREGUNTA 3 :  Evolución de ventas mes a mes



df_orders_clean["mes"] = df_orders_clean["order_date"].dt.to_period("M")

ventas_mes = (
    df_orders_clean
    .groupby("mes")["total_amount"]
    .sum()
    .reset_index()
)

ventas_mes.columns = ["mes", "total_ventas"]

print("\n Ventas mes a mes:")
print(ventas_mes)


#################################
#PASO 8 :  LOAD - Guardá en CSV
#################################

# Crear carpeta output si no existe
import os
os.makedirs('output', exist_ok=True)

# Guardar métricas en CSV
ventas_cliente.to_csv('output/ventas_por_cliente.csv', index=False)
ventas_mes.to_csv('output/ventas_por_mes.csv', index=False)

# Guardar datos limpios
df_orders_clean.to_csv('output/orders_clean.csv', index=False)

print(" Archivos CSV guardados en output/")


#########################################################
# Paso 9 : LOAD - Guardár en Parquet (formato profesional)
#########################################################


#Instalar pyarrow en caso de no tenerlo : pip install pyarrow

# Guardar en Parquet
df_orders_clean.to_parquet('output/orders_clean.parquet', index=False)

# Comparar tamaños
csv_size = os.path.getsize('output/orders_clean.csv') / 1024
parquet_size = os.path.getsize('output/orders_clean.parquet') / 1024

print(f"Tamaño CSV: {csv_size:.1f} KB")
print(f"Tamaño Parquet: {parquet_size:.1f} KB")
print(f"Parquet es {csv_size/parquet_size:.1f}x más chico")