# ETL con Python – E-commerce

## 📌 Descripción
Este proyecto implementa un pipeline **ETL (Extract, Transform, Load)** en Python
sobre un dataset de e-commerce.

El objetivo del proyecto es:
- Limpiar y preparar datos
- Responder preguntas de negocio reales
- Generar outputs listos para análisis y reporting

---

## 🧰 Tecnologías usadas
- Python 3
- Pandas
- PyArrow (Parquet)

---

## ▶️ Cómo ejecutar el proyecto

### 1. Clonar el repositorio  

```bash
git clone https://github.com/victoriadercole1209/ETL-con-Python.git
cd ETL-con-Python
```

### 2. Instalar dependencias:

```bash
pip install pandas pyarrow
```

### 3. Ejecutar el script ETL:
```
python etl.py
```


# 🧪 Flujo ETL
## 🔹 Extract
Lectura de archivos CSV desde la carpeta data/.

Tablas principales:

- orders
- order_items
- customers
- products


## 🔹 Transform

- Exploración inicial: 

  - head(), info()

  - Conteo de valores nulos

- Manejo de nulos:

  - Eliminación de filas con campos críticos faltantes

  - Relleno de campos opcionales (promotion_id, notes)

- Duplicados: 

  - Verificación y eliminación por order_id

- Tipos de datos:

Conversión de order_date a datetime

- Análisis de negocio:

  - Top 5 clientes por gasto total

  - Producto más vendido por cantidad

  - Evolución mensual de ventas
 

## 🔹 Load

- Exportación a CSV:

  - ventas_por_cliente.csv

  - ventas_por_mes.csv

  - orders_clean.csv

- Exportación a Parquet:

  - orders_clean.parquet

 
# 📂 Estructura del proyecto
```
ETL-con-Python/
├── data/
│   └── ecommerce_*.csv
├── output/
│   ├── ventas_por_cliente.csv
│   ├── ventas_por_mes.csv
│   ├── orders_clean.csv
│   └── orders_clean.parquet
├── etl.py
└── README.md
```

✍️ Autor: 
Maria Victoria D'Ercole
