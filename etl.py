import pandas as pd
from pathlib import Path
import logging

# EXTRACCIÓN / INGESTA (CAPA BRONZE)
def leer_csv_relevantes(ruta_directorio: Path, archivos_requeridos: list) -> dict:
    """
    Lee una lista de archivos CSV requeridos y retorna un diccionario de DataFrames.
    Implementa validaciones de existencia y tamaño de archivo.
    """
    dataframes_relevantes = {}
    
    # Verificación de existencia de la ruta
    if not ruta_directorio.exists():
        logging.error(f"La ruta '{ruta_directorio}' no existe.")
        return dataframes_relevantes

    # Iteración de archivos requeridos
    for nombre_archivo in archivos_requeridos:
        # pathlib permite unir rutas dinámicamente usando el operador '/'
        ruta_archivo = ruta_directorio / nombre_archivo
        
        # Validación de existencia
        if not ruta_archivo.exists():
            logging.warning(f"NO EXISTE ARCHIVO: {nombre_archivo} no fue encontrado en {ruta_directorio}")
            continue
            
        # Validación de bytes
        if ruta_archivo.stat().st_size == 0:
            logging.warning(f"ARCHIVO VACÍO: {nombre_archivo} pesa 0 bytes. Se omite.")
            continue

        # Lectura del dataframe con manejo de excepciones
        try:
            df = pd.read_csv(ruta_archivo, encoding='utf-8')
            dataframes_relevantes[ruta_archivo.name] = df
            
            logging.info(f"Cargado exitosamente (Bronze): {ruta_archivo.name} | Dimensiones: {df.shape}")

        except pd.errors.EmptyDataError:
            logging.warning(f"El archivo no tiene columnas válidas: {ruta_archivo.name}")
        except PermissionError:
            logging.error(f"Sin permisos para leer: {ruta_archivo.name}")
        except Exception as e:
            logging.error(f"Error inesperado al leer {ruta_archivo.name}: {e}")

    return dataframes_relevantes


# TRANSFORMACIÓN / ENRIQUECIMIENTO (CAPA SILVER)
def transformar_orders(df: pd.DataFrame) -> pd.DataFrame:
    """Aplica reglas de negocio y limpieza al df de Orders."""
    logging.info("Iniciando transformación (Silver) de orders")
    df_clean = df.copy()
    
    # Casteo de fechas
    if 'order_date' in df_clean.columns:
        df_clean['order_date'] = pd.to_datetime(df_clean['order_date'], errors='coerce')
    
    # Si hay IDs duplicados, quedarse con el más reciente
    if 'order_id' in df_clean.columns:
        df_clean = df_clean.sort_values('order_date').drop_duplicates(subset=['order_id'], keep='last')
    
    # Manejo de estado nulo
    if 'status' in df_clean.columns:
        df_clean['status'] = df_clean['status'].fillna('UNKNOWN').str.upper() # Nulos como unknown y estandarizamos a mayúsculas

    # Manejo de promotion_id nulo
    if 'promotion_id' in df_clean.columns:
        df_clean['promotion_id'] = df_clean['promotion_id'].fillna(0).astype(int) # Se rellena con 0 y se convierte a entero para evitar problemas de lectura
        
    # Manejo de notas nulas
    if 'notes' in df_clean.columns:
        df_clean['notes'] = df_clean['notes'].fillna('Sin notas') # Rellenamos con un texto genérico
    
    # Tipado estricto para columnas numéricas
    columnas_numericas = ['subtotal', 'shipping_cost', 'tax_amount', 'total_amount']
    
    for col in columnas_numericas:
        if col in df_clean.columns:
            df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce').fillna(0.0)

    logging.info(f"Transformación de orders completada.")
    return df_clean

def transformar_customers(df_customers: pd.DataFrame) -> pd.DataFrame:
    # Implementar lógica de transformación para customers
    return df_customers

def transformar_products(df_products: pd.DataFrame) -> pd.DataFrame:
    # Implementar lógica de transformación para products
    return df_products

def transformar_order_items(df_order_items: pd.DataFrame) -> pd.DataFrame:
    # Implementar lógica de transformación para order_items
    return df_order_items

def transformar_categories(df_categories: pd.DataFrame) -> pd.DataFrame:
    # Implementar lógica de transformación para categories
    return df_categories


# MODELADO ANALÍTICO (CAPA GOLD / DATA MARTS)
def crear_tabla_gold_clientes(df_orders: pd.DataFrame, df_customers: pd.DataFrame) -> pd.DataFrame:
    """Cruza órdenes y clientes para ver quién gastó más."""
    logging.info("Creando tabla Gold: Top Clientes")
    
    # Inner join entre clientes y órdenes para obtener el total gastado por cliente
    df_join = pd.merge(
        df_customers[['customer_id', 'first_name', 'last_name']], 
        df_orders[['customer_id', 'total_amount', 'order_id']], 
        on='customer_id', 
        how='left'  # Usamos left join para incluir clientes sin órdenes
    )
    df_join['total_amount'] = df_join['total_amount'].fillna(0.0)   # Clientes sin órdenes tendrán total_amount como 0.0
    
    # Agrupamos por cliente para sumar el total gastado y contar la cantidad de órdenes
    df_gold = df_join.groupby(['customer_id', 'first_name', 'last_name']).agg(
        total_gastado=('total_amount', 'sum'),
        cantidad_ordenes=('order_id', 'count')
    ).reset_index().sort_values(by='total_gastado', ascending=False)
    
    return df_gold

def crear_tabla_gold_productos_categoria(df_order_items: pd.DataFrame, df_products: pd.DataFrame, df_categories: pd.DataFrame) -> pd.DataFrame:
    """Cruza items y productos para obtener los más vendidos por categoría."""
    logging.info("Creando tabla Gold: Productos más vendidos por categoría")
    
    df_join = pd.merge(
        df_order_items[['product_id', 'quantity', 'subtotal']], 
        df_products[['product_id', 'product_name', 'category_id']], 
        on='product_id', 
        how='inner'
    )
    
    # Agrupamos para saber cuánto se vendió de cada producto dentro de su categoría
    df_gold = df_join.groupby(['category_id', 'product_id', 'product_name']).agg(
        unidades_vendidas=('quantity', 'sum'),
        ingresos_totales=('subtotal', 'sum')
    ).reset_index()
    
    # Ordenamos primero por categoría, y luego por unidades vendidas (de mayor a menor)
    df_gold = df_gold.sort_values(by=['category_id', 'unidades_vendidas'], ascending=[True, False])
    
    # Agregamos el nombre de la categoría
    df_gold = pd.merge(
        df_gold,
        df_categories[['category_id', 'category_name']],
        on='category_id',
        how='inner'
    )
    
    # Ordenamos y reorganizamos los datos
    df_gold = df_gold.sort_values(by=['category_name', 'unidades_vendidas'], ascending=[True, False])
    
    columnas_finales = ['category_id', 'category_name', 'product_id', 'product_name', 'unidades_vendidas', 'ingresos_totales']
    df_gold = df_gold[columnas_finales]
    
    return df_gold


# CARGA (LOAD)
def cargar_datos(dataframes: dict, ruta_destino: Path):
    """
    Guarda los DataFrames transformados en la carpeta de destino en formato Parquet.
    """
    # Verificamos si la carpeta existe
    if not ruta_destino.exists():
        ruta_destino.mkdir(parents=True, exist_ok=True)
        logging.info(f"Directorio creado: {ruta_destino}")

    for nombre_archivo, df in dataframes.items():
        # Extraemos el nombre base sin la extensión y guardamos
        nombre_base = Path(nombre_archivo).stem
        
        ruta_parquet = ruta_destino / f"{nombre_base}.parquet"
        try:
            df.to_parquet(ruta_parquet, index=False)
            logging.info(f"Carga exitosa (Parquet): {ruta_parquet}")
        except Exception as e:
            logging.error(f"Error al guardar {ruta_parquet} en Parquet: {e}")
            
        # Guardado en formato CSV (Opcional)
        # ruta_csv = ruta_destino / f"{nombre_base}.csv"
        # df.to_csv(ruta_csv, index=False, encoding='utf-8')
        # logging.info(f"Carga exitosa (CSV): {ruta_csv}")


# EJECUCIÓN PRINCIPAL
def main():
    # Configuraciónde logging para registrar eventos
    logging.basicConfig(
        level=logging.INFO, 
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("pipeline.log", encoding='utf-8', mode='w'), # Guarda el log en un .log
            logging.StreamHandler() # Muestra el log en la consola
        ]
    )
    
    # Definimos las rutas usando pathlib
    ruta_origen = Path('data/')
    ruta_destino = Path('output/')
    
    # Definimos la lista de archivos que nos importan
    archivos_requeridos = [
        'ecommerce_orders.csv',
        'ecommerce_order_items.csv',
        'ecommerce_customers.csv',
        'ecommerce_products.csv',
        'ecommerce_categories.csv'
    ]
    
    # EXTRAER
    logging.info("Iniciando extracción de datos, capa Bronze")
    dataframes = leer_csv_relevantes(ruta_origen, archivos_requeridos)
    if not dataframes:
        logging.error("ETL cancelado: No se cargaron datos.")
        return
    
    # TRASNFORMAR
    logging.info("Iniciando transformaciones para capa Silver")
    # Diccionario que mapea "Nombre del archivo" -> "Función que lo transforma"
    rutas_transformacion = {
        'ecommerce_orders.csv': transformar_orders,
        'ecommerce_customers.csv': transformar_customers,
        'ecommerce_products.csv': transformar_products,
        'ecommerce_order_items.csv': transformar_order_items,
        'ecommerce_categories.csv': transformar_categories
    }
    
    dataframes_transformados = {}
    
    # Iteramos sobre los dataframes que ya tenemos cargados y transformamos
    for nombre_archivo, df in dataframes.items():
        # Verificamos si existe una función de transformación para este archivo
        if nombre_archivo in rutas_transformacion:
            # Obtenemos la función del diccionario y la ejecutamos en su df correspondiente
            funcion_transformadora = rutas_transformacion[nombre_archivo]
            df_transformado = funcion_transformadora(df)
            dataframes_transformados[nombre_archivo] = df_transformado
            logging.info(f"Transformación (Silver) exitosa para {nombre_archivo}")
        else:
            # Si no, conservamos el original
            dataframes_transformados[nombre_archivo] = df
    
    
    # MODELAR
    logging.info("Iniciando validaciones para capa Gold")
    # Validamos que las tablas existan antes de intentar cruzarlas
    if 'ecommerce_orders.csv' in dataframes_transformados and 'ecommerce_customers.csv' in dataframes_transformados:
        df_gold_clientes = crear_tabla_gold_clientes(
            dataframes_transformados['ecommerce_orders.csv'], 
            dataframes_transformados['ecommerce_customers.csv']
        )
        dataframes_transformados['gold_top_clientes'] = df_gold_clientes

    if 'ecommerce_order_items.csv' in dataframes_transformados and 'ecommerce_products.csv' in dataframes_transformados and 'ecommerce_categories.csv' in dataframes_transformados:
        df_gold_productos = crear_tabla_gold_productos_categoria(
            dataframes_transformados['ecommerce_order_items.csv'], 
            dataframes_transformados['ecommerce_products.csv'],
            dataframes_transformados['ecommerce_categories.csv']
        )
        dataframes_transformados['gold_top_productos_categoria'] = df_gold_productos


    # CARGAR
    logging.info("Iniciando fase de Carga")
    # Cargamos/subimos los dataframes transformados a nuestro destino final (puede ser un data lake, un data warehouse, o simplemente una carpeta local)
    cargar_datos(dataframes_transformados, ruta_destino)
    
    logging.info("Pipeline ETL finalizado exitosamente.")

if __name__ == "__main__":
    main()