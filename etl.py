import pandas as pd
from pathlib import Path
import logging

# Configuraciónde logging para registrar eventos
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("pipeline.log", encoding='utf-8', mode='w'),
        logging.StreamHandler() # Muestra el log en la consola
    ]
)

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
            
            logging.info(f"Cargado exitosamente: {ruta_archivo.name} | Dimensiones: {df.shape}")

        except pd.errors.EmptyDataError:
            logging.warning(f"El archivo no tiene columnas válidas: {ruta_archivo.name}")
        except PermissionError:
            logging.error(f"Sin permisos para leer: {ruta_archivo.name}")
        except Exception as e:
            logging.error(f"Error inesperado al leer {ruta_archivo.name}: {e}")

    return dataframes_relevantes

def exploracion_inicial(dataframes: dict):
    """
    Realiza una exploración inicial de los DataFrames cargados.
    Imprime información básica y estadísticas descriptivas.
    """
    for nombre, df in dataframes.items():
        logging.info(f"\nExplorando: {nombre}")
        logging.info(f"Dimensiones: {df.shape}")
        logging.info(f"Columnas: {df.columns.tolist()}")
        logging.info(f"Primeras filas:\n{df.head(10)}")
        logging.info(f"Tipos de datos:\n{df.dtypes}")
        logging.info(f"Valores nulos por columna:\n{df.isnull().sum()}")
        logging.info(f"Estadísticas descriptivas:\n{df.describe(include='all')}")

def transformar_orders(df: pd.DataFrame) -> pd.DataFrame:
    """Aplica reglas de negocio y limpieza al df de Orders."""
    logging.info("Iniciando transformación de orders...")
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

def main():
    # Definimos la ruta usando pathlib
    ruta_data = Path('data/')
    
    # Definimos la lista de archivos que nos importan
    archivos_requeridos = [
        'ecommerce_orders.csv',
        'ecommerce_order_items.csv',
        'ecommerce_customers.csv',
        'ecommerce_products.csv'
    ]
    
    # EXTRAER
    dataframes = leer_csv_relevantes(ruta_data, archivos_requeridos)

    if not dataframes:
        logging.error("ETL cancelado: No se cargaron datos.")
        return

    # Exploración opcional de los dataframes cargados
    # exploracion_inicial(dataframes)
    
    # TRASNFORMAR
    # Diccionario que mapea "Nombre del archivo" -> "Función que lo transforma"
    rutas_transformacion = {
        'ecommerce_orders.csv': transformar_orders,
        'ecommerce_customers.csv': transformar_customers,
        'ecommerce_products.csv': transformar_products,
        'ecommerce_order_items.csv': transformar_order_items
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
            logging.info(f"Transformación exitosa para {nombre_archivo}")
        else:
            # Si no, conservamos el original
            logging.info(f"Sin transformación para {nombre_archivo}. Se mantiene original.")
            dataframes_transformados[nombre_archivo] = df

    logging.info("Fase de Transformación finalizada.")
    
    # CARGA
    # Aqui iría la lógica para guardar los dataframes_transformados en una Base de Datos o Data Lake.

if __name__ == "__main__":
    main()