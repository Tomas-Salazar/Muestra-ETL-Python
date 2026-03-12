# Mini-ETL Pipeline: Arquitectura Medallion en Python

Un pipeline de datos end-to-end desarrollado en Python utilizando la librería Pandas. Este proyecto implementa una **Arquitectura Medallón (Bronze, Silver, Gold)** para simular el ciclo de vida de los datos de un e-commerce, desde la ingesta cruda hasta el modelado analítico.

Este repositorio sirve como demostración práctica de conceptos fundamentales de Ingeniería de Datos, incluyendo el manejo de calidad de datos, logging estructurado y almacenamiento eficiente.

## Arquitectura del Proyecto

El flujo de datos sigue el estándar de la industria separando responsabilidades por capas:

* **Capa Bronze (Raw):** Ingesta de archivos CSV originales. Se implementan validaciones de existencia, control de archivos vacíos y manejo de excepciones por permisos o errores de lectura.
* **Capa Silver (Cleansed):** Limpieza y estandarización de los datos. Incluye casteo de tipos (fechas, numéricos), manejo de valores nulos (estrategias de relleno), deduplicación de registros y tipado estricto.
* **Capa Gold (Curated/Data Marts):** Modelado analítico listo para consumo. Se generan tablas desnormalizadas como por ejemplo:
    * `gold_top_clientes`: Total gastado y cantidad de órdenes por cliente.
    * `gold_top_productos_categoria`: Ranking de productos más vendidos y sus ingresos, agrupados por categoría.

## Stack Tecnológico y Herramientas

* **Lenguaje:** Python 3.13
* **Procesamiento:** Pandas
* **Almacenamiento destino:** Formato columnar `.parquet` (para analítica).
* **Manejo de rutas:** `pathlib`.
* **Monitoreo:** Librería estándar `logging` (salida en consola y archivo `.log`).

## Consideraciones de Diseño y Buenas Prácticas

Si bien este proyecto utiliza procesamiento *in-memory* con Pandas para fines de demostración local, el código está estructurado pensando en sistemas productivos:

1.  **Resiliencia:** El proceso no falla silenciosamente. Se registran *warnings* y *errores* específicos si un archivo crítico falta o está corrupto.
2.  **Desacoplamiento funcional:** Cada tabla tiene su propia función de transformación (ej. `transformar_orders`), lo que facilita el mantenimiento y la realización de pruebas unitarias (testing).
3.  **Optimización:** La salida final se persiste en formato Parquet, reduciendo el tamaño de almacenamiento y mejorando drásticamente los tiempos de lectura para herramientas de BI posteriores.

## Cómo ejecutar el proyecto

Clonar el repositorio:
   ```bash
   git clone https://github.com/Tomas-Salazar/Muestra-ETL-Python
   cd Muestra-ETL-Python
   ```

Instalar dependencias:
   ```bash
   pip install -r requirements.txt
   ```


## Roadmap y Mejoras Futuras

Para perfilar este proyecto a un entorno empresarial de gran escala y con mejores prácticas, los siguientes pasos incluyen:

**Metadatos y Trazabilidad:** Incorporar marcas de agua (timestamps de ingesta) y columnas de linaje (origen del archivo) en las capas Bronze y Silver para futuras auditorías.

**Orquestación:** Trasladar la ejecución a un orquestador como Apache Airflow o integrarlo en un flujo de Azure Data Factory.

**Procesamiento Distribuido:** Refactorizar la lógica de transformación hacia PySpark para permitir la ejecución sobre clusters en entornos como Databricks, solucionando las limitaciones de memoria de Pandas ante grandes volúmenes de datos.

**Parametrización:** Migración de rutas y nombres de archivos a un archivo `config.yaml` para facilitar la ejecución en distintos entornos (Dev/Prod).

**Gestión de Secretos:** Integración futura con Azure Key Vault o uso de variables de entorno (`.env`) para asegurar credenciales y cadenas de conexión sin exponerlas en el código fuente.