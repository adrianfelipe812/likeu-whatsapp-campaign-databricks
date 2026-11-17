# Databricks notebook source
# MAGIC %md
# MAGIC #Introducción:
# MAGIC Eres el nuevo constructor de soluciones analíticas en nuestro BPO. El equipo de operaciones lanzó la "Campaña" vía WhatsApp para motivar a los clientes a realizar pedidos. Sin embargo, los supervisores intuyen que la campaña está generando fricción, los asesores parecen saturados y no hay visibilidad real sobre el éxito de las entregas.
# MAGIC
# MAGIC Operamos con una infraestructura de bajo costo (herramientas Open Source) y no contamos con conectores automatizados costosos. Se te ha entregado un extracto crudo del sistema transaccional en un archivo llamado prueba.txt.
# MAGIC
# MAGIC > **Nota de publicación:** el dataset de la prueba no se incluye en este repositorio porque contiene mensajes de clientes. Para ejecutar el notebook, reemplaza `<ID_DEL_ARCHIVO_EN_DRIVE>` por el identificador de tu propio archivo o carga el extracto directamente al Volume.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Para practicidad del almacenamiento de la información, instalamos gdown para permitir la descarga del drive

# COMMAND ----------

# MAGIC %pip install gdown

# COMMAND ----------

# MAGIC %md
# MAGIC #### Realizamos la descarga del drive y lo guardamos en formato .csv

# COMMAND ----------

import gdown
import os

file_id = '<ID_DEL_ARCHIVO_EN_DRIVE>'
download_url = f'https://drive.google.com/uc?id={file_id}'

# Guardamos temporalmente el CSV en el Workspace.
workspace_path = os.path.join(os.getcwd(), 'dataset_descargado.csv')

print("Descargando archivo...")
gdown.download(download_url, workspace_path, quiet=False)
print(f"Archivo descargado en: {workspace_path}")


# COMMAND ----------

# MAGIC %md
# MAGIC ### 0.1. Almacenamiento en Unity Catalog
# MAGIC
# MAGIC El archivo se descarga una sola vez y después se copia a un Volume. A partir de ese punto, el procesamiento se realiza con **Spark**, evitando depender de Pandas para leer el dataset completo y dejando el flujo más alineado con una implementación escalable.

# COMMAND ----------

import os

catalog_name = "workspace"
schema_name = "likeu"
volume_name = "archivos_csv"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog_name}.{schema_name}.{volume_name}")

volume_path = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/dataset_descargado.csv"

dbutils.fs.cp(f"file:{workspace_path}", volume_path, True)

print(f"Archivo disponible en: {volume_path}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Procesamiento de la información bruta sin herramientas de pago

# COMMAND ----------

from pyspark.sql.functions import col, trim, get_json_object, when, lower, to_timestamp, to_date, hour

file_path = "/Volumes/workspace/likeu/archivos_csv/dataset_descargado.csv"
delimiter = "|"


# 1. Lectura y limpieza estructural
df = (spark.read
       .option("header", "true")
       .option("sep", delimiter)
       .option("encoding", "UTF-8")
       .option("inferSchema", "true")
       .csv(file_path))

# Limpiar nombres de columnas.
for col_name in df.columns:
    df = df.withColumnRenamed(col_name, col_name.strip())

# Eliminar columnas completamente vacías.
cols_to_drop = [c for c in df.columns if c == "" or df.filter(col(c).isNotNull()).limit(1).count() == 0]
df = df.drop(*cols_to_drop)

# Eliminar filas separadoras.
df = df.filter(~col("id").cast("string").rlike("^-+$"))

# Limpiar espacios en columnas tipo string.
string_columns = [f.name for f in df.schema.fields if f.dataType.typeName() == "string"]
for c in string_columns:
    df = df.withColumn(c, trim(col(c)))


# 2. Parsing de JSON: errores y plantilla

df = df.withColumn(
    "error_exacto",
    when(
        col("status") == "failed",
        get_json_object(col("content_attributes"), "$.external_error")
    )
)

df = df.withColumn(
    "nombre_plantilla",
    when(
        col("status") == "failed",
        get_json_object(col("additional_attributes"), "$.template_params.name")
    )
)


# 3. Clasificación de mensajes entrantes
df = df.withColumn("content_lower", lower(col("content")))

df = df.withColumn(
    "categoria_mensaje",
    when(col("message_type") != "incoming", None)
    .when(col("content_lower").rlike("ayuda|problema|error|no funciona|soporte|factura|no llego"), "Soporte")
    .when(col("content_lower").rlike("comprar|precio|pedido|catalog|catálogo|cuánto cuesta|ofertas"), "Pedido")
    .when(col("content_lower").rlike("queja|reclamo|molesto|demora|terrible|malo"), "Queja")
    .otherwise("General")
)


# 4. Campos tipados reutilizables

df = (df
      .withColumn("created_at_ts", to_timestamp(col("created_at")))
      .withColumn("fecha_mensaje", to_date(col("created_at_ts")))
      .withColumn("hora_mensaje", hour(col("created_at_ts"))))

df = df.drop("content_lower")

display(df)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Explicación del procesamiento
# MAGIC
# MAGIC 1. **Limpieza estructural:** se eliminan columnas completamente vacías, filas separadoras y espacios residuales.
# MAGIC 2. **Extracción de información operativa:** de los campos JSON se obtiene `error_exacto` y `nombre_plantilla` para identificar causas y origen de los fallos.
# MAGIC 3. **Clasificación de mensajes entrantes:** se asigna una categoría básica (`Soporte`, `Pedido`, `Queja` o `General`) mediante reglas de palabras clave.
# MAGIC 4. **Campos analíticos tipados:** se crean `created_at_ts`, `fecha_mensaje` y `hora_mensaje` para reutilizar los timestamps en las consultas sin recalcularlos en cada métrica.

# COMMAND ----------

# creación de tabla para que Metabase u otros usuarios la consulten directamente:
df.write.format("delta").mode("overwrite").saveAsTable("workspace.likeu.mensajes_procesados")

print("¡Vista/Tabla 'mensajes_procesados' creada y lista para consultar en workspace.likeu!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Desarrollo de consultas para insights

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Funnel de Entrega
# MAGIC
# MAGIC Definición usada para el caso:
# MAGIC
# MAGIC - **Éxito:** `read` + `delivered`
# MAGIC - **Fallo:** `failed`
# MAGIC
# MAGIC La métrica de éxito se calcula sobre los mensajes salientes que tienen uno de estos tres estados.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH ConteoEstados AS (
# MAGIC     SELECT
# MAGIC         status AS estado,
# MAGIC         COUNT(*) AS cantidad_mensajes,
# MAGIC         -- Window Function: Calcula el total global filtrado en un solo paso
# MAGIC         SUM(COUNT(*)) OVER() AS total_global
# MAGIC     FROM workspace.likeu.mensajes_procesados
# MAGIC     WHERE message_type = 'outgoing'
# MAGIC       AND status IN ('read', 'delivered', 'failed')
# MAGIC     GROUP BY status
# MAGIC )
# MAGIC SELECT
# MAGIC     CASE estado
# MAGIC         WHEN 'read' THEN 'Leídos (read)'
# MAGIC         WHEN 'delivered' THEN 'Entregados (delivered)'
# MAGIC         WHEN 'failed' THEN 'Fallidos (failed)'
# MAGIC     END AS estado,
# MAGIC     cantidad_mensajes AS cantidad,
# MAGIC     ROUND((cantidad_mensajes * 100.0) / total_global, 2) AS porcentaje
# MAGIC FROM ConteoEstados
# MAGIC ORDER BY
# MAGIC     CASE estado
# MAGIC         WHEN 'delivered' THEN 1
# MAGIC         WHEN 'read' THEN 2
# MAGIC         WHEN 'failed' THEN 3
# MAGIC         ELSE 4
# MAGIC     END;

# COMMAND ----------

# MAGIC %md
# MAGIC **KPI ejecutivo del funnel:** esta consulta deja una sola fila lista para usar como tarjetas KPI en un dashboard.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH Base AS (
# MAGIC     SELECT
# MAGIC         COUNT(*) AS total_outgoing,
# MAGIC         SUM(CASE WHEN status = 'read' THEN 1 ELSE 0 END) AS total_read,
# MAGIC         SUM(CASE WHEN status = 'delivered' THEN 1 ELSE 0 END) AS total_delivered,
# MAGIC         SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS total_failed
# MAGIC     FROM mensajes_procesados
# MAGIC     WHERE message_type = 'outgoing'
# MAGIC       AND status IN ('read', 'delivered', 'failed')
# MAGIC )
# MAGIC SELECT
# MAGIC     total_outgoing,
# MAGIC     total_read,
# MAGIC     total_delivered,
# MAGIC     total_failed,
# MAGIC     ROUND((total_read + total_delivered) * 100.0 / total_outgoing, 2) AS tasa_exito_pct,
# MAGIC     ROUND(total_failed * 100.0 / total_outgoing, 2) AS tasa_fallo_pct
# MAGIC FROM Base;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lectura de negocio del funnel
# MAGIC
# MAGIC El resultado actual no respalda afirmar que "casi todos" los clientes leen el mensaje. En el extracto analizado se observan **2.187 envíos salientes**, de los cuales **1.396 quedaron en `failed` (63,83%)**, **708 en `read` (32,37%)** y **83 en `delivered` (3,80%)**.
# MAGIC
# MAGIC Por lo tanto, bajo la definición `read + delivered` como resultado exitoso, la **tasa de éxito es 36,17%** y la **tasa de fallo es 63,83%**.
# MAGIC
# MAGIC La prioridad operativa es investigar la concentración de los fallos y comprobar si una plantilla o un error técnico explica una parte significativa del problema. La siguiente consulta cuantifica ese punto.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1.1 Causas de los `failed`
# MAGIC
# MAGIC Esta consulta permite determinar si los fallos están concentrados en una plantilla o mensaje de error específico.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COALESCE(error_exacto, 'Sin error identificado') AS error_exacto,
# MAGIC     COALESCE(nombre_plantilla, 'Sin plantilla') AS nombre_plantilla,
# MAGIC     COUNT(*) AS cantidad_fallos,
# MAGIC     ROUND(
# MAGIC         COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (),
# MAGIC         2
# MAGIC     ) AS porcentaje_de_fallos
# MAGIC FROM mensajes_procesados
# MAGIC WHERE message_type = 'outgoing'
# MAGIC   AND status = 'failed'
# MAGIC GROUP BY
# MAGIC     COALESCE(error_exacto, 'Sin error identificado'),
# MAGIC     COALESCE(nombre_plantilla, 'Sin plantilla')
# MAGIC ORDER BY cantidad_fallos DESC;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 SLA de Respuesta Operativa
# MAGIC
# MAGIC Se calcula el tiempo entre un mensaje `incoming` y el siguiente mensaje `outgoing` de la misma conversación. Para el caso se toma `outgoing` como respuesta operativa válida; si en producción existen otros tipos de eventos que deban contar como respuesta, la regla debe ajustarse con Operaciones.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH TiemposRespuesta AS (
# MAGIC     SELECT
# MAGIC         conversation_id,
# MAGIC         message_type,
# MAGIC         created_at_ts,
# MAGIC         MIN(
# MAGIC             CASE
# MAGIC                 WHEN message_type = 'outgoing' THEN created_at_ts
# MAGIC             END
# MAGIC         ) OVER (
# MAGIC             PARTITION BY conversation_id
# MAGIC             ORDER BY created_at_ts
# MAGIC             ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING
# MAGIC         ) AS tiempo_siguiente_respuesta
# MAGIC     FROM mensajes_procesados
# MAGIC ),
# MAGIC Respuesta AS (
# MAGIC     SELECT
# MAGIC         conversation_id,
# MAGIC         created_at_ts,
# MAGIC         tiempo_siguiente_respuesta,
# MAGIC         TIMESTAMPDIFF(MINUTE, created_at_ts, tiempo_siguiente_respuesta) AS minutos_respuesta
# MAGIC     FROM TiemposRespuesta
# MAGIC     WHERE message_type = 'incoming'
# MAGIC       AND tiempo_siguiente_respuesta IS NOT NULL
# MAGIC       AND TIMESTAMPDIFF(MINUTE, created_at_ts, tiempo_siguiente_respuesta) >= 0
# MAGIC )
# MAGIC SELECT
# MAGIC     COUNT(*) AS mensajes_con_respuesta,
# MAGIC     ROUND(AVG(minutos_respuesta), 2) AS sla_promedio_minutos,
# MAGIC     ROUND(PERCENTILE_APPROX(minutos_respuesta, 0.50), 2) AS sla_mediana_minutos,
# MAGIC     ROUND(PERCENTILE_APPROX(minutos_respuesta, 0.90), 2) AS sla_p90_minutos
# MAGIC FROM Respuesta;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Mensajes Entrantes por Intención
# MAGIC
# MAGIC La distribución de categorías ayuda a distinguir si la campaña está generando oportunidades de pedido, solicitudes de soporte o fricción/quejas.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     categoria_mensaje,
# MAGIC     COUNT(*) AS cantidad_mensajes,
# MAGIC     ROUND(
# MAGIC         COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (),
# MAGIC         2
# MAGIC     ) AS porcentaje
# MAGIC FROM mensajes_procesados
# MAGIC WHERE message_type = 'incoming'
# MAGIC GROUP BY categoria_mensaje
# MAGIC ORDER BY cantidad_mensajes DESC;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.3 Curva de Calor Horaria
# MAGIC
# MAGIC Esta consulta agrupa los mensajes entrantes por hora.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     hora_mensaje AS hora_del_dia,
# MAGIC     COUNT(*) AS volumen_mensajes_entrantes
# MAGIC FROM mensajes_procesados
# MAGIC WHERE message_type = 'incoming'
# MAGIC GROUP BY hora_mensaje
# MAGIC ORDER BY hora_mensaje ASC;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lectura de negocio de la curva horaria
# MAGIC
# MAGIC - El volumen entrante está concentrado principalmente entre la tarde y la noche.
# MAGIC - El pico observado es **15:00 con 83 mensajes**, seguido por **16:00 con 65** y **19:00 con 62**.
# MAGIC - La programación de capacidad debería considerar estas franjas de mayor demanda.
# MAGIC - No se recomienda asumir de forma rígida una dotación mínima para la mañana sin contrastarla con el SLA objetivo, el AHT y la disponibilidad real de los asesores.
# MAGIC
# MAGIC La gráfica horaria debe utilizarse como una señal para redistribuir capacidad, no como único criterio de dimensionamiento.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Nota de dimensionamiento
# MAGIC
# MAGIC Como ejemplo ilustrativo, si se asume una capacidad de **20 chats por asesor/hora** y un margen de seguridad del **20%**, para el pico de 83 mensajes/hora:
# MAGIC
# MAGIC `Asesores ≈ (83 / 20) × 1,20 = 4,98 → 5 asesores`
# MAGIC
# MAGIC Este valor es un **supuesto de trabajo**, no una capacidad calculada a partir del dataset. En producción debería reemplazarse por AHT, ocupación, shrinkage, pausas y SLA objetivo.

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Capa de datos para Dashboard y Metabase
# MAGIC
# MAGIC El notebook deja una tabla limpia persistida y varias tablas agregadas de tipo **Gold**. La idea es separar el procesamiento de los datos del consumo del dashboard: Metabase consulta resultados preparados, en lugar de recalcular JSON y agregaciones sobre el histórico cada vez que un usuario pulsa "Actualizar".

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Tabla limpia y persistente para consumo analítico.
# MAGIC CREATE OR REPLACE TABLE workspace.likeu.mensajes_procesados
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT *
# MAGIC FROM mensajes_procesados;
# MAGIC
# MAGIC SELECT COUNT(*) AS registros_persistidos
# MAGIC FROM workspace.likeu.mensajes_procesados;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.1 Tablas Gold recomendadas

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Funnel por fecha y estado
# MAGIC CREATE OR REPLACE TABLE workspace.likeu.gold_campaign_funnel
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC     fecha_mensaje,
# MAGIC     status,
# MAGIC     COUNT(*) AS cantidad_mensajes
# MAGIC FROM workspace.likeu.mensajes_procesados
# MAGIC WHERE message_type = 'outgoing'
# MAGIC   AND status IN ('read', 'delivered', 'failed')
# MAGIC GROUP BY fecha_mensaje, status;
# MAGIC
# MAGIC -- Volumen entrante por fecha y hora
# MAGIC CREATE OR REPLACE TABLE workspace.likeu.gold_campaign_hourly
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC     fecha_mensaje,
# MAGIC     hora_mensaje,
# MAGIC     COUNT(*) AS volumen_mensajes_entrantes
# MAGIC FROM workspace.likeu.mensajes_procesados
# MAGIC WHERE message_type = 'incoming'
# MAGIC GROUP BY fecha_mensaje, hora_mensaje;
# MAGIC
# MAGIC -- Categorías de mensajes entrantes
# MAGIC CREATE OR REPLACE TABLE workspace.likeu.gold_campaign_categories
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC     fecha_mensaje,
# MAGIC     categoria_mensaje,
# MAGIC     COUNT(*) AS cantidad_mensajes
# MAGIC FROM workspace.likeu.mensajes_procesados
# MAGIC WHERE message_type = 'incoming'
# MAGIC GROUP BY fecha_mensaje, categoria_mensaje;
# MAGIC
# MAGIC -- Errores y plantillas asociadas
# MAGIC CREATE OR REPLACE TABLE workspace.likeu.gold_campaign_errors
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC     fecha_mensaje,
# MAGIC     COALESCE(error_exacto, 'Sin error identificado') AS error_exacto,
# MAGIC     COALESCE(nombre_plantilla, 'Sin plantilla') AS nombre_plantilla,
# MAGIC     COUNT(*) AS cantidad_fallos
# MAGIC FROM workspace.likeu.mensajes_procesados
# MAGIC WHERE message_type = 'outgoing'
# MAGIC   AND status = 'failed'
# MAGIC GROUP BY fecha_mensaje, error_exacto, nombre_plantilla;
# MAGIC
# MAGIC -- SLA diario: incoming -> siguiente outgoing
# MAGIC CREATE OR REPLACE TABLE workspace.likeu.gold_campaign_sla
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH Tiempos AS (
# MAGIC     SELECT
# MAGIC         conversation_id,
# MAGIC         fecha_mensaje,
# MAGIC         message_type,
# MAGIC         created_at_ts,
# MAGIC         MIN(CASE WHEN message_type = 'outgoing' THEN created_at_ts END)
# MAGIC             OVER (
# MAGIC                 PARTITION BY conversation_id
# MAGIC                 ORDER BY created_at_ts
# MAGIC                 ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING
# MAGIC             ) AS siguiente_respuesta
# MAGIC     FROM workspace.likeu.mensajes_procesados
# MAGIC ),
# MAGIC Respuesta AS (
# MAGIC     SELECT
# MAGIC         fecha_mensaje,
# MAGIC         TIMESTAMPDIFF(MINUTE, created_at_ts, siguiente_respuesta) AS minutos_respuesta
# MAGIC     FROM Tiempos
# MAGIC     WHERE message_type = 'incoming'
# MAGIC       AND siguiente_respuesta IS NOT NULL
# MAGIC       AND TIMESTAMPDIFF(MINUTE, created_at_ts, siguiente_respuesta) >= 0
# MAGIC )
# MAGIC SELECT
# MAGIC     fecha_mensaje,
# MAGIC     COUNT(*) AS mensajes_con_respuesta,
# MAGIC     ROUND(AVG(minutos_respuesta), 2) AS sla_promedio_minutos
# MAGIC FROM Respuesta
# MAGIC GROUP BY fecha_mensaje;
# MAGIC
# MAGIC -- Tabla para analizar el comportamiento de los agentes
# MAGIC CREATE OR REPLACE TABLE workspace.likeu.gold_campaign_outgoing_category_status
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC     fecha_mensaje,
# MAGIC     COALESCE(categoria_mensaje, 'Sin Categoría') AS categoria_mensaje,
# MAGIC     status,
# MAGIC     COUNT(*) AS cantidad_mensajes
# MAGIC FROM workspace.likeu.mensajes_procesados
# MAGIC WHERE message_type = 'outgoing'
# MAGIC   AND status IN ('read', 'delivered', 'failed')
# MAGIC GROUP BY
# MAGIC     fecha_mensaje,
# MAGIC     COALESCE(categoria_mensaje, 'Sin Categoría'),
# MAGIC     status;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.2 Filtros de notebook para pruebas operativas
# MAGIC
# MAGIC Estos widgets permiten probar el mismo concepto de filtrado que después se puede implementar en Metabase. El filtro de fecha se aplica por rango y el estado permite seleccionar `Todos`, `read`, `delivered` o `failed`.

# COMMAND ----------

# Widgets Databricks para pruebas del dashboard.
for widget_name in ["fecha_inicio", "fecha_fin", "estado_filtro"]:
    try:
        dbutils.widgets.remove(widget_name)
    except Exception:
        pass

dbutils.widgets.text("fecha_inicio", "2023-07-01", "Fecha inicio (YYYY-MM-DD)")
dbutils.widgets.text("fecha_fin", "2023-08-31", "Fecha fin (YYYY-MM-DD)")
dbutils.widgets.dropdown(
    "estado_filtro",
    "Todos",
    ["Todos", "read", "delivered", "failed"],
    "Estado"
)

print(
    "Filtros activos:",
    dbutils.widgets.get("fecha_inicio"),
    dbutils.widgets.get("fecha_fin"),
    dbutils.widgets.get("estado_filtro")
)


# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 Consulta operacional filtrada
# MAGIC
# MAGIC Esta consulta está pensada para una tabla de detalle del dashboard. En Metabase, los filtros equivalentes deben conectarse a campos de fecha y estado mediante sus parámetros/Field Filters.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     id,
# MAGIC     conversation_id,
# MAGIC     created_at_ts,
# MAGIC     message_type,
# MAGIC     status,
# MAGIC     categoria_mensaje,
# MAGIC     error_exacto,
# MAGIC     nombre_plantilla
# MAGIC FROM workspace.likeu.mensajes_procesados
# MAGIC WHERE fecha_mensaje BETWEEN
# MAGIC       COALESCE(TRY_CAST('${fecha_inicio}' AS DATE), DATE'1900-01-01')
# MAGIC       AND COALESCE(TRY_CAST('${fecha_fin}' AS DATE), DATE'2999-12-31')
# MAGIC ORDER BY created_at_ts DESC;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.4 Validación de la creación de las tablas para escalabilidad

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN workspace.likeu;

# COMMAND ----------

# MAGIC %md
# MAGIC # 4. Visión de Negocio – Diagnóstico y acciones
# MAGIC
# MAGIC ### Diagnóstico
# MAGIC
# MAGIC El resultado del funnel muestra una tasa de fallo de **63,83%** en los mensajes salientes del extracto analizado, frente a **36,17%** de resultado exitoso bajo la definición `read + delivered`. Esto hace que la primera prioridad sea recuperar la efectividad de entrega antes de escalar el volumen de la campaña.
# MAGIC
# MAGIC La consulta de errores debe utilizarse para determinar qué causas y plantillas concentran los `failed`. En los registros observados aparece el error `(#132000) Number of parameters does not match the expected number of params`, asociado a la plantilla `lanzamiento_`; la concentración exacta debe medirse con la tabla de errores y no asumirse a partir de un ejemplo.
# MAGIC
# MAGIC El SLA promedio del extracto es de **126,95 minutos** bajo la definición `incoming -> siguiente outgoing`. El promedio debe contrastarse con el SLA oficial de la operación y complementarse con mediana y P90 para detectar colas o casos extremos.
# MAGIC
# MAGIC La curva horaria muestra mayor concentración en la tarde/noche, con máximos observados a las **15:00 (83 mensajes)**, **16:00 (65)** y **19:00 (62)**. Esto sugiere revisar la distribución de capacidad en esas franjas.
# MAGIC
# MAGIC ### Acciones inmediatas
# MAGIC
# MAGIC 1. Identificar y corregir las plantillas que concentran `failed`, verificando cantidad/posición de parámetros y ejecutando una prueba controlada antes de un nuevo envío masivo.
# MAGIC 2. Monitorear diariamente `failed`, `delivered` y `read` como KPIs de salud de la campaña.
# MAGIC 3. Analizar la distribución de `categoria_mensaje` para separar demanda comercial (`Pedido`) de soporte y quejas.
# MAGIC 4. Revisar la cobertura de asesores en las franjas 15:00–16:00 y 19:00 frente al SLA objetivo.
# MAGIC 5. Medir SLA por hora y no solo como promedio global.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Escalabilidad y enfoque FinOps
# MAGIC
# MAGIC Para garantizar que el dashboard operativo pueda soportar múltiples usuarios concurrentes sin incrementar innecesariamente los costos, se implementa una arquitectura basada en una capa Gold sobre Delta Lake y un Databricks SQL Warehouse como capa de servicio para Metabase.
# MAGIC
# MAGIC Las consultas que alimentan el dashboard no acceden directamente a la tabla transaccional `mensajes_procesados`, sino a las tablas agregadas `gold_campaign_funnel`, `gold_campaign_hourly`, `gold_campaign_categories`, `gold_campaign_errors` y `gold_campaign_sla`. Esto reduce significativamente la cantidad de datos que debe procesarse en cada consulta y desacopla la visualización del procesamiento de los datos fuente.
# MAGIC
# MAGIC Para atender la concurrencia se propone utilizar un SQL Warehouse dedicado para BI, preferiblemente Serverless, configurado con autoscaling y Auto Stop. De esta forma, los recursos de cómputo pueden aumentar cuando exista concurrencia y detenerse durante periodos de inactividad, evitando mantener infraestructura activa de manera innecesaria.
# MAGIC
# MAGIC La solución también aprovecha el result cache de Databricks SQL. Las consultas repetidas sobre tablas que no hayan cambiado pueden reutilizar resultados previamente calculados, disminuyendo el consumo de cómputo. Esto es especialmente relevante en un escenario en el que 50 usuarios podrían abrir simultáneamente el dashboard y actualizarlo cada cinco minutos.
# MAGIC
# MAGIC Adicionalmente, la actualización de las tablas Gold debe mantenerse separada de la interacción de los usuarios con Metabase. Es decir, el botón de actualización del dashboard solamente debe volver a consultar los datos disponibles en Gold y no ejecutar nuevamente el proceso ETL completo. Esta separación evita que múltiples usuarios desencadenen operaciones costosas sobre la capa de procesamiento.
# MAGIC
# MAGIC Desde el punto de vista de FinOps, se aplican cuatro principios: preagregación de datos, reutilización de resultados mediante caching, escalamiento automático según demanda y apagado automático de recursos durante periodos de inactividad. Finalmente, se recomienda monitorear la concurrencia, tiempos de consulta y consumo del SQL Warehouse para ajustar el número máximo de clusters de acuerdo con el comportamiento real de la aplicación.
# MAGIC
# MAGIC De esta manera, la arquitectura permite soportar incrementos de usuarios sin depender de un procesamiento completo de los datos para cada interacción y mantiene el consumo de infraestructura proporcional a la demanda.
