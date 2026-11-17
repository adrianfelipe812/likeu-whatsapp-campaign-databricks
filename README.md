# Analítica de una campaña de WhatsApp en un BPO – Caso técnico LikeU (Databricks)

Caso técnico que resolví de forma individual para el proceso de selección de **LikeU**. Un BPO lanzó una campaña por WhatsApp para motivar pedidos; los supervisores sentían que generaba fricción, los asesores parecían saturados y no había visibilidad sobre el éxito de las entregas. La tarea era construir, con herramientas de bajo costo, una solución analítica a partir de un extracto crudo del sistema transaccional.

> **Sobre los datos:** el extracto original contiene mensajes de clientes y **no se incluye** en este repositorio. El notebook se publica sin salidas; las cifras de abajo son resultados agregados del análisis.

## Qué hice

| Etapa | Detalle |
|---|---|
| **Ingesta** | Descarga del extracto y copia a un **Volume de Unity Catalog**; lectura con **PySpark** de un archivo separado por `\|`. |
| **Limpieza** | Nombres de columnas, columnas vacías, filas separadoras y espacios residuales. |
| **Parsing de JSON** | `get_json_object` para extraer el **error exacto** y la **plantilla** de cada mensaje fallido. |
| **NLP basado en reglas** | Clasificación de la intención de los mensajes entrantes con expresiones regulares: **Soporte, Pedido, Queja, General**. |
| **Modelo de datos** | Tabla limpia `mensajes_procesados` y **6 tablas Gold en Delta Lake**: funnel, volumen por hora, intenciones, errores, SLA diario y estado por categoría. |
| **KPIs en SQL** | Funnel de entrega, causas de fallo, **SLA de respuesta** (promedio, mediana y P90 con funciones de ventana), intención y curva horaria. |
| **Dashboard** | Widgets de fecha y estado para probar los filtros que luego se implementan en **Metabase**. |
| **Escalabilidad / FinOps** | Diseño para 50 usuarios concurrentes que actualizan cada 5 minutos: SQL Warehouse serverless con autoscaling, Auto Stop, result cache y consultas solo sobre la capa Gold. |

## Resultados principales

| Indicador | Valor |
|---|---|
| Mensajes salientes analizados | 2.187 |
| Fallidos (`failed`) | 1.396 → **63,83 %** |
| Leídos (`read`) | 708 → 32,37 % |
| Entregados (`delivered`) | 83 → 3,80 % |
| Tasa de éxito (`read` + `delivered`) | **36,17 %** |
| SLA promedio de respuesta (entrante → siguiente saliente) | **126,95 minutos** |
| Picos de mensajes entrantes | 15:00 (83), 16:00 (65) y 19:00 (62) |

**Diagnóstico:** la campaña no funcionaba como se creía. La mayoría de los envíos fallaba, y en los registros aparece el error `(#132000) Number of parameters does not match the expected number of params`, asociado a una plantilla de lanzamiento. La prioridad es corregir la entrega antes de escalar el volumen, medir el SLA por hora y reforzar la cobertura de asesores en las franjas de la tarde y la noche.

**Dimensionamiento ilustrativo:** con un supuesto de 20 chats por asesor/hora y 20 % de margen, el pico de 83 mensajes/hora requiere unos 5 asesores. Es un supuesto de trabajo; en producción debe calcularse con AHT, ocupación, shrinkage y el SLA objetivo.

## Arquitectura

```text
Extracto crudo (.txt, separado por |)
        │  gdown + dbutils.fs.cp
        ▼
Volume de Unity Catalog ──► PySpark: limpieza, JSON, NLP por reglas, timestamps
                                   │
                                   ▼
                     Delta: workspace.likeu.mensajes_procesados
                                   │  SQL (CTE + funciones de ventana)
                                   ▼
             Gold: funnel · hourly · categories · errors · sla · outgoing_category_status
                                   │
                                   ▼
             SQL Warehouse serverless (autoscaling, Auto Stop, cache) ──► Metabase
```

## Estructura

```text
notebooks/caso_tecnico_likeu.py   Notebook de Databricks (formato source, sin salidas)
```

## Cómo ejecutarlo

1. En Databricks: **Workspace → Import** y selecciona `notebooks/caso_tecnico_likeu.py`.
2. Reemplaza `<ID_DEL_ARCHIVO_EN_DRIVE>` por el identificador de tu archivo, o sube tu extracto al Volume `/Volumes/workspace/likeu/archivos_csv/`.
3. Ejecuta las celdas en orden. Se crean el esquema `workspace.likeu`, la tabla limpia y las tablas Gold.

## Herramientas

Databricks (Free Edition) · PySpark · Spark SQL · Delta Lake · Unity Catalog · Metabase (diseño) · Python

---

Autor: **Adrian Felipe Grateron Ramirez** · [LinkedIn](https://www.linkedin.com/in/adrian-grateron) · [GitHub](https://github.com/adrianfelipe812)
