# GUÍA MAESTRA Y LIBRETO DE SUSTENTACIÓN CON BANCO DE PREGUNTAS INTEGRADO
**Caso de Estudio:** Caracterización y Brecha Territorial del Registro Único de Víctimas (RUV)  
**Asignatura:** Big Data 2026-II · Universidad Externado de Colombia  
**Evaluadoras:** Profesora titular Yessica Velásquez y docente invitada experta Martha Quevedo  
**Equipo Consultor:** Julián Steven Duarte, Julián Camilo Jiménez, Juan Tomás Rincón  
**Tiempo:** Exactamente 3 minutos por expositor (9 minutos en total, corte estricto con cronómetro).  
**Soporte Visual:** Presentación oficial de 10 diapositivas proyectada por la docente.

---

> ### ⚠️ REGLA DE ORO DEL SORTEO Y DOMINIO CRUZADO
> Al inicio de la sesión, la docente sorteará con tarjetas el componente que expone cada estudiante (**A, B o C**), el cual será **diferente al que declaraste como propio en la bitácora**.  
> Por tanto, **los tres integrantes deben dominar la totalidad del libreto y el banco de preguntas técnicas**.  
> Cada suceso, número o métrica cuenta con su **evidencia verificable entre paréntesis**, para que puedan indicarle a las profesoras exactamente en qué captura, celda del notebook o página de los anexos está respaldada.

---

## 📌 TABLA MAESTRA DE DATOS, PARÁMETROS Y UBICACIÓN DE EVIDENCIAS

| Métrica / Parámetro | Valor Exacto Verificado | Dónde se verifica la evidencia (Citar entre paréntesis) |
| :--- | :--- | :--- |
| **Peso original en plataforma (datos.gov.co / Drive)** | **3,33 GB** (3.330.779.150 bytes / 3,10 GiB) | *(Ver Presentación Diapositiva 4, Anexo A4 en Anexos pág. 21 y archivo local en `Data/`)* |
| **Peso descargado e ingresado a HDFS** | **2,65 GB** decimal / **2,47 GiB** binario (2.649.266.066 bytes) | *(Ver Captura C2 en Anexos pág. 3, Captura C3 en Anexos pág. 4 y Diapositivas 2 y 4)* |
| **Registros totales del censo** | **8.860.888 registros** | *(Ver Notebook celda `S4a`, Captura C8 Input Records y Diapositiva 2)* |
| **Columnas del dataset** | **19 variables** | *(Ver Notebook celda `S3` StructType explícito y Diapositiva 2)* |
| **Cobertura territorial** | **1.126 municipios** en los 34 departamentos | *(Ver Notebook celda `S4b`, Informe pág. 1 y Diapositiva 2)* |
| **Registros vigentes año 2026** | **3.882.164 filas** (~3,88 millones) | *(Ver Notebook celda `S4d` y Diapositiva 2)* |
| **Tamaño de bloque en HDFS** | **128 MB** (134.217.728 bytes) | *(Ver Captura C2 en Anexos pág. 3 y detalle de bloque 0 en Captura C3 pág. 4)* |
| **Número de bloques físicos** | **20 bloques físicos** (19 de 128 MB + 1 de ~99,1 MB) | *(Ver Salida `hdfs fsck` en Captura C3 pág. 4 y Diapositiva 3)* |
| **Factor de replicación HDFS** | **2** (Replicación promedio: 2.0 = 40 réplicas totales) | *(Ver Captura C2 en Anexos pág. 3, Captura C3 pág. 4 y Diapositiva 3)* |
| **Nodos trabajadores (DataNodes)** | `node2` (192.168.56.102) y `node3` (192.168.56.103) | *(Ver Captura C1 en Anexos pág. 2, pestaña Datanodes del HDFS Web UI puerto 9870)* |
| **Capacidad física por DataNode** | **58,26 GB** cada nodo | *(Ver Captura C1 en Anexos pág. 2 y Diapositiva 3)* |
| **Estado de salud del HDFS** | **100 % HEALTHY** (0 corruptos, 0 under/over-replicated) | *(Ver Captura C3 en Anexos pág. 4, terminal `hdfs fsck`)* |
| **Incidente de corte de descarga** | Corte HTTP a 2,65 GB de 3,33 GB (Sesión 1, 9/11/26 9:30) | *(Ver Anexo A4 en Anexos pág. 21, Bitácora sesión 1 pág. 2 y Diapositiva 4)* |
| **Application ID de referencia (YARN)** | `application_1794236810279_0001` (Sesión 1, 9/nov 10:26:54) | *(Ver Captura C4 en Anexos pág. 5, Resource Manager puerto 8088 y Diapositiva 5)* |
| **Application ID de análisis (Sesión 2)** | `application_1794406653870_0001` y `..._0002` | *(Ver Captura C5 Spark UI Environment pág. 6 y Captura C7 SQL pág. 9)* |
| **Recursos asignados en YARN** | **2 ejecutores** (en node2 y node3) + 1 driver, **2 cores** y **2 GB RAM** c/u | *(Ver Captura C4 pág. 5, Captura C5b Executors pág. 7 y Diapositiva 5)* |
| **shuffle.partitions configurado** | **8 particiones** (ajustado de 200 a 8 = 2 olas de 4 tareas) | *(Ver Captura C5 Environment en Anexos pág. 6 y Diapositiva 5)* |
| **Limpieza de fechas multiformato** | Funciones nativas Spark SQL (`substring`, `coalesce`, `to_date`) | *(Ver Notebook celdas `S4c` y `S4d` y Diapositiva 6)* |
| **Stage cuello de botella (crítico)** | **Stage 30** (36 s acumulados, 20 tareas, I/O HDFS) | *(Ver Captura C8 Summary Metrics en Anexos pág. 11 y Diapositiva 8)* |
| **Shuffle Write del Stage 30** | Entrada: 2,5 GiB (8,86 M reg) -> Shuffle Write: **13,8 MiB** (318.416 reg) | *(Ver Captura C8 en Anexos pág. 11 y Diapositiva 8)* |
| **Stage rápido de shuffle** | **Stage 48** (duración: 0,8 s, 6 tareas, 9,8 MiB leídos) | *(Ver Captura C9 Summary Metrics en Anexos pág. 13 y Diapositiva 8)* |
| **Spill a disco o memoria** | **0 Bytes (cero spill)** en todo el procesamiento | *(Ver Capturas C8 y C9 en Anexos págs. 11 y 13 y Diapositiva 8)* |
| **Nivel de persistencia (Cache)** | `MEMORY_AND_DISK` (RDD 162 al 100% en RAM, 625,5 KiB, 0 B disco) | *(Ver Captura C10 pestaña Storage en Anexos pág. 15 y Diapositiva 8)* |
| **Bonificación (+0,3): Joins** | SMJ (-1): 9,21 s y 31,2 KiB shuffle vs BHJ (10MB): 8,96 s y 354 B shuffle | *(Ver Captura C11 en Anexos págs. 16-18, Notebook celdas `S7a`-`S7c` y Diapositiva 9)* |
| **Tiempo de análisis en clúster propio** | **10,28 segundos** (sin cobro variable por consulta) | *(Ver Notebook celda `S8b` y Sección A5 en Anexos pág. 21)* |
| **Google BigQuery: bytes facturados** | **830 MB facturados** (0 B en caché de slots, tabla de 6,97 GB) | *(Ver Hoja A2 en Anexos pág. 20, Sección A5 pág. 21 y Diapositiva 10)* |
| **Brecha asistencial detectada** | **28 % de víctimas declarantes** sin ubicación territorial | *(Ver Notebook celda `S10a`, Informe pág. 2 y Diapositiva 10)* |

---

## INTRODUCCIÓN GENERAL (0:00 - 0:15) — *A cargo del primer expositor*
*(Diapositiva 1 proyectada en pantalla)*

> *"Buenos días profesoras Yessica y Martha. Somos Julián Steven Duarte, Julián Camilo Jiménez y Juan Tomás Rincón. Presentamos nuestro ejercicio de consultoría Big Data para la Unidad para las Víctimas (UARIV). Abordamos el censo municipal histórico mediante un flujo distribuido por lotes en Apache Spark sobre YARN y almacenamiento HDFS, resolviendo las brechas territoriales de atención y evaluando la viabilidad de migración a Google BigQuery."* *(Ver Presentación Diapositiva 1 e Informe de Consultoría pág. 1)*

---

## COMPONENTE A: DATOS Y ARQUITECTURA DE ALMACENAMIENTO (HDFS)
**Tiempo asignado:** Exactamente 3 minutos (180 segundos).  
**Diapositivas:** 2, 3 y 4.  
**Objetivo de evaluación:** Explicar el desafío del cliente, la representatividad del dataset, la distribución física de bloques en HDFS y el control de incidentes.

---

### 🕒 Minuto 0:00 a 1:00 — Diapositiva 2: El Desafío, Dimensiones y Relevancia del RUV
*(Decir con naturalidad al pasar a la diapositiva 2)*

* **El cliente y el problema de negocio:**  
  *"La UARIV centraliza el censo oficial del conflicto armado en Colombia, pero su gran volumen dificultaba el análisis integral en servidores convencionales. La entidad nos contrató para responder dos preguntas clave: primero, ¿en qué municipios se concentran históricamente los hechos victimizantes?; y segundo, ¿dónde está la brecha entre quienes declaran un hecho y quienes logran ser atendidos y ubicados en el territorio?"* *(Ver Informe pág. 1 y Diapositiva 2)*
* **El conjunto de datos y cifras oficiales verificadas:**  
  *"Seleccionamos el dataset oficial de datos.gov.co con ID `9qih-4vkc`, corte a septiembre de 2026. Procesamos **8.860.888 registros** y **19 columnas**, con una cobertura de **1.126 municipios** de los 34 departamentos del país* *(Ver Notebook celda S4a y S4b)*. *El archivo CSV pesa **2,47 GB** (2.649.266.066 bytes) en HDFS, cumpliendo holgadamente el criterio de la rúbrica de entre 2 y 5 GB* *(Ver Captura C2 en Anexos pág. 3)*, *y contiene **3,88 millones de filas del año 2026**, lo que garantiza que la información está vigente y no es una simple actualización de metadatos"* *(Ver Notebook celda S4d y Diapositiva 2)*.

#### ❓ BANCO DE PREGUNTAS DEL JURADO — DIAPOSITIVA 2
* **Pregunta 1: ¿Por qué en algunos reportes se lee 2,65 GB y en HDFS dice 2,47 GB? ¿Hubo pérdida de datos?**
  * **Respuesta técnica exacta:**  
    > *"No hubo pérdida de datos, profesora. La diferencia radica en la base de cálculo de unidades:  
    > El archivo físico contiene exactamente **2.649.266.066 bytes**.  
    > Si lo expresamos en base decimal del Sistema Internacional ($10^9$), equivale a **2,65 GB** ($2.649.266.066 / 1.000.000.000 = 2,649\text{ GB}$), que fue la cifra reportada por la herramienta de descarga en terminal* *(Ver Anexo A4 en Anexos pág. 21)*.  
    > En cambio, los sistemas de archivos como HDFS y Spark UI calculan en base binaria de potencias de 2 ($1024^3 = 1.073.741.824\text{ bytes}$ por GiB):  
    > $$\frac{2.649.266.066}{1.073.741.824} = 2,4673\text{ GiB} \longrightarrow \textbf{2,47 GB}$$  
    > Como se verifica en el campo 'Size' de la interfaz Web de HDFS* *(Ver Captura C2 en Anexos pág. 3)* *y en el 'Total size' de `hdfs fsck`* *(Ver Captura C3 pág. 4)*, *el número exacto de bytes es idéntico: 2.649.266.066 B."*
* **Pregunta 2: ¿Cómo garantizan que el censo es representativo y que no se sesgó el análisis geográfico?**
  * **Respuesta técnica exacta:**  
    > *"Lo comprobamos mediante la celda `S4b` del Notebook: el archivo cubre **1.126 municipios** distribuidos en los 34 departamentos oficiales del catálogo DANE (incluyendo Bogotá D.C. y las zonas no municipalizadas). Además, en la celda `S4a` comprobamos **cero valores nulos** en las variables geográficas y demográficas operativas (`COD_ESTADO_DEPTO`, `CIUDAD_MUNICIPIO`, `HECHO`, `ETNIA`, `SEXO`)."*

---

### 🕒 Minuto 1:00 a 2:00 — Diapositiva 3: Arquitectura HDFS: Replicación y Distribución Física
*(Pasar a la diapositiva 3)*

* **Topología física del clúster:**  
  *"El almacenamiento distribuido se implementó sobre un clúster físico de 3 máquinas en la sala Deep Learning: un nodo maestro que aloja el NameNode y dos nodos trabajadores, `node2` y `node3`, cada uno con 58,26 GB de capacidad de disco"* *(Ver Captura C1 en Anexos pág. 2, Web UI puerto 9870)*.
* **Distribución física de bloques y replicación:**  
  *"Cargamos el archivo en la ruta `/caso` con un tamaño de bloque estándar de **128 MB** y factor de **replicación 2**. Como el CSV pesa 2.649.266.066 bytes, esto generó matemáticamente **20 bloques físicos**, que fueron duplicados de forma balanceada y simultánea entre `node2` y `node3` (20 réplicas en cada nodo, sumando 40 réplicas totales)* *(Ver Captura C2 en Anexos pág. 3 y Captura C3 pág. 4)*. *En la captura C1 verificamos que ambos DataNodes están en estado 'In Service', y ejecutamos en terminal el comando `hdfs fsck`, el cual certificó un estado **100 % HEALTHY**, sin bloques corruptos ni réplicas faltantes, garantizando tolerancia total ante la caída de cualquiera de los dos trabajadores"* *(Ver Captura C3 en Anexos pág. 4)*.

#### ❓ BANCO DE PREGUNTAS DEL JURADO — DIAPOSITIVA 3
* **Pregunta 1: ¿Por qué el archivo generó exactamente 20 bloques en HDFS? Demuéstrenlo con números.**
  * **Respuesta técnica exacta:**  
    > *"Profesora, el cálculo físico es directo:  
    > 1. El archivo pesa exactamente **2.649.266.066 bytes** en HDFS* *(Ver Captura C2 pág. 3 y C3 pág. 4)*.  
    > 2. El tamaño de bloque estándar configurado en el clúster es de **128 MB**, que en bytes corresponde a:  
    > $$128\text{ MB} = 128 \times 1024 \times 1024 = 134.217.728\text{ bytes}$$  
    > 3. Al dividir el tamaño total entre el tamaño del bloque:  
    > $$\frac{2.649.266.066}{134.217.728} = 19,7384$$  
    > Dado que HDFS no almacena fracciones de bloque incompletas sin asignar un bloque nuevo, se generan **19 bloques completos de 128 MB** ($19 \times 134.217.728 = 2.550.136.832\text{ bytes}$) más **1 vigésimo bloque final** con los $99.129.234\text{ bytes}$ restantes (~94,5 MB).  
    > Esto da exactamente **20 bloques físicos** (numerados del bloque 0 al bloque 19), como certifica la línea `Total blocks: 20` de la salida de `hdfs fsck`"* *(Ver Captura C3 en Anexos pág. 4)*.
* **Pregunta 2: ¿Por qué eligieron factor de replicación 2 y no 3 o 1?**
  * **Respuesta técnica y arquitectural:**  
    > *"Por la topología física real del clúster:  
    > Nuestro clúster cuenta con **únicamente 2 DataNodes trabajadores físicos** (`node2` y `node3`)* *(Ver Captura C1 en Anexos pág. 2)*.  
    > En la arquitectura HDFS, la política de colocación de bloques (*Block Placement Policy*) **prohíbe tajantemente almacenar dos réplicas del mismo bloque en el mismo nodo físico**, ya que no aportaría tolerancia a fallos ante una falla de hardware.  
    > Por lo tanto:  
    > - Si configurábamos **replicación 3**, HDFS intentaría buscar un tercer nodo que no existe y marcaría permanentemente los 20 bloques como **under-replicated** (sub-replicados), arrojando advertencias en el NameNode.  
    > - Si dejábamos **replicación 1**, perdíamos la tolerancia a fallos: la falla de un solo DataNode corrompería la mitad del archivo.  
    > - Con **replicación 2**, cada bloque físico tiene una copia idéntica en `node2` y otra en `node3`. Cada nodo almacena exactamente 20 réplicas ($20 \times 2 = 40$ réplicas en total), equilibrando el almacenamiento en 1,32 GB por nodo y garantizando alta disponibilidad sin degradar el clúster."*
* **Pregunta 3: ¿Qué significa exactamente que `hdfs fsck` devuelva «HEALTHY» y «Average block replication = 2.0»?**
  * **Respuesta técnica exacta:**  
    > *"Significa que el NameNode auditó la lista completa de bloques en memoria contra los reportes de bloques (*block reports*) de los DataNodes:  
    > 1. `Status: HEALTHY`: no existe ningún bloque corrupto (`Corrupt blocks: 0`) ni bloques perdidos (`Missing blocks: 0`)* *(Ver Captura C3 pág. 4)*.  
    > 2. `Average block replication = 2.0`: cada uno de los 20 bloques tiene exactamente 2 réplicas activas (`Under-replicated blocks: 0`, `Over-replicated blocks: 0`).  
    > 3. En la segunda imagen de C3, inspeccionamos el bloque 0 (ID `1073743300`, longitud 134.217.728 B) y el sistema certifica sus ubicaciones físicas simultáneas en `192.168.56.102` (`node2`) y `192.168.56.103` (`node3`)."*

---

### 🕒 Minuto 2:00 a 3:00 — Diapositiva 4: Flujo de Ingesta y Control Operativo (Incidente Resuelto)
*(Pasar a la diapositiva 4)*

* **Flujo de ingesta segura:**  
  *"Para evitar bloqueos de red por políticas institucionales del portal gubernamental, mantuvimos una copia persistente en Google Drive. Desde allí, mediante un comando `curl` con gestión de cookies y confirmación de token (`confirm=t`), descargamos el archivo directamente al nodo maestro y lo subimos a HDFS mediante `hdfs dfs -put` hacia `/caso/`"* *(Ver comandos en Anexo A3, Anexos pág. 21)*.
* **Manejo del Incidente Operativo (Anexo A4):**  
  *"Durante la sesión 1 del 9 de noviembre a las 9:30 a.m. se presentó un incidente operativo real: la descarga por red HTTP se cortó a los **2,65 GB** (2.649.266.066 bytes) en lugar de los 3,33 GB originales de la plataforma* *(Ver Anexo A4 en Anexos pág. 21 y Bitácora pág. 2)*.  
  *En lugar de descartar el archivo a ciegas o reiniciar una descarga pesada que consumiría el ancho de banda del laboratorio, realizamos una auditoría técnica inmediata en terminal Linux: comprobamos que la estructura tabular UTF-8, los delimitadores por coma y las 19 columnas estaban intactos, y que el corte coincidía con un fin de línea limpio. Al verificar que conservaba **8,86 millones de registros** representativos de los 1.126 municipios y superaba los 2 GB mínimos exigidos por la rúbrica, validamos técnicamente que no existía sesgo y procedimos con la ingesta a HDFS"* *(Ver Diapositiva 4 y Anexo A4)*.

#### ❓ BANCO DE PREGUNTAS DEL JURADO — DIAPOSITIVA 4
* **Pregunta 1: ¿Cómo justifican haber continuado con el archivo tras el corte de descarga en vez de volver a descargarlo?**
  * **Respuesta técnica y de gestión:**  
    > *"Profesora, la decisión se tomó aplicando criterios de ingeniería y auditoría de datos, no por azar:  
    > 1. **Auditoría de integridad tabular:** Ejecutamos en terminal comandos Linux de inspección:  
    >    - `tail -n 5`: verificó que la última línea no estaba truncada a mitad de un campo numérico o texto, sino que cerraba con un salto de línea limpio `\n`.  
    >    - `head -n 1`: verificó los 19 encabezados oficiales.  
    >    - `awk -F',' '{print NF}'`: certificó que todas las filas mantenían exactamente 19 columnas.  
    > 2. **Cumplimiento estricto de rúbrica:** La rúbrica exigía un dataset descomprimido de entre 2 y 5 GB. El archivo descargado pesó **2,47 GB** (2.649.266.066 B), cumpliendo el requerimiento.  
    > 3. **Representatividad estadística:** Comprobamos que incluía **8.860.888 registros** cubriendo la totalidad de los 1.126 municipios y 34 departamentos del país, y más de 3,88 millones de filas del año 2026. No hubo sesgo geográfico ni temporal.  
    > 4. **Eficiencia operativa:** Reiniciar una descarga de 3,3 GB sobre el canal compartido de la sala Deep Learning habría consumido la mayor parte de la sesión práctica sin aportar valor metodológico ni alterar los hallazgos analíticos. Toda la decisión quedó documentada en el Incidente I1 del Anexo A4"* *(Ver Anexo A4 pág. 21)*.
* **Pregunta 2: ¿Cuáles fueron los comandos de terminal exactos usados para la ingesta y verificación?**
  * **Respuesta técnica:**  
    > *"Los comandos exactos ejecutados en el nodo maestro fueron* *(Ver Anexo A3 en Anexos pág. 21)*:  
    > 1. Descarga desde respaldo: `curl -L -c ./cookies.txt "https://docs.google.com/uc?export=download&confirm=t&id=..." -o Cifras_de_V_ctimas_por_Hechos_Municipal.csv`  
    > 2. Creación de carpeta HDFS: `hdfs dfs -mkdir -p /caso`  
    > 3. Subida distribuida con réplica 2: `hdfs dfs -put Cifras_de_V_ctimas_por_Hechos_Municipal.csv /caso/`  
    > 4. Verificación de bloques y salud: `hdfs fsck /caso/Cifras_de_V_ctimas_por_Hechos_Municipal.csv -files -blocks -locations`  
    > 5. Verificación de réplica: `hdfs dfs -stat %r /caso/Cifras_de_V_ctimas_por_Hechos_Municipal.csv` (retornó `2`)."*

---

## COMPONENTE B: PROCESAMIENTO DISTRIBUIDO EN APACHE SPARK
**Tiempo asignado:** Exactamente 3 minutos (180 segundos).  
**Diapositivas:** 5, 6 y 7.  
**Objetivo de evaluación:** Justificar la configuración de SparkSession sobre YARN, el esquema `StructType`, el tratamiento de fechas, las transformaciones complejas y la auditoría del plan físico con AQE.

---

### 🕒 Minuto 0:00 a 1:00 — Diapositiva 5: Ejecución Distribuida: Spark sobre YARN
*(Decir con naturalidad al pasar a la diapositiva 5)*

* **Configuración de recursos sobre YARN (justificación técnica):**  
  *"Configuramos explícitamente la SparkSession sobre YARN bajo el ID de referencia `application_1794236810279_0001` de la sesión 1, visible en la Captura C4* *(Ver Anexos pág. 5)*. *Asignamos **2 ejecutores en YARN** (uno en `node2` y otro en `node3`), cada uno con **2 núcleos** y **2 GB de memoria RAM**, sumando 4 núcleos de cómputo y 1,0 GiB de almacenamiento de ejecución por ejecutor"* *(Ver Capturas C4 y C5b en Anexos págs. 5 y 7)*.
* **Ajuste de particiones de shuffle y esquema estricto:**  
  *"Un parámetro crítico que modificamos fue `spark.sql.shuffle.partitions`, reduciéndolo de su valor por defecto de 200 a **8** particiones* *(Ver Captura C5 en Anexos pág. 6)*. *Esto evitó generar cientos de tareas minúsculas y permitió procesar exactamente 2 olas balanceadas de 4 tareas sobre los 4 núcleos del clúster. Asimismo, prohibimos tajantemente `inferSchema=true` y definimos un esquema explícito con `StructType`* *(Ver Notebook celdas S2 y S3)*: *esto evitó que Spark realizara un primer escaneo completo sobre los 2,47 GB de HDFS solo para inferir tipos de datos, ahorrando millones de lecturas de disco y protegiendo la memoria del driver"* *(Ver Diapositiva 5)*.

#### ❓ BANCO DE PREGUNTAS DEL JURADO — DIAPOSITIVA 5
* **Pregunta 1: ¿Por qué configuraron `spark.sql.shuffle.partitions = 8` y qué habría ocurrido con el valor por defecto de 200?**
  * **Respuesta técnica y matemática:**  
    > *"Profesora, por defecto Spark asigna 200 particiones en cada operación de shuffle (`groupBy`, `join`).  
    > Sin embargo, la topología de nuestro clúster solo tiene **2 ejecutores $\times$ 2 cores = 4 cores de cómputo en total**.* *(Ver Captura C4 pág. 5 y C5b pág. 7)*.  
    > - Si dejábamos 200 particiones, Spark intentaría despachar 200 tareas. Cada una procesaría fragmentos ínfimos de datos, pero acarrearía el sobrecosto (*overhead*) de scheduling, serialización, apertura de conexiones HTTP y gestión de estados en el DAGScheduler y TaskScheduler. 200 tareas sobre 4 núcleos habrían requerido 50 olas secuenciales de 4 tareas, colapsando el driver y degradando el tiempo de ejecución.  
    > - Al configurar `spark.sql.shuffle.partitions = 8`, Spark despacha exactamente:  
    > $$\frac{8\text{ tareas}}{4\text{ núcleos}} = \textbf{2 olas de tareas concurrentes}$$  
    > Cada núcleo ejecuta exactamente 2 tareas con un tamaño de datos óptimo, manteniendo el clúster ocupado al 100 % sin generar saturación de red ni congestión de buffers de shuffle."*
* **Pregunta 2: ¿Por qué prohibieron `inferSchema=true` y qué beneficio aportó `StructType`?**
  * **Respuesta técnica de Big Data:**  
    > *"En Big Data, `inferSchema=true` es un antipatrón sobre archivos masivos. Si se activa sobre un CSV de 2,47 GB en HDFS:  
    > 1. Spark está obligado a lanzar un job preliminar que lee los **20 bloques completos de HDFS (2,47 GB)** solo para analizar texto y adivinar si una columna es String, Integer o Timestamp.  
    > 2. Luego, lanza el job de carga real, volviendo a leer los **20 bloques por segunda vez**. Duplica el I/O de red y disco.  
    > 3. Al definir el esquema de 19 columnas de forma explícita mediante `StructType` y `StructField` en la celda `S2` del Notebook, el driver inyecta los metadatos directamente a Catalyst en microsegundos. Spark realiza **una única pasada de lectura**, ahorrando tiempo de cómputo y evitando fallos de memoria en el driver."*
* **Pregunta 3: ¿Dónde verificamos que la aplicación corrió en YARN y no en modo local?**
  * **Respuesta técnica con evidencia:**  
    > *"En la Captura C4 del Anexo A1* *(pág. 5)*, *se observa el ResourceManager de YARN en el puerto 8088 con el ID `application_1794236810279_0001` en estado RUNNING/FINISHED, tipo SPARK, cola 'default', con 2 contenedores de ejecutores asignados en los DataNodes `node2` y `node3`."*

---

### 🕒 Minuto 1:00 a 2:00 — Diapositiva 6: Calidad de Datos y Limpieza Nativa de Fechas
*(Pasar a la diapositiva 6)*

* **Calidad y completitud del censo:**  
  *"En la exploración inicial comprobamos una calidad técnica excepcional: las variables clave del negocio (Departamento, Municipio, Hecho Victimizante, Etnia y Sexo) registraron **0 % de valores nulos**, cubriendo los 1.126 municipios del país"* *(Ver Notebook celda S4a y S4b)*.
* **El reto técnico de las fechas multiformato (2026):**  
  *"El gran obstáculo técnico estuvo en la columna `FECHA_CORTE`: el dataset mezclaba registros con año de 4 dígitos (`dd/MM/yyyy`), de 2 dígitos (`dd/MM/yy`), formato ISO (`yyyy/MM/dd`) y estampas con hora* *(Ver Notebook celda S4c)*.  
  *En lugar de recurrir a una función definida por el usuario (UDF) de Python —lo cual habría destruido el rendimiento del clúster al obligar a serializar millones de filas entre la JVM de Java y el intérprete de Python fila por fila—, implementamos una solución distribuida 100 % nativa en Spark SQL: combinamos `substring`, `length` y `coalesce(to_date)`. Esto permitió aislar con precisión matemática los **3,88 millones de filas del año 2026** evaluadas de forma totalmente paralela y vectorizada"* *(Ver Notebook celda S4d y Diapositiva 6)*.

#### ❓ BANCO DE PREGUNTAS DEL JURADO — DIAPOSITIVA 6
* **Pregunta 1: ¿Por qué es tan grave usar una UDF de Python (`@udf`) en PySpark y cómo lo resolvieron de forma nativa?**
  * **Respuesta técnica profunda (JVM vs Py4J):**  
    > *"Profesora, PySpark corre sobre la Java Virtual Machine (JVM). Cuando se escribe una UDF en Python puro (`def parse_fecha(f): ...`):  
    > 1. Para cada una de las 8,86 millones de filas, los ejecutores JVM deben **serializar** los datos con Py4J/Pickle y transferirlos por sockets IPC hacia un proceso trabajador de Python (`python daemon`).  
    > 2. El proceso Python ejecuta la función en una sola CPU y **vuelve a serializar** el resultado para enviarlo de vuelta a la JVM.  
    > 3. Esto destruye las optimizaciones del optimizador Catalyst y del compilador de código en memoria Tungsten, multiplicando el uso de CPU y provocando cuellos de botella por recolección de basura (*Garbage Collection*).  
    > **Nuestra solución nativa:** Usamos funciones internas de `pyspark.sql.functions`:  
    > ```python
    > F.coalesce(
    >     F.to_date(F.col("FECHA_CORTE"), "dd/MM/yyyy"),
    >     F.to_date(F.col("FECHA_CORTE"), "yyyy/MM/dd"),
    >     F.to_date(F.substring(F.col("FECHA_CORTE"), 1, 10), "dd/MM/yyyy")
    > )
    > ```  
    > Al ser nativas de Spark SQL, Catalyst compila las expresiones directamente en bytecode de Java en Tungsten, ejecutándose a nivel de registros en memoria a velocidad de C/Java sin tocar el intérprete de Python"* *(Ver Notebook celda S4c y S4d)*.

---

### 🕒 Minuto 2:00 a 3:00 — Diapositiva 7: Transformaciones Complejas y Auditoría AQE
*(Pasar a la diapositiva 7)*

* **Las dos transformaciones complejas del caso:**  
  *"Cumplimos con las transformaciones complejas mediante dos operaciones mayores:  
  1. Una **agregación multidimensional pesada** (`groupBy` por departamento, municipio, hecho victimizante, etnia y sexo), calculando la brecha real restando personas ubicadas de declaradas, lo que redujo el censo de 8,86 millones a **318.416 registros agregados**.* *(Ver Notebook celdas S5 y S6)*.  
  2. Un **Broadcast Hash Join** donde cruzamos este consolidado contra un catálogo maestro del DANE de solo 33 filas para asignar la Región Natural de cada evento sin mover la tabla de víctimas por la red"* *(Ver Notebook celda S6 y Diapositiva 7)*.
* **Auditoría Catalyst y Adaptive Query Execution (AQE):**  
  *"Auditamos el plan físico mediante `.explain()`. Como muestra la celda del notebook y la Captura C6* *(Ver Anexos pág. 8)*, *el plan preliminar reportó `AdaptiveSparkPlan isFinalPlan=false`, pues Spark tenía planificados intercambios de shuffle genéricos. Al ejecutarse la acción en el clúster, AQE analizó las estadísticas en tiempo de ejecución: comprobó que el catálogo DANE era diminuto, activó `AQEShuffleRead`, fusionó particiones intermedias y consolidó el plan adaptativo final con `isFinalPlan=true`, visible en la interfaz SQL de la Spark UI (Captura C7)"* *(Ver Anexos págs. 9 y 10 y Diapositiva 7)*.

#### ❓ BANCO DE PREGUNTAS DEL JURADO — DIAPOSITIVA 7
* **Pregunta 1: ¿Qué significa exactamente `AdaptiveSparkPlan isFinalPlan=false` y en qué momento exacto cambia a `true`?**
  * **Respuesta técnica (Catalyst vs AQE):**  
    > *"Profesora:  
    > - `isFinalPlan=false` aparece en la celda `S8a` del Notebook y en la Captura C6* *(Anexos pág. 8)* *cuando invocamos `.explain()`. En este punto, Spark Catalyst solo ha creado el **plan lógico y físico inicial**, previo a la ejecución. Como Spark evalúa de forma perezosa (*lazy evaluation*), aún no ha leído los datos reales de HDFS y no conoce sus tamaños exactos; por tanto, asume un plan conservador con operadores genéricos `Exchange hashpartitioning`.  
    > - El cambio a `isFinalPlan=true` ocurre en el momento exacto en que se dispara una **acción** (en nuestro caso, `consulta_principal.persist()` y el primer `.count()` en la celda `S8b`).  
    > Al ejecutarse la acción, se activa **Adaptive Query Execution (AQE)**. AQE monitorea los datos al final de cada stage intermedio:  
    > 1. Detectó que la tabla DANE tenía apenas 33 filas (menos de 10 MB).  
    > 2. Reemplazó el operador de Sort Merge Join planificado por un `BroadcastHashJoin`.  
    > 3. Aplicó `CoalesceShufflePartitions` para unir particiones vacías en el operador `AQEShuffleRead`.  
    > El plan adaptativo final optimizado se certifica en la pestaña SQL de Spark UI con `isFinalPlan=true`"* *(Ver Captura C7 en Anexos págs. 9 y 10)*.
* **Pregunta 2: ¿Cuáles son las columnas agrupadas y qué calculó la métrica de brecha?**
  * **Respuesta técnica:**  
    > *"Agrupamos por `(COD_ESTADO_DEPTO, ESTADO_DEPTO, CIUDAD_MUNICIPIO, HECHO, ETNIA, SEXO)`.  
    > Agregamos `sum(TOTAL_DECLARANTES)` y `sum(TOTAL_UBICADOS)`.  
    > Y calculamos la columna `BRECHA_ATENCION = TOTAL_DECLARANTES - TOTAL_UBICADOS`. Esto refleja cuántas víctimas registraron el hecho en el municipio pero no han sido efectivamente atendidas o asentadas en el territorio"* *(Ver Notebook celda S5)*.

---

## COMPONENTE C: DESEMPEÑO EN SPARK UI, OPTIMIZACIONES Y DICTAMEN CLOUD
**Tiempo asignado:** Exactamente 3 minutos (180 segundos).  
**Diapositivas:** 8, 9 y 10.  
**Objetivo de evaluación:** Analizar cuellos de botella en Spark UI, justificar la persistencia, defender la bonificación de joins (+0,3), presentar hallazgos del RUV y dictaminar la viabilidad cloud frente a BigQuery.

---

### 🕒 Minuto 0:00 a 1:00 — Diapositiva 8: Análisis de Stages en Spark UI y Persistencia
*(Decir con naturalidad al pasar a la diapositiva 8)*

* **Identificación del cuello de botella (Stage 30 vs Stage 48):**  
  *"Al inspeccionar la Spark UI identificamos que el **Stage 30** fue el verdadero cuello de botella de todo el procesamiento: demandó **36 segundos de tiempo acumulado de tareas** leyendo los 2,47 GB desde los 20 bloques de HDFS y realizando la agregación parcial en cada nodo, escribiendo **13,8 MiB de shuffle** (318.416 registros)* *(Ver Captura C8 en Anexos pág. 11)*. *En contraste, el **Stage 48** consumió ese shuffle en solo **0,8 segundos** a través de 6 tareas con una mediana de 0,2 segundos por tarea* *(Ver Captura C9 en Anexos pág. 13)*. *En todo el job verificamos **cero bytes de spill a memoria y cero bytes de spill a disco**, demostrando que la asignación de 2 GB de RAM por ejecutor fue perfectamente dimensionada"* *(Ver Diapositiva 8)*.
* **Persistencia en memoria (Req 8):**  
  *"Dado que el DataFrame consolidado fue reutilizado por dos acciones consecutivas (el reporte regional y el top 10 municipal), aplicamos persistencia con nivel `MEMORY_AND_DISK`. En la pestaña Storage (Captura C10) comprobamos que el RDD 162 quedó cacheado al **100 % en memoria**, ocupando únicamente **625,5 KiB** sin tocar disco, y liberamos los recursos al finalizar con `.unpersist()`"* *(Ver Captura C10 en Anexos pág. 15, Notebook celda S10f y Diapositiva 8)*.

#### ❓ BANCO DE PREGUNTAS DEL JURADO — DIAPOSITIVA 8
* **Pregunta 1: Explíqueme por qué el Stage 30 tardó 36 segundos mientras que el Stage 48 tardó menos de un segundo. ¿Qué ocurrió técnicamente?**
  * **Respuesta técnica detallada con métricas:**  
    > *"Profesora, la diferencia se explica por el tipo de operación computacional de cada stage* *(Ver Capturas C8 y C9 en Anexos págs. 11 y 13)*:  
    > 1. **Stage 30 (Cuello de botella de I/O y procesamiento Map):**  
    >    - Despachó **20 tareas**, una por cada uno de los 20 bloques de 128 MB en HDFS.  
    >    - Tuvo que leer físicamente **2,5 GiB (2,47 GB)** de datos crudos desde el disco de los DataNodes (`node2` y `node3`).  
    >    - Tuvo que deserializar **8.860.888 filas**, evaluar el `StructType`, aplicar el filtrado y limpieza de fechas de 2026, y ejecutar el hash map preliminar de la agregación por 6 dimensiones.  
    >    - La suma de tiempo de CPU de esas 20 tareas fue de **36 segundos**.  
    >    - Su salida fue escribir **13,8 MiB de Shuffle Write** (318.416 filas consolidadas).  
    > 2. **Stage 48 (Procesamiento Reduce ligero en memoria):**  
    >    - Solo tuvo que leer a través de la red los **13,8 MiB** generados por el Stage 30 (en 6 tareas).  
    >    - Como los datos ya venían reducidos en un 99 % desde el Stage 30, no hubo lectura de disco masiva. Completó la agregación global en **0,8 segundos**, con una mediana de 0,2 segundos por tarea.  
    > Ambas tablas Summary Metrics confirman **Spill (Memory): 0 B** y **Spill (Disk): 0 B**."*
* **Pregunta 2: ¿Por qué aplicaron persistencia (`cache`/`persist`), qué Storage Level usaron y por qué liberaron la memoria?**
  * **Respuesta técnica:**  
    > *"1. **Justificación:** El DataFrame enriquecido (`consulta_principal`) alimentó dos consultas de salida consecutivas: la agregación por Región Natural (Acción 2) y el cálculo del Top 10 de municipios con mayor brecha (Acción 3). Si no persistíamos, el DAG de Spark habría vuelto a leer los 2,47 GB de HDFS y repetido el Stage 30 completo para la segunda acción.  
    > 2. **Storage Level:** Usamos `StorageLevel.MEMORY_AND_DISK`. En la Captura C10* *(Anexos pág. 15)* *se observa que el RDD 162 se almacenó al **100 % en memoria (Cached Partitions: 8)**, consumiendo apenas **625,5 KiB** de RAM y **0 B en disco**.  
    > 3. **Liberación con `.unpersist()`:** En la celda `S10f` ejecutamos `.unpersist()` para liberar inmediatamente los buffers en los ejecutores YARN, una práctica obligatoria de ingeniería para prevenir fugas de memoria (*memory leaks*) en clústeres multi-usuario."*

---

### 🕒 Minuto 1:00 a 2:00 — Diapositiva 9: Bonificación (+0,3): Comparación Empírica de Joins
*(Pasar a la diapositiva 9)*

* **El experimento físico de optimización:**  
  *"Optamos por la bonificación técnica comparando empíricamente dos estrategias físicas de join sobre la misma operación de cruce entre el censo de víctimas y el catálogo del DANE* *(Ver Notebook celdas S7a, S7b y S7c)*:  
  *En el primer escenario forzamos **Sort Merge Join** desactivando el broadcast con `spark.sql.autoBroadcastJoinThreshold = -1`. Esto obligó a Spark a ordenar ambas tablas por clave y redistribuirlas a través de la red, generando **31,2 KiB de Shuffle Write** en el Stage 48 y un tiempo de ejecución de **9,21 segundos**"* *(Ver Captura C11 en Anexos págs. 16 y 17)*.
* **Eliminación del shuffle con Broadcast Hash Join:**  
  *"En el segundo escenario restauramos el umbral por defecto (10 MB). Spark identificó que el catálogo DANE pesaba menos de 10 MB y envió una copia ligera a la memoria de cada ejecutor mediante **Broadcast Hash Join**. Como vemos en la Captura C11* *(Anexos pág. 18)*, *la tabla grande de víctimas no tuvo que redistribuirse por la red, el shuffle write colapsó a apenas **354 bytes** y el tiempo bajó a **8,96 segundos**, eliminando por completo el tráfico de red entre nodos"* *(Ver Diapositiva 9)*.

#### ❓ BANCO DE PREGUNTAS DEL JURADO — DIAPOSITIVA 9
* **Pregunta 1: ¿Cómo funciona internamente el Broadcast Hash Join y por qué colapsó el shuffle a 354 bytes?**
  * **Respuesta técnica:**  
    > *"Profesora:  
    > - En un **Sort Merge Join (SMJ)**, Spark debe calcular el hash de la clave de unión en ambas tablas, redistribuir ambas tablas a través de la red hacia los ejecutores según ese hash (*Shuffle Exchange*), y luego ordenar (*sort*) los registros en cada ejecutor antes de combinarlos. Eso generó **31,2 KiB de shuffle** y tardó **9,21 segundos** en el Stage 48* *(Ver Captura C11 pág. 17)*.  
    > - En el **Broadcast Hash Join (BHJ)**, el driver toma la tabla pequeña del DANE (33 filas), construye una tabla hash en memoria y transmite (*broadcast*) esa copia a los 2 ejecutores. Los ejecutores leen sus particiones locales de la tabla grande de víctimas y hacen una búsqueda directa (*lookup*) en memoria contra la tabla hash pequeña.  
    > La tabla grande **nunca se mueve por la red ni se ordena en disco**. Los 354 bytes restantes de shuffle write corresponden únicamente a metadatos de sincronización entre tareas, reduciendo el tráfico de red a prácticamente cero y bajando el tiempo a **8,96 segundos**"* *(Ver Captura C11 pág. 18)*.

---

### 🕒 Minuto 2:00 a 3:00 — Diapositiva 10: Hallazgos de Consultoría y Dictamen Cloud (BigQuery)
*(Pasar a la diapositiva 10)*

* **Hallazgos del negocio y Patrón Big Data:**  
  *"Aplicamos con rigor el patrón Big Data: procesamos 8,86 millones de registros de forma paralela en los ejecutores del clúster y solo realizamos la reducción a **10 filas** antes de ejecutar `.toPandas()` para graficar* *(Ver Notebook celda S10a)*.  
  *Los resultados revelan una brecha asistencial alarmante para la UARIV: **el 28 % de las personas declarantes en el RUV no cuenta con registro de ubicación territorial ni atención efectiva en el municipio receptor**. Además, Buenaventura lidera el impacto nacional con cerca de 9,88 millones de hechos acumulados, seguido por Medellín y Tumaco"* *(Ver Informe pág. 2 y Diapositiva 10)*.
* **Recomendación tecnológica: Clúster On-Premise vs Google BigQuery:**  
  *"Para la entidad contratante, aunque nuestro clúster local procesó el análisis en **10,28 segundos** sin cobro directo de licencia* *(Ver Notebook celda S8b)*, *mantener infraestructura propia exige asumir costos fijos de energía, mantenimiento, soporte ante caídas de red y una alta curva de administración de Hadoop y YARN* *(Ver Anexo A5 en Anexos pág. 21)*.  
  *En contraste, en la práctica oficial demostramos que **Google BigQuery**, gracias a su modelo columnar serverless (Capacitor), solo requirió escanear **830 MB facturados** de una tabla de 6,97 GB (y **0 B cuando reutiliza el resultado en caché**)* *(Ver Hoja A2 en Anexos pág. 20)*, *cabiendo con holgura en el nivel gratuito de 1 TB mensual. Dictaminamos a la UARIV adoptar una **arquitectura híbrida**: conservar Spark para transformaciones batch nocturnas pesadas y migrar la analítica ad-hoc y tableros interactivos a BigQuery por su autoescalabilidad e integración nativa con streaming"* *(Ver Diapositiva 10 e Informe pág. 3)*.
* **Cierre formal:** *"Muchas gracias profesoras, quedamos atentos a sus preguntas individuales."*

#### ❓ BANCO DE PREGUNTAS DEL JURADO — DIAPOSITIVA 10
* **Pregunta 1: ¿Por qué y cómo aplicaron el patrón Big Data antes de `.toPandas()`? ¿Qué pasa si hago `.toPandas()` en el DataFrame original?**
  * **Respuesta técnica:**  
    > *"Si se ejecuta `df.toPandas()` sobre el DataFrame completo de 8,86 millones de filas, PySpark intentará serializar y enviar los 2,47 GB desde los ejecutores hacia el proceso driver en una sola máquina. Esto desbordaría la memoria RAM del heap de Java del driver, arrojando un error fatal `java.lang.OutOfMemoryError: Java heap space` y abortando la sesión.  
    > **Patrón aplicado:** El trabajo pesado de filtrado, agregación y ordenamiento se realiza distribuido en el clúster. Únicamente cuando los datos están reducidos a una matriz final minúscula (el Top 10 municipal de **10 filas** o la serie de tiempo de **9 filas**), se ejecuta `.toPandas()` en el driver para renderizar las figuras con Matplotlib usando paleta accesible Okabe-Ito y formato numérico colombiano"* *(Ver Notebook celda S10a a S10e)*.
* **Pregunta 2: Si BigQuery es tan eficiente, ¿por qué recomiendan una arquitectura híbrida en lugar de migrar el 100 % a la nube?**
  * **Respuesta de consultoría y arquitectura:**  
    > *"Por gobernanza de datos y costos de procesamiento por lotes repetitivos:  
    > 1. BigQuery cobra por bytes escaneados ($6,25 USD por TB excedente) o por tiempo de ranura (slots). Si la UARIV ejecuta pipelines de limpieza ETL pesados cada noche que reescanean terabytes de datos no particionados, el costo mensual en nube puede dispararse.  
    > 2. Mantener Spark en clúster (on-premise o cloud gestionado) permite ejecutar transformaciones complejas por lotes con costo predecible.  
    > 3. En cambio, para los analistas de negocio que lanzan consultas SQL interactivas y tableros BI, BigQuery es imbatible: escaneó apenas **830 MB** (menos de un gigabyte) gracias a que solo leyó las columnas involucradas en el query, tardó segundos y costó 0 dólares dentro del 1 TB gratis"* *(Ver Anexo A5 pág. 21)*.

---

## 🎤 SIMULACRO DE PREGUNTAS INDIVIDUALES Y DEFENSA CRUZADA

### Caso 1: Julián Steven Duarte
* **Si defiende Componente A (Almacenamiento/HDFS):**  
  > *"Profesora, el censo se almacenó en HDFS bajo replicación 2 balanceada entre `node2` y `node3`, generando 20 bloques físicos de 128 MB (40 réplicas en total). El comando `hdfs fsck` certificó estado 100 % HEALTHY y auditamos el bloque 0 en ambos nodos (Captura C3 pág. 4). Además, ante el corte de descarga a 2,65 GB (Anexo A4 pág. 21), auditamos en Linux la integridad de las 19 columnas y la representación en 1.126 municipios antes de proceder."*
* **Pregunta cruzada si le sortean Componente B (Spark):**  
  > *"Configuramos la sesión con 2 ejecutores de 2 cores y 2 GB de RAM, ajustando `shuffle.partitions` a 8 para procesar exactamente 2 olas de 4 tareas sobre los 4 cores del clúster (Captura C5 pág. 6). Limpiamos las fechas multiformato con funciones nativas de Spark SQL (`substring` y `coalesce`) evitando UDFs de Python para mantener la ejecución vectorizada en Catalyst (Notebook celda S4c)."*
* **Pregunta cruzada si le sortean Componente C (Spark UI/Cloud):**  
  > *"En la Spark UI demostramos que el Stage 30 fue el cuello de botella por leer 2,47 GB de HDFS en 36 segundos (Captura C8 pág. 11), sin ningún spill a disco. Comprobamos la superioridad de Broadcast Hash Join reduciendo el shuffle de 31,2 KiB a 354 bytes (Captura C11 pág. 18), y dictaminamos arquitectura híbrida con BigQuery, que solo escaneó 830 MB facturados (Anexo A5 pág. 21)."*

### Caso 2: Juan Tomás Rincón
* **Si defiende Componente B (Procesamiento/Spark):**  
  > *"Profesora, el esquema se definió con `StructType` para evitar que Spark escaneara los 2,47 GB dos veces. Auditamos el plan físico: al inicio aparecía `AdaptiveSparkPlan isFinalPlan=false` en Catalyst (Captura C6 pág. 8), pero en tiempo de ejecución AQE analizó los tamaños, reemplazó el join por Broadcast Hash Join y consolidó el plan final con `isFinalPlan=true` (Captura C7 pág. 9)."*
* **Pregunta cruzada si le sortean Componente A (HDFS/Ingesta):**  
  > *"El dataset se descargó vía curl y se subió con `hdfs dfs -put` a `/caso`. Sus 2.649.266.066 bytes generaron 20 bloques físicos de 128 MB con factor de replicación 2 en `node2` y `node3`, auditados con `hdfs fsck` sin bloques corruptos ni bajo-replicados (Captura C2 y C3 págs. 3 y 4)."*
* **Pregunta cruzada si le sortean Componente C (Spark UI/Cloud):**  
  > *"Persistimos el DataFrame consolidado con nivel `MEMORY_AND_DISK`, verificando en la pestaña Storage que el RDD 162 quedó al 100 % en RAM ocupando apenas 625,5 KiB (Captura C10 pág. 15). Aplicamos el patrón Big Data reduciendo a 10 filas antes de `.toPandas()` para proteger la memoria del driver (Notebook celda S10a)."*

### Caso 3: Julián Camilo Jiménez
* **Si defiende Componente C (Spark UI/Optimización/Cloud):**  
  > *"Profesora, analizamos la disparidad entre el Stage 30 (36 s por I/O pesada de 2,47 GB) y el Stage 48 (0,8 s por leer shuffle ya reducido a 13,8 MiB). En la bonificación (+0,3), desactivar el broadcast forzó Sort Merge Join con 31,2 KiB de shuffle y 9,21 s, mientras que Broadcast Hash Join eliminó el tráfico de red bajando a 354 bytes y 8,96 s (Captura C11 págs. 16-18). Para BigQuery, solo se escanearon 830 MB facturados (Anexo A5 pág. 21)."*
* **Pregunta cruzada si le sortean Componente A (HDFS/Ingesta):**  
  > *"La topología del clúster cuenta con 2 DataNodes de 58,2 GB cada uno (`node2` y `node3`). Por eso el factor de replicación se fijó en 2: poner replicación 3 habría dejado bloques under-replicated porque HDFS prohíbe poner dos réplicas en una misma máquina física. Los 20 bloques quedaron perfectamente balanceados con 20 réplicas por nodo (Captura C1 y C3 págs. 2 y 4)."*
* **Pregunta cruzada si le sortean Componente B (Spark/Fechas):**  
  > *"Prohibimos `inferSchema` para evitar doble escaneo del disco y configuramos `shuffle.partitions = 8` adaptado a los 4 cores del clúster (Capturas C4 y C5 págs. 5-6). Las dos transformaciones complejas fueron el groupBy multidimensional que redujo de 8,86 millones a 318.416 filas calculando la brecha de atención, y el join con la tabla DANE de 33 filas (Notebook celdas S5 y S6)."*

---

## ⏱️ RESUMEN DE TIEMPOS POR PARTICIPANTE
| Segmento | Diapositivas | Duración Máxima | Control del Tiempo |
| :--- | :---: | :---: | :--- |
| **Intro breve** | Diapositiva 1 | 15 segundos | A cargo del primer expositor sorteado |
| **Componente A** | Diapositivas 2, 3 y 4 | 2 min 45 s | Diapositiva 2 (1 min) · Diapositiva 3 (1 min) · Diapositiva 4 (45 s) |
| **Componente B** | Diapositivas 5, 6 y 7 | 3 min 00 s | Diapositiva 5 (1 min) · Diapositiva 6 (1 min) · Diapositiva 7 (1 min) |
| **Componente C** | Diapositivas 8, 9 y 10 | 3 min 00 s | Diapositiva 8 (1 min) · Diapositiva 9 (1 min) · Diapositiva 10 (1 min) |
| **Total exposición equipo** | 10 diapositivas | **9 minutos exactos** | Cronómetro en mano; corte automático a los 3 min |
