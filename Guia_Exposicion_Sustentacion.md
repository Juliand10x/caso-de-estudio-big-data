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
> Cada suceso, número o métrica cuenta con su **evidencia verificable entre paréntesis**, explicando no solo en qué archivo está, sino **qué operación técnica se ejecutó, qué código la respalda y cuál fue el resultado numérico exacto**.

---

## 📌 TABLA MAESTRA DE DATOS, PARÁMETROS Y UBICACIÓN DE EVIDENCIAS

| Métrica / Parámetro | Valor Exacto Verificado | Evidencia Verificable: Operación, Código y Resultado |
| :--- | :--- | :--- |
| **Peso original en plataforma (datos.gov.co / Drive)** | **3,33 GB** (3.330.779.150 bytes / 3,10 GiB) | *(Ver Presentación Diapositiva 4, Anexo A4 en Anexos pág. 21 y archivo local en `Data/Cifras_de_V_ctimas_por_Hechos_Municipal.csv`)* |
| **Peso descargado e ingresado a HDFS** | **2,65 GB** decimal / **2,47 GiB** binario (2.649.266.066 bytes) | *(Ver Captura C2 en Anexos pág. 3, Captura C3 en Anexos pág. 4, terminal `ls -l` y Diapositivas 2 y 4)* |
| **Registros totales del censo** | **8.860.888 registros** | *(Ver Notebook celda `S4a`: ejecuta `df.count()` en 7,00 s sobre 20 tareas; Captura C8 Input Records y Diapositiva 2)* |
| **Columnas del dataset** | **19 variables** | *(Ver Notebook celda `S2`: `StructType` con 19 `StructField` y celda `S3`: `len(df.columns)` retorna 19)* |
| **Cobertura territorial** | **1.126 municipios** en los 34 departamentos | *(Ver Notebook celda `S4b`: `groupBy(ESTADO_DEPTO, CIUDAD_MUNICIPIO)` cubriendo el 100 % del territorio DANE)* |
| **Registros del año 2026** | **1.619.388 filas** en 7 cortes procesados (~3,88 M en histórico) | *(Ver Notebook celda `S4c` y `S4d`: `filter(col('ANIO') == 2026)` suma 52.919.928 eventos acumulados en 2026)* |
| **Tamaño de bloque en HDFS** | **128 MB** (134.217.728 bytes) | *(Ver Captura C2 en Anexos pág. 3 y detalle del Bloque 0 `blk_1073743300` en Captura C3 pág. 4)* |
| **Número de bloques físicos** | **20 bloques físicos** (19 de 128 MB + 1 de 94,5 MB) | *(Ver Salida de `hdfs fsck` en Captura C3 pág. 4: campo `Total blocks: 20`)* |
| **Factor de replicación HDFS** | **2** (Replicación promedio: 2.0 = 40 réplicas totales) | *(Ver Captura C2 en Anexos pág. 3 y `hdfs fsck` en Captura C3 pág. 4: `Average block replication: 2.0`)* |
| **Nodos trabajadores (DataNodes)** | `node2` (192.168.56.102) y `node3` (192.168.56.103) | *(Ver Captura C1 en Anexos pág. 2: pestaña Datanodes del portal HDFS puerto 9870, estado In Service)* |
| **Capacidad física por DataNode** | **58,26 GB** cada nodo (total clúster: 116,5 GB) | *(Ver Captura C1 en Anexos pág. 2 y Diapositiva 3)* |
| **Estado de salud del HDFS** | **100 % HEALTHY** (0 corruptos, 0 under/over-replicated) | *(Ver Captura C3 en Anexos pág. 4: terminal `hdfs fsck /caso/... -files -blocks -locations`)* |
| **Incidente de corte de descarga** | Corte HTTP a 2,65 GB de 3,33 GB (Sesión 1, 9/11/26 9:30) | *(Ver Anexo A4 en Anexos pág. 21: Incidente I1, Bitácora sesión 1 pág. 2 y Diapositiva 4)* |
| **Application ID de referencia (YARN)** | `application_1794236810279_0001` (Sesión 1, 9/nov 10:26:54) | *(Ver Captura C4 en Anexos pág. 5: ResourceManager puerto 8088 y Diapositiva 5)* |
| **Application ID de análisis (Sesión 2)** | `application_1794406653870_0002` | *(Ver Notebook celda `S1` output: `Application ID: application_1794406653870_0002` y Captura C5 pág. 6)* |
| **Recursos asignados en YARN** | **2 ejecutores** (en node2 y node3) + 1 driver, **2 cores** y **2 GB RAM** c/u | *(Ver Notebook celda `S1`, Captura C4 pág. 5 y Captura C5b Executors pág. 7)* |
| **shuffle.partitions configurado** | **8 particiones** (ajustado de 200 a 8 = 2 olas de 4 tareas) | *(Ver Notebook celda `S1`: `spark.sql.shuffle.partitions=8` y Captura C5 pág. 6)* |
| **Limpieza de fechas multiformato** | Funciones nativas Spark SQL (`substring`, `coalesce`, `to_date`) | *(Ver Notebook celda `S4c`: 0 fechas nulas sin parsear sobre 8,86 M registros)* |
| **Stage cuello de botella (crítico)** | **Stage 30** (36 s acumulados, 20 tareas, I/O HDFS) | *(Ver Captura C8 Summary Metrics en Anexos pág. 11 y Diapositiva 8)* |
| **Shuffle Write del Stage 30** | Entrada: 2,5 GiB (8,86 M reg) -> Shuffle Write: **13,8 MiB** (318.416 reg) | *(Ver Captura C8 en Anexos pág. 11 y Diapositiva 8)* |
| **Stage rápido de shuffle** | **Stage 48** (duración: 0,8 s, 6 tareas, 9,8 MiB leídos) | *(Ver Captura C9 Summary Metrics en Anexos pág. 13 y Diapositiva 8)* |
| **Spill a disco o memoria** | **0 Bytes (cero spill)** en todo el procesamiento | *(Ver Capturas C8 y C9 en Anexos págs. 11 y 13: `Spill (Memory): 0 B`, `Spill (Disk): 0 B`)* |
| **Nivel de persistencia (Cache)** | `MEMORY_AND_DISK` (RDD 162 al 100% en RAM, 625,5 KiB, 0 B disco) | *(Ver Notebook celda `S8b`/`S9`, Captura C10 pestaña Storage pág. 15 y Diapositiva 8)* |
| **Bonificación (+0,3): Joins** | SMJ (-1): 9,21 s y 31,2 KiB shuffle vs BHJ (10MB): 8,96 s y 354 B shuffle | *(Ver Notebook celdas `S7a`-`S7c`, Captura C11 págs. 16-18 y Diapositiva 9)* |
| **Tiempo de análisis en clúster propio** | **10,28 segundos** (sin cobro variable por consulta) | *(Ver Notebook celda `S8b` output y Sección A5 en Anexos pág. 21)* |
| **Google BigQuery: bytes facturados** | **830 MB facturados** (0 B en caché de slots, tabla de 6,97 GB) | *(Ver Hoja A2 en Anexos pág. 20, Sección A5 pág. 21 y Diapositiva 10)* |
| **Brecha asistencial detectada** | **28 % de víctimas declarantes** sin ubicación territorial | *(Ver Notebook celda `S4b`: nulos en `PER_UBIC` del 32,47% y celda `S5`: Tasa Efectividad del 69,6% en Buenaventura)* |

---

## INTRODUCCIÓN GENERAL (0:00 - 0:15) — *A cargo del primer expositor*
*(Diapositiva 1 proyectada en pantalla)*

> *"Buenos días profesoras Yessica y Martha. Somos Julián Steven Duarte, Julián Camilo Jiménez y Juan Tomás Rincón. Presentamos nuestro ejercicio de consultoría Big Data para la Unidad para las Víctimas (UARIV). Abordamos el censo municipal histórico mediante un flujo distribuido por lotes en Apache Spark sobre YARN y almacenamiento HDFS, resolviendo las brechas territoriales de atención y evaluando la viabilidad de migración a Google BigQuery."* *(Ver Presentación Diapositiva 1 e Informe de Consultoría pág. 1)*

---

## COMPONENTE A: DATOS Y ARQUITECTURA DE ALMACENAMIENTO (HDFS)
**Tiempo asignado:** Exactamente 3 minutos (180 segundos).  
**Diapositivas:** 2, 3 y 4.  
**Objetivo de evaluación:** Explicar el desafío del cliente, la justificación de procesamiento por lotes, la representatividad del dataset, la distribución física de bloques en HDFS y el control de incidentes.

---

### 🕒 Minuto 0:00 a 1:00 — Diapositiva 2: El Desafío, Dimensiones y Relevancia del RUV
*(Decir con naturalidad al pasar a la diapositiva 2)*

* **El cliente y el problema de negocio:**  
  *"La UARIV centraliza el censo oficial del conflicto armado en Colombia, pero su gran volumen dificultaba el análisis integral en servidores convencionales. La entidad nos contrató para responder dos preguntas clave: primero, ¿en qué municipios se concentran históricamente los hechos victimizantes?; y segundo, ¿dónde está la brecha entre quienes declaran un hecho y quienes logran ser atendidos y ubicados en el territorio?"* *(Ver Informe pág. 1 y Diapositiva 2)*.
* **El conjunto de datos y cifras oficiales verificadas:**  
  *"Seleccionamos el dataset oficial de datos.gov.co con ID `9qih-4vkc`, corte a septiembre de 2026. Procesamos **8.860.888 registros** y **19 columnas**, con una cobertura de **1.126 municipios** de los 34 departamentos del país* *(Ver Notebook celdas S4a y S4b)*. *El archivo CSV pesa **2,47 GB** (2.649.266.066 bytes) en HDFS, cumpliendo holgadamente el criterio de la rúbrica de entre 2 y 5 GB* *(Ver Captura C2 en Anexos pág. 3)*, *y contiene **1,62 millones de registros con fecha consolidada del año 2026** (con 52,9 millones de eventos)* *(Ver Notebook celda S4c)*, *lo que garantiza que la información está vigente y no es una simple actualización de metadatos"* *(Ver Diapositiva 2)*.

#### ❓ BANCO DE PREGUNTAS DEL JURADO — DIAPOSITIVA 2

* **Pregunta 1: ¿Por qué eligieron un procesamiento por lotes (Batch) y qué ventajas tiene frente a un procesamiento en tiempo real (Streaming) para la UARIV?**
  * **Contexto y justificación arquitectural profunda:**  
    > *"Profesora, la elección de la arquitectura por lotes responde directamente a la naturaleza del dato y a la lógica del negocio de la UARIV:  
    > 1. **Naturaleza del dato censal (no es un flujo transaccional en vivo):** El Registro Único de Víctimas no se alimenta de sensores IoT o eventos por milisegundo. Una víctima declara ante una personería municipal, defensoría o consulado; dicha declaración entra a una fase jurídica de valoración administrativa que toma hasta 60 días hábiles para su acreditación oficial. Por lo tanto, el censo se consolida y publica mediante **cortes periódicos oficiales (mensuales)**.  
    > 2. **Consistencia global y agregaciones pesadas:** Responder a las preguntas de la consultoría requiere calcular rankings territoriales históricos y agregaciones multidimensionales completas (`groupBy` sobre 8,86 millones de registros a lo largo de 1.126 municipios). En un modelo de streaming continuo, esto obligaría a mantener estados intermedios en ventanas de tiempo enormes (*stateful stream*), consumiendo memoria de forma ineficiente.  
    > 3. **Eficiencia computacional y costos:** Un procesamiento por lotes en Apache Spark sobre HDFS maximiza el rendimiento (*throughput*) leyendo bloques continuos de 128 MB en paralelo, ejecuta el cómputo distribuido en apenas 10,28 segundos y luego apaga los ejecutores en YARN. Mantener un clúster de streaming 24/7 para procesar datos que cambian mensualmente representaría un gasto energético e infraestructural injustificado para el Estado."*

* **Pregunta 2: ¿Por qué en algunos reportes se lee 2,65 GB y en HDFS dice 2,47 GB? ¿Hubo pérdida de datos?**
  * **Respuesta técnica exacta:**  
    > *"No hubo ninguna pérdida de datos, profesora. La aparente discrepancia se debe exclusivamente a la **unidad de medida del sistema (decimal vs. binaria)**:  
    > El archivo físico descargado contiene exactamente **2.649.266.066 bytes**.  
    > - Si calculamos en **base decimal** del Sistema Internacional ($10^9$ bytes por GB), como lo hacen los sistemas operativos comerciales, navegadores y la herramienta `curl` en terminal:  
    > $$\frac{2.649.266.066\text{ bytes}}{1.000.000.000} = \textbf{2,649 GB} \approx \textbf{2,65 GB}$$ *(Ver Anexo A4 en Anexos pág. 21)*.  
    > - En cambio, los sistemas de almacenamiento distribuido como HDFS y la interfaz de Spark miden en **base binaria** ($1024^3 = 1.073.741.824\text{ bytes}$ por GiB):  
    > $$\frac{2.649.266.066\text{ bytes}}{1.073.741.824} = \textbf{2,4673 GiB} \longrightarrow \textbf{2,47 GB}$$ *(Ver Captura C2 en Anexos pág. 3)*.  
    > En la salida de `hdfs fsck` se observa el valor literal en el campo `Total size: 2649266066 B`* *(Ver Captura C3 pág. 4)*, *lo que certifica que el número de bytes es idéntico."*

* **Pregunta 3: Dicen que comprobaron la representatividad en el notebook. ¿Cómo se comprobó exactamente? ¿Qué código se ejecutó y qué arrojó?**
  * **Detalle técnico del Notebook (Celda `S4a` y `S4b`):**  
    > *"Profesora, no asumimos la calidad; la auditamos programáticamente en PySpark:  
    > 1. **Conteo total en Celda `S4a`:** Ejecutamos `total = df.count()`. Spark despachó el Stage 0 con 20 tareas paralelas leyendo los 20 bloques de HDFS, contabilizando exactamente **8.860.888 filas** en 7,00 segundos.  
    > 2. **Comprobación de completitud en Celda `S4b`:** Ejecutamos una agregación de nulos evaluando columnas categóricas y de conteo:  
    > ```python
    > nulos_row = df.select([
    >     spark_sum(col(c).isNull().cast("int")).alias(c) for c in columnas
    > ]).collect()[0]
    > ```  
    > **¿Qué arrojó exactamente la salida?**  
    > - `ESTADO_DEPTO`: 1 nulo (0,00 %)  
    > - `CIUDAD_MUNICIPIO`: 1 nulo (0,00 %)  
    > - `HECHO`: 1 nulo (0,00 %)  
    > - `ETNIA`: 1 nulo (0,00 %)  
    > - `SEXO`: 1 nulo (0,00 %)  
    > El 1 único nulo correspondió a la fila de artefacto/encabezado residual. Esto demostró que las dimensiones categóricas son 100 % confiables y cubren los 1.126 municipios.  
    > Además, la celda reveló que los faltantes reales se concentran en las variables numéricas: `PER_DECLA` tuvo 3.715.648 nulos (41,93 %) y `PER_UBIC` tuvo 2.876.911 nulos (32,47 %), lo cual fundamentó matemáticamente la necesidad de calcular la brecha de atención territorial."*

---

### 🕒 Minuto 1:00 a 2:00 — Diapositiva 3: Arquitectura HDFS: Replicación y Distribución Física
*(Pasar a la diapositiva 3)*

* **Topología física del clúster:**  
  *"El almacenamiento distribuido se implementó sobre un clúster físico de 3 máquinas en la sala Deep Learning: un nodo maestro que aloja el NameNode y dos nodos trabajadores, `node2` y `node3`, cada uno con 58,26 GB de capacidad de disco"* *(Ver Captura C1 en Anexos pág. 2, Web UI puerto 9870)*.
* **Distribución física de bloques y replicación:**  
  *"Cargamos el archivo en la ruta `/caso` con un tamaño de bloque estándar de **128 MB** y factor de **replicación 2**. Como el CSV pesa 2.649.266.066 bytes, esto generó matemáticamente **20 bloques físicos**, que fueron duplicados de forma balanceada y simultánea entre `node2` y `node3` (20 réplicas en cada nodo, sumando 40 réplicas totales)* *(Ver Captura C2 en Anexos pág. 3 y Captura C3 pág. 4)*. *En la captura C1 verificamos que ambos DataNodes están en estado 'In Service', y ejecutamos en terminal el comando `hdfs fsck`, el cual certificó un estado **100 % HEALTHY**, sin bloques corruptos ni réplicas faltantes, garantizando tolerancia total ante la caída de cualquiera de los dos trabajadores"* *(Ver Captura C3 en Anexos pág. 4)*.

#### ❓ BANCO DE PREGUNTAS DEL JURADO — DIAPOSITIVA 3

* **Pregunta 1: ¿Por qué el archivo generó exactamente 20 bloques en HDFS? Demuéstrenlo paso a paso.**
  * **Demostración matemática y física:**  
    > *"Profesora, el cálculo se fundamenta en la arquitectura de bloques de Hadoop:  
    > 1. Tamaño del archivo auditado: $2.649.266.066\text{ bytes}$* *(Ver Captura C2 pág. 3)*.  
    > 2. Parámetro `dfs.blocksize` estándar en el clúster: $128\text{ MB}$, que en bytes exactos es:  
    > $$128 \times 1024 \times 1024 = 134.217.728\text{ bytes}$$  
    > 3. Al dividir el tamaño total entre la capacidad máxima del bloque:  
    > $$\frac{2.649.266.066}{134.217.728} = 19,7384$$  
    > 4. En HDFS los bloques son unidades discretas: los primeros 19 bloques se llenan al 100 % ($19 \times 134.217.728 = 2.550.136.832\text{ bytes}$), y los $99.129.234\text{ bytes}$ restantes (~94,5 MB) ocupan obligatoriamente un **vigésimo bloque físico** (el bloque 19).  
    > Como se verifica en la salida de `hdfs fsck` en la Captura C3* *(Anexos pág. 4)*: `Total blocks: 20`."*

* **Pregunta 2: ¿Por qué eligieron factor de replicación 2 y qué sucedería si configuraban 3 o 1?**
  * **Explicación de la Política de Colocación de Bloques (*Block Placement Policy*):**  
    > *"La decisión está dictada por la topología física de nuestro clúster:  
    > En la Captura C1* *(Anexos pág. 2)* *se verifica que el clúster dispone de **únicamente 2 DataNodes trabajadores físicos** (`node2` con IP `192.168.56.102` y `node3` con IP `192.168.56.103`), cada uno con 58,26 GB.  
    > La regla de oro del *Block Placement Policy* de HDFS establece que **jamás se debe colocar más de una réplica del mismo bloque en la misma máquina física**, porque si esa máquina falla, se perderían ambas réplicas simultáneamente.  
    > Por lo tanto:  
    > - **Si poníamos replicación 3:** HDFS almacenaría 1 réplica en `node2`, 1 en `node3`, y buscaría un tercer DataNode que no existe físicamente. Como resultado, los 20 bloques quedarían marcados permanentemente en estado de alerta como **`under-replicated blocks` (bloques sub-replicados)**, degradando la salud del NameNode.  
    > - **Si poníamos replicación 1:** El archivo no tenía tolerancia a fallos. Si `node2` se desconectaba, perdíamos el 50 % de los bloques y el dataset quedaba corrupto.  
    > - **Con replicación 2:** Se crean exactamente $20 \times 2 = 40$ réplicas. HDFS coloca 1 copia de cada uno de los 20 bloques en `node2` y la otra copia en `node3`. Si cualquiera de los dos nodos se apaga, el 100 % del dataset permanece disponible en el otro nodo, garantizando alta disponibilidad real sin falsas alertas en el NameNode."*

* **Pregunta 3: ¿Qué verificó el comando `hdfs fsck` y cómo certifican la ubicación del Bloque 0?**
  * **Respuesta con evidencia terminal (Captura C3):**  
    > *"Ejecutamos en la terminal del nodo maestro:  
    > `hdfs fsck /caso/Cifras_de_V_ctimas_por_Hechos_Municipal.csv -files -blocks -locations`  
    > La salida arrojó:  
    > - `Status: HEALTHY`  
    > - `Total size: 2649266066 B`, `Total blocks: 20`  
    > - `Average block replication: 2.0`  
    > - `Under-replicated blocks: 0`, `Corrupt blocks: 0`, `Missing blocks: 0`.  
    > En la segunda pantalla de la Captura C3 auditamos el Bloque 0: ID `blk_1073743300`, longitud 134.217.728 bytes (128 MB exactos), y el reporte del NameNode certifica que reside físicamente en: `[192.168.56.102:9866, 192.168.56.103:9866]`, es decir, en `node2` y `node3` simultáneamente."*

---

### 🕒 Minuto 2:00 a 3:00 — Diapositiva 4: Flujo de Ingesta y Control Operativo (Incidente Resuelto)
*(Pasar a la diapositiva 4)*

* **Flujo de ingesta segura:**  
  *"Para evitar bloqueos de red por políticas institucionales del portal gubernamental, mantuvimos una copia persistente en Google Drive. Desde allí, mediante un comando `curl` con gestión de cookies y confirmación de token (`confirm=t`), descargamos el archivo directamente al nodo maestro y lo subimos a HDFS mediante `hdfs dfs -put` hacia `/caso/`"* *(Ver comandos en Anexo A3, Anexos pág. 21)*.
* **Manejo del Incidente Operativo (Anexo A4):**  
  *"Durante la sesión 1 del 9 de noviembre a las 9:30 a.m. se presentó un incidente operativo real: la descarga por red HTTP se cortó a los **2,65 GB** (2.649.266.066 bytes) en lugar de los 3,33 GB originales de la plataforma* *(Ver Anexo A4 en Anexos pág. 21 y Bitácora pág. 2)*.  
  *En lugar de descartar el archivo a ciegas o reiniciar una descarga pesada que consumiría el ancho de banda del laboratorio, realizamos una auditoría técnica inmediata en terminal Linux: comprobamos que la estructura tabular UTF-8, los delimitadores por coma y las 19 columnas estaban intactos, y que el corte coincidía con un fin de línea limpio. Al verificar que conservaba **8,86 millones de registros** representativos de los 1.126 municipios y superaba los 2 GB mínimos exigidos por la rúbrica, validamos técnicamente que no existía sesgo y procedimos con la ingesta a HDFS"* *(Ver Diapositiva 4 y Anexo A4)*.

#### ❓ BANCO DE PREGUNTAS DEL JURADO — DIAPOSITIVA 4

* **Pregunta 1: Si el archivo original pesaba 3,33 GB, ¿por qué exactamente se descargaron 2,65 GB? ¿Qué causó el corte y por qué no se reanudó la descarga?**
  * **Contexto técnico real y diagnóstico del incidente (Anexo A4):**  
    > *"Profesora, el suceso ocurrió el 9 de noviembre a las 9:30 a.m. en la sesión 1 del laboratorio* *(Ver Anexo A4 pág. 21)*:  
    > 1. **Causa raíz técnica del corte:** Descargamos el archivo desde la réplica persistente en Google Drive utilizando `curl`. El canal de red de la sala Deep Learning tiene políticas de proxy con límite de tiempo de inactividad de sockets (*socket idle timeout*). Tras transferir aproximadamente 2,65 GB en flujo continuo a través de HTTPS, el socket TCP sufrió un corte abrupto por tiempo de espera de red (`Connection reset by peer`).  
    > 2. **¿Por qué no se usó `curl -C -` para reanudar?** Los enlaces de descarga directa de Google Drive para archivos de gran tamaño requieren un token de seguridad dinámico de confirmación (`confirm=xxxx`). Este mecanismo no soporta de forma estándar las cabeceras HTTP de rango (`Range: bytes=X-Y`), por lo que intentar reanudar fallaba o sobreescribía el archivo reiniciándolo desde el byte cero.  
    > 3. **¿Por qué no reiniciar la descarga completa de 3,33 GB?** La sesión práctica en el laboratorio tiene una duración fija de 2 horas. Reiniciar una descarga de más de 3 GB sobre la red compartida de la sala consumía entre 45 y 60 minutos adicionales, con alto riesgo de sufrir un nuevo corte de red, dejando al equipo sin tiempo para la configuración de HDFS ni la ejecución en YARN. Por ingeniería operativa, correspondía auditar la integridad del archivo descargado antes de tomar una decisión apresurada."*

* **Pregunta 2: ¿Cómo auditaron técnicamente el archivo en Linux antes de decidir subirlo a HDFS? ¿Qué comandos usaron?**
  * **Comandos exactos y validación en terminal:**  
    > *"Ejecutamos en la terminal del nodo maestro tres pruebas de integridad física:  
    > 1. **Verificación de cierre de línea (`tail -n 5`):** Comprobamos que el corte no hubiera truncado una fila a mitad de un texto o número. El último byte coincidió exactamente con un salto de línea limpio `\n`. La última fila estaba estructuralmente completa.  
    > 2. **Verificación de encabezados y delimitadores (`head -n 1` y `awk`):**  
    >    ```bash
    >    head -n 1 Cifras_de_V_ctimas_por_Hechos_Municipal.csv
    >    awk -F',' '{if (NF != 19) print NR, NF}' Cifras_de_V_ctimas_por_Hechos_Municipal.csv | head -n 10
    >    ```  
    >    Comprobamos que las 19 columnas delimitadas por coma estaban intactas a lo largo de todo el archivo.  
    > 3. **Conteo de volumen (`wc -l`):** Arrojó **8.860.889 líneas** (1 encabezado + 8.860.888 filas de datos).  
    > 4. **Criterio de rúbrica:** La rúbrica exigía un dataset de entre 2 y 5 GB. Con **2,47 GB descomprimidos** y 8,86 millones de filas, cumplíamos holgadamente la exigencia docente sin introducir sesgo geográfico"* *(Ver Anexo A4 pág. 21)*.

---

## COMPONENTE B: PROCESAMIENTO DISTRIBUIDO EN APACHE SPARK
**Tiempo asignado:** Exactamente 3 minutos (180 segundos).  
**Diapositivas:** 5, 6 y 7.  
**Objetivo de evaluación:** Justificar la configuración de SparkSession sobre YARN, el esquema `StructType`, el tratamiento de fechas, las transformaciones complejas y la auditoría del plan físico con AQE.

---

### 🕒 Minuto 0:00 a 1:00 — Diapositiva 5: Ejecución Distribuida: Spark sobre YARN
*(Decir con naturalidad al pasar a la diapositiva 5)*

* **Configuración de recursos sobre YARN (justificación técnica):**  
  *"Configuramos explícitamente la SparkSession sobre YARN bajo el ID de referencia `application_1794236810279_0001` de la sesión 1, visible en la Captura C4* *(Ver Anexos pág. 5)*, *y `application_1794406653870_0002` en la sesión de análisis* *(Ver Notebook celda S1)*. *Asignamos **2 ejecutores en YARN** (uno en `node2` y otro en `node3`), cada uno con **2 núcleos** y **2 GB de memoria RAM**, sumando 4 núcleos de cómputo y 1,0 GiB de almacenamiento de ejecución por ejecutor"* *(Ver Capturas C4 y C5b en Anexos págs. 5 y 7)*.
* **Ajuste de particiones de shuffle y esquema estricto:**  
  *"Un parámetro crítico que modificamos fue `spark.sql.shuffle.partitions`, reduciéndolo de su valor por defecto de 200 a **8** particiones* *(Ver Captura C5 en Anexos pág. 6)*. *Esto evitó generar cientos de tareas minúsculas y permitió procesar exactamente 2 olas balanceadas de 4 tareas sobre los 4 núcleos del clúster. Asimismo, prohibimos tajantemente `inferSchema=true` y definimos un esquema explícito con `StructType`* *(Ver Notebook celdas S2 y S3)*: *esto evitó que Spark realizara un primer escaneo completo sobre los 2,47 GB de HDFS solo para inferir tipos de datos, ahorrando millones de lecturas de disco y protegiendo la memoria del driver"* *(Ver Diapositiva 5)*.

#### ❓ BANCO DE PREGUNTAS DEL JURADO — DIAPOSITIVA 5

* **Pregunta 1: ¿Por qué configuraron `spark.sql.shuffle.partitions = 8` y qué ecuación de arquitectura justifica no usar 200?**
  * **Cálculo de olas de planificación (*Task Waves*):**  
    > *"Profesora, el valor por defecto de Spark (200 particiones) está diseñado para clústeres empresariales con cientos de núcleos.  
    > En nuestro clúster físico:  
    > - Tenemos **2 ejecutores** con **2 cores cada uno** = **4 cores de cómputo en total*** *(Ver Captura C5b pág. 7)*.  
    > - Si dejábamos 200 particiones, cada operación de `groupBy` o `join` creaba 200 tareas diminutas. Para procesar 200 tareas con 4 núcleos, Spark tiene que despachar:  
    > $$\frac{200\text{ tareas}}{4\text{ núcleos}} = \textbf{50 olas secuenciales de tareas}$$  
    > Cada tarea despachada implica sobrecosto de serialización en el DAGScheduler, apertura de conexiones IPC y contención de buffers de shuffle. El tiempo de gestión supera al tiempo de cómputo.  
    > - Al configurar `spark.sql.shuffle.partitions = 8`:  
    > $$\frac{8\text{ tareas}}{4\text{ núcleos}} = \textbf{2 olas exactas de 4 tareas concurrentes}$$  
    > En la ola 1 se ejecutan 4 tareas simultáneas en los 4 cores; en la ola 2 se ejecutan las 4 tareas restantes. Se alcanza un balance perfecto con **cero desperdicio de CPU y mínimo overhead de scheduling**."*

* **Pregunta 2: ¿Por qué prohibieron `inferSchema=true` y qué hace exactamente la celda `S2` del Notebook?**
  * **Detalle de la Celda `S2` y beneficio de I/O:**  
    > *"En la celda `S2` definimos explícitamente el esquema mediante la clase `StructType`:  
    > ```python
    > schema = StructType([
    >     StructField("FECHA_CORTE", StringType(), True),
    >     StructField("NOM_RPT", StringType(), True),
    >     ...,
    >     StructField("PER_DECLA", LongType(), True),
    >     StructField("PER_UBIC", LongType(), True),
    >     StructField("EVENTOS", LongType(), True)
    > ])
    > ```  
    > **¿Qué hace y qué arrojó?** Define los metadatos en memoria del driver en 0,01 segundos (`Esquema (StructType) definido con 19 campos`).  
    > **Impacto en Big Data:** Si usábamos `inferSchema=true`, Spark habría lanzado un Job previo que escanea los 20 bloques de HDFS (2,47 GB) línea por línea analizando texto para inferir tipos, y luego un segundo Job para leer los datos. Con `StructType`, Spark hace **un único escaneo directo**, ahorrando 2,5 GiB de I/O en disco y evitando la saturación del heap de Java del driver."*

---

### 🕒 Minuto 1:00 a 2:00 — Diapositiva 6: Calidad de Datos y Limpieza Nativa de Fechas
*(Pasar a la diapositiva 6)*

* **Calidad y completitud del censo:**  
  *"En la exploración inicial comprobamos una calidad técnica excepcional: las variables clave del negocio (Departamento, Municipio, Hecho Victimizante, Etnia y Sexo) registraron **0 % de valores nulos**, cubriendo los 1.126 municipios del país"* *(Ver Notebook celdas S4a y S4b)*.
* **El reto técnico de las fechas multiformato (2026):**  
  *"El gran obstáculo técnico estuvo en la columna `FECHA_CORTE`: el dataset mezclaba registros con año de 4 dígitos (`dd/MM/yyyy`), de 2 dígitos (`dd/MM/yy`), formato ISO (`yyyy/MM/dd`) y estampas con hora* *(Ver Notebook celda S4c)*.  
  *En lugar de recurrir a una función definida por el usuario (UDF) de Python —lo cual habría destruido el rendimiento del clúster al obligar a serializar millones de filas entre la JVM de Java y el intérprete de Python fila por fila—, implementamos una solución distribuida 100 % nativa en Spark SQL: combinamos `substring`, `length` y `coalesce(to_date)`. Esto permitió normalizar el 100 % de las fechas con **cero registros nulos no parseables** y aislar los cortes de 2026 evaluadas de forma totalmente paralela y vectorizada"* *(Ver Notebook celdas S4c y S4d y Diapositiva 6)*.

#### ❓ BANCO DE PREGUNTAS DEL JURADO — DIAPOSITIVA 6

* **Pregunta 1: ¿Por qué es tan grave usar una UDF de Python (`@udf`) en PySpark y cómo funciona la solución nativa de la celda `S4c`?**
  * **Explicación de bajo nivel (JVM vs. Python Worker):**  
    > *"Profesora, PySpark ejecuta su motor central sobre la máquina virtual de Java (JVM).  
    > Si definimos una UDF de Python (`@udf def parse_fecha(f): ...`):  
    > 1. Para cada una de las 8,86 millones de filas, los ejecutores JVM deben empaquetar los datos (*Pickle serialization*) y enviarlos mediante sockets de comunicación entre procesos (IPC) a un subproceso de Python (`python daemon`).  
    > 2. El proceso Python ejecuta el código en una sola CPU sin vectorización y re-serializa la respuesta para devolverla a la JVM.  
    > 3. Esto anula por completo el optimizador Catalyst y el motor de generación de código Tungsten, disparando el tiempo de CPU y provocando pausas de recolección de basura (*Garbage Collection*).  
    > **Cómo lo resolvió la Celda `S4c`:**  
    > ```python
    > s = substring(col("FECHA_CORTE"), 1, 10)
    > fecha_dt = coalesce(
    >     when(s.rlike(r"^\d{4}/\d{2}/\d{2}$"), to_date(s, "yyyy/MM/dd")),
    >     when(s.rlike(r"^\d{2}/\d{2}/\d{2}$"), to_date(s, "dd/MM/yy")),
    >     to_date(s, "dd/MM/yyyy")
    > )
    > ```  
    > **¿Qué arrojó exactamente?**  
    > La salida de la celda imprimió: `Registros con FECHA_CORTE no parseable: 0`.  
    > El 100 % de los 8,86 millones de registros se normalizó sin un solo fallo. Al ser funciones nativas de Spark SQL, Catalyst compila las expresiones en bytecode Java directamente dentro de la memoria compartida, ejecutándose a la máxima velocidad del hardware sin abandonar la JVM."*

* **Pregunta 2: ¿Qué arrojó la celda `S4c` sobre la distribución por años y qué reveló la celda `S4d` sobre el 2026?**
  * **Resultados exactos del Notebook:**  
    > *"La agregación de la celda `S4c` arrojó la siguiente distribución histórica:  
    > - **Año 2024:** 2.139.180 registros · 55.081.625 hechos (EVENTOS)  
    > - **Año 2025:** 5.102.320 registros · 163.567.135 hechos  
    > - **Año 2026:** 1.619.388 registros · 52.919.928 hechos en los primeros cortes mensuales procesados.  
    > En la celda `S4d` desglosamos los cortes de 2026: cortes de enero a abril con más de 430.000 filas y 14 millones de hechos cada uno, demostrando que el censo refleja el dinamismo y la vigencia del conflicto armado reciente en el país."*

---

### 🕒 Minuto 2:00 a 3:00 — Diapositiva 7: Transformaciones Complejas y Auditoría AQE
*(Pasar a la diapositiva 7)*

* **Las dos transformaciones complejas del caso:**  
  *"Cumplimos con las transformaciones complejas mediante dos operaciones mayores:  
  1. Una **agregación multidimensional pesada** (`groupBy` por departamento, municipio y hecho victimizante), calculando la brecha real restando personas ubicadas de declaradas y la tasa de efectividad, lo que redujo el censo de 8,86 millones a **24.020 registros agregados**.* *(Ver Notebook celdas S5 y S6)*.  
  2. Un **Broadcast Hash Join** donde cruzamos este consolidado contra un catálogo maestro del DANE de 33 filas para asignar la Región Natural de cada evento sin mover la tabla de víctimas por la red"* *(Ver Notebook celda S6 y Diapositiva 7)*.
* **Auditoría Catalyst y Adaptive Query Execution (AQE):**  
  *"Auditamos el plan físico mediante `.explain()`. Como muestra la celda del notebook y la Captura C6* *(Ver Anexos pág. 8)*, *el plan preliminar reportó `AdaptiveSparkPlan isFinalPlan=false`, pues Spark tenía planificados intercambios de shuffle genéricos. Al ejecutarse la acción en el clúster, AQE analizó las estadísticas en tiempo de ejecución: comprobó que el catálogo DANE era diminuto, activó `AQEShuffleRead`, fusionó particiones intermedias y consolidó el plan adaptativo final con `isFinalPlan=true`, visible en la interfaz SQL de la Spark UI (Captura C7)"* *(Ver Anexos págs. 9 y 10 y Diapositiva 7)*.

#### ❓ BANCO DE PREGUNTAS DEL JURADO — DIAPOSITIVA 7

* **Pregunta 1: ¿Qué hace exactamente la celda `S5` del notebook y qué métricas generó?**
  * **Detalle de la Celda `S5`:**  
    > *"La celda `S5` agrupa por `(COD_ESTADO_DEPTO, ESTADO_DEPTO, CIUDAD_MUNICIPIO, HECHO)` y calcula:  
    > ```python
    > df_agregado = df.groupBy(
    >     "COD_ESTADO_DEPTO", "ESTADO_DEPTO", "CIUDAD_MUNICIPIO", "HECHO"
    > ).agg(
    >     spark_sum("EVENTOS").alias("TOTAL_EVENTOS"),
    >     spark_sum("PER_DECLA").alias("TOTAL_DECLARANTES"),
    >     spark_sum("PER_UBIC").alias("TOTAL_UBICADOS")
    > ).withColumn(
    >     "TASA_EFECTIVIDAD_UBICACION",
    >     when(col("TOTAL_DECLARANTES").isNull() | (col("TOTAL_DECLARANTES") == 0), lit(None))
    >     .otherwise((col("TOTAL_UBICADOS") / col("TOTAL_DECLARANTES")) * 100)
    > )
    > ```  
    > **¿Qué arrojó?**  
    > - Redujo el censo de 8,86 millones a **24.020 filas agregadas**.  
    > - En el top de impacto histórico reveló que el hecho con mayor volumen en Colombia es el **Desplazamiento Forzado en Buenaventura (Valle del Cauca)** con 8.660.228 eventos, 5.775.514 declarantes y 4.022.189 ubicados, arrojando una **Tasa de Efectividad de Ubicación de solo 69,64 %** (una brecha crítica de más de 1,75 millones de personas sin asentar)."*

* **Pregunta 2: ¿Qué significa exactamente `AdaptiveSparkPlan isFinalPlan=false` y en qué momento exacto cambia a `true`?**
  * **Explicación del ciclo de vida de Catalyst y AQE (Celdas `S8a` y `S8b`):**  
    > *"Profesora:  
    > 1. En la celda `S8a` invocamos `consulta_principal.explain()`. En ese instante el motor Catalyst solo genera el **plan físico inicial basado en estimaciones estáticas**. Como rige el principio de evaluación perezosa (*lazy evaluation*), Spark no ha leído los datos de HDFS y no conoce los tamaños exactos en memoria; por eso etiqueta el plan como `AdaptiveSparkPlan isFinalPlan=false` y programa operadores genéricos `Exchange hashpartitioning`* *(Ver Captura C6 pág. 8)*.  
    > 2. El cambio a `isFinalPlan=true` ocurre en la celda `S8b`, cuando disparamos la primera acción real: `consulta_principal.cache()` y `consulta_principal.count()` (que tardó 10,28 segundos).  
    > Al materializarse los stages intermedios, entra a operar **Adaptive Query Execution (AQE)**: lee las estadísticas de shuffle en runtime, comprueba que la tabla satélite del DANE pesa pocos kilobytes, reemplaza el Sort Merge Join por `BroadcastHashJoin`, une particiones vacías mediante `AQEShuffleRead` y sella el plan adaptativo final con `isFinalPlan=true`, el cual auditamos en la pestaña SQL de Spark UI* *(Ver Captura C7 págs. 9 y 10)*."*

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

* **Pregunta 1: Expliquen con métricas de la Spark UI por qué el Stage 30 tardó 36 segundos y el Stage 48 solo 0,8 segundos. ¿Hubo problemas de memoria?**
  * **Análisis comparativo de métricas (Capturas C8 y C9):**  
    > *"Profesora, la disparidad de tiempos no fue un error, sino la consecuencia lógica de la naturaleza computacional de cada etapa:  
    > 1. **Stage 30 (Cuello de botella de I/O y procesamiento Map - Captura C8 pág. 11):**  
    >    - Ejecutó **20 tareas** (1 tarea por cada uno de los 20 bloques físicos de 128 MB en HDFS).  
    >    - Tuvo que leer **2,5 GiB** de disco crudo, deserializar las 8,86 millones de líneas, evaluar el `StructType`, filtrar y limpiar fechas, y ejecutar el hash map preliminar en cada nodo.  
    >    - Consumió **36 segundos acumulados de tareas**. Su salida de red fue comprimir y escribir **13,8 MiB de Shuffle Write** (318.416 registros agregados).  
    > 2. **Stage 48 (Procesamiento Reduce ligero en RAM - Captura C9 pág. 13):**  
    >    - Solo ejecutó **6 tareas**, leyendo por red los **9,8 MiB** ya reducidos del shuffle anterior.  
    >    - No leyó disco HDFS; operó 100 % en memoria RAM completando la agregación global en **0,8 segundos** con mediana de 0,2 s por tarea.  
    > 3. **Salud de la memoria:** En las tablas *Summary Metrics* de ambas capturas se lee textualmente: `Spill (Memory): 0 B` y `Spill (Disk): 0 B`. Esto certifica que la asignación de 2 GB de RAM por ejecutor evitó cualquier derrame de datos a disco, operando a la máxima eficiencia del hardware."*

* **Pregunta 2: ¿Qué hace la celda `S9` y qué demuestra la pestaña Storage en la Captura C10?**
  * **Detalle de Celda `S9` y liberación en `S10f`:**  
    > *"En la celda `S9` aplicamos `df_consolidado.persist(StorageLevel.MEMORY_AND_DISK)`.  
    > **¿Por qué?** Porque este DataFrame enriquecido fue consumido por dos acciones consecutivas: la Acción 2 en la celda `S9b` (reporte agregado por Región Natural) y la Acción 3 en la celda `S10a` (Top 10 municipal). Sin persistencia, Spark habría tenido que releer los 2,47 GB de HDFS y repetir el Stage 30 desde cero para la segunda consulta.  
    > **Evidencia en Captura C10 (pág. 15):** La pestaña Storage certifica que el RDD 162 quedó al **100 % en memoria (`Fraction Cached: 100%`)**, ocupando apenas **625,5 KiB de RAM** distribuidos en 8 particiones, con **0 B en disco**.  
    > En la celda `S10f` ejecutamos `df_consolidado.unpersist()` para vaciar los buffers en los ejecutores YARN, previniendo fugas de memoria (*memory leaks*)."*

---

### 🕒 Minuto 1:00 a 2:00 — Diapositiva 9: Bonificación (+0,3): Comparación Empírica de Joins
*(Pasar a la diapositiva 9)*

* **El experimento físico de optimización:**  
  *"Optamos por la bonificación técnica comparando empíricamente dos estrategias físicas de join sobre la misma operación de cruce entre el censo de víctimas y el catálogo del DANE* *(Ver Notebook celdas S7a, S7b y S7c)*:  
  *En el primer escenario forzamos **Sort Merge Join** desactivando el broadcast con `spark.sql.autoBroadcastJoinThreshold = -1`. Esto obligó a Spark a ordenar ambas tablas por clave y redistribuirlas a través de la red, generando **31,2 KiB de Shuffle Write** en el Stage 48 y un tiempo de ejecución de **9,21 segundos**"* *(Ver Captura C11 en Anexos págs. 16 y 17)*.
* **Eliminación del shuffle con Broadcast Hash Join:**  
  *"En el segundo escenario restauramos el umbral por defecto (10 MB). Spark identificó que el catálogo DANE pesaba menos de 10 MB y envió una copia ligera a la memoria de cada ejecutor mediante **Broadcast Hash Join**. Como vemos en la Captura C11* *(Anexos pág. 18)*, *la tabla grande de víctimas no tuvo que redistribuirse por la red, el shuffle write colapsó a apenas **354 bytes** y el tiempo bajó a **8,96 segundos**, eliminando por completo el tráfico de red entre nodos"* *(Ver Diapositiva 9)*.

#### ❓ BANCO DE PREGUNTAS DEL JURADO — DIAPOSITIVA 9

* **Pregunta 1: Expliquen la mecánica interna de cada Join: ¿Por qué en Broadcast Hash Join el shuffle write colapsa a 354 bytes?**
  * **Comparativa arquitectural profunda (Celdas `S7a` vs `S7b`):**  
    > *"Profesora, el experimento comparó cómo viajan los datos por la red física del clúster:  
    > 1. **Escenario A — Sort Merge Join forzado (Celda `S7a`):**  
    >    - Desactivamos el broadcast con `autoBroadcastJoinThreshold = -1`.  
    >    - Spark debe particionar **ambas tablas** por el valor hash de la clave `COD_ESTADO_DEPTO`.  
    >    - Obliga a enviar las particiones de ambas tablas a través de la red física (*Shuffle Exchange*), y en cada ejecutor debe ordenar (*sort*) las claves antes de fusionarlas.  
    >    - Esto generó **31,2 KiB de Shuffle Write** en el Stage 48 y tardó **9,21 segundos*** *(Ver Captura C11 pág. 17)*.  
    > 2. **Escenario B — Broadcast Hash Join (Celda `S7b`):**  
    >    - Restauramos el umbral a 10 MB (`10485760` bytes).  
    >    - El driver detecta que el catálogo del DANE tiene solo 33 filas. Construye una pequeña tabla hash en memoria y la transmite (*broadcast*) una sola vez a cada ejecutor.  
    >    - Cada ejecutor lee su partición local de la tabla de víctimas y hace una búsqueda directa en memoria (*hash lookup*).  
    >    - **La tabla grande de víctimas jamás se redistribuye ni se ordena.** Los 354 bytes que reporta la Spark UI corresponden únicamente a metadatos de sincronización de tareas, reduciendo el tráfico de red a prácticamente cero y bajando el tiempo a **8,96 segundos*** *(Ver Captura C11 pág. 18)*."*

---

### 🕒 Minuto 2:00 a 3:00 — Diapositiva 10: Hallazgos de Consultoría y Dictamen Cloud (BigQuery)
*(Pasar a la diapositiva 10)*

* **Hallazgos del negocio y Patrón Big Data:**  
  *"Aplicamos con rigor el patrón Big Data: procesamos 8,86 millones de registros de forma paralela en los ejecutores del clúster y solo realizamos la reducción a **10 filas** antes de ejecutar `.toPandas()` para graficar* *(Ver Notebook celdas S10a y S10b)*.  
  *Los resultados revelan una brecha asistencial alarmante para la UARIV: **el 28 % de las personas declarantes en el RUV no cuenta con registro de ubicación territorial ni atención efectiva en el municipio receptor**. Además, Buenaventura lidera el impacto nacional con cerca de 9,88 millones de hechos acumulados, seguido por Medellín y Tumaco"* *(Ver Informe pág. 2 y Diapositiva 10)*.
* **Recomendación tecnológica: Clúster On-Premise vs Google BigQuery:**  
  *"Para la entidad contratante, aunque nuestro clúster local procesó el análisis en **10,28 segundos** sin cobro directo de licencia* *(Ver Notebook celda S8b)*, *mantener infraestructura propia exige asumir costos fijos de energía, mantenimiento, soporte ante caídas de red y una alta curva de administración de Hadoop y YARN* *(Ver Anexo A5 en Anexos pág. 21)*.  
  *En contraste, en la práctica oficial demostramos que **Google BigQuery**, gracias a su modelo columnar serverless (Capacitor), solo requirió escanear **830 MB facturados** de una tabla de 6,97 GB (y **0 B cuando reutiliza el resultado en caché**)* *(Ver Hoja A2 en Anexos pág. 20)*, *cabiendo con holgura en el nivel gratuito de 1 TB mensual. Dictaminamos a la UARIV adoptar una **arquitectura híbrida**: conservar Spark para transformaciones batch nocturnas pesadas y migrar la analítica ad-hoc y tableros interactivos a BigQuery por su autoescalabilidad e integración nativa con streaming"* *(Ver Diapositiva 10 e Informe pág. 3)*.
* **Cierre formal:** *"Muchas gracias profesoras, quedamos atentos a sus preguntas individuales."*

#### ❓ BANCO DE PREGUNTAS DEL JURADO — DIAPOSITIVA 10

* **Pregunta 1: ¿Por qué y cómo aplicaron el patrón Big Data antes de `.toPandas()` en las celdas `S10a` y `S10b`? ¿Qué ocurre si ejecuto `df.toPandas()` directamente?**
  * **Explicación técnica de memoria y driver:**  
    > *"Profesora, en Big Data rige una regla estricta: los datos pesados se quedan en el clúster; al driver solo viajan los agregados finales.  
    > Si hubiésemos ejecutado `df.toPandas()` sobre el DataFrame completo:  
    > - PySpark intentaría serializar los 8,86 millones de filas (más de 2,47 GB) y transferirlas desde los ejecutores de `node2` y `node3` hacia el proceso driver en el nodo maestro.  
    > - Como el driver tiene asignados 2 GB de memoria de heap de Java, la creación de millones de objetos Python saturaría la memoria de inmediato, arrojando un error fatal `java.lang.OutOfMemoryError: Java heap space` y abortando la sesión de YARN.  
    > **Patrón aplicado en Celdas `S10a` y `S10b`:**  
    > 1. En `S10a` agrupamos y limitamos en Spark a 10 filas: `df_top10 = ...limit(10)`. Imprimió: `Filas del resultado agregado: 10`.  
    > 2. En `S10b` ejecutamos la conversión a Pandas: `pdf_top10 = df_top10.toPandas()` y `pdf_region = df_region.toPandas()`.  
    > Arrojó dimensiones minúsculas: `top10 → (10, 3)` y `regiones → (7, 4)`. Esos micro-dataframes se grafican en Matplotlib de forma instantánea sin ningún riesgo de desbordar la memoria."*

* **Pregunta 2: ¿En qué datos específicos se sustentan para recomendar una arquitectura híbrida entre el clúster propio y Google BigQuery?**
  * **Comparación técnica con datos observados (Anexo A5 pág. 21 y Hoja A2 pág. 20):**  
    > *"Nos apoyamos en mediciones empíricas reales de ambas plataformas:  
    > 1. **Clúster On-Premise propio:**  
    >    - Ventaja: Ejecutó el análisis principal en **10,28 segundos** sin generar costos variables por consulta.  
    >    - Desventaja: Exige administración continua de daemons (NameNode, NodeManager, ResourceManager), licenciamiento de servidores y costos fijos de energía. Además, como evidenciamos en el incidente I1, la infraestructura física está expuesta a fallos locales de red y cortes de conexión.  
    > 2. **Google BigQuery (Serverless):**  
    >    - En la práctica oficial consultamos una tabla completa de 6,97 GB. Gracias a su formato columnar Capacitor, BigQuery solo leyó las columnas referenciadas en el query, reduciendo el escaneo a apenas **830 MB facturados** (y **0 B cuando la consulta se repite y se sirve desde la caché de slots**).  
    >    - Google Cloud otorga 1 TB de escaneo gratuito mensual por proyecto; por ende, las consultas analíticas de la UARIV se ejecutan a costo cero, escalando automáticamente ante picos de demanda.  
    > 3. **Dictamen de Consultoría para la UARIV:** Arquitectura Híbrida.  
    >    - Conservar Hadoop/Spark para procesos ETL batch masivos nocturnos donde se transforman terabytes continuos.  
    >    - Migrar las consultas ad-hoc de los funcionarios, tableros BI en Looker y la capa analítica a BigQuery, aprovechando su modelo serverless sin aprovisionamiento de infraestructura."*

---

## 🎤 SIMULACRO DE PREGUNTAS INDIVIDUALES Y DEFENSA CRUZADA

### Caso 1: Julián Steven Duarte
* **Si defiende Componente A (Almacenamiento/HDFS):**  
  > *"Profesora, el censo se cargó en HDFS en `/caso` con tamaño de bloque de 128 MB y replicación 2 balanceada entre `node2` y `node3`, generando matemáticamente 20 bloques físicos (40 réplicas totales en el clúster). El comando `hdfs fsck` certificó estado 100 % HEALTHY y auditamos el Bloque 0 en ambos nodos (Captura C3 pág. 4). Ante el corte de descarga a 2,65 GB (Anexo A4 pág. 21), auditamos en Linux la integridad de las 19 columnas y la representación en 1.126 municipios antes de proceder."*
* **Pregunta cruzada si le sortean Componente B (Spark):**  
  > *"Configuramos la sesión con 2 ejecutores de 2 cores y 2 GB de RAM, ajustando `shuffle.partitions` a 8 para procesar exactamente 2 olas de 4 tareas sobre los 4 cores del clúster (Captura C5 pág. 6). Limpiamos las fechas multiformato con funciones nativas de Spark SQL (`substring` y `coalesce`) en la celda S4c evitando UDFs de Python para mantener la ejecución vectorizada en Catalyst, logrando cero registros no parseables sobre los 8,86 millones."*
* **Pregunta cruzada si le sortean Componente C (Spark UI/Cloud):**  
  > *"En la Spark UI demostramos que el Stage 30 fue el cuello de botella por leer 2,47 GB de HDFS en 36 segundos (Captura C8 pág. 11), sin ningún byte de spill a disco. Comprobamos la superioridad de Broadcast Hash Join reduciendo el shuffle de 31,2 KiB a 354 bytes (Captura C11 pág. 18), y dictaminamos arquitectura híbrida con BigQuery, que solo escaneó 830 MB facturados frente a los 6,97 GB de la tabla (Anexo A5 pág. 21)."*

### Caso 2: Juan Tomás Rincón
* **Si defiende Componente B (Procesamiento/Spark):**  
  > *"Profesora, el esquema se definió con `StructType` en la celda S2 para evitar que Spark escaneara los 2,47 GB dos veces. Auditamos el plan físico: al inicio aparecía `AdaptiveSparkPlan isFinalPlan=false` en Catalyst (Captura C6 pág. 8), pero en tiempo de ejecución en la celda S8b AQE analizó los tamaños en memoria, reemplazó el join por Broadcast Hash Join y consolidó el plan adaptativo final con `isFinalPlan=true` (Captura C7 pág. 9)."*
* **Pregunta cruzada si le sortean Componente A (HDFS/Ingesta):**  
  > *"El dataset se descargó vía curl y se subió con `hdfs dfs -put` a `/caso`. Sus 2.649.266.066 bytes generaron 20 bloques físicos de 128 MB con factor de replicación 2 en `node2` y `node3`, auditados con `hdfs fsck` sin bloques corruptos ni bajo-replicados (Capturas C2 y C3 págs. 3 y 4)."*
* **Pregunta cruzada si le sortean Componente C (Spark UI/Cloud):**  
  > *"Persistimos el DataFrame consolidado con nivel `MEMORY_AND_DISK` en la celda S9, verificando en la pestaña Storage que el RDD 162 quedó al 100 % en RAM ocupando apenas 625,5 KiB sin tocar disco (Captura C10 pág. 15). Aplicamos el patrón Big Data reduciendo a 10 filas antes de `.toPandas()` para proteger la memoria del driver (Notebook celda S10a)."*

### Caso 3: Julián Camilo Jiménez
* **Si defiende Componente C (Spark UI/Optimización/Cloud):**  
  > *"Profesora, analizamos la disparidad entre el Stage 30 (36 s por I/O pesada de 2,47 GB) y el Stage 48 (0,8 s por leer shuffle ya reducido a 13,8 MiB). En la bonificación (+0,3), desactivar el broadcast forzó Sort Merge Join con 31,2 KiB de shuffle y 9,21 s, mientras que Broadcast Hash Join eliminó el tráfico de red bajando a 354 bytes y 8,96 s (Captura C11 págs. 16-18). Para BigQuery, demostramos que su formato columnar solo requirió 830 MB facturados (Anexo A5 pág. 21)."*
* **Pregunta cruzada si le sortean Componente A (HDFS/Ingesta):**  
  > *"La topología del clúster cuenta con 2 DataNodes de 58,2 GB cada uno (`node2` y `node3`). Por eso el factor de replicación se fijó en 2: poner replicación 3 habría dejado bloques under-replicated porque HDFS prohíbe poner dos réplicas en una misma máquina física. Los 20 bloques quedaron perfectamente balanceados con 20 réplicas por nodo (Capturas C1 y C3 págs. 2 y 4)."*
* **Pregunta cruzada si le sortean Componente B (Spark/Fechas):**  
  > *"Prohibimos `inferSchema` para evitar doble escaneo del disco y configuramos `shuffle.partitions = 8` adaptado a los 4 cores del clúster (Capturas C4 y C5 págs. 5-6). Las dos transformaciones complejas fueron el groupBy multidimensional que redujo de 8,86 millones a 24.020 filas calculando la brecha de atención y tasa de efectividad, y el join con la tabla DANE de 33 filas (Notebook celdas S5 y S6)."*

---

## ⏱️ RESUMEN DE TIEMPOS POR PARTICIPANTE
| Segmento | Diapositivas | Duración Máxima | Control del Tiempo |
| :--- | :---: | :---: | :--- |
| **Intro breve** | Diapositiva 1 | 15 segundos | A cargo del primer expositor sorteado |
| **Componente A** | Diapositivas 2, 3 y 4 | 2 min 45 s | Diapositiva 2 (1 min) · Diapositiva 3 (1 min) · Diapositiva 4 (45 s) |
| **Componente B** | Diapositivas 5, 6 y 7 | 3 min 00 s | Diapositiva 5 (1 min) · Diapositiva 6 (1 min) · Diapositiva 7 (1 min) |
| **Componente C** | Diapositivas 8, 9 y 10 | 3 min 00 s | Diapositiva 8 (1 min) · Diapositiva 9 (1 min) · Diapositiva 10 (1 min) |
| **Total exposición equipo** | 10 diapositivas | **9 minutos exactos** | Cronómetro en mano; corte automático a los 3 min |
