# Laboratorio de arquitecturas Lambda y Kappa

Este laboratorio nos permitira comparar la arquitectura lambda y la arquitectura kappa. El objetivo es medir el rendimiento de lambda y kappa, aprender más sobre la contenedorización con Docker, aprender más sobre frameworks (Spark, Kafka y Hadoop), e implementar lambda y kappa. También, comparar la complejidad entre ambas implementaciones y dar una opinión personal sobre ambas arquitecturas, los problemas que ocurrieron y cómo se solucionaron.

## Introducción

Imaginemos que tenemos algunas máquinas expendedoras de snacks y queremos escalar nuestras máquinas expendedoras de forma infinita para aumentar nuestros ingresos. Para esto, necesitaríamos algún tipo de procesamiento de datos para analizar nuestras máquinas expendedoras.

EJEMPLO: Necesitamos saber:

  - ¿Qué máquina expendedora no es rentable?
  - ¿Dónde compra la mayoría de la gente?
  - ¿Vendemos más comida saludable o no saludable?

EJEMPLO: Necesitamos reaccionar:

  - ¿Qué snacks se agotarán pronto y dónde?
  - ¿Qué máquina expendedora se va a averiar?

Para este procesamiento, este repositorio muestra dos tipos de implementaciones de procesamiento de flujos. La Arquitectura Lambda y la Arquitectura Kappa. Centradas en una pregunta simple: "¿cuántos artículos de cada tipo se vendieron en total?"

## Requisitos de este Lab

  - Docker en tu sistema operativo.

```
conda create --name archs_py310 python=3.10
conda activate archs_py310
pip install -r requirements.txt
```

## Lambda

#### Iniciar

```
cd lambda
docker-compose up -d
python start_lambda.py
```

Después de que todos los contenedores estén en ejecución, necesitamos configurar e iniciar algunas cosas. Dentro de la carpeta `\lambda\src` podemos encontrar algunos archivos de Python. Esos archivos son necesarios
para generar datos e indicarle a Kafka, Spark, Hadoop y Cassandra qué hacer.

![lambda-architecture](./docs/img/lambda-architecture.svg)
Figura 1: Arquitectura Lambda

Toda la arquitectura lambda está creada a partir de varios contenedores de Docker.

  - **Kafka** (http://localhost:9021/)
  Apache Kafka es una plataforma de streaming de eventos distribuida que funciona como un sistema de mensajería de alto rendimiento, diseñada para manejar flujos masivos de datos en tiempo real. Permite que distintas aplicaciones, conocidas como "productores", publiquen grandes volúmenes de registros de datos en categorías organizadas llamadas "tópicos". A su vez, otras aplicaciones, o "consumidores", se suscriben a estos tópicos para leer y procesar la información de forma continua y ordenada. Gracias a su arquitectura escalable y tolerante a fallos, Kafka es fundamental para casos de uso como el seguimiento de actividad en sitios web, la ingesta de registros (logs), el análisis en tiempo real y la comunicación asíncrona entre microservicios.
  - **Hadoop** como capa de lote (*batch-layer*) con Namenode (http://localhost:9870/), Masternode, Datanodes y Resourcemanager (http://localhost:8088/)
  - **Cassandra**
  Apache Cassandra es una base de datos NoSQL altamente escalable y distribuida, diseñada para gestionar enormes cantidades de datos a través de múltiples servidores sin un único punto de fallo. Su arquitectura descentralizada le proporciona una altísima disponibilidad y tolerancia a fallos, lo que significa que la base de datos sigue funcionando incluso si algunos de sus nodos se caen. Utiliza un modelo de datos de "columna ancha" que ofrece gran flexibilidad y está optimizada para cargas de trabajo con un volumen de escritura masivo, haciéndola ideal para aplicaciones de Big Data, Internet de las Cosas (IoT) y sistemas en tiempo real que requieren un rendimiento constante y una capacidad de crecimiento horizontal casi ilimitada.
  - **Spark** con *workers* y *master* (http://localhost:8080/)
  - **Grafana** (http://localhost:3000/)
  Grafana es una plataforma de análisis y visualización interactiva de código abierto que te permite consultar, visualizar, alertar y comprender tus métricas sin importar dónde estén almacenadas. Funciona conectándose a una amplia variedad de fuentes de datos, como bases de datos (SQL y NoSQL), sistemas de monitoreo y servicios en la nube, para luego transformar esa información en paneles o dashboards dinámicos y visualmente atractivos. A través de gráficos, medidores, mapas de calor y tablas, Grafana es una herramienta fundamental para el monitoreo de aplicaciones, el análisis de rendimiento de servidores, la visualización de datos de sensores de IoT y, en general, para dar sentido a datos complejos de series de tiempo de una manera unificada y centralizada.

Cada contenedor se ejecuta dentro de la misma red (**lambda-network**).

Para validar los datos y la calidad de los mismos, así como los resultados, Kafka también escribe todos los datos en Cassandra. Mientras Hadoop está procesando, Spark va recogiendo los nuevos datos entrantes y los sirve como una vista en tiempo real. Cuando Hadoop termina de procesar, Spark se reinicia y comienza a recoger datos desde el principio.

## Kappa

#### Iniciar

```
cd kappa
docker-compose up -d # espera hasta que todos los contenedores estén completamente en ejecución (alrededor de 2 minutos)
python start_kappa.py
```

![lambda-architecture](./docs/img/kappa-architecture.svg)
Figura 2: Arquitectura Kappa

Toda la arquitectura kappa está creada a partir de varios contenedores de Docker.

  - **Kafka** con generador de datos y centro de control (*control-center*) (http://localhost:9021/)
  - **Cassandra** para guardar diferentes vistas
  - **Spark** con *workers* y *master* (http://localhost:8080/)
  - **Grafana** para visualización (http://localhost:3000/)

## Configuración de Grafana

Para visualizar todo el proceso, puedes usar las plantillas de kappa y lambda
para Grafana. Puedes encontrarlas en (`/grafana_templates/`)

### Conección a Apache Cassandra

1.  Ve a http://localhost:3000
2.  Inicia sesión (usuario=admin, contraseña=admin) \# se puede cambiar
3.  Ve a Connections -\> Add new connection -\> Apache Cassandra -\> Install
4.  Ve a Connections -\> Data sources -\> Apache Cassandra
5.  Rellena la siguiente entrada -- Host: cassandra1:9042
6.  Save & Test

### Dashboard de Lambda

![lambda-architecture](./docs/img/lambda_dashboard.png)
Figura 3: Dashboard de Lambda

El Dashboard de Lambda muestra la Vista en Tiempo Real (*Real Time View*), la Vista de Lote (*Batch View*) y la Vista de Validación (*Validation View*). La Vista en Tiempo Real + la Vista de Lote deberían ser iguales a la Vista de Validación.

### Panel de Kappa

![lambda-architecture](./docs/img/kappa_dashboard.png)
Figura 4: Dashboard de Kappa

Similar al panel de lambda, el panel de kappa muestra la Vista en Tiempo Real y la Vista de Validación. La Vista en Tiempo Real debería ser igual a la Vista de Validación.

### Pregunta a responder

¿Se necesita una respuesta rápida o está bien esperar un tiempo para el proceso por lotes? 

Si necesitas reaccionar rápido o si tienes muchos mensajes y necesitas escalar y pasar los mensajes a través del sistema lo antes posible, elige kappa. Si necesitas resultados precisos, elige lambda y tómate el tiempo para los resultados de la capa de lote.

### Tareas Futuras

Hay campo para más estudio aquí. Los siguientes puntos están abiertos:

  - Sincronización entre la capa de lote (*batch-layer*) y la capa de velocidad (*speed-layer*) para lambda (explorar la herramienta airflow).
  - Cálculos más complejos, por ejemplo: ARIMA o ML.
  - Más RAM vs. más Nodos, ¿qué pasa con los resultados?
  - Aumento o disminución del flujo de mensajes, ¿qué sucede? ¿Cuándo se atascará y por qué?