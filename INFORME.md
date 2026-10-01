# Informe del trabajo práctico

## Introducción

En este trabajo se implementó la coordinación de mensajes entre las instancias de `SumFilter`, `Aggregation` y `Join`. El objetivo es distribuir el procesamiento entre varias instancias sin perder la correspondencia entre los mensajes de una misma solicitud (`request_id`) y sin alterar el resultado final.

La coordinación se apoya en mensajes internos sobre RabbitMQ y en contadores asociados a cada solicitud. En la implementacion del mecanismo de coordinacion logre una abstraccion respecto al modelo de negocion, de forma que el criterio de orden de las frutas está encapsulado en `FruitItem` y las etapas que acumulan o combinan resultados utilizan esa comparación, de forma que de cambiar el criterio de orden la implementacion funcionara con ese mismo criterio.

## Protocolo de mensajes internos

Se agregaron cuatro tipos de mensaje para coordinar las instancias internas:

- **`DATA`**: lleva una fruta y su cantidad acumulada para una solicitud desde una instancia `SumFilter` hacia una instancia `Aggregation`.
- **`EOF`**: indica que una instancia `SumFilter` terminó de procesar los datos de una solicitud. Se envía a todas las instancias `Aggregation` para que cada una pueda contabilizar la finalización de todos los `SumFilter`.
- **`EOF_RECEIVED`**: comunica a los listeners de los `SumFilter` que llegó el fin de los datos de una solicitud, junto con la cantidad total de registros esperados.
- **`COUNT`**: informa cuántos registros de una solicitud procesó una instancia `SumFilter`. Los listeners comparten estos conteos para determinar si todos los registros fueron procesados.

La comunicación de `Aggregation` hacia `Join` y de `Join` hacia `Gateway` también utiliza mensajes serializados, pero no vi necesaria la creacion de un tipo al tratarse de un único formato de mensaje por canal: el top parcial en el primer caso y el top combinado en el segundo.

## Coordinación entre instancias SumFilter

### Estado de cada instancia

Cada SumFilter mantiene los siguientes atributos:
- **`input_queue`**: cola de entrada de mensajes.
- **`sum_control_main_exchange`**: exchange usado por el hilo main para publicar los mensajes de control.
- **`sum_control_listener_exchange`**: exchange usado por el hilo listener para recibir mensajes de control y publicar conteos.
- **`data_output_exchanges`**: lista de exchanges para enviar los resultados a las instancias Aggregation.
- **`amount_by_request`**: cantidades acumuladas localmente, agrupadas por request_id.
- **`count_by_sum_id`**: último conteo conocido de cada instancia SumFilter, para cada solicitud.
- **`total_count_by_request`**: cantidad total de registros esperados por solicitud.
- **`state_lock`**: lock que protege el acceso concurrente al estado compartido.
- **`flushed_requests`**: solicitudes cuyo procesamiento local ya se envió a Aggregation.
- **`listener_ready`**, **`listener_startup_error`** y **`shutdown_requested`**: atributos para coordinar el inicio del listener, registrar errores de inicio y señalar el cierre del proceso.

### Coordinación de fin de solicitud

Cada `SumFilter` procesa una partición de los registros de entrada. Por eso, que una instancia reciba el mensaje de fin de datos no significa que las demás hayan terminado de procesar su parte.

Para resolverlo se consideraron distintas alternativas. Una posibilidad era asignar la coordinación a una única instancia `Aggregation` con un identificador fijo pero la descarte considerando que no era una tarea del `Aggregation`. Lo cual me llevo a la idea de crear un hilo coordinador para cada solicitud que recibiera un `EOF` y que este les avise a hilos listener cuando podian enviar sus resultados hacia los `Aggregation`. Sin embargo considere que la creación de un hilo por solicitud podía ser costosa y que al concentrar la coordinacion en un único hilo se generaba un cuello de botella.

Por eso, finalmente se optó por un esquema distribuido: cada `SumFilter` crea al inicio un hilo listener que permanece suscripto al exchange de control. Así no se crea un hilo nuevo por solicitud ni se centraliza toda la coordinación en una única instancia. A pesar de que genera una mayor cantidad de mensajes entre instancias, evitar el cuello de botella y cada instancia puede determinar por sí misma cuándo se terminaron de procesar los registros de una solicitud.

El protocolo funciona de la siguiente manera:

1. Cuando un `SumFilter` recibe el `EOF` enviado por el `Gateway`, publica un `EOF_RECEIVED` con el `request_id` y la cantidad total de registros de esa solicitud (proveniendo del EOF del `Gateway`).
2. El exchange distribuye ese aviso a los listeners de todos los `SumFilter`. Al recibirlo, cada listener consulta el contador local de registros procesados para esa solicitud y publica un `COUNT` con su identificador y su conteo actual.
3. Cada listener recibe los `COUNT` publicados por todas las instancias, incluido el propio. Mantiene el último conteo conocido para cada `SumFilter` y compara la suma con el total informado en `EOF_RECEIVED`.
4. Si los conteos todavía no alcanzan el total, la instancia continúa esperando. Si luego procesa otro `DATA` de esa solicitud, publica un nuevo `COUNT` actualizado. 
5. Cuando la suma coincide con el total, cada `SumFilter` entiende que todas las particiones terminaron y envía sus sumas locales como mensajes `DATA` a las instancias `Aggregation` correspondientes y, enviando al final el `EOF` con el `request_id` a todas las instancias `Aggregation`.

El listener conserva el mayor conteo recibido para cada instancia, de modo que la llegada de un aviso anterior no haga retroceder el estado. La igualdad con el total esperado permite detectar que los registros de la solicitud ya fueron procesados por el conjunto de `SumFilter`.

### Sincronización del estado compartido

El hilo principal procesa los mensajes de datos y el hilo listener procesa los mensajes de control. Ambos acceden a estructuras compartidas por solicitud, como los acumuladores y los contadores. Se utiliza un lock para el acceso a esas estructuras, de modo que no se produzcan inconsistencias. Por ejemplo, si un `DATA` y un `EOF_RECEIVED` se procesan al mismo tiempo, el acceso a `amount_by_request` y `total_count_by_request` queda protegido.

El lock también ordena el caso límite en el que un `DATA` se procesa al mismo tiempo que llega el aviso `EOF_RECEIVED`. Si el dato se contabiliza antes de que el listener consulte el total, el conteo inicial ya lo incluye. Si se contabiliza después, como el total ya está registrado, el hilo principal publica un `COUNT` actualizado. De ese modo, el conteo final no depende de cuál de esos dos eventos ocurra primero. Si no hubiese un lock, podria enviarse un `COUNT` sin contar el ultimo dato, y luego nunca enviarse otro actualizado.

El uso de un exchange compartido evita que un coordinador central tenga que recibir y reenviar cada aviso. A cambio, el protocolo genera más tráfico de control: cada `COUNT` se distribuye a todos los listeners, por lo que la cantidad de entregas de control crece aproximadamente con el cuadrado del número de instancias `SumFilter` para cada solicitud. Esta decisión favorece una coordinación sin un único punto de procesamiento, con ese costo de mensajes como contrapartida como ya mencione anteriormente.

## Distribución y procesamiento en Aggregation

Para distribuir las frutas entre las instancias `Aggregation`, `SumFilter` calcula un hash SHA-256 de la combinación `request_id` y nombre de fruta, y aplica módulo `AGGREGATION_AMOUNT` al resultado. Así, para una misma solicitud, todas las apariciones de una fruta se envían siempre a la misma instancia `Aggregation`. A la vez, frutas distintas de un mismo `reques_id` pueden procesarse en paralelo en instancias diferentes.

Cada instancia `Aggregation` mantiene, por `request_id`, las cantidades recibidas y un contador de mensajes `EOF`, uno por cada instancia `SumFilter`. Al recibir un `DATA`, acumula la cantidad de la fruta en su top parcial y lo mantiene ordenado según la comparación de `FruitItem`. Al recibir un `EOF`, incrementa el contador de esa solicitud. Cuando recibió los `EOF` de todos los `SumFilter`, termina de construir su top parcial y lo publica hacia `Join`.

La partición determinista evita que una misma fruta de una solicitud aparezca en más de un `Aggregation`. Por eso, la acumulación de una fruta queda completa en una sola instancia antes de calcular los tops parciales.

## Combinación de resultados en Join

`Join` recibe un top parcial de cada instancia `Aggregation`. Para cada `request_id`, conserva el top combinado hasta el momento y la cantidad de mensajes parciales recibidos.

Como cada lista entrante ya está ordenada y tiene como máximo `TOP_SIZE` elementos, `Join` puede combinarla con el top acumulado mediante una fusión de dos listas ordenadas. En cada paso compara los siguientes elementos usando `FruitItem`, y agrega el que corresponde al top. Luego conserva solo los primeros `TOP_SIZE` elementos. La fusión es lineal respecto del tamaño de las listas; como ambas están acotadas por `TOP_SIZE`, el trabajo por mensaje es O(`TOP_SIZE`).

Descartar los elementos posteriores al top no afecta el resultado final: si una fruta queda fuera del top parcial de su instancia `Aggregation`, ya existen al menos `TOP_SIZE` frutas de esa partición que la preceden en el orden. Al combinar particiones, esa fruta tampoco puede desplazar a una fruta que ya esté entre las primeras `TOP_SIZE` globales. Una vez recibido un mensaje de cada instancia `Aggregation` para la solicitud, `Join` envía el top combinado a `Gateway`.

## Cierre ante SIGTERM

Se incorporó manejo de `SIGTERM` en los procesos consumidores para detener el consumo y cerrar sus conexiones con RabbitMQ antes de finalizar. En las pruebas realizadas, los contenedores de la aplicación registraron la recepción de la señal y finalizaron con código 0.

RabbitMQ también recibe `SIGTERM` durante `make down`, pero en algunas ejecuciones terminó con código 137. Ese código indica que Docker lo forzó a finalizar al vencer el límite de cinco segundos configurado para el apagado. En los logs observados, RabbitMQ alcanzó a detener listeners y message stores, pero no siempre terminó todo su proceso de cierre dentro de ese plazo. Por lo tanto, el resultado de esas pruebas confirma el cierre ordenado de los procesos de la aplicación, pero no un cierre limpio del contenedor de RabbitMQ en todas las ejecuciones.
