# RutaExpress Notify

Worker Java 21 / Spring Boot 4.1.1. Consume comandos RabbitMQ, envía email SMTP, genera tickets de
picking UTF-8 y etiquetas PDF reales. No utiliza base de datos, Kafka ni Azure Entra ID. No tiene API de
negocio. BFF autentica las peticiones; Shipments produce los eventos.

## Estado actual

Notify consume las tres colas de comandos con **el mismo flujo**: decodificar y validar, descartar
duplicados por `eventId`, ejecutar el efecto y **solo entonces** confirmar con `basicAck`. Cualquier
fallo (mensaje inválido, SMTP, almacenamiento) recibe `basicNack(requeue=false)` y RabbitMQ lo envía a
la DLQ de su cola. Nunca se registra un éxito si el efecto no se completó.

| Cola | Consumidor | Efecto | Cómo se activa |
| --- | --- | --- | --- |
| `q.cmd.email` | `EmailListener` → `EmailService` | Correo SMTP | Siempre arranca. El envío real exige `EMAIL_ENABLED=true` |
| `q.cmd.warehouse` | `WarehouseListener` → `WarehouseService` | Ticket de picking `.txt` | `WAREHOUSE_LISTENER_ENABLED=true` |
| `q.cmd.label` | `LabelListener` → `LabelService` | Etiqueta PDF | `LABEL_LISTENER_ENABLED=true` |

`q.cmd.email` acepta dos formatos: la notificación plana que publica Shipments y el envelope v1
(ver [Contrato JSON](#contrato-json)). Bodega y etiqueta aceptan únicamente el envelope v1.

### Correo con SMTP desactivado (`EMAIL_ENABLED=false`, valor por defecto)

Es una configuración válida y es la recomendada mientras no existan credenciales SMTP. `EmailService`
rechaza todo comando con `smtp_channel_disabled`: no se llama a SMTP, el mensaje recibe NACK sin
reencolado y termina en `q.cmd.email.dlq`. No se cuenta como procesado ni se registra en la
idempotencia, de modo que se puede **republicar desde la DLQ con el mismo `eventId`** cuando SMTP esté
configurado. Para demostrar el manejo de errores basta publicar un mensaje válido y ver cómo llega a la
DLQ con `x-first-death-reason=rejected`.

### Privacidad de los logs

Notify ya no registra el cuerpo de los mensajes ni datos del destinatario (nombre, email, direcciones).
Cada mensaje produce una línea con `eventId`, `traceId`, `correlationId`, cola, resultado
(`processed`, `duplicate` o `failed`) y, si falló, un código de motivo propio (`invalid_command`,
`smtp_channel_disabled`, …) o solo el nombre de la clase de la excepción. Los mensajes de excepción de
Jackson o SMTP no se registran porque pueden citar el contenido. Aun así, la DLQ y los artefactos
contienen datos personales: restringe su acceso.

### Variables de entorno para AWS

| Variable | Valor | Motivo |
| --- | --- | --- |
| `RABBITMQ_HOST` | Host del broker (no `localhost` dentro de Docker) | Obligatoria en AWS |
| `RABBITMQ_USERNAME` / `RABBITMQ_PASSWORD` | Usuario dedicado de Notify | Obligatorias, sin valor por defecto |
| `RABBITMQ_PORT` / `RABBITMQ_VHOST` | `5672` / `/` | Solo si difieren |
| `WAREHOUSE_LISTENER_ENABLED` | `true` | Sin ella `q.cmd.warehouse` queda sin consumidor |
| `LABEL_LISTENER_ENABLED` | `true` | Sin ella `q.cmd.label` queda sin consumidor |
| `EMAIL_ENABLED` | `false` | No enviar correo real; los comandos van a la DLQ |
| `NOTIFY_STORAGE_PATH` | `/app/data` (ya fijado en la imagen) | Monta un volumen para ticket y etiqueta |

No definas `SMTP_*` mientras `EMAIL_ENABLED=false`. Las colas y exchanges deben existir antes de arrancar:
los crea `ms-rutaexpress-rabbitmq-admin`, Notify no declara topología (`spring.rabbitmq.dynamic=false`,
`autoDeclare=false`). Ejemplo de ejecución en Docker (los valores entre `<>` los das tú, nunca los
guardes en el repositorio):

```bash
docker run -d --name notify --restart unless-stopped \
  -e RABBITMQ_HOST=<host-del-broker> -e RABBITMQ_USERNAME=<usuario> -e RABBITMQ_PASSWORD=<clave> \
  -e WAREHOUSE_LISTENER_ENABLED=true -e LABEL_LISTENER_ENABLED=true -e EMAIL_ENABLED=false \
  -v notify-data:/app/data rutaexpress-notify
```

### Prueba local desde PowerShell

Desde esta carpeta (la que contiene pom.xml), usa JDK 21 y un RabbitMQ existente con la topología ya
creada por rabbitmq-admin. Configura en tu terminal o en IntelliJ `RABBITMQ_HOST`, `RABBITMQ_PORT`,
`RABBITMQ_USERNAME` y `RABBITMQ_PASSWORD` (opcionalmente `RABBITMQ_VHOST`, por defecto `/`). No guardes
credenciales reales en archivos del repositorio.

```powershell
$env:RABBITMQ_HOST = 'localhost'
$env:RABBITMQ_PORT = '5672'
# RABBITMQ_USERNAME y RABBITMQ_PASSWORD deben estar configuradas en este proceso.
$env:WAREHOUSE_LISTENER_ENABLED = 'true'
$env:LABEL_LISTENER_ENABLED = 'true'
.\mvnw.cmd spring-boot:run
```

Publica desde RabbitMQ Management (exchange `cmd.direct`, routing key `email.send`) el contenido de
[examples/email-shipments.json](examples/email-shipments.json) o de [examples/email.json](examples/email.json).
Con `EMAIL_ENABLED=false` el mensaje aparece en `q.cmd.email.dlq`. Para ver un envío real sin salir a
internet usa Mailpit (ver Compose). Detén otras instancias de Notify que consuman la misma cola.

```powershell
.\mvnw.cmd test
# Prueba optativa con broker aislado; requiere Docker:
.\mvnw.cmd '-Drabbit.integration=true' clean verify
```

El test de contexto verifica que no existan beans de administración/declaración. La integración
aprovisiona recursos exclusivamente como fixture dentro del broker aislado de Testcontainers,
representando al administrador externo.

El Compose se conserva como entorno de desarrollo opcional; arrancar un broker vacío con él **no crea
las colas**. Usa rabbitmq-admin para aprovisionarlas. Compose activa `EMAIL_ENABLED=true` contra Mailpit
solo para desarrollo.

## Arquitectura y estructura

`Shipments → cmd.direct o cmd.topic → colas → listeners → servicios → SMTP / volumen`.
Errores: `basicNack(requeue=false) → cmd.dead.dlx → DLQ del flujo`.

Código bajo `src/main/java/cl/rutaexpress/ms_rutaexpress_notify/`:

- `config/RabbitConfig`: ACK manual y errores del contenedor; sin topología.
- `contract/Command`, `CommandDecoder`: envelope, payload y validación estricta.
- `contract/ShipmentNotificationFormat`: convierte la notificación plana de Shipments (solo email) al mismo `Command`.
- `messaging/*Listener`: un listener pequeño por flujo; `CommandHandler` coordina
  validación, idempotencia, métricas y ACK/NACK.
- `processing/EmailService`, `WarehouseService`, `LabelService`: efectos reales.
- `processing/ArtifactStore`: archivos deterministas y publicación atómica.
- `processing/IdempotencyGuard`: registro acotado en memoria, sin BD.

El formato plano se contrastó con el código de Shipments (`ShipmentNotification` y
`ShipmentNotificationPublisher`); el envelope v1 es el contrato propio de Notify, ilustrado en
`examples/`. No se ejecutaron los demás servicios y los cambios están limitados a Notify.

## RabbitMQ

Exchanges durables, sin auto-delete: `cmd.direct` (direct), `cmd.topic` (topic),
`cmd.dead.dlx` (direct). Todas las colas son durables y no exclusivas.

| Cola | Binding cmd.direct | Binding cmd.topic | x-dead-letter-routing-key |
| --- | --- | --- | --- |
| q.cmd.email | email.send | email.* | email.dlq |
| q.cmd.warehouse | warehouse.ticket | warehouse.# | warehouse.dlq |
| q.cmd.label | label.gen | label.* | label.dlq |

Esta es la topología que declara `ms-rutaexpress-rabbitmq-admin` (Notify no la modifica): cada cola
principal tiene `x-dead-letter-exchange=cmd.dead.dlx`. Las colas `q.cmd.email.dlq`,
`q.cmd.warehouse.dlq` y `q.cmd.label.dlq` están enlazadas a ese exchange con `email.dlq`,
`warehouse.dlq` y `label.dlq`.
No hay TTL, consumidor de DLQ ni retorno automático a la cola principal.
Una topología preexistente incompatible exige migración operativa; RabbitMQ no
permite cambiar argumentos de una cola existente por redeclaración.

**El productor debe publicar cada comando una sola vez, eligiendo UNO de los dos
exchanges.** Publicarlo por ambos entrega dos copias a la misma cola. La deduplicación
no sustituye esta regla. Usar mensajes persistentes (`delivery_mode=2`), publisher
confirms, mandatory/returns y conservar eventId al reintentar.

Autenticar RabbitMQ con usuario y vhost propios de Notify, nunca con credenciales
del BFF. Notify no necesita administrar recursos; el operador configura permisos para
consumir las colas habilitadas. La administración pertenece a rabbitmq-admin. El productor solo necesita publicar sobre
los exchanges de comandos. TLS en producción (`RABBITMQ_SSL_ENABLED=true`, normalmente
puerto 5671), certificados de confianza del JRE y redes privadas.
Este repositorio no construye el clúster de producción ni infraestructura AWS.

## Contrato JSON

`q.cmd.email` acepta dos formatos; `q.cmd.warehouse` y `q.cmd.label` solo el envelope v1. Ambos formatos
comparten límite de tamaño (64 KiB) y se analizan de forma estricta. En cualquier otro caso el
mensaje se rechaza (NACK sin reencolado, DLQ).

### Envelope v1 (email, bodega y etiqueta)

Enviar JSON UTF-8 con `content_type=application/json`, tamaño máximo 64 KiB.
Se rechazan JSON inválido, propiedades desconocidas, claves repetidas, JSON adicional,
coerción de strings a números y números decimales en campos enteros.

| Campo del envelope | Tipo / regla |
| --- | --- |
| type | String exacto: email.send.v1, warehouse.ticket.v1 o label.gen.v1, acorde a la cola |
| eventId | UUID obligatorio, identidad inmutable de este comando; distinto por efecto/flujo/estado |
| timestamp | Fecha/hora ISO-8601 con offset obligatorio, p. ej. 2026-10-15T18:00:00-03:00 o Z |
| traceId | String obligatorio, 1–128 caracteres ASCII alfanuméricos, guion o guion bajo |
| correlationId | Mismo formato; relaciona comandos de un envío u operación |
| payload | Objeto obligatorio con los campos siguientes |

No se impone caducidad del timestamp: se permite recuperar mensajes antiguos de DLQ.
Shipments conserva fechas locales sin zona: el productor deberá acordar la zona
operativa (propuesta: America/Santiago) y convertir a offset, considerando horario de verano.
No adjuntar JWT, credenciales ni objetos completos del modelo de persistencia.

| Campo del payload | Tipo / validación |
| --- | --- |
| id | Entero Long positivo, ID del envío |
| trackingNumber | String obligatorio, máximo 80, ASCII alfanumérico, guion o guion bajo |
| destinatarioNombre | String no vacío, máximo 160, sin caracteres de control |
| destinatarioEmail | Email no vacío, máximo 254, sin caracteres de control |
| direccionOrigen, direccionDestino | Strings no vacíos, máximo 300 cada uno, sin caracteres de control |
| servicioId | Entero Long positivo |
| status | CREADO, ACEPTADO, EN_BODEGA, EN_RUTA, ENTREGADO o CANCELADO |
| channel | EMAIL o WEBPUSH, obligatorio para email.send.v1; omitido/null para bodega/etiqueta |

Todos los campos del payload salvo la excepción de channel son obligatorios en el envelope v1.
La propuesta usa una instantánea común con los datos disponibles en Shipments;
no inventa SKU, pesos, cantidades, teléfono ni capacidad. `clienteId`, `creadoPor`
y fechas del modelo no se necesitan para estos efectos y no se transmiten.
Email solo admite ACEPTADO, EN_RUTA o ENTREGADO. Bodega/etiqueta admiten los estados
del modelo para permitir reprocesar; sus momentos de emisión se acuerdan aparte.

Ejemplos completos: [email](examples/email.json), [bodega](examples/warehouse.json),
[etiqueta](examples/label.json). Para EN_RUTA y ENTREGADO usar el mismo esquema de
email con el estado correspondiente y **un nuevo eventId**. Reintentar un mismo
comando conserva eventId y contenido; no reutilizar IDs con datos distintos.
Los ejemplos contienen datos ficticios.

### Formato plano de Shipments (solo `q.cmd.email`)

Es lo que publica hoy `ShipmentNotificationPublisher` en `cmd.direct` con la clave `email.send`
([ejemplo](examples/email-shipments.json)):

```json
{"shipmentId":42,"trackingNumber":"RUTA-0123456789AB","status":"ACEPTADO",
 "destinatarioNombre":"Ana Pérez","destinatarioEmail":"ana@example.com","timestamp":"2026-10-18T19:30:00.123456Z"}
```

| Campo | Tipo y regla |
| --- | --- |
| shipmentId | Número entero positivo (`Long`). Se rechazan texto, decimales, `null` y desbordes |
| trackingNumber | Texto, máx. 80, ASCII alfanumérico, guion o guion bajo |
| status | `ACEPTADO`, `EN_RUTA` o `ENTREGADO`. `CREADO`, `EN_BODEGA`, `CANCELADO` o un valor desconocido se rechazan |
| destinatarioNombre | Texto no vacío, máx. 160, sin caracteres de control |
| destinatarioEmail | Email, máx. 254, sin caracteres de control |
| timestamp | Texto ISO-8601 en UTC terminado en `Z` (el `Instant` de Shipments), p. ej. `2026-10-18T19:30:00.123456Z` |

Los seis campos son obligatorios y no puede haber otros: se rechazan propiedades desconocidas, claves
repetidas, JSON adicional, `null` y tipos distintos. Si el objeto trae `type`, `payload` o `eventId` se
trata como envelope v1 (y fallará por propiedades desconocidas), nunca como una mezcla.

Shipments no envía `eventId`, `traceId`, `correlationId`, direcciones ni `servicioId`, así que Notify los
completa al convertir el mensaje al mismo modelo interno:

- `eventId`: UUID determinista (por nombre) calculado con `shipmentId`, `status` y el instante
  normalizado a segundos y nanosegundos. **El mismo evento reentregado da el mismo `eventId`** (se
  descarta como duplicado) y estados distintos del mismo envío dan identidades distintas, por lo que
  ACEPTADO, EN_RUTA y ENTREGADO se envían los tres. `…:00Z` y `…:00.000Z` son el mismo instante.
- `traceId` y `correlationId`: `shipment-<shipmentId>`.
- `channel`: `EMAIL`. Las direcciones y `servicioId` quedan ausentes: el correo omite la línea `Destino`.

Si Shipments reintentara una publicación creando un `Instant` nuevo, se consideraría otro evento; hoy no
reintenta. Para el envelope v1 el `eventId` lo aporta el productor.

Publicación de desarrollo desde PowerShell, con el broker de Compose iniciado y
Notify conectado y topología ya aprovisionada por rabbitmq-admin:

```powershell
$credential = Get-Credential -UserName notify_dev
$example = Get-Content -Raw examples/email.json
$body = @{ properties = @{ content_type = 'application/json'; delivery_mode = 2 }; routing_key = 'email.send'; payload = $example; payload_encoding = 'string' } | ConvertTo-Json -Depth 5
Invoke-RestMethod -Method Post -Uri 'http://localhost:15672/api/exchanges/%2F/cmd.direct/publish' -Authentication Basic -Credential $credential -AllowUnencryptedAuthentication -ContentType 'application/json' -Body ([Text.Encoding]::UTF8.GetBytes($body))
```

El comando de HTTP usa PowerShell 7 y Basic solo contra localhost. La contraseña de
Compose es `local_only_change_me`, exclusiva de desarrollo. Verificar `routed: true`.
Para bodega usar `examples/warehouse.json` y `warehouse.ticket`; para etiqueta,
`examples/label.json` y `label.gen`. Alternativa: publicar en `cmd.topic` con las
mismas routing keys (coinciden con los patrones), sin volver a publicar por direct.
Repetir un ejemplo conserva eventId y produce un duplicado intencional.
La API management es una herramienta de prueba; el productor real usará AMQP.

## Momentos e integración pendiente

Requisito explícito: notificación email/push en **ACEPTADO, EN_RUTA y ENTREGADO**.
`EmailService` envía el correo cuando `EMAIL_ENABLED=true`; con `false` los comandos van a la DLQ. Propuesta para acordar con operaciones: generar ticket
de picking y etiqueta al persistir **ACEPTADO**, para preparar el envío antes de
EN_BODEGA/EN_RUTA. Si operaciones prefiere etiquetar al entrar EN_BODEGA, cambiar
el productor; Notify no realiza transiciones de estado ni decide cuándo emitir.

Shipments necesitará:

1. Hoy Shipments publica **solo** el formato plano en `email.send`, en ACEPTADO, EN_RUTA y ENTREGADO. No
   publica `warehouse.ticket` ni `label.gen`, así que esas colas no reciben tickets ni etiquetas mientras
   no lo haga. Tampoco envía `eventId`, `traceId` ni `correlationId` (Notify los deriva) ni usa publisher
   confirms: si el publish falla se pierde la notificación, como indica su propio código.
2. Insertar comandos en una **outbox en Oracle dentro de la misma transacción** que
   persiste el cambio de estado. Un relay publicará y marcará enviados usando
   confirms y detección de mensajes no enrutados. Reintentos acotados/alertas y
   publicación al menos una vez con eventId estable.
3. Evitar publicar antes del commit. Guardar en Oracle y publicar en RabbitMQ como
   operaciones independientes tiene dos fallos: commit sin publicación pierde la
   notificación; publicación antes de rollback notifica un estado inexistente.
   Incluso publicar después del commit deja una ventana de caída. Un callback
   afterCommit por sí solo no resuelve la pérdida. La outbox pertenece a Shipments,
   **no se implementa en Notify**.
4. Completar el endpoint externo pendiente: Shipments llama a
   `POST /api/catalog/services/{serviceId}/capacity/decrement`, pero el Catalog
   revisado no lo incluye. Sin resolverlo no se puede validar el flujo completo
   de aceptación. No se modificó Catalog.

Webpush está **deshabilitado sin opción de activación ficticia**. WEBPUSH en la cola
email se valida y falla de manera controlada hacia DLQ. Para implementarlo faltan:
suscripción por destinatario/dispositivo (`endpoint` HTTPS, `keys.p256dh`, `keys.auth`,
expiración opcional), origen/consentimiento, contrato de contenido (title, body,
URL permitida, tag/TTL), política para varias suscripciones, VAPID subject/public
key/private key, rotación y manejo de 404/410/429. Acordar además el transporte
seguro de suscripciones y mitigación SSRF del endpoint. Estos campos aún no forman
parte del payload aceptado. No se simula éxito. Si se desean ambos canales,
emitir dos comandos con eventId distintos, una vez implementado push.

## ACK/NACK, DLQ e idempotencia

En los tres flujos (email, bodega y etiqueta) los listeners reciben el mensaje crudo y delegan en `CommandHandler`; la conversión JSON ocurre dentro del
handler. Solo después de validar, ejecutar y registrar éxito se envía
`basicAck(tag, false)`. Un duplicado registrado se confirma sin repetir el efecto.
Todo fallo de validación, SMTP, canal o almacenamiento provoca
`basicNack(tag, false, false)`. RabbitMQ enruta el mensaje original a su DLQ.
No hay reintentos automáticos ni requeue de fallos de aplicación; se evita el bucle
infinito. Operaciones debe reparar la causa y republicar desde DLQ conservando el
eventId. Timeout SMTP es ambiguo: el servidor podría haber aceptado el correo.

El error handler del contenedor también fuerza rechazo en modo MANUAL para errores
del adaptador anteriores al listener. No imprime el cuerpo ni el texto de la
excepción. Un fallo de transporte al confirmar/rechazar puede dejar la disposición
sin llegar al broker; tras cerrar el canal RabbitMQ puede reentregar. Las caídas
de conexión no se pueden convertir en una garantía de entrega a DLQ.

Registro en memoria por `flujo:eventId` (para Shipments, el `eventId` derivado), TTL 24 horas, máximo 10.000 éxitos;
al llenarse expulsa el más antiguo. Un bloqueo en esta JVM serializa
check/efecto/registro, incluso con entregas concurrentes. Se registra antes del ACK
y solo tras éxito; una pérdida del ACK puede deduplicarse mientras viva la entrada.
El bloqueo incluye SMTP, por lo que su timeout puede retrasar los demás flujos.
Los límites se ajustan por variables; se recomienda **una réplica** para este diseño.

No se promete exactly-once. Un reinicio, TTL o expulsión pierde la deduplicación
de SMTP; varias réplicas tienen mapas independientes. Una caída entre aceptación
SMTP y registro puede duplicar el email; los servidores SMTP tampoco ofrecen una
transacción común con RabbitMQ. Escalar requiere acordar un registro compartido
o un proveedor con claves idempotentes. No se agregó una base de datos a Notify.

Archivos: `<raíz>/warehouse/<eventId>.txt` y `<raíz>/label/<eventId>.pdf`.
Nunca se construyen rutas con tracking/nombres del destinatario. Se escribe un
temporal en el mismo directorio y se mueve con ATOMIC_MOVE; si el filesystem no lo
soporta se falla hacia DLQ. Un archivo final existente se conserva ante reentregas,
también después de reiniciar; no regenerar un eventId con otro contenido. Se
rechazan symlinks en directorio de flujo/archivo. La raíz y sus padres son
configuración confiable y deben estar protegidos contra escritura de otros usuarios.
Los archivos finales se consideran propiedad exclusiva del servicio e íntegros;
no hay validación contra modificaciones externas. No usar múltiples réplicas
escribiendo en el mismo volumen. Una caída puede dejar temporales `.pending-*`;
limpiarlos con el worker detenido. No hay fsync ni garantía contra pérdida física
del disco. El volumen persiste artefactos, no el mapa de idempotencia.

Los PDF usan Helvetica estándar, soportan español, parten líneas largas y reemplazan
glifos no soportados por `?`. Pendiente incrustar una fuente Unicode si se requieren
otros alfabetos. Son etiquetas legibles, sin código de barras ni formato de impresora
específico. No se inventan esos requisitos.

## Configuración

| Variable | Valor inicial / uso |
| --- | --- |
| RABBITMQ_HOST / RABBITMQ_PORT | localhost / 5672 |
| RABBITMQ_USERNAME / RABBITMQ_PASSWORD | Obligatorias; usuario dedicado |
| RABBITMQ_VHOST | / |
| RABBITMQ_SSL_ENABLED | false; activar TLS en producción |
| EMAIL_ENABLED | false; con false los comandos EMAIL fallan con `smtp_channel_disabled` y van a la DLQ |
| WAREHOUSE_LISTENER_ENABLED | false; `true` para consumir q.cmd.warehouse |
| LABEL_LISTENER_ENABLED | false; `true` para consumir q.cmd.label |
| SMTP_HOST / SMTP_PORT | localhost / 1025 |
| SMTP_USERNAME / SMTP_PASSWORD | Vacíos para servidor local; secretos por entorno |
| SMTP_AUTH / SMTP_STARTTLS | false / false; STARTTLS exige negociación si true |
| SMTP_FROM | notify@example.invalid; configurar dirección autorizada |
| NOTIFY_STORAGE_PATH | ./data; /app/data en Docker |
| IDEMPOTENCY_CAPACITY / IDEMPOTENCY_TTL_SECONDS | 10000 / 86400, positivos |
| MANAGEMENT_ADDRESS / MANAGEMENT_PORT | 127.0.0.1 / 8083 |

SMTP tiene timeouts de conexión 5s y lectura/escritura 10s. Usar el puerto de
submission STARTTLS del proveedor al salir de desarrollo, con auth y credenciales
externas. Mailpit permite inspeccionar mensajes en http://localhost:8025 sin enviarlos
a destinatarios reales. GreenMail se usa solo en tests con SMTP por socket.
Spring Boot no carga `.env` automáticamente. No hay secretos reales en la imagen.

## Ejecución local y Docker

Requiere JDK 21. Desde esta carpeta, PowerShell:

```powershell
docker compose -f compose.dev.yaml up -d rabbitmq mailpit
$env:RABBITMQ_USERNAME='notify_dev'
$env:RABBITMQ_PASSWORD='local_only_change_me'
$env:EMAIL_ENABLED='true'
$env:SMTP_FROM='notify@example.com'
.\mvnw.cmd spring-boot:run
```

Para ejecutar también Notify en Docker:

```powershell
docker compose -f compose.dev.yaml --profile app up -d --build
docker compose -f compose.dev.yaml logs -f notify
curl.exe http://localhost:8083/actuator/health
```

Detener la instancia Java local antes de usar el perfil app. Compose es
**exclusivo de desarrollo** y está separado de cualquier despliegue de producción.
Persiste broker y artefactos en volúmenes `rabbit-dev` y `notify-data`; `down` los
conserva, `down -v` los elimina. Ajustar permisos del volumen al UID/GID 10001.

```powershell
.\mvnw.cmd clean package
java -jar target/ms-rutaexpress-notify-0.0.1-SNAPSHOT.jar
docker build -t rutaexpress-notify .
```

La imagen usa Maven/JDK 21 para build y JRE 21 sin root para ejecución. En producción
montar volumen en `/app/data`, suministrar las variables por el gestor de secretos
y conectar al RabbitMQ y SMTP existentes. No publicar puertos de negocio. Si se
monitorea desde otro contenedor, configurar MANAGEMENT_ADDRESS=0.0.0.0 y restringir
8083 mediante la red privada/firewall; no enviarlo por BFF, ALB público ni API Gateway.
El valor por defecto escucha solo loopback. No se incluye autenticación HTTP:
health y métricas confían en ese aislamiento de red, no son endpoints públicos.

## Observabilidad y operación

`/actuator/health` no expone detalles; Rabbit y SMTP habilitado participan en health.
`/actuator/metrics` permite consultar los contadores Micrometer:

- `notify.processed`: efectos terminados; puede incrementarse antes de que llegue el ACK.
- `notify.failed`: fallos de procesamiento o del contenedor.
- `notify.duplicates`: entregas omitidas por el registro en memoria.
- `notify.dlq.rejections`: **rechazos solicitados**, con tag `stage=application|container`.

Tags de cardinalidad acotada por cola. eventId/traceId/correlationId se incluyen
en logs, no en tags. Mensajes ilegibles/no validados usan `unknown`; no se extraen
campos sin validar para evitar inyección en logs. Se registra cola, resultado y
código de motivo propio o clase de error, sin excepción completa. Ningún listener registra el
cuerpo del mensaje ni datos del destinatario (hay pruebas que lo comprueban).

Un incremento de rechazos **no acredita un mensaje observado en DLQ**. Monitorizar
por separado profundidad y tasas reales de las DLQ usando management/Prometheus de
RabbitMQ, con alertas por mensajes pendientes. No se consumen DLQ para contar:
permanecen disponibles para diagnóstico y replay. Durable no significa entrega
infalible: el dead-lettering estándar de colas clásicas puede perder mensajes si
el destino no está disponible. La topología solicitada no fuerza quorum ni
dead-lettering at-least-once; acordar esas políticas con el operador de producción.
Aplicar acceso restringido, retención y eliminación de DLQ/artefactos porque contienen
datos personales. No hay política automática de borrado implementada.

## Pruebas

```powershell
.\mvnw.cmd test
# Incluye RabbitMQ real; requiere Docker Linux disponible. Falla si el broker no puede iniciarse.
.\mvnw.cmd '-Drabbit.integration=true' clean verify
```

Sin la propiedad, RabbitIntegrationTests se omite explícitamente; las pruebas unitarias
no se presentan como validación del broker. La integración declara los exchanges/colas
reales, verifica ambos bindings, límites de comodines, aislamiento y x-death de las tres
DLQ, y ejecuta los listeners para archivos, duplicados y el rechazo a la DLQ del correo con SMTP
deshabilitado.

- `EmailListenerTests`: el consumidor de email con `JavaMailSender` y `Channel` simulados (sin SMTP ni
  broker): ambos formatos, ACK posterior al envío, duplicados, estados distintos del mismo envío,
  mensajes inválidos, SMTP desactivado y caído, fallo del ACK y ausencia de datos personales en logs.
- `WarehouseLabelListenerTests`: los otros dos consumidores con servicios simulados: éxito, duplicado,
  error de procesamiento con reintento, mensajes inválidos o de otro flujo y NACK sin reencolado.
- `CommandDecoderTests`: ambos contratos, derivación determinista del `eventId` y 76 entradas
  inválidas (tipos, `timestamp`, estados, propiedades desconocidas, claves repetidas).
- `ListenerActivationTests`: cola de cada consumidor y variables de entorno que los habilitan.
- `ProcessingTests`: ACK posterior al efecto, fallos sin ACK, JSON inválido, validación, concurrencia,
  error antes del listener, pérdida de ACK, contenido del PDF y envío SMTP real con GreenMail para los
  tres estados.
No prueba Oracle, Shipments/Catalog desplegados ni entregabilidad de un proveedor SMTP.

El wrapper se fijó en Maven 3.9.16; Java y Spring Boot se conservaron.
Consultar [docs/VERIFICATION.md](docs/VERIFICATION.md) para los comandos realmente ejecutados.
