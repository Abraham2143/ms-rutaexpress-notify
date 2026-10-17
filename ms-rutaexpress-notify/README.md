# RutaExpress Notify

## Estado actual: inspección de q.cmd.email

El listener de email recibe el cuerpo crudo, lo decodifica a String UTF-8 y registra
`Mensaje recibido de q.cmd.email: <payload>`. No exige JSON, DTO ni envelope y no
llama a SMTP, aunque EMAIL_ENABLED esté habilitado. Bytes UTF-8 inválidos se muestran
con caracteres de reemplazo. Se confirma con ACK manual después del log: esta prueba
consume el mensaje, no lo deja pendiente para enviarlo después.

Notify **no declara ni administra exchanges, colas o bindings**: se eliminó la
topología de RabbitConfig, `spring.rabbitmq.dynamic=false` deshabilita RabbitAdmin
y el contenedor tiene autoDeclare=false. `ms-rutaexpress-rabbitmq-admin` debe crear
`q.cmd.email` en el mismo virtual host antes del arranque. No requiere SMTP.

Bodega y etiquetas conservan su implementación, pero sus listeners arrancan solo
con WAREHOUSE_LISTENER_ENABLED=true y LABEL_LISTENER_ENABLED=true respectivamente.
Así la inspección inicial solo necesita q.cmd.email. Sus colas también deben ser
aprovisionadas externamente. Los servicios SMTP y el contrato anterior se conservan
para trabajo futuro y pruebas; el listener de email no los utiliza.

### Prueba local desde PowerShell

Desde esta carpeta (la que contiene pom.xml), usa JDK 21 y un RabbitMQ existente en
localhost:5672. Configura en tu terminal o en IntelliJ las variables RABBITMQ_HOST,
RABBITMQ_PORT, RABBITMQ_USERNAME y RABBITMQ_PASSWORD; opcionalmente RABBITMQ_VHOST
(por defecto `/`). No guardes credenciales reales en archivos del repositorio.

```powershell
$env:RABBITMQ_HOST = 'localhost'
$env:RABBITMQ_PORT = '5672'
# RABBITMQ_USERNAME y RABBITMQ_PASSWORD deben estar configuradas en este proceso.
.\mvnw.cmd spring-boot:run
```

Publica con el productor existente o desde RabbitMQ Management: en la cola
q.cmd.email, usa Publish message con texto libre, por ejemplo `prueba Notify`, o
un JSON de cualquier estructura. Comprueba el payload en el log de Notify.
No necesitas acordar un formato ni iniciar Mailpit. Detén otras instancias de
Notify que consuman la misma cola para que no reciban el mensaje de prueba.

```powershell
.\mvnw.cmd test
# Prueba optativa con broker aislado; requiere Docker:
.\mvnw.cmd '-Drabbit.integration=true' clean verify
```

El test de contexto verifica que no existan beans de administración/declaración.
La integración aprovisiona recursos exclusivamente como fixture dentro del broker
aislado de Testcontainers, representando al administrador externo.

El Compose anterior se conserva como entorno de desarrollo opcional; arrancar un
broker vacío con él **ya no crea las colas**. Usa rabbitmq-admin para aprovisionarlas.

## Implementación previa y propuesta de contrato (referencia)

El contenido siguiente describe los servicios previos y la propuesta v1. No es un
requisito del listener de inspección de email ni implica administración por Notify.


Worker Java 21 / Spring Boot 4.1.1. Consume comandos RabbitMQ, envía email SMTP,
genera tickets de picking UTF-8 y etiquetas PDF reales. No utiliza base de datos,
Kafka ni Azure Entra ID. No tiene API de negocio. BFF autentica las peticiones;
Shipments será responsable de producir los comandos.

## Arquitectura y estructura

`Shipments → cmd.direct o cmd.topic → colas → listeners → servicios → SMTP / volumen`.
Errores: `basicNack(requeue=false) → cmd.dead.dlx → DLQ del flujo`.

Código bajo `src/main/java/cl/rutaexpress/ms_rutaexpress_notify/`:

- `config/RabbitConfig`: ACK manual y errores del contenedor; sin topología.
- `contract/Command`, `CommandDecoder`: envelope, payload y validación estricta.
- `messaging/*Listener`: un listener pequeño por flujo; `CommandHandler` coordina
  validación, idempotencia, métricas y ACK/NACK.
- `processing/EmailService`, `WarehouseService`, `LabelService`: efectos reales.
- `processing/ArtifactStore`: archivos deterministas y publicación atómica.
- `processing/IdempotencyGuard`: registro acotado en memoria, sin BD.

Se consultaron los README de [BFF](https://github.com/Abraham2143/ms-rutaexpress-bff),
[Catalog](https://github.com/Abraham2143/ms-rutaexpress-catalog) y
[Shipments](https://github.com/Abraham2143/ms-rutaexpress-shipments).
No se inspeccionaron todos sus fuentes ni se ejecutaron esos servicios.
El contrato siguiente es una **propuesta de integración v1**, no un contrato RabbitMQ
ya implementado en Shipments. Los cambios están limitados a Notify.

## RabbitMQ

Exchanges durables, sin auto-delete: `cmd.direct` (direct), `cmd.topic` (topic),
`cmd.dead.dlx` (direct). Todas las colas son durables y no exclusivas.

| Cola | Binding cmd.direct | Binding cmd.topic | x-dead-letter-routing-key |
| --- | --- | --- | --- |
| q.cmd.email | email.send | email.* | email.dead |
| q.cmd.warehouse | warehouse.ticket | warehouse.# | warehouse.dead |
| q.cmd.label | label.gen | label.* | label.dead |

En la topología propuesta, el administrador externo configura `x-dead-letter-exchange=cmd.dead.dlx`.
Las colas `q.cmd.email.dlq`, `q.cmd.warehouse.dlq` y `q.cmd.label.dlq` están
enlazadas a ese exchange con `email.dead`, `warehouse.dead` y `label.dead`.
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

## Contrato JSON v1

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

Todos los campos del payload salvo la excepción de channel son obligatorios.
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
El servicio SMTP previo está implementado, pero el listener actual solo registra el payload. Propuesta para acordar con operaciones: generar ticket
de picking y etiqueta al persistir **ACEPTADO**, para preparar el envío antes de
EN_BODEGA/EN_RUTA. Si operaciones prefiere etiquetar al entrar EN_BODEGA, cambiar
el productor; Notify no realiza transiciones de estado ni decide cuándo emitir.

Shipments necesitará:

1. Acordar/versionar estos DTOs y el canal deseado, producir una instantánea válida
   y propagar traceId/correlationId. Generar eventId estable por comando.
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

En los flujos previos de bodega/etiqueta, los listeners reciben el mensaje crudo; la conversión JSON ocurre dentro del
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

Registro en memoria por `flujo:eventId`, TTL 24 horas, máximo 10.000 éxitos;
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
| EMAIL_ENABLED | false; comandos EMAIL fallan si no está habilitado |
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
clase de error sin excepción completa. Excepción temporal: el listener de email
registra el cuerpo completo para inspección; utiliza datos de prueba y restringe
el acceso a esos logs. No publiques secretos en los mensajes.

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
DLQ, y ejecuta los listeners para archivos, duplicados y SMTP deshabilitado.
ProcessingTests verifica ACK posterior al efecto, fallos sin ACK, JSON inválido,
validación, concurrencia, error antes del listener, pérdida de ACK, contenido del PDF
y envío SMTP real con GreenMail para los tres estados.
No prueba Oracle, Shipments/Catalog desplegados ni entregabilidad de un proveedor SMTP.

El wrapper se fijó en Maven 3.9.16; Java y Spring Boot se conservaron.
Consultar [docs/VERIFICATION.md](docs/VERIFICATION.md) para los comandos realmente ejecutados.
