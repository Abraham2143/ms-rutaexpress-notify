# Verificación del consumidor mínimo de email

Fecha: 2026-10-15. Windows, Temurin Java 21.0.12.1, Spring Boot 4.1.1.

Comandos ejecutados desde la carpeta que contiene pom.xml:

```powershell
.\mvnw.cmd -B -ntp test
.\mvnw.cmd -B -ntp '-Drabbit.integration=true' clean verify
```

- test: BUILD SUCCESS; 31 casos, 0 fallos, 0 errores, 1 omitido
  (integración optativa con Docker).
- clean verify con integración habilitada: BUILD SUCCESS; 31 casos, 0 fallos,
  0 errores, 0 omitidos. JAR generado y reempaquetado.
- EmailListenerTests: 7 casos para texto UTF-8, JSON desconocido/malformado,
  vacío, null textual, metadata incompatible, bytes UTF-8 inválidos y fallo del ACK.
  Verifica que el log preceda al ACK y que no se emita otra disposición.
- Contexto: sin beans AmqpAdmin, Declarable ni Declarables; listeners detenidos
  durante esta prueba. No se requieren credenciales SMTP reales.
- ProcessingTests: 22 casos existentes conservados para los servicios previos;
  SMTP se prueba directamente mediante CommandHandler/EmailService y GreenMail,
  no mediante el listener actual de inspección.
- RabbitIntegrationTests: broker real aislado por Testcontainers. El fixture
  crea inicialmente solo q.cmd.email y verifica recepción de texto libre,
  JSON inesperado y vacío, sin que Notify cree colas warehouse/label.
  Luego el fixture aprovisiona la topología previa para comprobar regresiones
  de bindings, DLQ y artefactos de bodega/etiquetas. Email registra y confirma
  un mensaje de la propuesta v1 aunque SMTP esté deshabilitado.

La aplicación no declara topología; las declaraciones de esta integración son
exclusivamente preparación del broker de pruebas y no forman parte del runtime.
No se modificó el broker del usuario en localhost:5672. No se inició una instancia
adicional de Notify contra él. No hubo push ni despliegue.

El primer intento de compilación detectó que autoDeclare debe configurarse en
el contenedor mediante ContainerCustomizer, no directamente en la factory.
Se corrigió antes de las ejecuciones satisfactorias anteriores.
Las advertencias de deprecación en componentes previos y del agente Mockito
no impidieron el build. No se añadieron dependencias ni secretos.

## Verificación histórica de la implementación anterior

Lo siguiente corresponde al comportamiento anterior, no al listener mínimo actual.

# Verificación ejecutada

Fecha: 2026-10-15. Host Windows, Temurin Java 21.0.12.1, Spring Boot 4.1.1.

Comando final desde Notify:

```powershell
.\mvnw.cmd -B -ntp '-Drabbit.integration=true' clean verify
```

Resultado: **BUILD SUCCESS**, 24 pruebas, 0 fallos, 0 errores, 0 omitidas.

- ProcessingTests: 22 pruebas/casos, incluidas validación y JSON malformado,
  ACK posterior al efecto, NACK sin ACK de éxito, duplicados, concurrencia,
  fallos reales de almacenamiento/SMTP, error del contenedor, pérdida de ACK,
  contenido PDF y ajuste de texto al ancho de página.
- Tres de esos casos envían correos para ACEPTADO, EN_RUTA y ENTREGADO mediante
  sockets SMTP reales a GreenMail con puerto efímero, y comprueban destinatario,
  asunto y contenido. No se envía correo por un proveedor externo.
- MsRutaexpressNotifyApplicationTests: 1 prueba del contexto con listeners detenidos.
- RabbitIntegrationTests: 1 prueba con RabbitMQ **real**, imagen
  `rabbitmq:4.1-management-alpine` iniciada por Testcontainers y eliminada al terminar.
  Comprueba ambos exchanges de comandos, las tres DLQ con x-death, límites de
  patrones, aislamiento de colas, procesamiento de archivos y duplicados.

El primer intento de pruebas encontró un NullPointerException al etiquetar métricas
con una cola ausente en un error previo al listener. Se corrigió usando `unknown`;
la suite final anterior incluye esa regresión y pasó.

También se ejecutó correctamente:

```powershell
docker build -t rutaexpress-notify:local .
docker compose -f compose.dev.yaml --profile app config --quiet
docker image inspect rutaexpress-notify:local --format '{{.Config.User}}'
git diff --check
```

La imagen final declara usuario `10001:10001`. El build Docker ejecutó 24 casos,
0 fallos, 0 errores y 1 omitido: RabbitIntegrationTests se omite allí porque el build
no tiene socket Docker ni recibe `rabbit.integration=true`. Ese test sí se ejecutó
en el host con el comando final de verify. La imagen se construyó para Linux con JDK/JRE 21.

No se inició la aplicación con Compose, no se hizo despliegue, push ni cierre de issues.
No se verificó el flujo entre los servicios desplegados, Oracle, un SMTP externo ni
Webpush. Catalog sigue pendiente de implementar el endpoint de decremento; Shipments
necesita el productor fiable/outbox y acordar el contrato propuesto.

Los logs locales `verify-run.log` y `docker-build.log` están ignorados por Git;
los informes JUnit están en `target/surefire-reports` y se regeneran con las pruebas.
