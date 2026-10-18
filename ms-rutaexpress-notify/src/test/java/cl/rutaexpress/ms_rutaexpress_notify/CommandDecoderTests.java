package cl.rutaexpress.ms_rutaexpress_notify;

import cl.rutaexpress.ms_rutaexpress_notify.contract.Command;
import cl.rutaexpress.ms_rutaexpress_notify.contract.CommandDecoder;
import jakarta.validation.Validation;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import static org.junit.jupiter.api.Assertions.*;

/** Contract tests: the v1 envelope and the flat notification published by ms-rutaexpress-shipments. */
class CommandDecoderTests {
    private static final String TS = "2026-10-18T19:30:00.123456Z";
    private final CommandDecoder decoder = new CommandDecoder(Validation.buildDefaultValidatorFactory().getValidator());

    private Command decode(String flow, String json) throws Exception {
        return decoder.decode(json.getBytes(StandardCharsets.UTF_8), flow);
    }
    private static String flat(String status) { return Fixtures.shipmentsJson("42", status, TS); }
    private static String v1(String flow) { return new String(Fixtures.json(flow, UUID.randomUUID()), StandardCharsets.UTF_8); }

    // ---------------------------------------------------------------- accepted formats

    @ParameterizedTest @ValueSource(strings = {"email", "warehouse", "label"})
    void v1EnvelopeIsStillAcceptedByEveryFlow(String flow) throws Exception {
        Command command = decode(flow, v1(flow));
        assertEquals(flow.equals("email") ? "email.send.v1" : flow.equals("warehouse") ? "warehouse.ticket.v1" : "label.gen.v1", command.type());
        assertEquals("Avenida Ejemplo 123, Santiago", command.payload().direccionDestino());
        assertEquals(1L, command.payload().servicioId());
    }

    @ParameterizedTest @ValueSource(strings = {"ACEPTADO", "EN_RUTA", "ENTREGADO"})
    void shipmentsFormatIsConvertedToTheInternalCommand(String status) throws Exception {
        Command command = decode("email", flat(status));
        assertEquals("email.send.v1", command.type());
        assertEquals("shipment-42", command.traceId());
        assertEquals("shipment-42", command.correlationId());
        assertEquals("2026-10-18T19:30:00.123456Z", command.timestamp().toString());
        var payload = command.payload();
        assertEquals(42L, payload.id());
        assertEquals("RUTA-0123456789AB", payload.trackingNumber());
        assertEquals("Ana Pérez", payload.destinatarioNombre());
        assertEquals("ana@example.com", payload.destinatarioEmail());
        assertEquals(status, payload.status());
        assertEquals("EMAIL", payload.channel());
        assertNull(payload.direccionOrigen());
        assertNull(payload.direccionDestino());
        assertNull(payload.servicioId());
    }

    // ---------------------------------------------------------------- deterministic eventId

    @Test void sameEventGivesSameEventIdAcrossRedeliveries() throws Exception {
        assertEquals(decode("email", flat("ACEPTADO")).eventId(), decode("email", flat("ACEPTADO")).eventId());
    }

    @Test void differentStatusShipmentOrInstantDoNotCollide() throws Exception {
        UUID base = decode("email", flat("ACEPTADO")).eventId();
        assertNotEquals(base, decode("email", flat("EN_RUTA")).eventId());
        assertNotEquals(base, decode("email", flat("ENTREGADO")).eventId());
        assertNotEquals(base, decode("email", Fixtures.shipmentsJson("43", "ACEPTADO", TS)).eventId());
        assertNotEquals(base, decode("email", Fixtures.shipmentsJson("42", "ACEPTADO", "2026-10-18T19:30:01.123456Z")).eventId());
    }

    @Test void equivalentInstantRepresentationsAndFieldOrderGiveTheSameEventId() throws Exception {
        assertEquals(decode("email", Fixtures.shipmentsJson("42", "ACEPTADO", "2026-10-18T19:30:00Z")).eventId(),
                decode("email", Fixtures.shipmentsJson("42", "ACEPTADO", "2026-10-18T19:30:00.000Z")).eventId());
        String reordered = "{\"timestamp\":\"" + TS + "\",\"destinatarioEmail\":\"ana@example.com\","
                + "\"destinatarioNombre\":\"Ana Pérez\",\"status\":\"ACEPTADO\","
                + "\"trackingNumber\":\"RUTA-0123456789AB\",\"shipmentId\":42}";
        assertEquals(decode("email", flat("ACEPTADO")).eventId(), decode("email", reordered).eventId());
    }

    // ---------------------------------------------------------------- rejected input

    static Stream<Arguments> invalidInputs() {
        String ok = flat("ACEPTADO");
        String v1 = v1("email");
        return Stream.of(
                // not JSON / wrong JSON shape
                Arguments.of("free text", "email", "Hola José"),
                Arguments.of("unterminated JSON", "email", "{json sin terminar"),
                Arguments.of("empty body", "email", ""),
                Arguments.of("null literal", "email", "null"),
                Arguments.of("empty object", "email", "{}"),
                Arguments.of("array", "email", "[]"),
                Arguments.of("number", "email", "42"),
                Arguments.of("unknown JSON object", "email", "{\"nuevoCampo\":42}"),
                // flat format: status
                Arguments.of("status CREADO not notifiable", "email", flat("CREADO")),
                Arguments.of("status EN_BODEGA not notifiable", "email", flat("EN_BODEGA")),
                Arguments.of("status CANCELADO not notifiable", "email", flat("CANCELADO")),
                Arguments.of("unknown status", "email", flat("PERDIDO")),
                Arguments.of("lowercase status", "email", flat("aceptado")),
                // flat format: shipmentId types
                Arguments.of("shipmentId string", "email", Fixtures.shipmentsJson("\"42\"", "ACEPTADO", TS)),
                Arguments.of("shipmentId decimal", "email", Fixtures.shipmentsJson("42.0", "ACEPTADO", TS)),
                Arguments.of("shipmentId zero", "email", Fixtures.shipmentsJson("0", "ACEPTADO", TS)),
                Arguments.of("shipmentId negative", "email", Fixtures.shipmentsJson("-3", "ACEPTADO", TS)),
                Arguments.of("shipmentId overflow", "email", Fixtures.shipmentsJson("99999999999999999999", "ACEPTADO", TS)),
                Arguments.of("shipmentId null", "email", Fixtures.shipmentsJson("null", "ACEPTADO", TS)),
                Arguments.of("shipmentId boolean", "email", Fixtures.shipmentsJson("true", "ACEPTADO", TS)),
                // flat format: timestamp
                Arguments.of("timestamp without zone", "email", Fixtures.shipmentsJson("42", "ACEPTADO", "2026-10-18T19:30:00")),
                Arguments.of("timestamp with offset", "email", Fixtures.shipmentsJson("42", "ACEPTADO", "2026-10-18T19:30:00-03:00")),
                Arguments.of("timestamp with space", "email", Fixtures.shipmentsJson("42", "ACEPTADO", "2026-10-18 19:30:00Z")),
                Arguments.of("timestamp month 13", "email", Fixtures.shipmentsJson("42", "ACEPTADO", "2026-13-18T19:30:00Z")),
                Arguments.of("timestamp hour 25", "email", Fixtures.shipmentsJson("42", "ACEPTADO", "2026-10-18T25:30:00Z")),
                Arguments.of("timestamp text", "email", Fixtures.shipmentsJson("42", "ACEPTADO", "hoy")),
                Arguments.of("timestamp empty", "email", Fixtures.shipmentsJson("42", "ACEPTADO", "")),
                Arguments.of("timestamp numeric", "email", ok.replace("\"" + TS + "\"", "1760815800")),
                Arguments.of("timestamp null", "email", ok.replace("\"" + TS + "\"", "null")),
                // flat format: required fields missing or null
                Arguments.of("missing shipmentId", "email", ok.replace("\"shipmentId\":42,", "")),
                Arguments.of("missing trackingNumber", "email", ok.replace("\"trackingNumber\":\"RUTA-0123456789AB\",", "")),
                Arguments.of("missing status", "email", ok.replace("\"status\":\"ACEPTADO\",", "")),
                Arguments.of("missing destinatarioNombre", "email", ok.replace("\"destinatarioNombre\":\"Ana Pérez\",", "")),
                Arguments.of("missing destinatarioEmail", "email", ok.replace("\"destinatarioEmail\":\"ana@example.com\",", "")),
                Arguments.of("missing timestamp", "email", ok.replace(",\"timestamp\":\"" + TS + "\"", "")),
                Arguments.of("null trackingNumber", "email", ok.replace("\"RUTA-0123456789AB\"", "null")),
                Arguments.of("null status", "email", ok.replace("\"ACEPTADO\"", "null")),
                Arguments.of("null destinatarioNombre", "email", ok.replace("\"Ana Pérez\"", "null")),
                Arguments.of("null destinatarioEmail", "email", ok.replace("\"ana@example.com\"", "null")),
                // flat format: unknown properties and structure
                Arguments.of("unknown property", "email", ok.replace("}", ",\"extra\":1}")),
                Arguments.of("channel is not part of the Shipments format", "email", ok.replace("}", ",\"channel\":\"EMAIL\"}")),
                Arguments.of("duplicate key", "email", ok.replace("\"shipmentId\":42", "\"shipmentId\":42,\"shipmentId\":43")),
                Arguments.of("trailing JSON", "email", ok + "{}"),
                Arguments.of("trailing garbage", "email", ok + " x"),
                Arguments.of("mixed with type", "email", ok.replace("{", "{\"type\":\"email.send.v1\",")),
                Arguments.of("mixed with payload", "email", ok.replace("{", "{\"payload\":{},")),
                Arguments.of("mixed with eventId", "email", ok.replace("{", "{\"eventId\":\"504d2040-011d-4ab1-a6c5-b71df96c3111\",")),
                // flat format: field contents
                Arguments.of("invalid email address", "email", ok.replace("ana@example.com", "no-es-email")),
                Arguments.of("control character in name", "email", ok.replace("Ana Pérez", "Ana\\u0007")),
                Arguments.of("blank name", "email", ok.replace("Ana Pérez", "  ")),
                Arguments.of("tracking with path characters", "email", ok.replace("RUTA-0123456789AB", "../../x")),
                Arguments.of("tracking too long", "email", ok.replace("RUTA-0123456789AB", "A".repeat(81))),
                Arguments.of("message too large", "email", ok.replace("Ana Pérez", "A".repeat(70000))),
                // the Shipments format only exists for the email flow
                Arguments.of("Shipments format sent to warehouse", "warehouse", ok),
                Arguments.of("Shipments format sent to label", "label", ok),
                // v1 envelope keeps its strict rules
                Arguments.of("v1 for another flow", "label", v1),
                Arguments.of("v1 unknown type", "email", v1.replace("email.send.v1", "unknown")),
                Arguments.of("v1 unknown property", "email", v1.replace("\"traceId\"", "\"extra\":1,\"traceId\"")),
                Arguments.of("v1 timestamp without offset", "email", v1.replace("-03:00", "")),
                Arguments.of("v1 id as string", "email", v1.replace("\"id\":42", "\"id\":\"42\"")),
                Arguments.of("v1 duplicate key", "email", v1.replace("\"id\":42", "\"id\":42,\"id\":43")),
                Arguments.of("v1 trailing JSON", "email", v1 + "{}"),
                Arguments.of("v1 status CREADO", "email", v1.replace("ACEPTADO", "CREADO")),
                Arguments.of("v1 channel SMS", "email", v1.replace("\"EMAIL\"", "\"SMS\"")),
                Arguments.of("v1 email without channel", "email", v1.replace(",\"channel\":\"EMAIL\"", "")),
                Arguments.of("v1 email missing direccionOrigen", "email", v1.replace("\"direccionOrigen\":\"Bodega Central\",", "")),
                Arguments.of("v1 email missing direccionDestino", "email", v1.replace("\"direccionDestino\":\"Avenida Ejemplo 123, Santiago\",", "")),
                Arguments.of("v1 email missing servicioId", "email", v1.replace("\"servicioId\":1,", "")),
                Arguments.of("v1 email null direccionDestino", "email", v1.replace("\"Avenida Ejemplo 123, Santiago\"", "null")),
                // warehouse and label are not relaxed
                Arguments.of("warehouse with channel", "warehouse", v1("email").replace("email.send.v1", "warehouse.ticket.v1")),
                Arguments.of("warehouse missing direccionOrigen", "warehouse", v1("warehouse").replace("\"direccionOrigen\":\"Bodega Central\",", "")),
                Arguments.of("warehouse missing direccionDestino", "warehouse", v1("warehouse").replace("\"direccionDestino\":\"Avenida Ejemplo 123, Santiago\",", "")),
                Arguments.of("warehouse missing servicioId", "warehouse", v1("warehouse").replace("\"servicioId\":1,", "")),
                Arguments.of("label missing direccionOrigen", "label", v1("label").replace("\"direccionOrigen\":\"Bodega Central\",", "")),
                Arguments.of("label missing direccionDestino", "label", v1("label").replace("\"direccionDestino\":\"Avenida Ejemplo 123, Santiago\",", "")),
                Arguments.of("label missing servicioId", "label", v1("label").replace("\"servicioId\":1,", "")));
    }

    @ParameterizedTest(name = "{0}") @MethodSource("invalidInputs")
    void invalidInputIsRejected(String name, String flow, String json) {
        assertThrows(Exception.class, () -> decode(flow, json), name);
    }

    @Test void bytesThatAreNotUtf8AreRejected() {
        assertThrows(Exception.class, () -> decoder.decode(new byte[]{(byte) 0xff, 'x'}, "email"));
    }

    @Test void unknownFlowIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> decode("sms", v1("email")));
    }
}
