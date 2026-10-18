package cl.rutaexpress.ms_rutaexpress_notify.contract;

import com.fasterxml.jackson.databind.JsonNode;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * Adapts the flat JSON published by ms-rutaexpress-shipments (record {@code ShipmentNotification}) to the
 * {@link Command} model used by Notify. Shipments serializes:
 * {@code shipmentId} (Long), {@code trackingNumber} (String), {@code status} (enum name),
 * {@code destinatarioNombre} (String), {@code destinatarioEmail} (String) and {@code timestamp}
 * (Instant as an ISO-8601 string ending in {@code Z}).
 *
 * <p>Shipments sends no eventId, traceId, correlationId or addresses, so they are derived here. Field
 * content rules (patterns, sizes, allowed status) are enforced afterwards by the same Bean Validation
 * constraints and flow rules that protect the v1 envelope.
 */
final class ShipmentNotificationFormat {
    static final Set<String> FIELDS = Set.of("shipmentId", "trackingNumber", "status",
            "destinatarioNombre", "destinatarioEmail", "timestamp");
    // Instant.toString() always yields this shape: seconds present, optional fraction, UTC 'Z'.
    private static final Pattern INSTANT = Pattern.compile(
            "\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(\\.\\d{1,9})?Z");

    private ShipmentNotificationFormat() {}

    /** True when the JSON looks like the flat Shipments notification and not like a v1 envelope. */
    static boolean matches(JsonNode node) {
        return node != null && node.isObject() && node.has("shipmentId")
                && !node.has("type") && !node.has("payload") && !node.has("eventId");
    }

    static Command toCommand(JsonNode node) {
        Set<String> names = new HashSet<>();
        node.fieldNames().forEachRemaining(names::add);
        if (!names.equals(FIELDS)) throw new IllegalArgumentException("invalid_shipment_notification");

        long shipmentId = integral(node.get("shipmentId"));
        String trackingNumber = text(node.get("trackingNumber"));
        String status = text(node.get("status"));
        String name = text(node.get("destinatarioNombre"));
        String email = text(node.get("destinatarioEmail"));
        Instant instant = instant(text(node.get("timestamp")));

        String correlation = "shipment-" + shipmentId;
        return new Command("email.send.v1", eventId(shipmentId, status, instant),
                instant.atOffset(ZoneOffset.UTC), correlation, correlation,
                new Command.Payload(shipmentId, trackingNumber, name, email, null, null, null, status, "EMAIL"));
    }

    /**
     * Stable identity of one notification: the same shipment, status and instant always give the same UUID
     * (a redelivery is a duplicate); another status or instant gives another one. The instant is
     * canonicalized to epoch seconds and nanos, so "…:00Z" and "…:00.000Z" are the same event.
     */
    static UUID eventId(long shipmentId, String status, Instant instant) {
        String key = "shipments.notification.v1|" + shipmentId + "|" + status + "|"
                + instant.getEpochSecond() + "." + String.format("%09d", instant.getNano());
        return UUID.nameUUIDFromBytes(key.getBytes(StandardCharsets.UTF_8));
    }

    private static long integral(JsonNode node) {
        // isIntegralNumber() is false for 42.0 and for strings; canConvertToLong() rejects overflow.
        if (node == null || !node.isIntegralNumber() || !node.canConvertToLong())
            throw new IllegalArgumentException("invalid_shipment_notification");
        return node.longValue();
    }

    private static String text(JsonNode node) {
        if (node == null || !node.isTextual()) throw new IllegalArgumentException("invalid_shipment_notification");
        return node.textValue();
    }

    private static Instant instant(String value) {
        if (!INSTANT.matcher(value).matches()) throw new IllegalArgumentException("invalid_timestamp");
        try {
            return Instant.parse(value);
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException("invalid_timestamp");
        }
    }
}
