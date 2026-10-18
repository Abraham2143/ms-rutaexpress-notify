package cl.rutaexpress.ms_rutaexpress_notify;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.springframework.amqp.core.*;

final class Fixtures {
    static byte[] json(String flow, UUID eventId) {
        String type=switch(flow) { case "email" -> "email.send.v1"; case "warehouse" -> "warehouse.ticket.v1"; default -> "label.gen.v1"; };
        return ("""
                {"type":"%s","eventId":"%s","timestamp":"2026-10-15T18:00:00-03:00",
                 "traceId":"trace-123","correlationId":"shipment-42","payload":{
                 "id":42,"trackingNumber":"RE-42","destinatarioNombre":"Ana Pérez",
                 "destinatarioEmail":"ana@example.com","direccionOrigen":"Bodega Central",
                 "direccionDestino":"Avenida Ejemplo 123, Santiago","servicioId":1,"status":"ACEPTADO"%s}}
                """).formatted(type,eventId,flow.equals("email") ? ",\"channel\":\"EMAIL\"" : "").getBytes(StandardCharsets.UTF_8);
    }
    /** Flat JSON exactly as ms-rutaexpress-shipments publishes it (record ShipmentNotification). */
    static String shipmentsJson(String shipmentId, String status, String timestamp) {
        return ("""
                {"shipmentId":%s,"trackingNumber":"RUTA-0123456789AB","status":"%s",
                 "destinatarioNombre":"Ana Pérez","destinatarioEmail":"ana@example.com","timestamp":"%s"}
                """).formatted(shipmentId,status,timestamp);
    }
    static byte[] shipments(String status) {
        return shipmentsJson("42",status,"2026-10-18T19:30:00.123456Z").getBytes(StandardCharsets.UTF_8);
    }
    static byte[] shipments() { return shipments("ACEPTADO"); }
    static Message message(byte[] bytes) {
        var properties=new MessageProperties(); properties.setDeliveryTag(7);
        properties.setContentType("application/json");
        return new Message(bytes,properties);
    }
}
