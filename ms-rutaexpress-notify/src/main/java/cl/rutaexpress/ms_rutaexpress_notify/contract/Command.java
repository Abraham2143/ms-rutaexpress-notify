package cl.rutaexpress.ms_rutaexpress_notify.contract;

import java.time.OffsetDateTime;
import java.util.UUID;
import jakarta.validation.Valid;
import jakarta.validation.constraints.*;

public record Command(
        @NotBlank @Pattern(regexp="email.send.v1|warehouse.ticket.v1|label.gen.v1") String type,
        @NotNull UUID eventId, @NotNull OffsetDateTime timestamp,
        @NotBlank @Pattern(regexp="[A-Za-z0-9_-]{1,128}") String traceId,
        @NotBlank @Pattern(regexp="[A-Za-z0-9_-]{1,128}") String correlationId,
        @NotNull @Valid Payload payload) {
    public record Payload(
            @NotNull @Positive Long id,
            @NotBlank @Size(max=80) @Pattern(regexp="[A-Za-z0-9_-]+") String trackingNumber,
            @NotBlank @Size(max=160) @Pattern(regexp="[^\\p{Cc}]+") String destinatarioNombre,
            @NotBlank @Email @Size(max=254) @Pattern(regexp="[^\\p{Cc}]+") String destinatarioEmail,
            // Optional at the record level: only the flat Shipments email notification omits them.
            // CommandDecoder still requires all three for the v1 envelope (email, warehouse, label).
            @Size(max=300) @Pattern(regexp="[^\\p{Cc}]+") String direccionOrigen,
            @Size(max=300) @Pattern(regexp="[^\\p{Cc}]+") String direccionDestino,
            @Positive Long servicioId,
            @NotBlank @Pattern(regexp="CREADO|ACEPTADO|EN_BODEGA|EN_RUTA|ENTREGADO|CANCELADO") String status,
            @Pattern(regexp="EMAIL|WEBPUSH") String channel) {}
}
