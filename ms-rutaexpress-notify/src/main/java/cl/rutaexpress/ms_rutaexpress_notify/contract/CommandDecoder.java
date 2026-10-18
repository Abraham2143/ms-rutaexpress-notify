package cl.rutaexpress.ms_rutaexpress_notify.contract;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import jakarta.validation.Validator;
import org.springframework.stereotype.Component;

@Component
public class CommandDecoder {
    private final ObjectMapper mapper = new ObjectMapper(JsonFactory.builder()
            .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION).build())
            .registerModule(new JavaTimeModule())
            .disable(DeserializationFeature.ADJUST_DATES_TO_CONTEXT_TIME_ZONE)
            .disable(DeserializationFeature.ACCEPT_FLOAT_AS_INT)
            .disable(MapperFeature.ALLOW_COERCION_OF_SCALARS)
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
    private final Validator validator;
    public CommandDecoder(Validator validator) { this.validator = validator; }

    /**
     * Decodes a message body for the given flow ("email", "warehouse" or "label").
     * The email flow accepts two formats: the v1 envelope and the flat notification published by
     * ms-rutaexpress-shipments, which is converted to the same {@link Command}. Warehouse and label
     * accept only the v1 envelope.
     */
    public Command decode(byte[] body, String flow) throws Exception {
        if (body.length > 65536) throw new IllegalArgumentException("message_too_large");
        // Only the email flow looks at the structure first, to tell the Shipments format from the envelope.
        // Parsing is strict (duplicate keys, trailing tokens), so both formats are rejected the same way.
        JsonNode tree = "email".equals(flow) ? mapper.readTree(body) : null;
        boolean fromShipments = ShipmentNotificationFormat.matches(tree);
        Command command = fromShipments
                ? ShipmentNotificationFormat.toCommand(tree)
                : mapper.readValue(body, Command.class);
        if (command == null || !validator.validate(command).isEmpty())
            throw new IllegalArgumentException("invalid_command");
        String expected = switch(flow) {
            case "email" -> "email.send.v1";
            case "warehouse" -> "warehouse.ticket.v1";
            case "label" -> "label.gen.v1";
            default -> throw new IllegalArgumentException("unknown_flow");
        };
        if (!expected.equals(command.type())) throw new IllegalArgumentException("wrong_type");
        var payload = command.payload();
        if (!fromShipments && (payload.direccionOrigen() == null || payload.direccionDestino() == null
                || payload.servicioId() == null))
            throw new IllegalArgumentException("invalid_command");
        if (flow.equals("email")) {
            if (payload.channel() == null ||
                    !java.util.Set.of("ACEPTADO", "EN_RUTA", "ENTREGADO").contains(payload.status()))
                throw new IllegalArgumentException("invalid_notification");
        } else if (payload.channel() != null) {
            throw new IllegalArgumentException("channel_not_applicable");
        }
        return command;
    }
}
