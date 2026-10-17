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
    public Command decode(byte[] body, String flow) throws Exception {
        if (body.length > 65536) throw new IllegalArgumentException("message_too_large");
        Command command = mapper.readValue(body, Command.class);
        if (command == null || !validator.validate(command).isEmpty())
            throw new IllegalArgumentException("invalid_command");
        String expected = switch(flow) {
            case "email" -> "email.send.v1";
            case "warehouse" -> "warehouse.ticket.v1";
            case "label" -> "label.gen.v1";
            default -> throw new IllegalArgumentException("unknown_flow");
        };
        if (!expected.equals(command.type())) throw new IllegalArgumentException("wrong_type");
        if (flow.equals("email")) {
            if (command.payload().channel() == null ||
                    !java.util.Set.of("ACEPTADO", "EN_RUTA", "ENTREGADO").contains(command.payload().status()))
                throw new IllegalArgumentException("invalid_notification");
        } else if (command.payload().channel() != null) {
            throw new IllegalArgumentException("channel_not_applicable");
        }
        return command;
    }
}
