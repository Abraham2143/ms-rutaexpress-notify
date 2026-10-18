package cl.rutaexpress.ms_rutaexpress_notify.messaging;

import cl.rutaexpress.ms_rutaexpress_notify.processing.EmailService;
import com.rabbitmq.client.Channel;
import java.io.IOException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

/**
 * Consumes q.cmd.email through the same flow as the other consumers: decode and validate, skip
 * duplicates, send, and only then acknowledge. Failures are rejected without requeue (dead-letter queue).
 * With EMAIL_ENABLED=false {@link EmailService} fails every command, so nothing is reported as sent.
 */
@Component
public class EmailListener {
    private final CommandHandler handler;
    private final EmailService service;

    public EmailListener(CommandHandler handler, EmailService service) {
        this.handler = handler;
        this.service = service;
    }

    @RabbitListener(queues = "q.cmd.email")
    public void receive(Message message, Channel channel) throws IOException {
        handler.handle("email", message, channel, service::process);
    }
}
