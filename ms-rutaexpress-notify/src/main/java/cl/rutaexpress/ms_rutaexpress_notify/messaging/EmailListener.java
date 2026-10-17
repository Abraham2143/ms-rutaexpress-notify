package cl.rutaexpress.ms_rutaexpress_notify.messaging;

import com.rabbitmq.client.Channel;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

@Component
public class EmailListener {
    private static final Logger LOG = LoggerFactory.getLogger(EmailListener.class);

    @RabbitListener(queues = "q.cmd.email")
    public void receive(Message message, Channel channel) throws IOException {
        // Decode raw bytes rather than relying on content-type, DTOs or Java deserialization.
        String payload = new String(message.getBody(), StandardCharsets.UTF_8);
        LOG.info("Mensaje recibido de q.cmd.email: {}", payload);
        // The shared factory uses MANUAL acknowledgements. No SMTP is called here.
        channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
    }
}
