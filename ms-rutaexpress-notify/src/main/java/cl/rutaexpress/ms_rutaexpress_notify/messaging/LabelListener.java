package cl.rutaexpress.ms_rutaexpress_notify.messaging;
import cl.rutaexpress.ms_rutaexpress_notify.processing.LabelService;
import com.rabbitmq.client.Channel;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;
import java.io.IOException;
@Component
public class LabelListener {
    private final CommandHandler handler;
    private final LabelService service;
    public LabelListener(CommandHandler handler, LabelService service) { this.handler=handler; this.service=service; }
    @RabbitListener(queues="q.cmd.label",
            autoStartup="${notify.listeners.label-enabled:false}")
    public void receive(Message message, Channel channel) throws IOException { handler.handle("label",message,channel,service::process); }
}
