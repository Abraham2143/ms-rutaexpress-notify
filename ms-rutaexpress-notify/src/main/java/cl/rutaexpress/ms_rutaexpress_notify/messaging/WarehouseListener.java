package cl.rutaexpress.ms_rutaexpress_notify.messaging;
import cl.rutaexpress.ms_rutaexpress_notify.processing.WarehouseService;
import com.rabbitmq.client.Channel;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;
import java.io.IOException;
@Component
public class WarehouseListener {
    private final CommandHandler handler;
    private final WarehouseService service;
    public WarehouseListener(CommandHandler handler, WarehouseService service) { this.handler=handler; this.service=service; }
    @RabbitListener(queues="q.cmd.warehouse",
            autoStartup="${notify.listeners.warehouse-enabled:false}")
    public void receive(Message message, Channel channel) throws IOException { handler.handle("warehouse",message,channel,service::process); }
}
