package cl.rutaexpress.ms_rutaexpress_notify;

import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.Declarable;
import org.springframework.amqp.core.Declarables;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(properties={"spring.rabbitmq.username=test", "spring.rabbitmq.password=test", "spring.rabbitmq.listener.simple.auto-startup=false"})
class MsRutaexpressNotifyApplicationTests {
    @Autowired ApplicationContext context;
    @Autowired RabbitListenerEndpointRegistry listeners;

    @Test
    void contextLoadsWithoutTopologyAdministrationOrSmtpRequirements() {
        assertEquals(Map.of(), context.getBeansOfType(AmqpAdmin.class));
        assertEquals(Map.of(), context.getBeansOfType(Declarable.class));
        assertEquals(Map.of(), context.getBeansOfType(Declarables.class));
        assertEquals(3, listeners.getListenerContainers().size());
        assertTrue(listeners.getListenerContainers().stream().noneMatch(c -> c.isRunning()));
    }
}
