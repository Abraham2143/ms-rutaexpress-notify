package cl.rutaexpress.ms_rutaexpress_notify.config;

import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.support.ListenerExecutionFailedException;
import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.context.annotation.*;
import org.springframework.beans.factory.annotation.Value;
import org.slf4j.LoggerFactory;

@Configuration
public class RabbitConfig {
    @Bean public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory(
            ConnectionFactory connectionFactory, MeterRegistry metrics,
            @Value("${spring.rabbitmq.listener.simple.auto-startup:true}") boolean autoStartup) {
        var factory = new SimpleRabbitListenerContainerFactory();
        factory.setConnectionFactory(connectionFactory);
        factory.setAutoStartup(autoStartup);
        // Topology belongs exclusively to ms-rutaexpress-rabbitmq-admin.
        factory.setContainerCustomizer(container -> container.setAutoDeclare(false));
        factory.setAcknowledgeMode(AcknowledgeMode.MANUAL);
        factory.setDefaultRequeueRejected(false);
        factory.setConcurrentConsumers(1);
        factory.setPrefetchCount(10);
        // Raw Message listeners avoid JSON conversion before the handler. Other adapter failures
        // must also be rejected in MANUAL mode; never log exception messages/bodies here.
        factory.setErrorHandler(error -> {
            String queue = "unknown";
            if (error instanceof ListenerExecutionFailedException failed && failed.getFailedMessage()!=null)
                queue = failed.getFailedMessage().getMessageProperties().getConsumerQueue();
            if (queue == null) queue = "unknown";
            metrics.counter("notify.dlq.rejections", "queue", queue, "stage", "container").increment();
            metrics.counter("notify.failed", "queue", queue).increment();
            LoggerFactory.getLogger(RabbitConfig.class).warn(
                    "eventId=unknown traceId=unknown correlationId=unknown queue={} result=container_rejected error={}",
                    queue, error.getClass().getSimpleName());
            throw new AmqpRejectAndDontRequeueException("container_rejected", true, error);
        });
        return factory;
    }
}
