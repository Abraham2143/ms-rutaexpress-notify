package cl.rutaexpress.ms_rutaexpress_notify;

import cl.rutaexpress.ms_rutaexpress_notify.messaging.EmailListener;
import cl.rutaexpress.ms_rutaexpress_notify.messaging.LabelListener;
import cl.rutaexpress.ms_rutaexpress_notify.messaging.WarehouseListener;
import com.rabbitmq.client.Channel;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Which queue each consumer reads and which environment variables switch it on, resolved against the real
 * application.yaml. No broker is started: only the annotation and the property placeholders are inspected.
 */
class ListenerActivationTests {
    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withInitializer(new ConfigDataApplicationContextInitializer());

    private static RabbitListener annotation(Class<?> type) throws NoSuchMethodException {
        return type.getMethod("receive", Message.class, Channel.class).getAnnotation(RabbitListener.class);
    }

    @Test void eachConsumerReadsItsOwnQueue() throws Exception {
        assertArrayEquals(new String[]{"q.cmd.email"}, annotation(EmailListener.class).queues());
        assertArrayEquals(new String[]{"q.cmd.warehouse"}, annotation(WarehouseListener.class).queues());
        assertArrayEquals(new String[]{"q.cmd.label"}, annotation(LabelListener.class).queues());
    }

    @Test void emailConsumerAlwaysStartsWhileWarehouseAndLabelNeedTheirOwnVariable() throws Exception {
        assertEquals("", annotation(EmailListener.class).autoStartup(), "no switch: starts with the application");
        runner.run(context -> {
            var environment = context.getEnvironment();
            assertEquals("false", environment.resolvePlaceholders(annotationOf(WarehouseListener.class)));
            assertEquals("false", environment.resolvePlaceholders(annotationOf(LabelListener.class)));
        });
    }

    @Test void warehouseAndLabelStartWhenTheirEnvironmentVariablesAreTrue() {
        runner.withPropertyValues("WAREHOUSE_LISTENER_ENABLED=true", "LABEL_LISTENER_ENABLED=true").run(context -> {
            var environment = context.getEnvironment();
            assertEquals("true", environment.resolvePlaceholders(annotationOf(WarehouseListener.class)));
            assertEquals("true", environment.resolvePlaceholders(annotationOf(LabelListener.class)));
        });
    }

    @Test void eachVariableOnlyAffectsItsOwnConsumer() {
        runner.withPropertyValues("WAREHOUSE_LISTENER_ENABLED=true").run(context -> {
            var environment = context.getEnvironment();
            assertEquals("true", environment.resolvePlaceholders(annotationOf(WarehouseListener.class)));
            assertEquals("false", environment.resolvePlaceholders(annotationOf(LabelListener.class)));
        });
    }

    @Test void smtpIsDisabledByDefaultAndOnlyEnabledExplicitly() {
        runner.run(context -> assertEquals("false", context.getEnvironment().getProperty("notify.email.enabled")));
        runner.withPropertyValues("EMAIL_ENABLED=true").run(context ->
                assertEquals("true", context.getEnvironment().getProperty("notify.email.enabled")));
    }

    private static String annotationOf(Class<?> type) {
        try { return annotation(type).autoStartup(); }
        catch (NoSuchMethodException e) { throw new IllegalStateException(e); }
    }
}
