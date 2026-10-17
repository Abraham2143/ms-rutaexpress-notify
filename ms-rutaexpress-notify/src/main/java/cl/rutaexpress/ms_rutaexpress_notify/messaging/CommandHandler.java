package cl.rutaexpress.ms_rutaexpress_notify.messaging;

import cl.rutaexpress.ms_rutaexpress_notify.contract.*;
import cl.rutaexpress.ms_rutaexpress_notify.processing.*;
import com.rabbitmq.client.Channel;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.*;
import org.springframework.amqp.core.Message;
import org.springframework.stereotype.Component;
import java.io.IOException;

@Component
public class CommandHandler {
    private static final Logger LOG=LoggerFactory.getLogger(CommandHandler.class);
    private final CommandDecoder decoder;
    private final IdempotencyGuard guard;
    private final MeterRegistry metrics;
    public CommandHandler(CommandDecoder decoder, IdempotencyGuard guard, MeterRegistry metrics) {
        this.decoder=decoder; this.guard=guard; this.metrics=metrics;
    }
    @FunctionalInterface public interface Processor { void process(Command command) throws Exception; }
    public void handle(String flow, Message message, Channel channel, Processor processor) throws IOException {
        String queue="q.cmd."+flow;
        long tag=message.getMessageProperties().getDeliveryTag();
        Command command=null;
        boolean processed;
        try {
            command=decoder.decode(message.getBody(),flow);
            final Command valid=command;
            processed=guard.execute(flow+":"+command.eventId(), () -> processor.process(valid));
        } catch (Exception error) {
            metrics.counter("notify.failed", "queue", queue).increment();
            log(command, queue, "failed", error.getClass().getSimpleName());
            metrics.counter("notify.dlq.rejections", "queue", queue, "stage", "application").increment();
            channel.basicNack(tag,false,false);
            return;
        }
        // Outside the processing catch: a transport failure while ACKing must not undo success
        // or issue a second disposition. Broker redelivery is handled by the guard.
        metrics.counter(processed ? "notify.processed" : "notify.duplicates", "queue", queue).increment();
        channel.basicAck(tag,false);
        log(command,queue,processed ? "processed" : "duplicate", "none");
    }
    private void log(Command command, String queue, String result, String error) {
        LOG.info("eventId={} traceId={} correlationId={} queue={} result={} error={}",
                command==null ? "unknown" : command.eventId(), command==null ? "unknown" : command.traceId(),
                command==null ? "unknown" : command.correlationId(),queue,result,error);
    }
}
