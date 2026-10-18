package cl.rutaexpress.ms_rutaexpress_notify;

import cl.rutaexpress.ms_rutaexpress_notify.contract.Command;
import cl.rutaexpress.ms_rutaexpress_notify.contract.CommandDecoder;
import cl.rutaexpress.ms_rutaexpress_notify.messaging.CommandHandler;
import cl.rutaexpress.ms_rutaexpress_notify.messaging.LabelListener;
import cl.rutaexpress.ms_rutaexpress_notify.messaging.WarehouseListener;
import cl.rutaexpress.ms_rutaexpress_notify.processing.IdempotencyGuard;
import cl.rutaexpress.ms_rutaexpress_notify.processing.LabelService;
import cl.rutaexpress.ms_rutaexpress_notify.processing.WarehouseService;
import com.rabbitmq.client.Channel;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import jakarta.validation.Validation;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.springframework.amqp.core.Message;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * WarehouseListener and LabelListener through the real common flow. The processing services are mocks, so
 * the tests prove consumer behaviour (decode, idempotency, ACK/NACK) and not file generation, which
 * ProcessingTests covers separately.
 */
class WarehouseLabelListenerTests {
    private final Channel channel = mock(Channel.class);
    private final SimpleMeterRegistry metrics = new SimpleMeterRegistry();
    private final CommandHandler handler = new CommandHandler(
            new CommandDecoder(Validation.buildDefaultValidatorFactory().getValidator()),
            new IdempotencyGuard(100, 86400), metrics);
    private final WarehouseService warehouse = mock(WarehouseService.class);
    private final LabelService label = mock(LabelService.class);
    private final WarehouseListener warehouseListener = new WarehouseListener(handler, warehouse);
    private final LabelListener labelListener = new LabelListener(handler, label);

    // The same scenarios run against both consumers; these helpers hide which one is under test.
    private void receive(String flow, byte[] body) throws IOException {
        Message message = Fixtures.message(body);
        if (flow.equals("warehouse")) warehouseListener.receive(message, channel);
        else labelListener.receive(message, channel);
    }
    private void receive(String flow, String body) throws IOException { receive(flow, body.getBytes(StandardCharsets.UTF_8)); }
    private void verifyProcessed(String flow, int times) throws Exception {
        if (flow.equals("warehouse")) verify(warehouse, times(times)).process(any(Command.class));
        else verify(label, times(times)).process(any(Command.class));
    }
    private void failNextProcessing(String flow) throws Exception {
        if (flow.equals("warehouse")) doThrow(new IOException("disk full")).doNothing().when(warehouse).process(any(Command.class));
        else doThrow(new IOException("disk full")).doNothing().when(label).process(any(Command.class));
    }
    private double count(String name) {
        var counter = metrics.find(name).counter();
        return counter == null ? 0 : counter.count();
    }

    @ParameterizedTest @ValueSource(strings = {"warehouse", "label"})
    void validMessageIsProcessedAndAckedOnlyAfterProcessing(String flow) throws Exception {
        UUID eventId = UUID.randomUUID();
        receive(flow, Fixtures.json(flow, eventId));

        InOrder order = flow.equals("warehouse") ? inOrder(warehouse, channel) : inOrder(label, channel);
        var captor = ArgumentCaptor.forClass(Command.class);
        if (flow.equals("warehouse")) order.verify(warehouse).process(captor.capture());
        else order.verify(label).process(captor.capture());
        order.verify(channel).basicAck(7, false);
        verifyNoMoreInteractions(channel);
        assertEquals(eventId, captor.getValue().eventId());
        assertEquals(1, count("notify.processed"));
        assertEquals(0, count("notify.failed"));
    }

    @ParameterizedTest @ValueSource(strings = {"warehouse", "label"})
    void duplicateEventIsAckedWithoutProcessingAgain(String flow) throws Exception {
        byte[] body = Fixtures.json(flow, UUID.randomUUID());
        receive(flow, body);
        receive(flow, body);

        verifyProcessed(flow, 1);
        verify(channel, times(2)).basicAck(7, false);
        verify(channel, never()).basicNack(anyLong(), anyBoolean(), anyBoolean());
        assertEquals(1, count("notify.duplicates"));
    }

    @ParameterizedTest @ValueSource(strings = {"warehouse", "label"})
    void processingErrorIsRejectedWithoutAckAndMayBeRetried(String flow) throws Exception {
        failNextProcessing(flow);
        byte[] body = Fixtures.json(flow, UUID.randomUUID());

        receive(flow, body);
        verify(channel).basicNack(7, false, false);
        verify(channel, never()).basicAck(anyLong(), anyBoolean());
        assertEquals(0, count("notify.processed"));
        assertEquals(1, count("notify.failed"));

        receive(flow, body); // republished from the DLQ with the same eventId
        verifyProcessed(flow, 2);
        verify(channel).basicAck(7, false);
        assertEquals(1, count("notify.processed"));
        assertEquals(0, count("notify.duplicates"));
    }

    @ParameterizedTest @ValueSource(strings = {"warehouse", "label"})
    void invalidMessageIsRejectedWithoutRequeueAndNotProcessed(String flow) throws Exception {
        receive(flow, "{json sin terminar");
        receive(flow, "Hola José");
        receive(flow, "{}");

        verify(channel, times(3)).basicNack(7, false, false);
        verify(channel, never()).basicAck(anyLong(), anyBoolean());
        verifyProcessed(flow, 0);
        assertEquals(3, count("notify.failed"));
    }

    @ParameterizedTest @ValueSource(strings = {"warehouse", "label"})
    void messagesForAnotherFlowOrFormatAreRejected(String flow) throws Exception {
        receive(flow, Fixtures.json("email", UUID.randomUUID())); // email envelope
        receive(flow, Fixtures.json(flow.equals("warehouse") ? "label" : "warehouse", UUID.randomUUID())); // other flow
        receive(flow, Fixtures.shipments()); // flat Shipments format is only accepted by the email consumer

        verify(channel, times(3)).basicNack(7, false, false);
        verify(channel, never()).basicAck(anyLong(), anyBoolean());
        verifyProcessed(flow, 0);
    }

    @ParameterizedTest @ValueSource(strings = {"warehouse", "label"})
    void messageWithoutAddressesIsStillRejected(String flow) throws Exception {
        String body = new String(Fixtures.json(flow, UUID.randomUUID()), StandardCharsets.UTF_8)
                .replace("\"direccionOrigen\":\"Bodega Central\",", "");
        receive(flow, body);

        verify(channel).basicNack(7, false, false);
        verifyProcessed(flow, 0);
    }
}
