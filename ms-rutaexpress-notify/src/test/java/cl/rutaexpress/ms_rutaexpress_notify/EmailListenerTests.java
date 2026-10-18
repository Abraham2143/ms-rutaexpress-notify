package cl.rutaexpress.ms_rutaexpress_notify;

import cl.rutaexpress.ms_rutaexpress_notify.contract.CommandDecoder;
import cl.rutaexpress.ms_rutaexpress_notify.messaging.CommandHandler;
import cl.rutaexpress.ms_rutaexpress_notify.messaging.EmailListener;
import cl.rutaexpress.ms_rutaexpress_notify.processing.EmailService;
import cl.rutaexpress.ms_rutaexpress_notify.processing.IdempotencyGuard;
import com.rabbitmq.client.Channel;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import jakarta.validation.Validation;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.springframework.amqp.core.Message;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.mail.MailSendException;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * EmailListener through the real common flow (CommandDecoder, IdempotencyGuard, CommandHandler, EmailService).
 * Only the SMTP sender and the RabbitMQ channel are mocked, so no SMTP credentials or broker are needed.
 */
@ExtendWith(OutputCaptureExtension.class)
class EmailListenerTests {
    private static final String PERSONAL_DATA = "ana@example.com|Ana Pérez|Avenida Ejemplo|secreto@example.com";

    private final JavaMailSender sender = mock(JavaMailSender.class);
    private final Channel channel = mock(Channel.class);
    private final SimpleMeterRegistry metrics = new SimpleMeterRegistry();
    private final CommandHandler handler = new CommandHandler(
            new CommandDecoder(Validation.buildDefaultValidatorFactory().getValidator()),
            new IdempotencyGuard(100, 86400), metrics);

    private EmailListener listener(boolean emailEnabled) {
        return new EmailListener(handler, new EmailService(sender, emailEnabled, "notify@example.com"));
    }
    private static Message message(byte[] body) { return Fixtures.message(body); }
    private static Message message(String body) { return Fixtures.message(body.getBytes(StandardCharsets.UTF_8)); }
    private double count(String name) {
        var counter = metrics.find(name).counter();
        return counter == null ? 0 : counter.count();
    }
    private SimpleMailMessage sentMail() {
        var captor = ArgumentCaptor.forClass(SimpleMailMessage.class);
        verify(sender).send(captor.capture());
        return captor.getValue();
    }

    // ---------------------------------------------------------------- successful processing

    @Test void shipmentsFormatSendsEmailAndAcksOnlyAfterSending() throws Exception {
        listener(true).receive(message(Fixtures.shipments("EN_RUTA")), channel);

        InOrder order = inOrder(sender, channel);
        order.verify(sender).send(any(SimpleMailMessage.class));
        order.verify(channel).basicAck(7, false);
        verifyNoMoreInteractions(channel);
        SimpleMailMessage mail = sentMail();
        assertArrayEquals(new String[]{"ana@example.com"}, mail.getTo());
        assertEquals("notify@example.com", mail.getFrom());
        assertEquals("RutaExpress - RUTA-0123456789AB - EN_RUTA", mail.getSubject());
        assertTrue(mail.getText().contains("Hola Ana Pérez"));
        assertTrue(mail.getText().contains("en ruta"));
        assertFalse(mail.getText().contains("Destino:"), "Shipments sends no address");
        assertEquals(1, count("notify.processed"));
        assertEquals(0, count("notify.failed"));
    }

    @Test void v1EnvelopeSendsEmailWithDestinationAndAcksAfterSending() throws Exception {
        listener(true).receive(message(Fixtures.json("email", UUID.randomUUID())), channel);

        InOrder order = inOrder(sender, channel);
        order.verify(sender).send(any(SimpleMailMessage.class));
        order.verify(channel).basicAck(7, false);
        verifyNoMoreInteractions(channel);
        assertTrue(sentMail().getText().contains("Destino: Avenida Ejemplo 123, Santiago"));
    }

    @ParameterizedTest @ValueSource(strings = {"ACEPTADO", "EN_RUTA", "ENTREGADO"})
    void everyNotifiableStatusIsSent(String status) throws Exception {
        listener(true).receive(message(Fixtures.shipments(status)), channel);
        assertTrue(sentMail().getSubject().endsWith(" - " + status));
        verify(channel).basicAck(7, false);
    }

    // ---------------------------------------------------------------- idempotency

    @Test void redeliveredShipmentsEventIsAckedButNotSentTwice() throws Exception {
        var listener = listener(true);
        listener.receive(message(Fixtures.shipments()), channel);
        listener.receive(message(Fixtures.shipments()), channel);

        verify(sender, times(1)).send(any(SimpleMailMessage.class));
        verify(channel, times(2)).basicAck(7, false);
        verify(channel, never()).basicNack(anyLong(), anyBoolean(), anyBoolean());
        assertEquals(1, count("notify.processed"));
        assertEquals(1, count("notify.duplicates"));
    }

    @Test void redeliveredV1EventIsAckedButNotSentTwice() throws Exception {
        var listener = listener(true);
        var body = Fixtures.json("email", UUID.randomUUID());
        listener.receive(message(body), channel);
        listener.receive(message(body), channel);

        verify(sender, times(1)).send(any(SimpleMailMessage.class));
        verify(channel, times(2)).basicAck(7, false);
        assertEquals(1, count("notify.duplicates"));
    }

    @Test void differentStatesOfTheSameShipmentAreAllSent() throws Exception {
        var listener = listener(true);
        for (String status : new String[]{"ACEPTADO", "EN_RUTA", "ENTREGADO"})
            listener.receive(message(Fixtures.shipments(status)), channel);

        verify(sender, times(3)).send(any(SimpleMailMessage.class));
        verify(channel, times(3)).basicAck(7, false);
        assertEquals(0, count("notify.duplicates"));
    }

    // ---------------------------------------------------------------- invalid messages: NACK, no requeue

    @ParameterizedTest @ValueSource(strings = {
            "Hola José", "{json sin terminar", "", "null", "{}", "[]",
            "{\"nuevoCampo\":42}",
            // Shipments format with a bad field, an unknown field and a status that is not notifiable
            "{\"shipmentId\":\"42\",\"trackingNumber\":\"RUTA-0123456789AB\",\"status\":\"ACEPTADO\",\"destinatarioNombre\":\"Ana\",\"destinatarioEmail\":\"ana@example.com\",\"timestamp\":\"2026-10-18T19:30:00Z\"}",
            "{\"shipmentId\":42,\"trackingNumber\":\"RUTA-0123456789AB\",\"status\":\"ACEPTADO\",\"destinatarioNombre\":\"Ana\",\"destinatarioEmail\":\"ana@example.com\",\"timestamp\":\"2026-10-18T19:30:00Z\",\"extra\":1}",
            "{\"shipmentId\":42,\"trackingNumber\":\"RUTA-0123456789AB\",\"status\":\"CREADO\",\"destinatarioNombre\":\"Ana\",\"destinatarioEmail\":\"ana@example.com\",\"timestamp\":\"2026-10-18T19:30:00Z\"}",
            "{\"shipmentId\":42,\"trackingNumber\":\"RUTA-0123456789AB\",\"status\":\"ACEPTADO\",\"destinatarioNombre\":\"Ana\",\"destinatarioEmail\":\"ana@example.com\",\"timestamp\":\"ayer\"}"})
    void invalidMessagesAreRejectedWithoutRequeueAndNothingIsSent(String body) throws Exception {
        listener(true).receive(message(body), channel);

        verify(channel).basicNack(7, false, false);
        verify(channel, never()).basicAck(anyLong(), anyBoolean());
        verifyNoInteractions(sender);
        assertEquals(1, count("notify.failed"));
        assertEquals(0, count("notify.processed"));
    }

    @Test void nonUtf8BytesAreRejectedWithoutRequeue() throws Exception {
        listener(true).receive(message(new byte[]{(byte) 0xff, 'x'}), channel);
        verify(channel).basicNack(7, false, false);
        verify(channel, never()).basicAck(anyLong(), anyBoolean());
        verifyNoInteractions(sender);
    }

    // ---------------------------------------------------------------- SMTP disabled: DLQ, never "sent"

    @Test void disabledEmailRejectsToDeadLetterAndIsNotMarkedAsSentOrProcessed() throws Exception {
        listener(false).receive(message(Fixtures.shipments()), channel);

        verify(channel).basicNack(7, false, false);
        verify(channel, never()).basicAck(anyLong(), anyBoolean());
        verifyNoInteractions(sender);
        assertEquals(0, count("notify.processed"));
        assertEquals(1, count("notify.failed"));
        assertEquals(1, metrics.get("notify.dlq.rejections").tag("stage", "application").counter().count());
    }

    @Test void eventRejectedWhileDisabledCanBeReplayedOnceEmailIsEnabled() throws Exception {
        listener(false).receive(message(Fixtures.shipments()), channel);
        verify(channel).basicNack(7, false, false);

        // Same event republished from the DLQ after SMTP was configured: the failure was not recorded as done.
        listener(true).receive(message(Fixtures.shipments()), channel);
        verify(sender, times(1)).send(any(SimpleMailMessage.class));
        verify(channel).basicAck(7, false);
        assertEquals(0, count("notify.duplicates"));
    }

    // ---------------------------------------------------------------- processing errors

    @Test void smtpFailureIsRejectedWithoutAckAndTheEventCanBeRetried() throws Exception {
        doThrow(new MailSendException("smtp down")).doNothing().when(sender).send(any(SimpleMailMessage.class));
        var listener = listener(true);

        listener.receive(message(Fixtures.shipments()), channel);
        verify(channel).basicNack(7, false, false);
        verify(channel, never()).basicAck(anyLong(), anyBoolean());
        assertEquals(0, count("notify.processed"));

        listener.receive(message(Fixtures.shipments()), channel);
        verify(sender, times(2)).send(any(SimpleMailMessage.class));
        verify(channel).basicAck(7, false);
        assertEquals(1, count("notify.processed"));
        assertEquals(0, count("notify.duplicates"));
    }

    @Test void ackTransportFailureDoesNotResendNorIssueASecondDisposition() throws Exception {
        doThrow(new IOException("connection lost")).doNothing().when(channel).basicAck(7, false);
        var listener = listener(true);
        var message = message(Fixtures.shipments());

        assertThrows(IOException.class, () -> listener.receive(message, channel));
        verify(channel, never()).basicNack(anyLong(), anyBoolean(), anyBoolean());

        listener.receive(message, channel); // broker redelivers after the channel closed
        verify(sender, times(1)).send(any(SimpleMailMessage.class));
        verify(channel, times(2)).basicAck(7, false);
        verify(channel, never()).basicNack(anyLong(), anyBoolean(), anyBoolean());
    }

    // ---------------------------------------------------------------- privacy

    @Test void successLogsNeverContainTheBodyOrPersonalData(CapturedOutput output) throws Exception {
        listener(true).receive(message(Fixtures.shipments()), channel);
        listener(true).receive(message(Fixtures.json("email", UUID.randomUUID())), channel);

        assertTrue(output.getAll().contains("result=processed"));
        assertNoPersonalData(output);
    }

    @Test void failureLogsNeverContainTheBodyOrPersonalData(CapturedOutput output) throws Exception {
        listener(false).receive(message(Fixtures.shipments()), channel);
        // Parsing errors quote the input in their message; that text must not reach the log.
        listener(true).receive(message("{\"destinatarioEmail\":\"secreto@example.com\",\"destinatarioNombre\":"), channel);
        listener(true).receive(message("Hola Ana Pérez, Avenida Ejemplo 123"), channel);

        assertTrue(output.getAll().contains("result=failed"));
        assertTrue(output.getAll().contains("smtp_channel_disabled"), "controlled reason code is logged");
        assertNoPersonalData(output);
    }

    private static void assertNoPersonalData(CapturedOutput output) {
        for (String data : PERSONAL_DATA.split("\\|"))
            assertFalse(output.getAll().contains(data), "log leaks: " + data);
        assertFalse(output.getAll().contains("Mensaje recibido"), "body logging was removed");
    }
}
