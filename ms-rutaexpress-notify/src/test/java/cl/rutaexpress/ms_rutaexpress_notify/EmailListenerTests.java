package cl.rutaexpress.ms_rutaexpress_notify;

import cl.rutaexpress.ms_rutaexpress_notify.messaging.EmailListener;
import com.rabbitmq.client.Channel;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(OutputCaptureExtension.class)
class EmailListenerTests {
    @ParameterizedTest
    @ValueSource(strings = {"Hola José", "{\"nuevoCampo\":42}", "{json sin terminar", "", "null"})
    void logsUnknownPayloadBeforeAcknowledging(String payload, CapturedOutput output) throws Exception {
        Channel channel = mock(Channel.class);
        MessageProperties properties = new MessageProperties();
        properties.setDeliveryTag(19);
        // No content-type: inspection must not depend on producer metadata.
        Message message = new Message(payload.getBytes(StandardCharsets.UTF_8), properties);
        doAnswer(invocation -> {
            assertTrue(output.getOut().contains("Mensaje recibido de q.cmd.email: " + payload));
            return null;
        }).when(channel).basicAck(19, false);

        new EmailListener().receive(message, channel);

        verify(channel).basicAck(19, false);
        verifyNoMoreInteractions(channel);
    }

    @Test
    void ignoresContentTypeAndAcceptsNonUtf8Bytes(CapturedOutput output) throws Exception {
        Channel channel = mock(Channel.class);
        MessageProperties properties = new MessageProperties();
        properties.setContentType("application/x-java-serialized-object");
        properties.setContentEncoding("unsupported-charset");
        properties.setDeliveryTag(23);
        byte[] body = {(byte) 0xff, 'x'};
        new EmailListener().receive(new Message(body, properties), channel);
        assertTrue(output.getOut().contains(new String(body, StandardCharsets.UTF_8)));
        verify(channel).basicAck(23, false);
        verifyNoMoreInteractions(channel);
    }

    @Test
    void ackTransportFailurePropagatesWithoutSecondDisposition() throws Exception {
        Channel channel = mock(Channel.class);
        doThrow(new IOException("connection lost")).when(channel).basicAck(7, false);
        assertThrows(IOException.class, () -> new EmailListener().receive(
                Fixtures.message("payload".getBytes(StandardCharsets.UTF_8)), channel));
        verify(channel).basicAck(7, false);
        verifyNoMoreInteractions(channel);
    }
}
