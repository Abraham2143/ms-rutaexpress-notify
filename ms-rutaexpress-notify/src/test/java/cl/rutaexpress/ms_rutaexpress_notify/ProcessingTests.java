package cl.rutaexpress.ms_rutaexpress_notify;

import cl.rutaexpress.ms_rutaexpress_notify.contract.*;
import cl.rutaexpress.ms_rutaexpress_notify.processing.*;
import cl.rutaexpress.ms_rutaexpress_notify.messaging.*;
import cl.rutaexpress.ms_rutaexpress_notify.config.RabbitConfig;
import com.rabbitmq.client.Channel;
import com.icegreen.greenmail.util.*;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import jakarta.validation.Validation;
import org.apache.pdfbox.Loader;
import org.apache.pdfbox.text.PDFTextStripper;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.mail.javamail.JavaMailSenderImpl;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.support.ListenerExecutionFailedException;
import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import java.nio.file.*;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class ProcessingTests {
    @TempDir Path directory;
    private final CommandDecoder decoder=new CommandDecoder(Validation.buildDefaultValidatorFactory().getValidator());
    private final SimpleMeterRegistry metrics=new SimpleMeterRegistry();
    private final CommandHandler handler=new CommandHandler(decoder,new IdempotencyGuard(100,86400),metrics);
    private final Channel channel=mock(Channel.class);

    @ParameterizedTest @ValueSource(strings={"email","warehouse","label"})
    void ackOnlyAfterEffectAndSkipDuplicate(String flow) throws Exception {
        var message=Fixtures.message(Fixtures.json(flow,UUID.randomUUID()));
        var effect=mock(CommandHandler.Processor.class);
        doAnswer(invocation -> { verify(channel,never()).basicAck(anyLong(),anyBoolean()); return null; }).when(effect).process(any());
        handler.handle(flow,message,channel,effect);
        handler.handle(flow,message,channel,effect);
        verify(effect,times(1)).process(any()); verify(channel,times(2)).basicAck(7,false);
        verify(channel,never()).basicNack(anyLong(),anyBoolean(),anyBoolean());
        assertEquals(1,metrics.get("notify.duplicates").counter().count());
    }
    @ParameterizedTest @ValueSource(strings={"email","warehouse","label"})
    void failuresNackWithoutAckAndMayBeReplayed(String flow) throws Exception {
        var message=Fixtures.message(Fixtures.json(flow,UUID.randomUUID()));
        handler.handle(flow,message,channel,c -> { throw new java.io.IOException("secret must not be logged"); });
        verify(channel).basicNack(7,false,false); verify(channel,never()).basicAck(anyLong(),anyBoolean());
        handler.handle(flow,message,channel,c -> {});
        verify(channel).basicAck(7,false);
        assertEquals(1,metrics.get("notify.dlq.rejections").counter().count());
    }
    @ParameterizedTest @ValueSource(strings={"{", "null", "{}", "[]"})
    void malformedMessagesAreRejected(String body) throws Exception {
        handler.handle("email",Fixtures.message(body.getBytes(StandardCharsets.UTF_8)),channel,c -> fail("invalid processed"));
        verify(channel).basicNack(7,false,false); verify(channel,never()).basicAck(anyLong(),anyBoolean());
    }
    @Test void validatesTypesDatesFieldsAndFlow() {
        String valid=new String(Fixtures.json("email",UUID.randomUUID()),StandardCharsets.UTF_8);
        for (String invalid:new String[]{valid.replace("email.send.v1","unknown"),valid.replace("RE-42","../../escape"),
                valid.replace("ana@example.com","invalid"),valid.replace("2026-10-15T18:00:00-03:00","2026-10-15T18:00:00"),
                valid.replace("\"id\":42","\"id\":\"42\""),valid.replace("\"id\":42","\"id\":-1"),
                valid.replace("\"EMAIL\"","\"SMS\""),valid.replace("ACEPTADO","CREADO"),
                valid.replace("\"id\":42","\"id\":42,\"id\":43"),valid+"{}"})
            assertThrows(Exception.class,() -> decoder.decode(invalid.getBytes(StandardCharsets.UTF_8),"email"));
        assertThrows(Exception.class,() -> decoder.decode(valid.getBytes(StandardCharsets.UTF_8),"label"));
    }
    @Test void producesPickingAndReadableDeterministicPdf() throws Exception {
        var store=new ArtifactStore(directory.toString());
        UUID id=UUID.randomUUID();
        var warehouse=decoder.decode(Fixtures.json("warehouse",id),"warehouse");
        new WarehouseService(store).process(warehouse);
        assertTrue(Files.readString(directory.resolve("warehouse/"+id+".txt")).contains("Ana Pérez"));
        var label=decoder.decode(Fixtures.json("label",id),"label");
        var service=new LabelService(store); service.process(label);
        Path pdf=directory.resolve("label/"+id+".pdf"); byte[] first=Files.readAllBytes(pdf);
        service.process(label); assertArrayEquals(first,Files.readAllBytes(pdf));
        try (var document=Loader.loadPDF(pdf.toFile())) {
            String text=new PDFTextStripper().getText(document);
            for(String expected:new String[]{"RE-42","Ana Pérez","Bodega Central","Avenida Ejemplo 123","42","ACEPTADO"}) assertTrue(text.contains(expected),text);
            assertEquals(1,document.getNumberOfPages());
        }
        try (var files=Files.list(pdf.getParent())) { assertEquals(1,files.count()); }
    }
    @ParameterizedTest @ValueSource(strings={"ACEPTADO","EN_RUTA","ENTREGADO"})
    void sendsSmtpThroughRealTestServer(String status) throws Exception {
        var smtp=new GreenMail(new ServerSetup(0,"127.0.0.1",ServerSetup.PROTOCOL_SMTP));
        smtp.start();
        try {
            var sender=new JavaMailSenderImpl(); sender.setHost("127.0.0.1"); sender.setPort(smtp.getSmtp().getPort());
            var service=new EmailService(sender,true,"notify@example.com");
            String json=new String(Fixtures.json("email",UUID.randomUUID()),StandardCharsets.UTF_8).replace("ACEPTADO",status);
            handler.handle("email", Fixtures.message(json.getBytes(StandardCharsets.UTF_8)), channel, service::process);
            assertTrue(smtp.waitForIncomingEmail(3000,1));
            var mail=smtp.getReceivedMessages()[0];
            assertTrue(mail.getSubject().contains(status)); assertTrue(mail.getContent().toString().contains("RE-42"));
            assertEquals("ana@example.com",mail.getAllRecipients()[0].toString()); verify(channel).basicAck(7,false);
        } finally { smtp.stop(); }
    }
    @Test void disabledChannelsFailControlled() throws Exception {
        var email=decoder.decode(Fixtures.json("email",UUID.randomUUID()),"email");
        var service=new EmailService(new JavaMailSenderImpl(),false,"notify@example.com");
        assertThrows(IllegalStateException.class,() -> service.process(email));
        String push=new String(Fixtures.json("email",UUID.randomUUID()),StandardCharsets.UTF_8).replace("EMAIL","WEBPUSH");
        handler.handle("email", Fixtures.message(push.getBytes(StandardCharsets.UTF_8)), channel, service::process);
        verify(channel).basicNack(7,false,false); verify(channel,never()).basicAck(anyLong(),anyBoolean());
    }
    @Test void storageFailuresFromBothListenersAreRejected() throws Exception {
        Path blocked=directory.resolve("file-not-directory"); Files.writeString(blocked,"blocked");
        var store=new ArtifactStore(blocked.toString());
        new WarehouseListener(handler,new WarehouseService(store)).receive(
                Fixtures.message(Fixtures.json("warehouse",UUID.randomUUID())),channel);
        new LabelListener(handler,new LabelService(store)).receive(
                Fixtures.message(Fixtures.json("label",UUID.randomUUID())),channel);
        verify(channel,times(2)).basicNack(7,false,false);
        verify(channel,never()).basicAck(anyLong(),anyBoolean());
    }
    @Test void smtpFailureIsRejectedWithoutSuccessfulAck() throws Exception {
        int closedPort;
        try(var socket=new java.net.ServerSocket(0)) { closedPort=socket.getLocalPort(); }
        var sender=new JavaMailSenderImpl(); sender.setHost("127.0.0.1"); sender.setPort(closedPort);
        sender.getJavaMailProperties().setProperty("mail.smtp.connectiontimeout","1000");
        var service = new EmailService(sender,true,"notify@example.com");
        handler.handle("email", Fixtures.message(Fixtures.json("email",UUID.randomUUID())), channel, service::process);
        verify(channel).basicNack(7,false,false); verify(channel,never()).basicAck(anyLong(),anyBoolean());
    }
    @Test void longWidePdfTextFitsWithinPage() throws Exception {
        UUID id=UUID.randomUUID();
        String body=new String(Fixtures.json("label",id),StandardCharsets.UTF_8)
                .replace("Bodega Central","W".repeat(300)).replace("Avenida Ejemplo 123, Santiago","W".repeat(300));
        var service=new LabelService(new ArtifactStore(directory.toString()));
        service.process(decoder.decode(body.getBytes(StandardCharsets.UTF_8),"label"));
        try(var pdf=Loader.loadPDF(directory.resolve("label/"+id+".pdf").toFile())) {
            var stripper=new PDFTextStripper() {
                @Override protected void processTextPosition(org.apache.pdfbox.text.TextPosition position) {
                    assertTrue(position.getXDirAdj()+position.getWidthDirAdj()<575,"horizontal overflow");
                    assertTrue(position.getYDirAdj()<750,"vertical overflow");
                    super.processTextPosition(position);
                }
            };
            assertTrue(stripper.getText(pdf).contains("WWWWW"));
        }
    }
    @Test void concurrentDuplicateRunsOnlyOnce() throws Exception {
        var guard=new IdempotencyGuard(10,60); var count=new AtomicInteger();
        try(var pool=Executors.newFixedThreadPool(8)) {
            var futures=new java.util.ArrayList<Future<?>>();
            for(int i=0;i<20;i++) futures.add(pool.submit(() -> { try { guard.execute("email:one",count::incrementAndGet); } catch(Exception e) { throw new RuntimeException(e); } }));
            for(var future:futures) future.get(3,TimeUnit.SECONDS);
        }
        assertEquals(1,count.get());
        guard.execute("label:one",count::incrementAndGet); assertEquals(2,count.get());
    }
    @Test void ackTransportFailureKeepsCompletedEffect() throws Exception {
        var message=Fixtures.message(Fixtures.json("email",UUID.randomUUID())); var count=new AtomicInteger();
        doThrow(new java.io.IOException("connection lost")).when(channel).basicAck(7,false);
        assertThrows(java.io.IOException.class,() -> handler.handle("email",message,channel,c -> count.incrementAndGet()));
        doNothing().when(channel).basicAck(7,false);
        handler.handle("email",message,channel,c -> count.incrementAndGet());
        assertEquals(1,count.get()); verify(channel,never()).basicNack(anyLong(),anyBoolean(),anyBoolean());
    }
    @Test void containerErrorsRejectInManualMode() {
        var factory=new RabbitConfig().rabbitListenerContainerFactory(mock(ConnectionFactory.class),metrics,false);
        var container=factory.createListenerContainer();
        // Exercise the same handler through container's configured error handler.
        var errorHandler=(org.springframework.util.ErrorHandler)org.springframework.test.util.ReflectionTestUtils.getField(container,"errorHandler");
        var failed=new ListenerExecutionFailedException("conversion",new IllegalArgumentException(),Fixtures.message(new byte[0]));
        var error=assertThrows(AmqpRejectAndDontRequeueException.class,() -> errorHandler.handleError(failed));
        assertTrue(error.isRejectManual());
        assertEquals(1,metrics.get("notify.dlq.rejections").tag("stage","container").counter().count());
    }
}
