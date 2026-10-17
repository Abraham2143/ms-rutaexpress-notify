package cl.rutaexpress.ms_rutaexpress_notify;

import java.nio.file.*;
import java.time.Duration;
import java.util.UUID;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.core.*;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.awaitility.Awaitility.await;

@Testcontainers
@ExtendWith(OutputCaptureExtension.class)
@EnabledIfSystemProperty(named="rabbit.integration",matches="true")
@SpringBootTest(properties={"spring.rabbitmq.listener.simple.auto-startup=false"})
class RabbitIntegrationTests {
    @Container static final RabbitMQContainer rabbit=new RabbitMQContainer("rabbitmq:4.1-management-alpine");
    static final Path storage=createStorage();
    static Path createStorage() { try { return Files.createTempDirectory("notify-broker-test-"); } catch(Exception e) { throw new ExceptionInInitializerError(e); } }
    @DynamicPropertySource static void properties(DynamicPropertyRegistry r) {
        r.add("spring.rabbitmq.host",rabbit::getHost); r.add("spring.rabbitmq.port",rabbit::getAmqpPort);
        r.add("spring.rabbitmq.username",rabbit::getAdminUsername); r.add("spring.rabbitmq.password",rabbit::getAdminPassword);
        r.add("notify.storage.path",storage::toString);
    }
    @Autowired ConnectionFactory connectionFactory;
    @Autowired RabbitTemplate template;
    @Autowired RabbitListenerEndpointRegistry listeners;

    @Test void externallyProvisionedQueuesAndSuccessfulArtifacts(CapturedOutput output) throws Exception {
        // Test fixture represents rabbitmq-admin. This is never part of the application context.
        var admin = new RabbitAdmin(connectionFactory);
        admin.declareQueue(QueueBuilder.durable("q.cmd.email").deadLetterExchange("cmd.dead.dlx").deadLetterRoutingKey("email.dead").build());
        // Start only email: prove warehouse/label are not prerequisites for inspection.
        var email = listeners.getListenerContainers().stream()
                .filter(c -> java.util.Arrays.asList(((org.springframework.amqp.rabbit.listener.AbstractMessageListenerContainer)c)
                        .getQueueNames()).contains("q.cmd.email")).findFirst().orElseThrow();
        email.start();
        try {
            for (String payload : new String[]{"texto sin contrato", "{json inesperado", ""}) {
                template.send("", "q.cmd.email", new Message(payload.getBytes(StandardCharsets.UTF_8), new MessageProperties()));
                await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                        assertTrue(output.getOut().contains("Mensaje recibido de q.cmd.email: " + payload)));
            }
            assertNull(admin.getQueueInfo("q.cmd.warehouse"));
            assertNull(admin.getQueueInfo("q.cmd.label"));
        } finally { email.stop(); }

        provisionTestTopology(admin);
        String[] flows={"email","warehouse","label"};
        String[] direct={"email.send","warehouse.ticket","label.gen"};
        String[] topics={"email.status","warehouse.picking.created","label.ready"};
        for(int i=0;i<flows.length;i++) {
            String queue="q.cmd."+flows[i];
            for(String exchange:new String[]{"cmd.direct","cmd.topic"}) {
                byte[] body=Fixtures.json(flows[i],UUID.randomUUID());
                template.send(exchange,exchange.equals("cmd.direct") ? direct[i] : topics[i],Fixtures.message(body));
                var received=template.receive(queue,5000); assertNotNull(received,queue+" "+exchange);
                assertArrayEquals(body,received.getBody());
                for(String other:flows) if(!other.equals(flows[i])) assertNull(template.receive("q.cmd."+other));
            }
        }
        // Prove wildcard boundaries, including warehouse.# matching zero words.
        template.send("cmd.topic","email.too.many",Fixtures.message(new byte[0]));
        template.send("cmd.topic","label.too.many",Fixtures.message(new byte[0]));
        assertNull(template.receive("q.cmd.email",200)); assertNull(template.receive("q.cmd.label",200));
        template.send("cmd.topic","warehouse",Fixtures.message(new byte[0]));
        assertNotNull(template.receive("q.cmd.warehouse",3000));

        listeners.start();
        try {
            for(int i=1;i<flows.length;i++) {
                template.send("cmd.direct",direct[i],Fixtures.message("{invalid".getBytes(java.nio.charset.StandardCharsets.UTF_8)));
                Message dead=template.receive("q.cmd."+flows[i]+".dlq",10000);
                assertNotNull(dead,flows[i]+" DLQ");
                assertEquals("q.cmd."+flows[i],dead.getMessageProperties().getHeaders().get("x-first-death-queue"));
                assertEquals("rejected",dead.getMessageProperties().getHeaders().get("x-first-death-reason"));
                for(String other:flows) if(!other.equals(flows[i])) assertNull(template.receive("q.cmd."+other+".dlq"));
            }
            for(String flow:new String[]{"warehouse","label"}) {
                UUID id=UUID.randomUUID(); var message=Fixtures.message(Fixtures.json(flow,id));
                template.send("cmd.direct",flow.equals("label") ? "label.gen" : "warehouse.ticket",message);
                Path artifact=storage.resolve(flow+"/"+id+(flow.equals("label") ? ".pdf" : ".txt"));
                await().atMost(Duration.ofSeconds(10)).until(() -> Files.exists(artifact));
                byte[] before=Files.readAllBytes(artifact);
                template.send("cmd.direct",flow.equals("label") ? "label.gen" : "warehouse.ticket",message);
                await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                    assertArrayEquals(before,Files.readAllBytes(artifact));
                    assertEquals(0,admin.getQueueInfo("q.cmd."+flow).getMessageCount());
                });
            }
            template.send("cmd.direct","email.send",Fixtures.message(Fixtures.json("email",UUID.randomUUID())));
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                    assertTrue(output.getOut().contains("email.send.v1")));
            assertNull(template.receive("q.cmd.email.dlq",500),"inspection must not call disabled SMTP");
        } finally { listeners.stop(); }
    }
    private static void provisionTestTopology(RabbitAdmin admin) {
        var direct = new DirectExchange("cmd.direct", true, false);
        var topic = new TopicExchange("cmd.topic", true, false);
        var dead = new DirectExchange("cmd.dead.dlx", true, false);
        admin.declareExchange(direct);
        admin.declareExchange(topic);
        admin.declareExchange(dead);
        String[] flows = {"email", "warehouse", "label"};
        String[] keys = {"email.send", "warehouse.ticket", "label.gen"};
        String[] patterns = {"email.*", "warehouse.#", "label.*"};
        for (int i = 0; i < flows.length; i++) {
            var queue = QueueBuilder.durable("q.cmd." + flows[i])
                    .deadLetterExchange("cmd.dead.dlx").deadLetterRoutingKey(flows[i] + ".dead").build();
            var dlq = QueueBuilder.durable("q.cmd." + flows[i] + ".dlq").build();
            admin.declareQueue(queue);
            admin.declareQueue(dlq);
            admin.declareBinding(BindingBuilder.bind(queue).to(direct).with(keys[i]));
            admin.declareBinding(BindingBuilder.bind(queue).to(topic).with(patterns[i]));
            admin.declareBinding(BindingBuilder.bind(dlq).to(dead).with(flows[i] + ".dead"));
        }
    }

}
