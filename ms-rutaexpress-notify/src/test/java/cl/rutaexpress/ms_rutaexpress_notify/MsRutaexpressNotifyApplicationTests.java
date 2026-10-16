package cl.rutaexpress.ms_rutaexpress_notify;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(properties={"spring.rabbitmq.username=test", "spring.rabbitmq.password=test", "spring.rabbitmq.listener.simple.auto-startup=false"})
class MsRutaexpressNotifyApplicationTests {

	@Test
	void contextLoads() {
	}

}
