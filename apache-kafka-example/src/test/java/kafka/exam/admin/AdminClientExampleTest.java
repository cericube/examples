package kafka.exam.admin;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class AdminClientExampleTest {

	static AdminClientExample adminExample;
	static String bootstrapServers = "192.168.56.101:9092,192.168.56.102:9092,192.168.56.103:9092";
	static Properties props;

	// JUnit 5, 모든 테스트 전에 1회 수행 (static 필요)
	@BeforeAll
	public static void setup() {
		System.out.println("setup....."); 
		props = new Properties();
		props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
	}

	// JUnit 5, 모든 테스트 후에 1회 수행 (static 필요)
	@AfterAll
	public static void tearDown() {
		System.out.println("tearDown.....");
	}

	@Test
	public void testCreateTopics() throws ExecutionException, InterruptedException {
		System.out.println("[TEST] 토픽 생성 테스트");
		Properties props = new Properties();
		props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		AdminClientExample adminExample = new AdminClientExample();

		try (AdminClient admin = AdminClient.create(props)) {
			adminExample.createTopic(admin);
		}
	}
	
	
	
}
