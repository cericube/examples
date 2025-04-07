package kafka.exam.consumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class ConsumerExample {

	// Kafka 브로커(서버)의 주소 목록
	String bootstrapServers = "192.168.56.101:9092,192.168.56.102:9092,192.168.56.103:9092";

	// Kafka Consumer 설정 정보를 담은 Properties 객체 생성
	private Properties setup() {
		Properties props;
		System.out.println(">>> Kafka Consumer 테스트 시작...");
		props = new Properties();

		// Kafka 클러스터의 주소 설정
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

		// 이 컨슈머가 속할 그룹 ID 설정 (같은 그룹 ID는 메시지를 나눠 소비)
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "my-consumer-group");

		// 메시지의 Key를 문자열로 변환하기 위한 설정
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

		// 메시지의 Value를 문자열로 변환하기 위한 설정
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

		// 처음 접속할 때 읽을 위치: earliest는 가장 오래된 메시지부터 읽음
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		// 오프셋(현재까지 읽은 위치)을 자동으로 저장할지 여부 (true면 자동 커밋)
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

		return props;
	}

	// Kafka 메시지를 읽고 출력하는 테스트용 스레드 실행
	public void runTest(String name) {

		// 새 스레드를 만들어 병렬로 실행 (여러 개 컨슈머를 동시에 실행 가능)
		Thread t = new Thread(new Runnable() {

			@Override
			public void run() {
				// 설정 정보 로딩
				Properties props = setup();

				// KafkaConsumer 생성 (자동으로 close 처리됨)
				try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {

					// 읽고자 하는 토픽 등록
					consumer.subscribe(Collections.singletonList("topic-basic"));

					// 무한 루프를 돌며 계속해서 메시지를 읽음
					while (true) {

						// poll(): 브로커로부터 메시지를 요청
						// 최대 100ms 동안 기다렸다가 한꺼번에 여러 메시지를 가져옴
						ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

						// 가져온 메시지들 하나씩 출력
						for (ConsumerRecord<String, String> record : records) {
							System.out.printf("%s: 파티션 %d, 값: %s%n", name, record.partition(), record.value());
						}
					}
				}
			}

		}, name); // 스레드 이름 지정 (예: CM_1, CM_2 등)

		t.start(); // 스레드 실행
	}

	// main 메서드에서 여러 컨슈머를 동시에 실행하여 병렬 처리 테스트
	public static void main(String args[]) {
		ConsumerExample consumer = new ConsumerExample();

		// 3개의 컨슈머 스레드를 실행 (하나의 토픽을 나눠서 소비)
		consumer.runTest("CM_1");
		consumer.runTest("CM_2");
		consumer.runTest("CM_3");
	}
}
