package kafka.exam.producer;

import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerExample {

	// Kafka 클러스터의 브로커 주소들 (쉼표로 구분된 리스트)
	String bootstrapServers = "192.168.56.101:9092,192.168.56.102:9092,192.168.56.103:9092";

	// Kafka Producer 설정을 위한 Properties 객체 생성 메서드
	private Properties setup() {
		Properties props;
		System.out.println(">>> Kafka Producer 테스트 시작...");

		props = new Properties();

		// Kafka 서버 주소 설정
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

		// 메시지의 Key를 직렬화할 클래스 (문자열 기준)
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// 메시지의 Value를 직렬화할 클래스 (문자열 기준)
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// acks 설정: 'all'은 모든 복제본(replica)이 ack 해야 전송 성공으로 간주
		props.put(ProducerConfig.ACKS_CONFIG, "all");

		return props;
	}

	// Kafka 메시지를 비동기로 전송하는 테스트 메서드
	public void runTest(String name) {

		// 각 runTest 호출마다 독립적인 스레드 생성
		Thread t = new Thread(new Runnable() {

			@Override
			public void run() {
				// Kafka 설정 정보 로딩
				Properties props = setup();

				// try-with-resources 구문으로 KafkaProducer 생성 (자동 close 처리)
				try (Producer<String, String> producer = new KafkaProducer<>(props)) {
					Random random = new Random();

					// 1만 건의 메시지 전송
					for (int x = 0; x < 10000; x++) {

						// 전송할 메시지 생성 (Key는 null, Value는 name + 인덱스)
						ProducerRecord<String, String> record = new ProducerRecord<>(
								"topic-basic", // 전송 대상 Kafka 토픽
								"> " + name + " : " + x // 메시지 내용
						);

						// 비동기 전송: 전송 성공 또는 실패 처리
						producer.send(record, (metadata, exception) -> {
							if (exception != null) {
								// 예외 발생 시 로그 출력
								System.err.println("전송 실패: " + exception.getMessage());
							} else {
								// 전송 성공 시 topic, partition, offset 정보 출력
								System.out.printf("전송 성공: %s-%d@%d%n",
										metadata.topic(), metadata.partition(), metadata.offset());
							}
						});

						try {
							// 메시지 간의 전송 간격을 무작위(0~999ms)로 설정하여 부하 테스트
							long sleepTime = random.nextInt(1000);
							Thread.sleep(sleepTime);
						} catch (Exception e) {
							// 예외 무시
						}
					}

					// 프로듀서의 내부 버퍼에 남은 메시지를 강제로 전송
					producer.flush();
				}
			}

		}, name); // 스레드 이름 설정 (PD_1, PD_2, ...)

		t.start(); // 스레드 실행
	}

	// main 메서드: 여러 개의 프로듀서 스레드를 동시에 실행
	public static void main(String args[]) {
		ProducerExample exam1  = new ProducerExample();

		// 6개의 독립적인 프로듀서 스레드를 실행 (병렬 테스트)
		exam1.runTest("PD_1");
		exam1.runTest("PD_2");
		exam1.runTest("PD_3");
		exam1.runTest("PD_4");
		exam1.runTest("PD_5");
		exam1.runTest("PD_6");
	}
}
