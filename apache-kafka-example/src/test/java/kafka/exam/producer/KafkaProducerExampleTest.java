package kafka.exam.producer;


import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.*;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaProducerExampleTest {

	String bootstrapServers = "192.168.56.101:9092,192.168.56.102:9092,192.168.56.103:9092";
    Properties props;

    @BeforeEach
    public void setup() {
        System.out.println(">>> Kafka Producer 테스트 시작...");
        props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all"); // 모든 replica가 ack해야 성공
    }

    @AfterEach
    public void tearDown() {
        System.out.println(">>> Kafka Producer 테스트 종료\n");
    }

    @Test
    public void testSendSimpleMessage() {
        // 기본 메시지 전송
        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
        	//key : 어떤 파티션에 메시지를 보낼지 결정하는 기준"
            ProducerRecord<String, String> record = new ProducerRecord<>("topic-basic", "key1", "Hello Kafka!");
            producer.send(record);
            producer.flush();
            System.out.println("메시지 전송 완료");
        }
    }

    @Test
    public void testSendWithCallback() {
        // 콜백을 사용한 비동기 전송
        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            ProducerRecord<String, String> record = new ProducerRecord<>("topic-basic", "key2", "With Callback");

            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    System.err.println("전송 실패: " + exception.getMessage());
                } else {
                    System.out.printf("전송 성공: %s-%d@%d%n",
                            metadata.topic(), metadata.partition(), metadata.offset());
                }
            });

            producer.flush();
        }
    }

    @Test
    public void testSendSync() {
        // 동기 전송 (get()으로 block)
        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            ProducerRecord<String, String> record = new ProducerRecord<>("topic-basic", "key3", "Sync Send");
            RecordMetadata metadata = producer.send(record).get();
            System.out.printf("동기 전송 성공: %s-%d@%d%n",
                    metadata.topic(), metadata.partition(), metadata.offset());
        } catch (InterruptedException | ExecutionException e) {
            System.err.println("동기 전송 실패: " + e.getMessage());
        }
    }

    @Test
    public void testSendToSpecificPartition() {
        // 파티션을 직접 지정하여 전송
        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            ProducerRecord<String, String> record =
                    new ProducerRecord<>("topic-basic", 1, "key4", "To partition...");
            producer.send(record);
            producer.flush();
            System.out.println("파티션 지정 메시지 전송 완료");
        }
    }

    @Test
    public void testSendWithHeaders() {
        // Kafka 메시지에 헤더 추가
        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
        	Headers headers = new RecordHeaders()
                    .add("app-id", "producer-test".getBytes())     // 애플리케이션 ID
                    .add("trace-id", "1234".getBytes());            // 트레이싱용 ID

            ProducerRecord<String, String> record =
                    new ProducerRecord<>("topic-basic", null, null, "key5", "Message with headers", headers);

            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    System.out.printf("헤더 포함 메시지 전송: %s-%d@%d%n",
                            metadata.topic(), metadata.partition(), metadata.offset());
                }
            });

            producer.flush();
        }
    }

    @Test
    public void testSendMultipleMessages() {
        // 여러 개 메시지를 반복 전송
        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < 5; i++) {
                String key = "key-" + i;
                String value = "message-" + i;
                ProducerRecord<String, String> record = new ProducerRecord<>("topic-basic", key, value);
                producer.send(record);
            }
            producer.flush();
            System.out.println("✔ 5개 메시지 배치 전송 완료");
        }
    }

    @Test
    public void testSendWithTimestamp() {
        // 타임스탬프 명시적 지정
        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            long now = System.currentTimeMillis();
            ProducerRecord<String, String> record =
                    new ProducerRecord<>("topic-basic", 0, now, "key6", "With timestamp");
            producer.send(record);
            producer.flush();
            System.out.println("✔ 타임스탬프 포함 메시지 전송 완료");
        }
    }

    @Test
    public void testSendWithCompression() {
        // 메시지 압축 설정 (gzip, snappy, lz4, zstd)
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");

        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            ProducerRecord<String, String> record =
                    new ProducerRecord<>("topic-basic", "key7", "Compressed message with gzip");
            producer.send(record);
            producer.flush();
            System.out.println("✔ gzip 압축 메시지 전송 완료");
        }
    }

    @Test
    public void testSendWithBatching() {
        // 배치 전송 최적화를 위한 설정
        props.put(ProducerConfig.LINGER_MS_CONFIG, "100"); // 100ms 기다려 배치
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 32 * 1024); // 32KB

        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < 10; i++) {
                ProducerRecord<String, String> record =
                        new ProducerRecord<>("topic-basic", "batch-key", "batch-message-" + i);
                producer.send(record);
            }
            producer.flush();
            System.out.println("✔ 배치 전송 완료");
        }
    }

    @Test
    public void testSendWithIdempotence() {
        // 중복 없는 전송 보장 (exactly-once semantics)
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            ProducerRecord<String, String> record =
                    new ProducerRecord<>("topic-basic", "key8", "Idempotent message");
            producer.send(record);
            producer.flush();
            System.out.println("✔ Idempotent 메시지 전송 완료");
        }
    }

    @Test
	public void testTransactionalSend() {
        // 트랜잭션 기반 메시지 전송
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "txn-producer-01");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            producer.initTransactions(); // 트랜잭션 초기화

            producer.beginTransaction(); // 트랜잭션 시작
            producer.send(new ProducerRecord<>("topic-basic", "key9", "Transactional message 1"));
            producer.send(new ProducerRecord<>("topic-basic", "key10", "Transactional message 2"));
            producer.commitTransaction(); // 커밋
            System.out.println("✔ 트랜잭션 메시지 전송 완료");
        } catch (Exception e) {
            System.err.println("❌ 트랜잭션 전송 실패: " + e.getMessage());
        }
    }
}
