package kafka.exam.stream;

import java.time.Duration;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;

public class CountStreamExample {
	String bootstrapServers = "192.168.56.101:9092,192.168.56.102:9092,192.168.56.103:9092";

	public void testStream() {
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "value-count-by-window-app");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

		StreamsBuilder builder = new StreamsBuilder();

		// 입력 스트림 (key 없음, value만 있음)
		KStream<String, String> inputStream  = builder.stream("topic-basic");

        // step 1: 모든 메시지를 동일한 key "fixed"로 설정 → 하나의 그룹으로 묶음
        KGroupedStream<String, String> groupedStream = inputStream
                .map((key, value) -> new KeyValue<>("fixed", value)) // key를 고정하여 그룹핑
                .groupByKey(); // key("fixed") 기준으로 그룹화'
        
     // step 2: 5분 단위 윈도우로 개수 세기
        TimeWindows windowSize = TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5)); // 5분 고정 윈도우
        KTable<Windowed<String>, Long> countedStream = groupedStream
                .windowedBy(windowSize)
                .count(); // 메시지 개수 집계

        // step 3: 집계 결과를 문자열로 변환하여 출력 토픽에 전송
        countedStream
                .toStream()
                .map((windowedKey, count) -> {
                    // 윈도우 시간 범위 표시
                    String timeWindow = windowedKey.window().startTime() + " ~ " + windowedKey.window().endTime();
                    String result = "Window: " + timeWindow + ", Count: " + count;

                    return new KeyValue<String, String>(null, result); // key 없이 전송
                }).to("event-counts-per-5min", Produced.with(Serdes.String(), Serdes.String())); // 출력 토픽

        // Kafka Streams 애플리케이션 시작
        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        // 종료 시 정상적으로 정리되도록 종료 훅 등록
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        // 스트림 실행
        streams.start();
        System.out.println("Kafka Streams 실행됨: 5분 단위 메시지 수를 집계 중...");
	}

	public static void main(String args[]) {

		 CountStreamExample streamExam = new CountStreamExample();
		 streamExam.testStream();
		
	}
}
