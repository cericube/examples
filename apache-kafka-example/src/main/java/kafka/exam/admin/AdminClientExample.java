package kafka.exam.admin;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigResource;

public class AdminClientExample {

	public void createTopic(AdminClient admin) throws ExecutionException, InterruptedException {
		System.out.println("1. >> 토픽 생성 요청...");
		// topic-basic 생성
		// - 파티션 수: 1
		// - 복제 계수: 1 (리더 + 팔로워 포함, short 타입으로 지정)
		NewTopic topic1 = new NewTopic("topic-basic", 1, (short) 1);

		// topic-auto-replica 생성
		// - 파티션 수: 3
		// - 복제 계수: 명시하지 않음 → Kafka 브로커 설정의 default.replication.factor 사용
		NewTopic topic2 = new NewTopic("topic-auto-replica", Optional.of(3), Optional.empty());

		// topic-config 생성
		// - 파티션 수: 3
		// - 복제 계수: 1
		// - 추가 설정:
		// - 메시지 보관 시간: 1시간 (retention.ms = 3600000ms)
		// - 로그 압축 방식: compact (cleanup.policy = compact)
		Map<String, String> configs = Map.of("retention.ms", "3600000", // 메시지 보관 시간 설정 (1시간)
				"cleanup.policy", "compact" // 로그 정리 방식 설정 (압축)
		);
		NewTopic topic3 = new NewTopic("topic-config", 3, (short) 1).configs(configs);

		// AdminClient를 통해 토픽 생성 요청
		CreateTopicsResult result = admin.createTopics(List.of(topic1, topic2, topic3));

		// 모든 토픽 생성 요청 완료까지 대기 (예외 발생 시 전체 실패로 간주됨)
		result.all().get(); // throws Exception

		// 각 토픽의 생성 결과 출력
		System.out.println(">> 토픽 생성 결과...");
		Map<String, KafkaFuture<Void>> futures = result.values();
		for (Map.Entry<String, KafkaFuture<Void>> entry : futures.entrySet()) {
			String topicName = entry.getKey();
			try {
				// 토픽 생성 성공 시
				entry.getValue().get();
				System.out.printf("토픽 생성 성공: %s%n", topicName);
			} catch (ExecutionException e) {
				// 토픽 생성 실패 시 예외 메시지 출력
				System.out.printf("토픽 생성 실패: %s - %s%n", topicName, e.getCause().getMessage());
			}
		}
	}

	public void alterTopics(AdminClient admin) throws ExecutionException, InterruptedException {
		System.out.println("2. >> 토픽 설정 변경...");

		String topicName = "topic-basic";
		String configKey = "retention.ms";
		String configValue = "300000";

		ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);

		// 1️⃣ 변경 전 설정 조회
		Config originalConfig = admin.describeConfigs(Collections.singleton(resource)).all().get().get(resource);
		System.out.println("변경 전 설정:");
		for (ConfigEntry entry : originalConfig.entries()) {
			if (entry.name().equals(configKey)) {
				System.out.printf("- %s = %s%n", entry.name(), entry.value());
			}
		}

		// 2. 설정 변경 요청
		// SET 값을 새로 설정 또는 변경
		// DELETE 해당 설정 키를 제거
		// APPEND 값에 항목 추가 (쉼표 구분 설정에 유용)
		// SUBTRACT 값에서 항목 제거
		AlterConfigOp op = new AlterConfigOp(new ConfigEntry(configKey, configValue), // 5분
				AlterConfigOp.OpType.SET // SET, DELETE, APPEND, SUBTRACT 중 하나
		);

		Map<ConfigResource, Collection<AlterConfigOp>> updates = Map.of(resource, List.of(op));

		AlterConfigsResult result = admin.incrementalAlterConfigs(updates);
		result.all().get(); // (예외 발생 시 전체 실패로 간주됨)
		System.out.println("설정 변경 완료");

		// 3️⃣ 변경 후 설정 조회
		Config updatedConfig = admin.describeConfigs(Collections.singleton(resource)).all().get().get(resource);

		System.out.println(" 변경 후 설정:");
		for (ConfigEntry entry : updatedConfig.entries()) {
			if (entry.name().equals(configKey)) {
				System.out.printf("- %s = %s%n", entry.name(), entry.value());
			}
		}
	}

	public void describeTopics(AdminClient admin) throws ExecutionException, InterruptedException {
		// 조회할 토픽 이름 목록
		List<String> topicNames = List.of("topic-basic", "topic-auto-replica", "topic-config");
		// 토픽 상세 정보 조회 요청
		DescribeTopicsResult result = admin.describeTopics(topicNames);
		// 토픽별 Future 결과 맵 반환 (String → KafkaFuture<TopicDescription>)
		Map<String, KafkaFuture<TopicDescription>> topicMap = result.topicNameValues();

		// 결과 출력
		for (Map.Entry<String, KafkaFuture<TopicDescription>> entry : topicMap.entrySet()) {
			String topicName = entry.getKey();
			try {
				TopicDescription desc = entry.getValue().get(); // 결과 받기
				System.out.printf("토픽명: %s, 파티션 수: %d, 복제본 수: %d%n", topicName, desc.partitions().size(),
						desc.partitions().get(0).replicas().size() // 복제본 수 확인 (첫 파티션 기준)
				);
			} catch (Exception e) {
				System.out.printf("토픽 정보 조회 실패: %s - %s%n", topicName, e.getMessage());
			}
		}
	}

	public void deleteTopics(AdminClient admin) throws ExecutionException, InterruptedException {
		List<String> topicNames = List.of("topic-auto-replica", "topic-config");
		DeleteTopicsResult result = admin.deleteTopics(topicNames);
		result.all().get(); // 개별 예외 발생 시 전체 실패로 간주됨
	}

	public void describeCluster(AdminClient admin) throws ExecutionException, InterruptedException {
		DescribeClusterResult cluster = admin.describeCluster();

		System.out.println("클러스터 ID: " + cluster.clusterId().get());
		System.out.println("컨트롤러: " + cluster.controller().get());
		System.out.println("브로커 목록:");
		// cluster.nodes().get().forEach(System.out::println);
		Collection<Node> nodes = cluster.nodes().get();
		for (Node node : nodes) {
			System.out.println(node);
		}
	}

	public void listConsumerGroups(AdminClient admin) throws ExecutionException, InterruptedException {
		Collection<ConsumerGroupListing> groups = admin.listConsumerGroups().all().get();

		for (ConsumerGroupListing group : groups) {
			System.out.println("그룹 ID: " + group.groupId());
		}
	}

}
