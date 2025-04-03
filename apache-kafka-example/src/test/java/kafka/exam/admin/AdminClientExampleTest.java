package kafka.exam.admin;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteConsumerGroupsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class AdminClientExampleTest {
	String bootstrapServers = "192.168.56.101:9092,192.168.56.102:9092,192.168.56.103:9092";

	Properties props;

	@BeforeEach
	public void setup() {
		System.out.println("...setup...");
		props = new Properties();
		props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
	}

	@AfterEach
	public void tearDown() {
		System.out.println("...tearDown...");
	}

	@Test
	public void testCreateTopics() {
		System.out.println("[TEST] 토픽 생성 테스트...");
		try (AdminClient admin = AdminClient.create(props)) {
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
			result.all().get();

			// result.all().get()에서 오류 발생히 호출 안됨
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

		} catch (ExecutionException | InterruptedException e) {
			System.out.println(e.getMessage());
		}
	}

	@Test
	public void testListTopics() {
		System.out.println("[TEST] 토픽 목록 조회 테스트...");
		try (AdminClient admin = AdminClient.create(props)) {
			ListTopicsResult listTopicsResult = admin.listTopics();
			// 2. Future에서 토픽 이름 Set 추출
			Set<String> topicNames = listTopicsResult.names().get();

			topicNames.forEach(t -> System.out.println("토픽: " + t));
		} catch (ExecutionException | InterruptedException e) {
			System.out.println(e.getMessage());
		}
	}

	@Test
	public void testDescribeTopics() {
		System.out.println("[TEST] 토픽 정보 조회 테스트...");

		List<String> topicNames = List.of("topic-basic", "topic-auto-replica", "topic-config");
		try (AdminClient admin = AdminClient.create(props)) {
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
	}

	@Test
	public void testDescribeTopicConfigs() {
		System.out.println("[TEST] 토픽 설정 조회 테스트...");

		String topicName = "topic-basic";
		String configKey = null;

		try (AdminClient admin = AdminClient.create(props)) {
			ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
			Config originalConfig = admin.describeConfigs(Collections.singleton(resource)).all().get().get(resource);
			for (ConfigEntry entry : originalConfig.entries()) {
				if (configKey == null) {
					System.out.printf("- %s = %s%n", entry.name(), entry.value());
				} else if (entry.name().equals(configKey)) {
					System.out.printf("- %s = %s%n", entry.name(), entry.value());
				}
			}

		} catch (ExecutionException | InterruptedException e) {
			System.out.println(e.getMessage());
		}
	}

	@Test
	public void testAlterTopics() {
		System.out.println("[TEST] 토픽 설정 변경 테스트...");

		String topicName = "topic-basic";
		String configKey = "retention.ms";
		String configValue = "500000";

		try (AdminClient admin = AdminClient.create(props)) {
			ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
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
		} catch (ExecutionException | InterruptedException e) {
			System.out.println(e.getMessage());
		}
	}

	@Test
	public void testDeleteTopics() {
		System.out.println("[TEST] 토픽 삭제 테스트...");

		// List<String> removeTopicNames = List.of("topic-auto-replica",
		// "topic-config");
		List<String> topicNames = List.of("topic-basic");

		try (AdminClient admin = AdminClient.create(props)) {
			DeleteTopicsResult result = admin.deleteTopics(topicNames);
			result.all().get(); // 개별 예외 발생 시 전체 실패로 간주됨
		} catch (ExecutionException | InterruptedException e) {
			System.out.println(e.getMessage());
		}
	}

	@Test
	public void testDescribeCluster() {
		System.out.println("[TEST] 클러스터 정보 조회 테스트...");

		try (AdminClient admin = AdminClient.create(props)) {
			DescribeClusterResult cluster = admin.describeCluster();

			System.out.println("클러스터 ID: " + cluster.clusterId().get());
			System.out.println("컨트롤러: " + cluster.controller().get());
			System.out.println("브로커 목록:");
			// cluster.nodes().get().forEach(System.out::println);
			Collection<Node> nodes = cluster.nodes().get();
			for (Node node : nodes) {
				System.out.println(node);
			}
		} catch (ExecutionException | InterruptedException e) {
			System.out.println(e.getMessage());
		}
	}

	@Test
	public void testListConsumerGroups() {
		System.out.println("[TEST] 컨슈머 그룹 목록 조회 테스트...");

		try (AdminClient admin = AdminClient.create(props)) {
			Collection<ConsumerGroupListing> groups = admin.listConsumerGroups().all().get();

			for (ConsumerGroupListing group : groups) {
				System.out.println("그룹 ID: " + group.groupId());
			}
		} catch (ExecutionException | InterruptedException e) {
			System.out.println(e.getMessage());
		}
	}

	@Test
	public void testDescribeConsumerGroups() {
		System.out.println("[TEST] 컨슈머 그룹 정보 조회 테스트...");

		List<String> groupIds = List.of("my-consumer-group");

		try (AdminClient admin = AdminClient.create(props)) {
			admin.describeConsumerGroups(groupIds).all().get().forEach((groupId, desc) -> {
				System.out.printf("▶ 그룹 ID: %s, 상태: %s, 멤버 수: %d%n", groupId, desc.state(), desc.members().size());

				desc.members().forEach(member -> {
					System.out.printf("  - 멤버: %s, 호스트: %s%n", member.consumerId(), member.host());
					System.out.printf("    파티션 할당: %s%n", member.assignment().topicPartitions());
				});
			});
		} catch (ExecutionException | InterruptedException e) {
			System.out.println(e.getMessage());
		}
	}

	@Test
	public void testDeleteConsumerGroups() {
		System.out.println("[TEST] 컨슈머 그룹 삭제 테스트...");

		List<String> groupIds = List.of("my-consumer-group");
		try (AdminClient admin = AdminClient.create(props)) {
			// admin.deleteConsumerGroups(groupIdsToDelete).all().get();
			// 삭제 요청 결과를 result 변수에 저장
			DeleteConsumerGroupsResult result = admin.deleteConsumerGroups(groupIds);
			// 삭제 완료 대기
			result.all().get();
		} catch (ExecutionException | InterruptedException e) {
			System.out.println(e.getMessage());
		}
	}

	@Test
	public void testListConsumerGroupOffsets() {
		System.out.println("[TEST] 컨슈머 그룹 Offset 조회 테스트...");

		String groupIds = "my-consumer-group";
		try (AdminClient admin = AdminClient.create(props)) {
			// 1. 오프셋 정보 요청 → Future로 비동기 반환됨
			ListConsumerGroupOffsetsResult offsetResult = admin.listConsumerGroupOffsets(groupIds);

			// 2. Future에서 오프셋 결과 Map<TopicPartition, OffsetAndMetadata> 추출
			Map<TopicPartition, OffsetAndMetadata> offsets = offsetResult.partitionsToOffsetAndMetadata().get();

			// 3. 결과 출력
			for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
				TopicPartition tp = entry.getKey(); // 토픽명 + 파티션 번호
				OffsetAndMetadata metadata = entry.getValue(); // 오프셋 정보

				System.out.printf("▶ 토픽: %s, 파티션: %d, offset: %d%n", tp.topic(), tp.partition(), metadata.offset());
			}
		} catch (ExecutionException | InterruptedException e) {
			System.out.println(e.getMessage());
		}
	}

	@Test
	public void testAlterConsumerGroupOffsets() {
		System.out.println("[TEST] 컨슈머 그룹 Offset 설정 테스트...");
		String consumerGroupId = "my-consumer-group";

		try (AdminClient admin = AdminClient.create(props)) {
			// 1. 오프셋을 변경할 대상 토픽과 파티션 지정
			TopicPartition targetPartition = new TopicPartition("topic-basic", 0);

			// 2. 새 오프셋 값 설정 (여기서는 0부터 다시 읽도록 설정)
			OffsetAndMetadata newOffset = new OffsetAndMetadata(0L); // 0: 처음부터

			// 3. 오프셋 변경 요청용 Map 구성
			Map<TopicPartition, OffsetAndMetadata> newOffsets = Map.of(targetPartition, newOffset);

			// 4. 오프셋 변경 요청 전송
			admin.alterConsumerGroupOffsets(consumerGroupId, newOffsets).all().get();

			System.out.println("오프셋 변경 완료: group = " + consumerGroupId);

		} catch (ExecutionException | InterruptedException e) {
			System.out.println(e.getMessage());
		}
	}

	@Test
	public void testAlterPartitionReassignments() {
		System.out.println("[TEST] 토픽 파티션 수동 재 할당  테스트...");

		try (AdminClient admin = AdminClient.create(props)) {
			// 1. 대상 토픽과 파티션 설정
			// "topic-basic"의 0번 파티션을 대상으로 재할당 진행
			TopicPartition targetPartition = new TopicPartition("topic-basic", 0);

			// 2. 새롭게 할당할 replica 브로커 목록 정의
			// 이 예제에서는 브로커 ID 1, 2, 3을 새 replica로 지정
			List<Integer> newReplicas = List.of(1, 2, 3);

			// 3. 재할당 요청을 위한 Map 구성
			// Map<파티션, 재할당 정보> 형태로 구성
			Map<TopicPartition, Optional<NewPartitionReassignment>> reassignment = Map.of(targetPartition,
					Optional.of(new NewPartitionReassignment(newReplicas)));

			// 4. AdminClient를 통해 재할당 요청 전송
			admin.alterPartitionReassignments(reassignment).all().get();

			System.out.println("파티션 replica 재할당 요청 완료!");

		} catch (ExecutionException | InterruptedException e) {
			System.out.println(e.getMessage());
		}
	}

}
