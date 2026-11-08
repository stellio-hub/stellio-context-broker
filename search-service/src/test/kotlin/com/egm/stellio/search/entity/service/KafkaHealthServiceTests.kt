package com.egm.stellio.search.entity.service

import com.egm.stellio.search.common.config.SearchProperties
import com.egm.stellio.search.entity.service.EntityEventService.Companion.ENTITY_EVENTS_TOPIC
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import kotlinx.coroutines.test.runTest
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.Config
import org.apache.kafka.clients.admin.ConfigEntry
import org.apache.kafka.clients.admin.TopicDescription
import org.apache.kafka.common.KafkaFuture
import org.apache.kafka.common.Node
import org.apache.kafka.common.TopicPartitionInfo
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.errors.TimeoutException
import org.apache.kafka.common.errors.TopicAuthorizationException
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException
import org.apache.kafka.common.internals.KafkaFutureImpl
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class KafkaHealthServiceTests {

    private lateinit var admin: Admin

    private lateinit var kafkaHealthService: KafkaHealthService

    private val broker1 = Node(1, "broker1", 9092)
    private val broker2 = Node(2, "broker2", 9092)

    @BeforeEach
    fun createService() {
        admin = mockk()
        kafkaHealthService = KafkaHealthService(admin, SearchProperties(onOwnerDeleteCascadeEntities = false))
    }

    @Test
    fun `isAvailable should return false before the first probe`() {
        assertThat(kafkaHealthService.isAvailable()).isFalse()
    }

    @Test
    fun `probe should mark Kafka available when every partition has a leader and enough in-sync replicas`() = runTest {
        mockTopic(partition(0, broker1, listOf(broker1, broker2)), partition(1, broker2, listOf(broker2, broker1)))
        mockMinInsyncReplicas("2")

        kafkaHealthService.probe()

        assertThat(kafkaHealthService.isAvailable()).isTrue()
    }

    @Test
    fun `probe should mark Kafka unavailable when a partition has no leader`() = runTest {
        mockTopic(partition(0, broker1, listOf(broker1)), partition(1, null, emptyList()))
        mockMinInsyncReplicas("1")

        kafkaHealthService.probe()

        assertThat(kafkaHealthService.isAvailable()).isFalse()
    }

    @Test
    fun `probe should mark Kafka unavailable when a partition leader is an empty node`() = runTest {
        mockTopic(partition(0, Node.noNode(), emptyList()))
        mockMinInsyncReplicas("1")

        kafkaHealthService.probe()

        assertThat(kafkaHealthService.isAvailable()).isFalse()
    }

    @Test
    fun `probe should mark Kafka unavailable when a partition has fewer in-sync replicas than required`() = runTest {
        mockTopic(partition(0, broker1, listOf(broker1)))
        mockMinInsyncReplicas("2")

        kafkaHealthService.probe()

        assertThat(kafkaHealthService.isAvailable()).isFalse()
    }

    @Test
    fun `probe should assume one in-sync replica and not retry when min insync replicas cannot be read`() =
        runTest {
            mockTopic(partition(0, broker1, listOf(broker1)))
            every { admin.describeConfigs(any<Collection<ConfigResource>>()).all() } returns
                failedFuture(TopicAuthorizationException("no describe configs ACL"))

            kafkaHealthService.probe()
            kafkaHealthService.probe()

            assertThat(kafkaHealthService.isAvailable()).isTrue()
            verify(exactly = 1) { admin.describeConfigs(any<Collection<ConfigResource>>()) }
        }

    @Test
    fun `probe should mark Kafka unavailable when the topic cannot be described`() = runTest {
        every { admin.describeTopics(listOf(ENTITY_EVENTS_TOPIC)).allTopicNames() } returns
            failedFuture(TimeoutException("timed out waiting for a node assignment"))

        kafkaHealthService.probe()

        assertThat(kafkaHealthService.isAvailable()).isFalse()
    }

    @Test
    fun `probe should mark Kafka available when the topic does not exist yet but the cluster has brokers`() = runTest {
        every { admin.describeTopics(listOf(ENTITY_EVENTS_TOPIC)).allTopicNames() } returns
            failedFuture(UnknownTopicOrPartitionException("unknown topic"))
        every { admin.describeCluster().nodes() } returns KafkaFuture.completedFuture(listOf(broker1))

        kafkaHealthService.probe()

        assertThat(kafkaHealthService.isAvailable()).isTrue()
    }

    @Test
    fun `probe should mark Kafka unavailable when the topic does not exist and the cluster cannot be described`() =
        runTest {
            every { admin.describeTopics(listOf(ENTITY_EVENTS_TOPIC)).allTopicNames() } returns
                failedFuture(UnknownTopicOrPartitionException("unknown topic"))
            every { admin.describeCluster().nodes() } returns failedFuture(TimeoutException("timed out"))

            kafkaHealthService.probe()

            assertThat(kafkaHealthService.isAvailable()).isFalse()
        }

    @Test
    fun `probe should mark Kafka unavailable when it times out`() = runTest {
        every { admin.describeTopics(listOf(ENTITY_EVENTS_TOPIC)).allTopicNames() } returns KafkaFutureImpl()

        kafkaHealthService.probe()

        assertThat(kafkaHealthService.isAvailable()).isFalse()
    }

    @Test
    fun `markUnavailable should last until the next successful probe`() = runTest {
        mockTopic(partition(0, broker1, listOf(broker1)))
        mockMinInsyncReplicas("1")
        kafkaHealthService.probe()

        kafkaHealthService.markUnavailable("not authorized to write to the topic")
        assertThat(kafkaHealthService.isAvailable()).isFalse()

        kafkaHealthService.probe()
        assertThat(kafkaHealthService.isAvailable()).isTrue()
    }

    private fun partition(id: Int, leader: Node?, isr: List<Node>) =
        TopicPartitionInfo(id, leader, listOf(broker1, broker2), isr)

    private fun mockTopic(vararg partitions: TopicPartitionInfo) {
        every { admin.describeTopics(listOf(ENTITY_EVENTS_TOPIC)).allTopicNames() } returns
            KafkaFuture.completedFuture(
                mapOf(ENTITY_EVENTS_TOPIC to TopicDescription(ENTITY_EVENTS_TOPIC, false, partitions.toList()))
            )
    }

    private fun mockMinInsyncReplicas(value: String) {
        val resource = ConfigResource(ConfigResource.Type.TOPIC, ENTITY_EVENTS_TOPIC)
        every { admin.describeConfigs(any<Collection<ConfigResource>>()).all() } returns
            KafkaFuture.completedFuture(
                mapOf(resource to Config(listOf(ConfigEntry(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, value))))
            )
    }

    private fun <T> failedFuture(exception: Throwable): KafkaFuture<T> =
        KafkaFutureImpl<T>().apply { completeExceptionally(exception) }
}
