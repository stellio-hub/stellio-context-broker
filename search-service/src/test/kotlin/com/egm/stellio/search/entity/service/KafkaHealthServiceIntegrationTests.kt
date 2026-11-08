package com.egm.stellio.search.entity.service

import com.egm.stellio.search.common.config.SearchProperties
import com.egm.stellio.search.entity.service.EntityEventService.Companion.ENTITY_EVENTS_TOPIC
import com.egm.stellio.search.support.WithKafkaContainer
import kotlinx.coroutines.future.await
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.errors.TopicExistsException
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.assertThrows
import java.util.Optional

// runBlocking, not runTest: the probe timeout must run on real time against a real broker
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class KafkaHealthServiceIntegrationTests : WithKafkaContainer() {

    private val admin =
        Admin.create(mapOf<String, Any>(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to kafkaContainer.bootstrapServers))

    private val kafkaHealthService = KafkaHealthService(admin, SearchProperties(onOwnerDeleteCascadeEntities = false))

    @AfterAll
    fun closeAdmin() = admin.close()

    @Test
    fun `probe should mark Kafka available against a running broker`() = runBlocking<Unit> {
        createEntityEventsTopic()

        kafkaHealthService.probe()

        assertThat(kafkaHealthService.isAvailable()).isTrue()
    }

    @Test
    fun `describeTopics should fail with UnknownTopicOrPartitionException for a missing topic`() = runBlocking<Unit> {
        val exception = assertThrows<Throwable> {
            admin.describeTopics(listOf("missing-topic")).allTopicNames().toCompletionStage().await()
        }

        assertThat(generateSequence(exception) { it.cause }.toList())
            .anyMatch { it is UnknownTopicOrPartitionException }
    }

    private suspend fun createEntityEventsTopic() {
        // the topic may already exist, created by another test on the reused container
        runCatching {
            admin.createTopics(listOf(NewTopic(ENTITY_EVENTS_TOPIC, Optional.empty(), Optional.empty())))
                .all().toCompletionStage().await()
        }.onFailure { exception ->
            if (generateSequence(exception) { it.cause }.none { it is TopicExistsException }) throw exception
        }
    }
}
