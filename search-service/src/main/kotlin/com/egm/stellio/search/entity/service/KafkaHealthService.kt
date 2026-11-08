package com.egm.stellio.search.entity.service

import arrow.core.Either
import arrow.core.flatMap
import arrow.core.left
import arrow.core.raise.either
import arrow.core.raise.ensure
import arrow.core.right
import com.egm.stellio.search.common.config.SearchProperties
import com.egm.stellio.search.entity.service.EntityEventService.Companion.ENTITY_EVENTS_TOPIC
import jakarta.annotation.PostConstruct
import jakarta.annotation.PreDestroy
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.future.await
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeoutOrNull
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.TopicDescription
import org.apache.kafka.common.KafkaFuture
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.util.concurrent.atomic.AtomicReference
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toKotlinDuration

/**
 * Tracks whether entity events can currently be published, so that writes are rejected instead of losing their
 * events. Probes from its own coroutine, as the single thread of the Spring scheduler is shared with long-running
 * jobs.
 */
@Component
class KafkaHealthService(
    private val admin: Admin,
    searchProperties: SearchProperties
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    private val interval = searchProperties.kafkaHealth.interval.toKotlinDuration().coerceAtLeast(MIN_INTERVAL)
    private val timeout = searchProperties.kafkaHealth.timeout.toKotlinDuration()

    // null until the first probe, so that the initial state is always logged
    private val available = AtomicReference<Boolean?>(null)

    @Volatile
    private var minInsyncReplicas: Int? = null

    private val probeScope = CoroutineScope(Dispatchers.Default + SupervisorJob())

    fun isAvailable(): Boolean = available.get() == true

    /**
     * Marks Kafka as unavailable after a send failed in a way the probe can't detect (e.g., a missing write ACL).
     * The next successful probe marks it available again.
     */
    fun markUnavailable(reason: String) = updateState(reason.left())

    @PostConstruct
    fun startProbing() {
        probeScope.launch {
            while (isActive) {
                probe()
                delay(interval)
            }
        }
    }

    @PreDestroy
    fun stopProbing() = probeScope.cancel()

    internal suspend fun probe() {
        val result = withTimeoutOrNull(timeout) { checkHealth() } ?: "probe timed out after $timeout".left()
        updateState(result)
    }

    private suspend fun checkHealth(): Either<String, Unit> =
        admin.describeTopics(listOf(ENTITY_EVENTS_TOPIC)).allTopicNames().awaitEither().fold(
            { exception ->
                // the topic is created by the broker on the first send
                if (exception.hasCause<UnknownTopicOrPartitionException>()) checkCluster()
                else "unable to describe topic $ENTITY_EVENTS_TOPIC (${exception.message})".left()
            },
            { topics -> checkPartitions(topics.getValue(ENTITY_EVENTS_TOPIC)) }
        )

    private suspend fun checkCluster(): Either<String, Unit> =
        admin.describeCluster().nodes().awaitEither()
            .mapLeft { "unable to describe the cluster (${it.message})" }
            .flatMap { nodes -> if (nodes.isEmpty()) "no broker available".left() else Unit.right() }

    private suspend fun checkPartitions(topicDescription: TopicDescription): Either<String, Unit> = either {
        val minIsr = getMinInsyncReplicas()
        topicDescription.partitions().forEach { partition ->
            ensure(partition.leader()?.isEmpty == false) { "partition ${partition.partition()} has no leader" }
            ensure(partition.isr().size >= minIsr) {
                "partition ${partition.partition()} has ${partition.isr().size} in-sync replicas, " +
                    "below ${TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG} ($minIsr)"
            }
        }
    }

    // only called once the topic could be described, so a failure here is not an outage (e.g., missing ACL)
    private suspend fun getMinInsyncReplicas(): Int =
        minInsyncReplicas ?: readMinInsyncReplicas().also { minInsyncReplicas = it }

    private suspend fun readMinInsyncReplicas(): Int {
        val resource = ConfigResource(ConfigResource.Type.TOPIC, ENTITY_EVENTS_TOPIC)
        return admin.describeConfigs(listOf(resource)).all().awaitEither()
            .map { it[resource]?.get(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG)?.value()?.toIntOrNull() }
            .onLeft { logger.warn("Unable to read {}, assuming 1", TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, it) }
            .getOrNull() ?: 1
    }

    private fun updateState(result: Either<String, Unit>) {
        val isAvailable = result.isRight()
        if (available.getAndSet(isAvailable) == isAvailable) return

        result.fold(
            { logger.warn("Kafka is unavailable, write requests are rejected: {}", it) },
            { logger.info("Kafka is available, write requests are accepted") }
        )
    }

    private suspend fun <T> KafkaFuture<T>.awaitEither(): Either<Throwable, T> =
        Either.catch { toCompletionStage().await() }

    companion object {
        private val MIN_INTERVAL = 1.seconds
    }
}

internal inline fun <reified T : Throwable> Throwable.hasCause(): Boolean =
    generateSequence(this) { it.cause }.any { it is T }
