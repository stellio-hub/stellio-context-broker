package com.egm.stellio.search.entity.service

import arrow.core.right
import com.egm.stellio.search.authorization.permission.service.AuthorizationService
import com.egm.stellio.search.entity.model.Entity
import com.egm.stellio.search.support.WithKafkaContainer
import com.egm.stellio.search.support.WithTimescaleContainer
import com.egm.stellio.shared.model.EventsType
import com.egm.stellio.shared.model.ExpandedEntity
import com.egm.stellio.shared.model.InternalErrorException
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.util.ErrorMessages.Kafka.EVENT_BROKER_UNAVAILABLE_MESSAGE
import com.egm.stellio.shared.util.loadAndPrepareSampleData
import com.egm.stellio.shared.util.shouldFail
import com.egm.stellio.shared.util.shouldSucceed
import com.egm.stellio.shared.util.shouldSucceedAndResult
import com.egm.stellio.shared.util.toUri
import com.ninjasquad.springmockk.MockkBean
import io.mockk.coEvery
import io.mockk.every
import io.mockk.verify
import kotlinx.coroutines.test.runTest
import org.apache.kafka.clients.producer.ProducerConfig
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.data.r2dbc.core.delete
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory
import org.springframework.test.context.ActiveProfiles
import org.springframework.transaction.ReactiveTransactionManager
import org.springframework.transaction.reactive.TransactionalOperator
import org.springframework.transaction.reactive.executeAndAwait
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CopyOnWriteArrayList

@SpringBootTest
@ActiveProfiles("test")
class EntityEventServiceIntegrationTests : WithTimescaleContainer, WithKafkaContainer() {

    @Autowired
    private lateinit var entityService: EntityService

    @Autowired
    private lateinit var entityQueryService: EntityQueryService

    @Autowired
    private lateinit var entityEventService: EntityEventService

    @Autowired
    private lateinit var transactionManager: ReactiveTransactionManager

    @Autowired
    private lateinit var r2dbcEntityTemplate: R2dbcEntityTemplate

    @Autowired
    private lateinit var producerFactory: ProducerFactory<String, String>

    @MockkBean(relaxed = true)
    private lateinit var kafkaTemplate: KafkaTemplate<String, String>

    @MockkBean
    private lateinit var kafkaHealthService: KafkaHealthService

    @MockkBean(relaxed = true)
    private lateinit var authorizationService: AuthorizationService

    private val beehiveTestCId = "urn:ngsi-ld:BeeHive:TESTC".toUri()

    private val sentPayloads = CopyOnWriteArrayList<String>()

    @BeforeEach
    fun mockKafka() {
        sentPayloads.clear()
        every { kafkaHealthService.isAvailable() } returns true
        every { kafkaTemplate.send(any(), any(), capture(sentPayloads)) } returns CompletableFuture()
        coEvery { authorizationService.userCanCreateEntities() } returns Unit.right()
        coEvery { authorizationService.createEntityOwnerRight(any()) } returns Unit.right()
    }

    @AfterEach
    fun clearEntityPayloadTable() {
        r2dbcEntityTemplate.delete<Entity>().from("entity_payload").all().block()
    }

    @Test
    fun `createEntity should publish the entity event then the attribute events after commit`() = runTest {
        val (expandedEntity, ngsiLdEntity) = loadAndPrepareSampleData().shouldSucceedAndResult()

        entityService.createEntity(ngsiLdEntity, expandedEntity).shouldSucceed()

        // beehive.jsonld has 4 attributes
        verify(timeout = SEND_TIMEOUT_MS, exactly = 5) { kafkaTemplate.send(any(), beehiveTestCId.toString(), any()) }
        assertThat(sentPayloads.first()).contains(EventsType.ENTITY_CREATE.name)
        assertThat(sentPayloads.drop(1)).allMatch { it.contains(EventsType.ATTRIBUTE_CREATE.name) }
    }

    @Test
    fun `createEntity should return an InternalError and store nothing when Kafka is unavailable`() = runTest {
        every { kafkaHealthService.isAvailable() } returns false
        val (expandedEntity, ngsiLdEntity) = loadAndPrepareSampleData().shouldSucceedAndResult()

        entityService.createEntity(ngsiLdEntity, expandedEntity).shouldFail {
            assertThat(it).isInstanceOf(InternalErrorException::class.java)
            assertThat(it.message).isEqualTo(EVENT_BROKER_UNAVAILABLE_MESSAGE)
        }

        entityQueryService.retrieve(beehiveTestCId).shouldFail {
            assertThat(it).isInstanceOf(ResourceNotFoundException::class.java)
        }
        verify(exactly = 0) { kafkaTemplate.send(any(), any(), any()) }
    }

    @Test
    fun `publishEntityCreateEvent should not publish anything when the transaction rolls back`() = runTest {
        assertThrows<IllegalStateException> {
            TransactionalOperator.create(transactionManager).executeAndAwait {
                entityEventService.publishEntityCreateEvent(null, beehiveExpandedEntity())
                error("rollback")
            }
        }

        verify(exactly = 0) { kafkaTemplate.send(any(), any(), any()) }
    }

    @Test
    fun `publishEntityCreateEvent should publish the events of a transaction only once it has committed`() = runTest {
        TransactionalOperator.create(transactionManager).executeAndAwait {
            entityEventService.publishEntityCreateEvent(null, beehiveExpandedEntity())
            entityEventService.publishEntityCreateEvent("sub", beehiveExpandedEntity())
            assertThat(sentPayloads).isEmpty()
        }

        verify(timeout = SEND_TIMEOUT_MS, exactly = 2) { kafkaTemplate.send(any(), any(), any()) }
        assertThat(sentPayloads).allMatch { it.contains(EventsType.ENTITY_CREATE.name) }
    }

    @Test
    fun `producerFactory should create a producer with the entity events tuning`() {
        assertThat(producerFactory.configurationProperties)
            .containsEntry(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "600000")
            .containsEntry(ProducerConfig.MAX_BLOCK_MS_CONFIG, "600000")
            .containsEntry(ProducerConfig.BUFFER_MEMORY_CONFIG, 64L * 1024 * 1024)

        // the client rejects a delivery timeout lower than linger.ms + request.timeout.ms when creating a producer
        producerFactory.createProducer().close()
    }

    private suspend fun beehiveExpandedEntity(): ExpandedEntity =
        loadAndPrepareSampleData().shouldSucceedAndResult().first

    companion object {
        private const val SEND_TIMEOUT_MS = 2_000L
    }
}
