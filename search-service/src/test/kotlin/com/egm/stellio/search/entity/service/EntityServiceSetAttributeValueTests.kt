package com.egm.stellio.search.entity.service

import arrow.core.right
import com.egm.stellio.search.authorization.permission.service.AuthorizationService
import com.egm.stellio.search.entity.model.Entity
import com.egm.stellio.search.support.WithKafkaContainer
import com.egm.stellio.search.support.WithTimescaleContainer
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.CompactedAttributeInstance
import com.egm.stellio.shared.model.EntityTypeSelection
import com.egm.stellio.shared.model.InvalidRequestException
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.util.APIARY_IRI
import com.egm.stellio.shared.util.APIC_COMPOUND_CONTEXT
import com.egm.stellio.shared.util.APIC_COMPOUND_CONTEXTS
import com.egm.stellio.shared.util.BEEHIVE_IRI
import com.egm.stellio.shared.util.BEEHIVE_TERM
import com.egm.stellio.shared.util.JsonLdUtils.compactEntity
import com.egm.stellio.shared.util.sampleDataToNgsiLdEntity
import com.egm.stellio.shared.util.shouldFail
import com.egm.stellio.shared.util.shouldSucceed
import com.egm.stellio.shared.util.shouldSucceedAndResult
import com.egm.stellio.shared.util.toUri
import com.ninjasquad.springmockk.MockkBean
import io.mockk.coEvery
import io.mockk.every
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.data.r2dbc.core.delete
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.test.context.ActiveProfiles
import java.net.URI
import java.time.ZonedDateTime
import java.util.concurrent.CompletableFuture
import java.util.stream.Stream

@SpringBootTest
@ActiveProfiles("test")
class EntityServiceSetAttributeValueTests : WithTimescaleContainer, WithKafkaContainer() {

    @Autowired
    private lateinit var entityService: EntityService

    @Autowired
    private lateinit var entityQueryService: EntityQueryService

    @Autowired
    private lateinit var r2dbcEntityTemplate: R2dbcEntityTemplate

    @MockkBean(relaxed = true)
    private lateinit var kafkaTemplate: KafkaTemplate<String, String>

    @MockkBean
    private lateinit var kafkaHealthService: KafkaHealthService

    @MockkBean
    private lateinit var authorizationService: AuthorizationService

    private val entityId = "urn:ngsi-ld:BeeHive:01".toUri()
    private val datasetId = "urn:ngsi-ld:Dataset:01".toUri()

    @BeforeEach
    fun createEntity() = runTest {
        every { kafkaHealthService.isAvailable() } returns true
        every { kafkaTemplate.send(any(), any(), any()) } returns CompletableFuture()
        coEvery { authorizationService.userCanCreateEntities() } returns Unit.right()
        coEvery { authorizationService.createEntityOwnerRight(any()) } returns Unit.right()
        coEvery { authorizationService.userCanUpdateEntity(any()) } returns Unit.right()
        coEvery { authorizationService.createScopesOwnerRights(any()) } returns Unit.right()

        val (expandedEntity, ngsiLdEntity) = entityPayload.sampleDataToNgsiLdEntity().shouldSucceedAndResult()
        entityService.createEntity(ngsiLdEntity, expandedEntity).shouldSucceed()
    }

    @AfterEach
    fun clearEntityPayloadTable() {
        r2dbcEntityTemplate.delete<Entity>().from("entity_payload").all().block()
    }

    @ParameterizedTest
    @MethodSource("valuesByAttributeType")
    fun `setAttributeValue should replace the value and keep the type of the attribute`(
        attrId: String,
        type: String,
        valueMember: String,
        value: Any
    ) = runTest {
        entityService.setAttributeValue(
            entityId,
            attrId,
            value,
            APIC_COMPOUND_CONTEXTS,
            null,
            null,
            null
        ).shouldSucceed()

        val attribute = retrieveAttribute(attrId)
        assertThat(attribute["type"]).isEqualTo(type)
        assertThat(attribute[valueMember]).isEqualTo(value)
    }

    @Test
    fun `setAttributeValue should store an object value as when the attribute is created with it`() = runTest {
        val value = mapOf("min" to 1, "max" to 2)
        entityService.setAttributeValue(
            entityId,
            "temperature",
            value,
            APIC_COMPOUND_CONTEXTS,
            null,
            null,
            null
        ).shouldSucceed()

        assertThat(retrieveAttribute("temperature")["value"]).isEqualTo(retrieveAttribute("range")["value"])
    }

    @Test
    fun `setAttributeValue should keep the other sub-attributes of the attribute`() = runTest {
        entityService.setAttributeValue(
            entityId,
            "temperature",
            25,
            APIC_COMPOUND_CONTEXTS,
            null,
            null,
            null
        ).shouldSucceed()

        assertThat(retrieveAttribute("temperature")).containsEntry("unitCode", "CEL")
    }

    @Test
    fun `setAttributeValue should update the observedAt with the observedAt parameter`() = runTest {
        val observedAt = ZonedDateTime.parse("2025-06-01T12:00:00Z")

        entityService.setAttributeValue(
            entityId,
            "temperature",
            25,
            APIC_COMPOUND_CONTEXTS,
            null,
            observedAt,
            null
        ).shouldSucceed()

        assertThat(retrieveAttribute("temperature")).containsEntry("observedAt", "2025-06-01T12:00:00Z")
    }

    @Test
    fun `setAttributeValue should remove the observedAt when no observedAt parameter is given`() = runTest {
        entityService.setAttributeValue(
            entityId,
            "temperature",
            25,
            APIC_COMPOUND_CONTEXTS,
            null,
            null,
            null
        ).shouldSucceed()

        assertThat(retrieveAttribute("temperature")).doesNotContainKey("observedAt")
    }

    @Test
    fun `setAttributeValue should not add an observedAt if the attribute did not have one`() = runTest {
        val observedAt = ZonedDateTime.parse("2025-06-01T12:00:00Z")

        entityService.setAttributeValue(
            entityId,
            "category",
            "residential",
            APIC_COMPOUND_CONTEXTS,
            null,
            observedAt,
            null
        ).shouldSucceed()

        assertThat(retrieveAttribute("category")).doesNotContainKey("observedAt")
    }

    @Test
    fun `setAttributeValue should only update the instance with the given datasetId`() = runTest {
        entityService.setAttributeValue(
            entityId,
            "temperature",
            30,
            APIC_COMPOUND_CONTEXTS,
            datasetId,
            null,
            null
        ).shouldSucceed()

        assertThat(retrieveAttribute("temperature")).containsEntry("value", 20)
        assertThat(retrieveAttribute("temperature", datasetId.toString()))
            .containsEntry("value", 30)
            .containsEntry("datasetId", datasetId.toString())
    }

    @Test
    fun `setAttributeValue should accept a null member inside the value of a JsonProperty`() = runTest {
        entityService.setAttributeValue(
            entityId,
            "config",
            mapOf("a" to null),
            APIC_COMPOUND_CONTEXTS,
            null,
            null,
            null
        ).shouldSucceed()

        assertThat(retrieveAttribute("config")["json"]).isEqualTo(mapOf("a" to null))
    }

    @Test
    fun `setAttributeValue should succeed when the type parameter matches the entity`() = runTest {
        entityService.setAttributeValue(entityId, "temperature", 25, APIC_COMPOUND_CONTEXTS, null, null, BEEHIVE_IRI)
            .shouldSucceed()
    }

    @ParameterizedTest
    @MethodSource("invalidRequests")
    fun `setAttributeValue should return an error`(
        attrId: String,
        value: Any?,
        datasetId: URI?,
        typeSelection: EntityTypeSelection?,
        expectedError: Class<out APIException>
    ) = runTest {
        entityService.setAttributeValue(entityId, attrId, value, APIC_COMPOUND_CONTEXTS, datasetId, null, typeSelection)
            .shouldFail { assertThat(it).isInstanceOf(expectedError) }
    }

    @Test
    fun `setAttributeValue should return a ResourceNotFound error if the entity does not exist`() = runTest {
        val unknownEntityId = "urn:ngsi-ld:BeeHive:unknown".toUri()

        entityService.setAttributeValue(unknownEntityId, "temperature", 25, APIC_COMPOUND_CONTEXTS, null, null, null)
            .shouldFail { assertThat(it).isInstanceOf(ResourceNotFoundException::class.java) }
    }

    private suspend fun retrieveAttribute(attrId: String, datasetId: String? = null): CompactedAttributeInstance {
        val entity = entityQueryService.retrieve(entityId).shouldSucceedAndResult().toExpandedEntity()
        val compactedAttribute = compactEntity(entity, APIC_COMPOUND_CONTEXTS)[attrId]
        val instances = compactedAttribute as? List<*> ?: listOf(compactedAttribute)
        return instances.map { it as CompactedAttributeInstance }.first { it["datasetId"] == datasetId }
    }

    companion object {
        @JvmStatic
        fun valuesByAttributeType(): Stream<Arguments> = Stream.of(
            Arguments.of("temperature", "Property", "value", 25),
            Arguments.of("owner", "Relationship", "object", "urn:ngsi-ld:Person:02"),
            Arguments.of(
                "location",
                "GeoProperty",
                "value",
                mapOf("type" to "Point", "coordinates" to listOf(3.5, 45.5))
            ),
            Arguments.of("config", "JsonProperty", "json", mapOf("b" to listOf(1, 2))),
            Arguments.of("label", "LanguageProperty", "languageMap", mapOf("fr" to "salut", "en" to "hi")),
            Arguments.of("category", "VocabProperty", "vocab", "residential"),
            Arguments.of("readings", "ListProperty", "valueList", listOf(3, 4, 5)),
            Arguments.of(
                "members",
                "ListRelationship",
                "objectList",
                listOf(mapOf("object" to "urn:ngsi-ld:Person:02"))
            )
        )

        @JvmStatic
        fun invalidRequests(): Stream<Arguments> = Stream.of(
            Arguments.of("scope", listOf("/B"), null, null, BadRequestDataException::class.java),
            Arguments.of("temperature", null, null, null, InvalidRequestException::class.java),
            Arguments.of(
                "temperature",
                "urn:ngsi-ld:null",
                null,
                null,
                InvalidRequestException::class.java
            ),
            Arguments.of("config", "urn:ngsi-ld:null", null, null, InvalidRequestException::class.java),
            Arguments.of("config", null, null, null, InvalidRequestException::class.java),
            Arguments.of("unknown", 25, null, null, ResourceNotFoundException::class.java),
            Arguments.of(
                "temperature",
                25,
                "urn:ngsi-ld:Dataset:unknown".toUri(),
                null,
                ResourceNotFoundException::class.java
            ),
            Arguments.of(
                "temperature",
                25,
                null,
                APIARY_IRI,
                ResourceNotFoundException::class.java
            ),
            Arguments.of("temperature", "a\u0000b", null, null, BadRequestDataException::class.java),
            Arguments.of("readings", listOf(1, null), null, null, BadRequestDataException::class.java),
            Arguments.of("owner", 42, null, null, BadRequestDataException::class.java),
            Arguments.of("location", "not a geometry", null, null, BadRequestDataException::class.java),
            Arguments.of("label", 42, null, null, BadRequestDataException::class.java),
            Arguments.of("label", mapOf("fr" to 42), null, null, BadRequestDataException::class.java),
            Arguments.of("category", 42, null, null, BadRequestDataException::class.java),
            Arguments.of("category", mapOf("a" to "b"), null, null, BadRequestDataException::class.java),
            Arguments.of("members", listOf("urn:ngsi-ld:Person:02"), null, null, BadRequestDataException::class.java),
            Arguments.of("members", listOf(mapOf("object" to 42)), null, null, BadRequestDataException::class.java),
            Arguments.of("members", 42, null, null, BadRequestDataException::class.java)
        )

        private val entityPayload = """
            {
                "id": "urn:ngsi-ld:BeeHive:01",
                "type": "$BEEHIVE_TERM",
                "scope": "/A",
                "temperature": [
                    {
                        "type": "Property",
                        "value": 20,
                        "unitCode": "CEL",
                        "observedAt": "2024-01-01T00:00:00Z"
                    },
                    {
                        "type": "Property",
                        "value": 21,
                        "datasetId": "urn:ngsi-ld:Dataset:01"
                    }
                ],
                "range": { "type": "Property", "value": { "min": 1, "max": 2 } },
                "owner": { "type": "Relationship", "object": "urn:ngsi-ld:Person:01" },
                "location": { "type": "GeoProperty", "value": { "type": "Point", "coordinates": [2.0, 44.0] } },
                "config": { "type": "JsonProperty", "json": { "a": 1 } },
                "label": { "type": "LanguageProperty", "languageMap": { "fr": "bonjour" } },
                "category": { "type": "VocabProperty", "vocab": "commercial" },
                "readings": { "type": "ListProperty", "valueList": [1, 2] },
                "members": { "type": "ListRelationship", "objectList": [{ "object": "urn:ngsi-ld:Person:01" }] },
                "@context": ["$APIC_COMPOUND_CONTEXT"]
            }
        """.trimIndent()
    }
}
