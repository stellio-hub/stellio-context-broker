package com.egm.stellio.search.service

import com.egm.stellio.search.config.TimescaleBasedTests
import com.egm.stellio.search.model.AttributeInstanceResult
import com.egm.stellio.shared.model.JsonLdEntity
import com.egm.stellio.shared.util.loadSampleData
import com.egm.stellio.shared.util.parseSampleDataToJsonLd
import com.egm.stellio.shared.util.toUri
import com.github.jsonldjava.core.JsonLdOptions
import com.github.jsonldjava.core.JsonLdProcessor
import com.github.jsonldjava.utils.JsonUtils
import com.ninjasquad.springmockk.SpykBean
import io.mockk.confirmVerified
import io.mockk.verify
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.data.r2dbc.core.DatabaseClient
import org.springframework.test.context.ActiveProfiles
import reactor.test.StepVerifier
import java.time.ZonedDateTime

@SpringBootTest
@ActiveProfiles("test")
class TemporalEntityAttributeServiceTests : TimescaleBasedTests() {

    @Autowired
    @SpykBean
    private lateinit var temporalEntityAttributeService: TemporalEntityAttributeService

    @Autowired
    private lateinit var databaseClient: DatabaseClient

    @Value("\${application.jsonld.apic_context}")
    val apicContext: String? = null

    @AfterEach
    fun clearPreviousTemporalEntityAttributesAndObservations() {
        databaseClient.delete()
            .from("entity_payload")
            .fetch()
            .rowsUpdated()
            .block()

        databaseClient.delete()
            .from("attribute_instance")
            .fetch()
            .rowsUpdated()
            .block()

        databaseClient.delete()
            .from("temporal_entity_attribute")
            .fetch()
            .rowsUpdated()
            .block()
    }

    @Test
    fun `it should retrieve a persisted temporal entity attribute`() {
        val rawEntity = loadSampleData("beehive_two_temporal_properties.jsonld")

        temporalEntityAttributeService.createEntityTemporalReferences(rawEntity).block()

        val temporalEntityAttributes =
            temporalEntityAttributeService.getForEntity(
                "urn:ngsi-ld:BeeHive:TESTD".toUri(),
                setOf(
                    "https://ontology.eglobalmark.com/apic#incoming",
                    "https://ontology.eglobalmark.com/apic#outgoing"
                ),
                apicContext!!
            )

        StepVerifier.create(temporalEntityAttributes)
            .expectNextMatches {
                it.entityId == "urn:ngsi-ld:BeeHive:TESTD".toUri() &&
                    it.type == "https://ontology.eglobalmark.com/apic#BeeHive" &&
                    it.attributeName == "https://ontology.eglobalmark.com/apic#incoming" &&
                    it.entityPayload !== null
            }
            .expectNextMatches {
                it.entityId == "urn:ngsi-ld:BeeHive:TESTD".toUri()
            }
            .expectComplete()
            .verify()
    }

    @Test
    fun `it should create one entry for an entity with one temporal property`() {
        val rawEntity = loadSampleData()

        val temporalReferencesResults = temporalEntityAttributeService.createEntityTemporalReferences(rawEntity)

        StepVerifier.create(temporalReferencesResults)
            .expectNextMatches {
                it == 2
            }
            .expectComplete()
            .verify()

        verify {
            temporalEntityAttributeService.createEntityPayload("urn:ngsi-ld:BeeHive:TESTC".toUri(), any())
        }

        confirmVerified()
    }

    @Test
    fun `it should create two entries for an entity with a two instances property`() {
        val rawEntity = loadSampleData("beehive_multi_instance_property.jsonld")

        val temporalReferencesResults = temporalEntityAttributeService.createEntityTemporalReferences(rawEntity)

        StepVerifier.create(temporalReferencesResults)
            .expectNextMatches {
                it == 3
            }
            .expectComplete()
            .verify()
    }

    @Test
    fun `it should create two entries for an entity with two temporal properties`() {
        val rawEntity = loadSampleData("beehive_two_temporal_properties.jsonld")

        val temporalReferencesResults = temporalEntityAttributeService.createEntityTemporalReferences(rawEntity)

        StepVerifier.create(temporalReferencesResults)
            .expectNextMatches {
                it == 3
            }
            .expectComplete()
            .verify()
    }

    @Test
    fun `it should inject temporal numeric values in temporalValues format into an entity`() {
        val rawEntity = parseSampleDataToJsonLd("beehive.jsonld")
        val rawResults = listOf(
            listOf(
                AttributeInstanceResult(
                    attributeName = "https://ontology.eglobalmark.com/apic#incoming",
                    value = 550.0,
                    observedAt = ZonedDateTime.parse("2020-03-25T08:29:17.965206Z")
                ),
                AttributeInstanceResult(
                    attributeName = "https://ontology.eglobalmark.com/apic#incoming",
                    value = 650.0,
                    observedAt = ZonedDateTime.parse("2020-03-25T08:33:00Z")
                )
            )
        )

        val enrichedEntity = temporalEntityAttributeService.injectTemporalValues(rawEntity, rawResults, true)
        val serializedEntity = JsonLdProcessor.compact(
            enrichedEntity.properties,
            mapOf("@context" to enrichedEntity.contexts),
            JsonLdOptions()
        )
        val finalEntity = JsonUtils.toPrettyString(serializedEntity)
        assertEquals(loadSampleData("expectations/beehive_with_incoming_temporal_values.jsonld").trim(), finalEntity)
    }

    @Test
    fun `it should inject temporal string values in temporalValues format into an entity`() {
        val rawEntity = parseSampleDataToJsonLd("subscription.jsonld")
        val rawResults = listOf(
            listOf(
                AttributeInstanceResult(
                    attributeName = "https://uri.etsi.org/ngsi-ld/notification",
                    value = "urn:ngsi-ld:Beehive:1234",
                    observedAt = ZonedDateTime.parse("2020-03-25T08:29:17.965206Z")
                ),
                AttributeInstanceResult(
                    attributeName = "https://uri.etsi.org/ngsi-ld/notification",
                    value = "urn:ngsi-ld:Beehive:5678",
                    observedAt = ZonedDateTime.parse("2020-03-25T08:33:17.965206Z")
                )
            )
        )

        val enrichedEntity = temporalEntityAttributeService.injectTemporalValues(rawEntity, rawResults, true)
        val serializedEntity = JsonLdProcessor.compact(
            enrichedEntity.properties,
            mapOf("@context" to enrichedEntity.contexts),
            JsonLdOptions()
        )
        val finalEntity = JsonUtils.toPrettyString(serializedEntity)
        assertEquals(
            loadSampleData("expectations/subscription_with_notifications_temporal_values.jsonld").trim(),
            finalEntity
        )
    }

    @Test
    fun `it should inject temporal string values in default format into an entity`() {
        val rawEntity = parseSampleDataToJsonLd("subscription.jsonld")
        val rawResults = listOf(
            listOf(
                AttributeInstanceResult(
                    attributeName = "https://uri.etsi.org/ngsi-ld/notification",
                    value = "urn:ngsi-ld:Beehive:1234",
                    instanceId = "urn:ngsi-ld:Beehive:notification:1234".toUri(),
                    observedAt = ZonedDateTime.parse("2020-03-25T08:29:17.965206Z")
                ),
                AttributeInstanceResult(
                    attributeName = "https://uri.etsi.org/ngsi-ld/notification",
                    value = "urn:ngsi-ld:Beehive:5678",
                    instanceId = "urn:ngsi-ld:Beehive:notification:4567".toUri(),
                    observedAt = ZonedDateTime.parse("2020-03-25T08:33:17.965206Z")
                )
            )
        )

        val enrichedEntity = temporalEntityAttributeService.injectTemporalValues(rawEntity, rawResults, false)
        val serializedEntity = JsonLdProcessor.compact(
            enrichedEntity.properties,
            mapOf("@context" to enrichedEntity.contexts),
            JsonLdOptions()
        )
        val finalEntity = JsonUtils.toPrettyString(serializedEntity)
        assertEquals(loadSampleData("expectations/subscription_with_notifications.jsonld").trim(), finalEntity)
    }

    @Test
    fun `it should return the entity untouched if it has no temporal history`() {
        val rawEntity = parseSampleDataToJsonLd("subscription.jsonld")
        val rawResults = emptyList<List<AttributeInstanceResult>>()

        val enrichedEntity = temporalEntityAttributeService.injectTemporalValues(rawEntity, rawResults, true)
        val serializedEntity = JsonLdProcessor.compact(
            enrichedEntity.properties,
            mapOf("@context" to enrichedEntity.contexts),
            JsonLdOptions()
        )
        val finalEntity = JsonUtils.toPrettyString(serializedEntity)
        assertEquals(loadSampleData("subscription.jsonld").trim(), finalEntity)
    }

    @Test
    fun `it should return the entity untouched if it has an empty temporal history`() {
        val rawEntity = parseSampleDataToJsonLd("subscription.jsonld")
        val rawResults = emptyList<List<AttributeInstanceResult>>()

        val enrichedEntity = temporalEntityAttributeService.injectTemporalValues(rawEntity, rawResults, true)
        val serializedEntity = JsonLdProcessor.compact(
            enrichedEntity.properties,
            mapOf("@context" to enrichedEntity.contexts),
            JsonLdOptions()
        )
        val finalEntity = JsonUtils.toPrettyString(serializedEntity)
        assertEquals(loadSampleData("subscription.jsonld").trim(), finalEntity)
    }

    @ParameterizedTest
    @MethodSource("com.egm.stellio.search.util.ParameterizedTests#rawResultsProvider")
    fun `it should inject temporal numeric values into an entity with two instances property`(
        rawEntity: JsonLdEntity,
        rawResults: List<List<AttributeInstanceResult>>,
        withTemporalValues: Boolean,
        expectation: String
    ) {
        val enrichedEntity =
            temporalEntityAttributeService.injectTemporalValues(rawEntity, rawResults, withTemporalValues)
        val serializedEntity = JsonLdProcessor.compact(
            enrichedEntity.properties,
            mapOf("@context" to enrichedEntity.contexts),
            JsonLdOptions()
        )
        val finalEntity = JsonUtils.toPrettyString(serializedEntity)
        assertEquals(expectation.trim(), finalEntity)
    }

    @Test
    fun `it should return the temporalEntityAttributeId of a given entityId and attributeName`() {
        val rawEntity = loadSampleData()

        temporalEntityAttributeService.createEntityTemporalReferences(rawEntity).block()

        val temporalEntityAttributeId = temporalEntityAttributeService.getForEntityAndAttribute(
            "urn:ngsi-ld:BeeHive:TESTC".toUri(), "https://ontology.eglobalmark.com/apic#incoming"
        )

        StepVerifier.create(temporalEntityAttributeId)
            .expectNextCount(1)
            .expectComplete()
            .verify()
    }

    @Test
    fun `it should return the temporalEntityAttributeId of a given entityId attributeName and datasetId`() {
        val rawEntity = loadSampleData("beehive_multi_instance_property.jsonld")

        temporalEntityAttributeService.createEntityTemporalReferences(rawEntity).block()

        val temporalEntityAttributeId = temporalEntityAttributeService.getForEntityAndAttribute(
            "urn:ngsi-ld:BeeHive:TESTC".toUri(),
            "https://ontology.eglobalmark.com/apic#incoming", "urn:ngsi-ld:Dataset:01234"
        )

        StepVerifier.create(temporalEntityAttributeId)
            .expectNextCount(1)
            .expectComplete()
            .verify()
    }

    @Test
    fun `it should not return a temporalEntityAttributeId if the datasetId is unknown`() {
        val rawEntity = loadSampleData("beehive_multi_instance_property.jsonld")

        temporalEntityAttributeService.createEntityTemporalReferences(rawEntity).block()

        val temporalEntityAttributeId = temporalEntityAttributeService.getForEntityAndAttribute(
            "urn:ngsi-ld:BeeHive:TESTC".toUri(),
            "https://ontology.eglobalmark.com/apic#incoming", "urn:ngsi-ld:Dataset:Unknown"
        )

        StepVerifier.create(temporalEntityAttributeId)
            .expectNextCount(0)
            .expectComplete()
            .verify()
    }
}
