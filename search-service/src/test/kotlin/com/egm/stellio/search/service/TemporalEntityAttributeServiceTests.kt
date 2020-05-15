package com.egm.stellio.search.service

import com.egm.stellio.shared.util.loadAndParseSampleData
import com.egm.stellio.shared.util.loadSampleData
import com.github.jsonldjava.core.JsonLdOptions
import com.github.jsonldjava.core.JsonLdProcessor
import com.github.jsonldjava.utils.JsonUtils
import org.flywaydb.core.Flyway
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.Import
import org.springframework.test.context.ActiveProfiles
import reactor.test.StepVerifier
import java.time.ZonedDateTime

@SpringBootTest
@ActiveProfiles("test")
@Import(R2DBCConfiguration::class)
class TemporalEntityAttributeServiceTests {

    @Autowired
    private lateinit var temporalEntityAttributeService: TemporalEntityAttributeService

    @Value("\${application.jsonld.apic_context}")
    val apicContext: String? = null

    init {
        Flyway.configure()
            .dataSource(MyPostgresqlContainer.instance.jdbcUrl, MyPostgresqlContainer.DB_USER, MyPostgresqlContainer.DB_PASSWORD)
            .load()
            .migrate()
    }

    @Test
    fun `it should retrieve a persisted temporal entity attribute`() {
        val rawEntity = loadSampleData("beehive_two_temporal_properties.jsonld")

        temporalEntityAttributeService.createEntityTemporalReferences(rawEntity).block()

        val temporalEntityAttributes =
            temporalEntityAttributeService.getForEntity("urn:ngsi-ld:BeeHive:TESTD", listOf("incoming", "outgoing"), apicContext!!)

        StepVerifier.create(temporalEntityAttributes)
            .expectNextMatches {
                it.entityId == "urn:ngsi-ld:BeeHive:TESTD" &&
                    it.type == "https://ontology.eglobalmark.com/apic#BeeHive" &&
                    it.attributeName == "https://ontology.eglobalmark.com/apic#incoming" &&
                    it.entityPayload !== null
            }
            .expectNextMatches {
                it.entityId == "urn:ngsi-ld:BeeHive:TESTD"
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
                it == 1
            }
            .expectComplete()
            .verify()
    }

    @Test
    fun `it should create two entries for an entity with two temporal properties`() {
        val rawEntity = loadSampleData("beehive2_two_temporal_properties.jsonld")

        val temporalReferencesResults = temporalEntityAttributeService.createEntityTemporalReferences(rawEntity)

        StepVerifier.create(temporalReferencesResults)
            .expectNextMatches {
                it == 2
            }
            .expectComplete()
            .verify()
    }

    @Test
    fun `it should inject temporal numeric values in temporalValues format into an entity`() {
        val rawEntity = loadAndParseSampleData("beehive.jsonld")
        val rawResults = listOf(
            listOf(
                mapOf(
                    "attribute_name" to "https://ontology.eglobalmark.com/apic#incoming",
                    "value" to 550.0,
                    "observed_at" to ZonedDateTime.parse("2020-03-25T10:29:17.965206+02:00")
                ),
                mapOf(
                    "attribute_name" to "https://ontology.eglobalmark.com/apic#incoming",
                    "value" to 650.0,
                    "observed_at" to ZonedDateTime.parse("2020-03-25T10:33:17.965206+02:00")
                )
            )
        )

        val enrichedEntity = temporalEntityAttributeService.injectTemporalValues(rawEntity, rawResults, true)
        val serializedEntity = JsonLdProcessor.compact(enrichedEntity.attributes, mapOf("@context" to enrichedEntity.contexts), JsonLdOptions())
        val finalEntity = JsonUtils.toPrettyString(serializedEntity)
        assertEquals(loadSampleData("expectations/beehive_with_incoming_temporal_values.jsonld").trim(), finalEntity)
    }

    @Test
    fun `it should inject temporal string values in temporalValues format into an entity`() {
        val rawEntity = loadAndParseSampleData("subscription.jsonld")
        val rawResults = listOf(
            listOf(
                mapOf(
                    "attribute_name" to "https://uri.etsi.org/ngsi-ld/notification",
                    "value" to "urn:ngsi-ld:Beehive:1234",
                    "observed_at" to ZonedDateTime.parse("2020-03-25T10:29:17.965206+02:00")
                ),
                mapOf(
                    "attribute_name" to "https://uri.etsi.org/ngsi-ld/notification",
                    "value" to "urn:ngsi-ld:Beehive:5678",
                    "observed_at" to ZonedDateTime.parse("2020-03-25T10:33:17.965206+02:00")
                )
            )
        )

        val enrichedEntity = temporalEntityAttributeService.injectTemporalValues(rawEntity, rawResults, true)
        val serializedEntity = JsonLdProcessor.compact(enrichedEntity.attributes, mapOf("@context" to enrichedEntity.contexts), JsonLdOptions())
        val finalEntity = JsonUtils.toPrettyString(serializedEntity)
        assertEquals(loadSampleData("expectations/subscription_with_notifications_temporal_values.jsonld").trim(), finalEntity)
    }

    @Test
    fun `it should inject temporal string values in default format into an entity`() {
        val rawEntity = loadAndParseSampleData("subscription.jsonld")
        val rawResults = listOf(
            listOf(
                mapOf(
                    "attribute_name" to "https://uri.etsi.org/ngsi-ld/notification",
                    "value" to "urn:ngsi-ld:Beehive:1234",
                    "instance_id" to "urn:ngsi-ld:Beehive:notification:1234",
                    "observed_at" to ZonedDateTime.parse("2020-03-25T10:29:17.965206+02:00")
                ),
                mapOf(
                    "attribute_name" to "https://uri.etsi.org/ngsi-ld/notification",
                    "value" to "urn:ngsi-ld:Beehive:5678",
                    "instance_id" to "urn:ngsi-ld:Beehive:notification:4567",
                    "observed_at" to ZonedDateTime.parse("2020-03-25T10:33:17.965206+02:00")
                )
            )
        )

        val enrichedEntity = temporalEntityAttributeService.injectTemporalValues(rawEntity, rawResults, false)
        val serializedEntity = JsonLdProcessor.compact(enrichedEntity.attributes, mapOf("@context" to enrichedEntity.contexts), JsonLdOptions())
        val finalEntity = JsonUtils.toPrettyString(serializedEntity)
        assertEquals(loadSampleData("expectations/subscription_with_notifications.jsonld").trim(), finalEntity)
    }

    @Test
    fun `it should return the entity untouched if it has no temporal history`() {
        val rawEntity = loadAndParseSampleData("subscription.jsonld")
        val rawResults = emptyList<List<Map<String, Any>>>()

        val enrichedEntity = temporalEntityAttributeService.injectTemporalValues(rawEntity, rawResults, true)
        val serializedEntity = JsonLdProcessor.compact(enrichedEntity.attributes, mapOf("@context" to enrichedEntity.contexts), JsonLdOptions())
        val finalEntity = JsonUtils.toPrettyString(serializedEntity)
        assertEquals(loadSampleData("subscription.jsonld").trim(), finalEntity)
    }

    @Test
    fun `it should return the entity untouched if it has an empty temporal history`() {
        val rawEntity = loadAndParseSampleData("subscription.jsonld")
        val rawResults = listOf(listOf(emptyMap<String, Any>()))

        val enrichedEntity = temporalEntityAttributeService.injectTemporalValues(rawEntity, rawResults, true)
        val serializedEntity = JsonLdProcessor.compact(enrichedEntity.attributes, mapOf("@context" to enrichedEntity.contexts), JsonLdOptions())
        val finalEntity = JsonUtils.toPrettyString(serializedEntity)
        assertEquals(loadSampleData("subscription.jsonld").trim(), finalEntity)
    }
}
