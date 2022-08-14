package com.egm.stellio.search.service

import com.egm.stellio.search.model.*
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CORE_CONTEXT
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import java.time.Instant
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.util.UUID

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = [TemporalEntityService::class])
@ActiveProfiles("test")
class TemporalEntityServiceTests {

    @Autowired
    private lateinit var temporalEntityService: TemporalEntityService

    private val now = Instant.now().atZone(ZoneOffset.UTC)

    @Test
    fun `it should return a temporal entity with an empty array of instances if it has no temporal history`() {
        val temporalEntityAttribute = TemporalEntityAttribute(
            entityId = "urn:ngsi-ld:Subscription:1234".toUri(),
            attributeName = "https://uri.etsi.org/ngsi-ld/notification",
            attributeValueType = TemporalEntityAttribute.AttributeValueType.ANY,
            createdAt = now,
            payload = EMPTY_PAYLOAD
        )
        val attributeAndResultsMap = mapOf(
            temporalEntityAttribute to emptyList<AttributeInstanceResult>()
        )
        val entityPayload = EntityPayload(
            entityId = "urn:ngsi-ld:Subscription:1234".toUri(),
            types = listOf("https://uri.etsi.org/ngsi-ld/Subscription"),
            createdAt = now,
            contexts = listOf(NGSILD_CORE_CONTEXT)
        )
        val temporalEntity = temporalEntityService.buildTemporalEntity(
            entityPayload,
            attributeAndResultsMap,
            TemporalQuery(),
            listOf(NGSILD_CORE_CONTEXT),
            withTemporalValues = false,
            withAudit = false
        )
        assertTrue(
            serializeObject(temporalEntity).matchContent(
                loadSampleData("expectations/subscription_empty_notification.jsonld")
            )
        )
    }

    @ParameterizedTest
    @MethodSource("com.egm.stellio.search.util.ParameterizedTests#rawResultsProvider")
    fun `it should correctly build a temporal entity`(
        attributeAndResultsMap: TemporalEntityAttributeInstancesResult,
        withTemporalValues: Boolean,
        withAudit: Boolean,
        expectation: String
    ) {
        val entityPayload = EntityPayload(
            entityId = "urn:ngsi-ld:BeeHive:TESTC".toUri(),
            types = listOf(BEEHIVE_TYPE),
            createdAt = now,
            contexts = listOf(APIC_COMPOUND_CONTEXT)
        )

        val temporalEntity = temporalEntityService.buildTemporalEntity(
            entityPayload,
            attributeAndResultsMap,
            TemporalQuery(),
            listOf(APIC_COMPOUND_CONTEXT),
            withTemporalValues,
            withAudit
        )
        assertJsonPayloadsAreEqual(expectation, serializeObject(temporalEntity))
    }

    @ParameterizedTest
    @MethodSource("com.egm.stellio.search.util.QueryParameterizedTests#rawResultsProvider")
    fun `it should correctly build temporal entities`(
        queryResult: List<Pair<EntityPayload, TemporalEntityAttributeInstancesResult>>,
        withTemporalValues: Boolean,
        withAudit: Boolean,
        expectation: String
    ) {
        val temporalEntity = temporalEntityService.buildTemporalEntities(
            queryResult,
            TemporalQuery(),
            listOf(APIC_COMPOUND_CONTEXT),
            withTemporalValues,
            withAudit
        )
        assertJsonPayloadsAreEqual(expectation, serializeObject(temporalEntity))
    }

    @Test
    fun `it should return a temporal entity with values aggregated`() {
        val temporalEntityAttribute = TemporalEntityAttribute(
            entityId = "urn:ngsi-ld:Subscription:1234".toUri(),
            attributeName = "https://uri.etsi.org/ngsi-ld/notification",
            attributeValueType = TemporalEntityAttribute.AttributeValueType.ANY,
            createdAt = now,
            payload = EMPTY_PAYLOAD
        )
        val attributeAndResultsMap = mapOf(
            temporalEntityAttribute to listOf(
                SimplifiedAttributeInstanceResult(
                    temporalEntityAttribute = UUID.randomUUID(),
                    value = "urn:ngsi-ld:Beehive:1234",
                    time = ZonedDateTime.parse("2020-03-25T08:29:17.965206Z")
                ),
                SimplifiedAttributeInstanceResult(
                    temporalEntityAttribute = UUID.randomUUID(),
                    value = "urn:ngsi-ld:Beehive:5678",
                    time = ZonedDateTime.parse("2020-03-25T08:33:17.965206Z")
                )
            )
        )
        val temporalQuery = TemporalQuery(
            TemporalQuery.Timerel.AFTER, Instant.now().atZone(ZoneOffset.UTC).minusHours(1),
            null, "1 day", TemporalQuery.Aggregate.SUM
        )
        val entityPayload = EntityPayload(
            entityId = "urn:ngsi-ld:Subscription:1234".toUri(),
            types = listOf("https://uri.etsi.org/ngsi-ld/Subscription"),
            createdAt = now,
            contexts = listOf(NGSILD_CORE_CONTEXT)
        )

        val temporalEntity = temporalEntityService.buildTemporalEntity(
            entityPayload,
            attributeAndResultsMap,
            temporalQuery,
            listOf(NGSILD_CORE_CONTEXT),
            withTemporalValues = false,
            withAudit = false
        )

        assertJsonPayloadsAreEqual(
            loadSampleData("expectations/subscription_with_notifications_aggregated.jsonld"),
            serializeObject(temporalEntity)
        )
    }
}
