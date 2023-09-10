package com.egm.stellio.search.service

import com.egm.stellio.search.model.*
import com.egm.stellio.search.model.AggregatedAttributeInstanceResult.AggregateResult
import com.egm.stellio.search.scope.ScopeInstanceResult
import com.egm.stellio.search.support.EMPTY_JSON_PAYLOAD
import com.egm.stellio.search.util.TemporalEntityAttributeInstancesResult
import com.egm.stellio.search.util.TemporalEntityBuilder
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import org.springframework.test.context.ActiveProfiles
import java.time.Instant
import java.time.ZoneOffset
import java.time.ZonedDateTime

@ActiveProfiles("test")
class TemporalEntityBuilderTests {

    private val now = ngsiLdDateTime()

    @Test
    fun `it should return a temporal entity with an empty array of instances if it has no temporal history`() {
        val temporalEntityAttribute = TemporalEntityAttribute(
            entityId = "urn:ngsi-ld:Beehive:1234".toUri(),
            attributeName = OUTGOING_PROPERTY,
            attributeValueType = TemporalEntityAttribute.AttributeValueType.STRING,
            createdAt = now,
            payload = EMPTY_JSON_PAYLOAD
        )
        val attributeAndResultsMap = mapOf(
            temporalEntityAttribute to emptyList<AttributeInstanceResult>()
        )
        val entityPayload = EntityPayload(
            entityId = "urn:ngsi-ld:Beehive:1234".toUri(),
            types = listOf(BEEHIVE_TYPE),
            createdAt = now,
            payload = EMPTY_JSON_PAYLOAD,
            contexts = listOf(APIC_COMPOUND_CONTEXT)
        )
        val temporalEntity = TemporalEntityBuilder.buildTemporalEntity(
            EntityTemporalResult(entityPayload, emptyList(), attributeAndResultsMap),
            TemporalEntitiesQuery(
                queryParams = buildDefaultQueryParams(),
                temporalQuery = TemporalQuery(),
                withTemporalValues = false,
                withAudit = false,
                withAggregatedValues = false
            ),
            listOf(APIC_COMPOUND_CONTEXT)
        )
        assertJsonPayloadsAreEqual(
            loadSampleData("expectations/beehive_empty_outgoing.jsonld"),
            serializeObject(temporalEntity)
        )
    }

    @ParameterizedTest
    @MethodSource("com.egm.stellio.search.util.ParameterizedTests#rawResultsProvider")
    fun `it should correctly build a temporal entity`(
        scopeHistory: List<ScopeInstanceResult>,
        attributeAndResultsMap: TemporalEntityAttributeInstancesResult,
        withTemporalValues: Boolean,
        withAudit: Boolean,
        expectation: String
    ) {
        val entityPayload = EntityPayload(
            entityId = "urn:ngsi-ld:BeeHive:TESTC".toUri(),
            types = listOf(BEEHIVE_TYPE),
            createdAt = now,
            payload = EMPTY_JSON_PAYLOAD,
            contexts = listOf(APIC_COMPOUND_CONTEXT)
        )

        val temporalEntity = TemporalEntityBuilder.buildTemporalEntity(
            EntityTemporalResult(entityPayload, scopeHistory, attributeAndResultsMap),
            TemporalEntitiesQuery(
                queryParams = buildDefaultQueryParams(),
                temporalQuery = TemporalQuery(),
                withTemporalValues,
                withAudit,
                false
            ),
            listOf(APIC_COMPOUND_CONTEXT)
        )
        assertJsonPayloadsAreEqual(expectation, serializeObject(temporalEntity))
    }

    @ParameterizedTest
    @MethodSource("com.egm.stellio.search.util.QueryParameterizedTests#rawResultsProvider")
    fun `it should correctly build temporal entities`(
        entityTemporalResults: List<EntityTemporalResult>,
        withTemporalValues: Boolean,
        withAudit: Boolean,
        expectation: String
    ) {
        val temporalEntity = TemporalEntityBuilder.buildTemporalEntities(
            entityTemporalResults,
            TemporalEntitiesQuery(
                queryParams = buildDefaultQueryParams(),
                temporalQuery = TemporalQuery(),
                withTemporalValues,
                withAudit,
                false
            ),
            listOf(APIC_COMPOUND_CONTEXT)
        )
        assertJsonPayloadsAreEqual(expectation, serializeObject(temporalEntity))
    }

    @Test
    fun `it should return a temporal entity with values aggregated`() {
        val temporalEntityAttribute = TemporalEntityAttribute(
            entityId = "urn:ngsi-ld:Beehive:1234".toUri(),
            attributeName = OUTGOING_PROPERTY,
            attributeValueType = TemporalEntityAttribute.AttributeValueType.NUMBER,
            createdAt = now,
            payload = EMPTY_JSON_PAYLOAD
        )
        val attributeAndResultsMap = mapOf(
            temporalEntityAttribute to listOf(
                AggregatedAttributeInstanceResult(
                    temporalEntityAttribute = temporalEntityAttribute.id,
                    listOf(
                        AggregateResult(
                            TemporalQuery.Aggregate.SUM,
                            12,
                            ZonedDateTime.parse("2020-03-25T08:29:17.965206Z"),
                            ZonedDateTime.parse("2020-03-25T10:29:17.965206Z")
                        ),
                        AggregateResult(
                            TemporalQuery.Aggregate.AVG,
                            2,
                            ZonedDateTime.parse("2020-03-25T08:29:17.965206Z"),
                            ZonedDateTime.parse("2020-03-25T10:29:17.965206Z")
                        )
                    )
                ),
                AggregatedAttributeInstanceResult(
                    temporalEntityAttribute = temporalEntityAttribute.id,
                    listOf(
                        AggregateResult(
                            TemporalQuery.Aggregate.SUM,
                            14,
                            ZonedDateTime.parse("2020-03-25T10:29:17.965206Z"),
                            ZonedDateTime.parse("2020-03-25T12:29:17.965206Z")
                        ),
                        AggregateResult(
                            TemporalQuery.Aggregate.AVG,
                            2.5,
                            ZonedDateTime.parse("2020-03-25T10:29:17.965206Z"),
                            ZonedDateTime.parse("2020-03-25T12:29:17.965206Z")
                        )
                    )
                )
            )
        )
        val temporalQuery = TemporalQuery(
            TemporalQuery.Timerel.AFTER,
            Instant.now().atZone(ZoneOffset.UTC).minusHours(1),
            null,
            "P1D",
            listOf(TemporalQuery.Aggregate.SUM, TemporalQuery.Aggregate.AVG)
        )
        val entityPayload = EntityPayload(
            entityId = "urn:ngsi-ld:Beehive:1234".toUri(),
            types = listOf(BEEHIVE_TYPE),
            createdAt = now,
            payload = EMPTY_JSON_PAYLOAD,
            contexts = listOf(APIC_COMPOUND_CONTEXT)
        )

        val temporalEntity = TemporalEntityBuilder.buildTemporalEntity(
            EntityTemporalResult(entityPayload, emptyList(), attributeAndResultsMap),
            TemporalEntitiesQuery(
                queryParams = buildDefaultQueryParams(),
                temporalQuery = temporalQuery,
                withTemporalValues = false,
                withAudit = false,
                withAggregatedValues = true
            ),
            listOf(APIC_COMPOUND_CONTEXT)
        )

        assertJsonPayloadsAreEqual(
            loadSampleData("expectations/beehive_aggregated_outgoing.jsonld"),
            serializeObject(temporalEntity)
        )
    }
}
