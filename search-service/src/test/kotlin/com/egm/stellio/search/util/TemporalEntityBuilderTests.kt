package com.egm.stellio.search.util

import com.egm.stellio.search.model.*
import com.egm.stellio.search.model.AggregatedAttributeInstanceResult.AggregateResult
import com.egm.stellio.search.scope.ScopeInstanceResult
import com.egm.stellio.search.support.EMPTY_JSON_PAYLOAD
import com.egm.stellio.search.support.buildDefaultQueryParams
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CREATED_AT_PROPERTY
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
            payload = EMPTY_JSON_PAYLOAD
        )
        val temporalEntity = TemporalEntityBuilder.buildTemporalEntity(
            EntityTemporalResult(entityPayload, emptyList(), attributeAndResultsMap),
            TemporalEntitiesQuery(
                entitiesQuery = buildDefaultQueryParams(),
                temporalQuery = TemporalQuery(),
                withTemporalValues = false,
                withAudit = false,
                withAggregatedValues = false
            ),
            APIC_COMPOUND_CONTEXTS
        )
        assertJsonPayloadsAreEqual(
            loadSampleData("expectations/beehive_empty_outgoing.jsonld"),
            serializeObject(temporalEntity.members),
            setOf(NGSILD_CREATED_AT_PROPERTY)
        )
    }

    @ParameterizedTest
    @MethodSource("com.egm.stellio.search.util.TemporalEntityParameterizedSource#rawResultsProvider")
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
            payload = EMPTY_JSON_PAYLOAD
        )

        val temporalEntity = TemporalEntityBuilder.buildTemporalEntity(
            EntityTemporalResult(entityPayload, scopeHistory, attributeAndResultsMap),
            TemporalEntitiesQuery(
                entitiesQuery = buildDefaultQueryParams(),
                temporalQuery = TemporalQuery(),
                withTemporalValues,
                withAudit,
                false
            ),
            APIC_COMPOUND_CONTEXTS
        )
        assertJsonPayloadsAreEqual(
            expectation,
            serializeObject(temporalEntity.members),
            setOf(NGSILD_CREATED_AT_PROPERTY)
        )
    }

    @ParameterizedTest
    @MethodSource("com.egm.stellio.search.util.TemporalEntitiesParameterizedSource#rawResultsProvider")
    fun `it should correctly build temporal entities`(
        entityTemporalResults: List<EntityTemporalResult>,
        withTemporalValues: Boolean,
        withAudit: Boolean,
        expectation: String
    ) {
        val temporalEntity = TemporalEntityBuilder.buildTemporalEntities(
            entityTemporalResults,
            TemporalEntitiesQuery(
                entitiesQuery = buildDefaultQueryParams(),
                temporalQuery = TemporalQuery(),
                withTemporalValues,
                withAudit,
                false
            ),
            APIC_COMPOUND_CONTEXTS
        )
        assertJsonPayloadsAreEqual(
            expectation,
            serializeObject(temporalEntity.map { it.members }),
            setOf(NGSILD_CREATED_AT_PROPERTY)
        )
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
            payload = EMPTY_JSON_PAYLOAD
        )

        val temporalEntity = TemporalEntityBuilder.buildTemporalEntity(
            EntityTemporalResult(entityPayload, emptyList(), attributeAndResultsMap),
            TemporalEntitiesQuery(
                entitiesQuery = buildDefaultQueryParams(),
                temporalQuery = temporalQuery,
                withTemporalValues = false,
                withAudit = false,
                withAggregatedValues = true
            ),
            APIC_COMPOUND_CONTEXTS
        )

        assertJsonPayloadsAreEqual(
            loadSampleData("expectations/beehive_aggregated_outgoing.jsonld"),
            serializeObject(temporalEntity.members),
            setOf(NGSILD_CREATED_AT_PROPERTY)
        )
    }
}
