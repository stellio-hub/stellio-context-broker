package com.egm.stellio.search.temporal.util

import com.egm.stellio.search.entity.model.Attribute
import com.egm.stellio.search.entity.model.Entity
import com.egm.stellio.search.scope.ScopeInstanceResult
import com.egm.stellio.search.support.EMPTY_JSON_PAYLOAD
import com.egm.stellio.search.support.buildDefaultQueryParams
import com.egm.stellio.search.support.buildDefaultTestTemporalQuery
import com.egm.stellio.search.temporal.model.AggregatedAttributeInstanceResult
import com.egm.stellio.search.temporal.model.AggregatedAttributeInstanceResult.AggregateResult
import com.egm.stellio.search.temporal.model.AttributeInstanceResult
import com.egm.stellio.search.temporal.model.EntityTemporalResult
import com.egm.stellio.search.temporal.model.TemporalEntitiesQueryFromGet
import com.egm.stellio.search.temporal.model.TemporalQuery
import com.egm.stellio.shared.util.BEEHIVE_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CREATED_AT_PROPERTY
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.util.OUTGOING_PROPERTY
import com.egm.stellio.shared.util.assertJsonPayloadsAreEqual
import com.egm.stellio.shared.util.loadSampleData
import com.egm.stellio.shared.util.ngsiLdDateTime
import com.egm.stellio.shared.util.toUri
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
        val attribute = Attribute(
            entityId = "urn:ngsi-ld:Beehive:1234".toUri(),
            attributeName = OUTGOING_PROPERTY,
            attributeValueType = Attribute.AttributeValueType.STRING,
            createdAt = now,
            payload = EMPTY_JSON_PAYLOAD
        )
        val attributeAndResultsMap = mapOf(
            attribute to emptyList<AttributeInstanceResult>()
        )
        val entity = Entity(
            entityId = "urn:ngsi-ld:Beehive:1234".toUri(),
            types = listOf(BEEHIVE_TYPE),
            createdAt = now,
            payload = EMPTY_JSON_PAYLOAD
        )
        val temporalEntity = TemporalEntityBuilder.buildTemporalEntity(
            EntityTemporalResult(entity, emptyList(), attributeAndResultsMap),
            TemporalEntitiesQueryFromGet(
                entitiesQuery = buildDefaultQueryParams(),
                temporalQuery = buildDefaultTestTemporalQuery(),
                temporalRepresentation = TemporalRepresentation.NONE,
                withAudit = false
            )
        )
        assertJsonPayloadsAreEqual(
            loadSampleData("expectations/beehive_empty_outgoing.jsonld"),
            serializeObject(temporalEntity.members),
            setOf(NGSILD_CREATED_AT_PROPERTY)
        )
    }

    @ParameterizedTest
    @MethodSource("com.egm.stellio.search.temporal.util.TemporalEntityParameterizedSource#rawResultsProvider")
    fun `it should correctly build a temporal entity`(
        scopeHistory: List<ScopeInstanceResult>,
        attributeAndResultsMap: AttributesWithInstances,
        temporalRepresentation: TemporalRepresentation,
        withAudit: Boolean,
        expectation: String
    ) {
        val entity = Entity(
            entityId = "urn:ngsi-ld:BeeHive:TESTC".toUri(),
            types = listOf(BEEHIVE_TYPE),
            createdAt = now,
            payload = EMPTY_JSON_PAYLOAD
        )

        val temporalEntity = TemporalEntityBuilder.buildTemporalEntity(
            EntityTemporalResult(entity, scopeHistory, attributeAndResultsMap),
            TemporalEntitiesQueryFromGet(
                entitiesQuery = buildDefaultQueryParams(),
                temporalQuery = buildDefaultTestTemporalQuery(),
                temporalRepresentation,
                withAudit
            )
        )
        assertJsonPayloadsAreEqual(
            expectation,
            serializeObject(temporalEntity.members),
            setOf(NGSILD_CREATED_AT_PROPERTY)
        )
    }

    @ParameterizedTest
    @MethodSource("com.egm.stellio.search.temporal.util.TemporalEntitiesParameterizedSource#rawResultsProvider")
    fun `it should correctly build temporal entities`(
        entityTemporalResults: List<EntityTemporalResult>,
        temporalRepresentation: TemporalRepresentation,
        withAudit: Boolean,
        expectation: String
    ) {
        val temporalEntity = TemporalEntityBuilder.buildTemporalEntities(
            entityTemporalResults,
            TemporalEntitiesQueryFromGet(
                entitiesQuery = buildDefaultQueryParams(),
                temporalQuery = buildDefaultTestTemporalQuery(),
                temporalRepresentation,
                withAudit
            )
        )
        assertJsonPayloadsAreEqual(
            expectation,
            serializeObject(temporalEntity.map { it.members }),
            setOf(NGSILD_CREATED_AT_PROPERTY)
        )
    }

    @SuppressWarnings("LongMethod")
    @Test
    fun `it should return a temporal entity with values aggregated`() {
        val attribute = Attribute(
            entityId = "urn:ngsi-ld:Beehive:1234".toUri(),
            attributeName = OUTGOING_PROPERTY,
            attributeValueType = Attribute.AttributeValueType.NUMBER,
            createdAt = now,
            payload = EMPTY_JSON_PAYLOAD
        )
        val attributeAndResultsMap = mapOf(
            attribute to listOf(
                AggregatedAttributeInstanceResult(
                    attributeUuid = attribute.id,
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
                    attributeUuid = attribute.id,
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
        val temporalQuery = buildDefaultTestTemporalQuery(
            TemporalQuery.Timerel.AFTER,
            Instant.now().atZone(ZoneOffset.UTC).minusHours(1),
            null,
            "P1D",
            listOf(TemporalQuery.Aggregate.SUM, TemporalQuery.Aggregate.AVG)
        )
        val entity = Entity(
            entityId = "urn:ngsi-ld:Beehive:1234".toUri(),
            types = listOf(BEEHIVE_TYPE),
            createdAt = now,
            payload = EMPTY_JSON_PAYLOAD
        )

        val temporalEntity = TemporalEntityBuilder.buildTemporalEntity(
            EntityTemporalResult(entity, emptyList(), attributeAndResultsMap),
            TemporalEntitiesQueryFromGet(
                entitiesQuery = buildDefaultQueryParams(),
                temporalQuery = temporalQuery,
                temporalRepresentation = TemporalRepresentation.AGGREGATED_VALUES,
                withAudit = false
            )
        )

        assertJsonPayloadsAreEqual(
            loadSampleData("expectations/beehive_aggregated_outgoing.jsonld"),
            serializeObject(temporalEntity.members),
            setOf(NGSILD_CREATED_AT_PROPERTY)
        )
    }
}
