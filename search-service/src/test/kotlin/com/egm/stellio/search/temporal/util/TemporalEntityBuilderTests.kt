package com.egm.stellio.search.temporal.util

import com.egm.stellio.search.entity.model.Attribute
import com.egm.stellio.search.entity.model.Entity
import com.egm.stellio.search.scope.ScopeInstanceResult
import com.egm.stellio.search.support.EMPTY_JSON_PAYLOAD
import com.egm.stellio.search.support.buildDefaultQueryParams
import com.egm.stellio.search.support.buildDefaultTestTemporalQuery
import com.egm.stellio.search.temporal.model.AggregatedAttributeInstanceResult
import com.egm.stellio.search.temporal.model.AggregatedAttributeInstanceResult.AggregateResult
import com.egm.stellio.search.temporal.model.EntityTemporalResult
import com.egm.stellio.search.temporal.model.TemporalEntitiesQueryFromGet
import com.egm.stellio.search.temporal.model.TemporalQuery
import com.egm.stellio.search.temporal.util.TemporalEntityBuilder.wrapSingleValuesToList
import com.egm.stellio.shared.model.NGSILD_CREATED_AT_IRI
import com.egm.stellio.shared.model.NGSILD_MODIFIED_AT_IRI
import com.egm.stellio.shared.util.BEEHIVE_IRI
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.util.NGSILD_TEST_CORE_CONTEXT
import com.egm.stellio.shared.util.OUTGOING_IRI
import com.egm.stellio.shared.util.assertJsonPayloadsAreEqual
import com.egm.stellio.shared.util.loadSampleData
import com.egm.stellio.shared.util.ngsiLdDateTime
import com.egm.stellio.shared.util.toUri
import org.junit.jupiter.api.Assertions.assertEquals
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
    fun `temporal values should normalize ListRelationship list containers after JSON-LD compaction`() {
        val compactedEntity = mapOf(
            "isParkedIn" to mapOf(
                "type" to "ListRelationship",
                "ngsi-ld:hasObjectLists" to mapOf(
                    "@list" to listOf(
                        mapOf(
                            "@list" to listOf(
                                mapOf(
                                    "objectList" to listOf(
                                        mapOf("object" to "urn:ngsi-ld:Parking:01")
                                    )
                                ),
                                "2020-03-25T08:29:17Z"
                            )
                        )
                    )
                )
            )
        )

        assertEquals(
            listOf(
                listOf(
                    mapOf("objectList" to listOf(mapOf("object" to "urn:ngsi-ld:Parking:01"))),
                    "2020-03-25T08:29:17Z"
                )
            ),
            (
                compactedEntity.wrapSingleValuesToList(TemporalRepresentation.TEMPORAL_VALUES)["isParkedIn"]
                    as Map<*, *>
                )["objectLists"]
        )
    }

    @Test
    fun `temporal values should preserve a null ListProperty value as a list`() {
        val compactedEntity = mapOf(
            "airQualityLevel" to mapOf(
                "type" to "ListProperty",
                "valueLists" to listOf(
                    listOf(
                        mapOf("ngsi-ld:hasValueList" to "urn:ngsi-ld:null"),
                        "2020-03-25T08:29:17Z"
                    )
                )
            )
        )

        val normalizedAttribute =
            compactedEntity.wrapSingleValuesToList(TemporalRepresentation.TEMPORAL_VALUES)["airQualityLevel"]
                as Map<*, *>
        assertEquals(
            mapOf("valueList" to listOf("urn:ngsi-ld:null")),
            (normalizedAttribute["valueLists"] as List<*>).first().let { it as List<*> }.first()
        )
    }

    @Test
    fun `temporal values should preserve a null ListRelationship value as a list`() {
        val compactedEntity = mapOf(
            "isParkedIn" to mapOf(
                "type" to "ListRelationship",
                "ngsi-ld:hasObjectLists" to mapOf(
                    "@list" to listOf(
                        mapOf(
                            "@list" to listOf(
                                mapOf("ngsi-ld:hasObjectList" to "urn:ngsi-ld:null"),
                                "2020-03-25T08:29:17Z"
                            )
                        )
                    )
                )
            )
        )

        val normalizedAttribute =
            compactedEntity.wrapSingleValuesToList(TemporalRepresentation.TEMPORAL_VALUES)["isParkedIn"]
                as Map<*, *>
        assertEquals(
            mapOf("objectList" to listOf("urn:ngsi-ld:null")),
            (normalizedAttribute["objectLists"] as List<*>).first().let { it as List<*> }.first()
        )
    }

    @ParameterizedTest
    @MethodSource("com.egm.stellio.search.temporal.util.TemporalEntityParameterizedSource#rawResultsProvider")
    fun `buildTemporalEntity should correctly build a temporal entity`(
        scopeHistory: List<ScopeInstanceResult>,
        attributeAndResultsMap: AttributesWithInstances,
        temporalRepresentation: TemporalRepresentation,
        withAudit: Boolean,
        expectation: String
    ) {
        val entity = Entity(
            entityId = "urn:ngsi-ld:BeeHive:TESTC".toUri(),
            types = listOf(BEEHIVE_IRI),
            createdAt = now,
            payload = EMPTY_JSON_PAYLOAD
        )

        val temporalEntity = TemporalEntityBuilder.buildTemporalEntity(
            EntityTemporalResult(entity, scopeHistory, attributeAndResultsMap),
            TemporalEntitiesQueryFromGet(
                entitiesQuery = buildDefaultQueryParams(),
                temporalQuery = buildDefaultTestTemporalQuery(),
                temporalRepresentation = temporalRepresentation,
                withAudit = withAudit
            ),
            NGSILD_TEST_CORE_CONTEXT
        )
        assertJsonPayloadsAreEqual(
            expectation,
            serializeObject(temporalEntity.members),
            setOf(NGSILD_CREATED_AT_IRI, NGSILD_MODIFIED_AT_IRI)
        )
    }

    @ParameterizedTest
    @MethodSource("com.egm.stellio.search.temporal.util.TemporalEntitiesParameterizedSource#rawResultsProvider")
    fun `buildTemporalEntities should correctly build temporal entities`(
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
                temporalRepresentation = temporalRepresentation,
                withAudit = withAudit
            ),
            NGSILD_TEST_CORE_CONTEXT
        )
        assertJsonPayloadsAreEqual(
            expectation,
            serializeObject(temporalEntity.map { it.members }),
            setOf(NGSILD_CREATED_AT_IRI, NGSILD_MODIFIED_AT_IRI)
        )
    }

    @SuppressWarnings("LongMethod")
    @Test
    fun `buildTemporalEntity should return a temporal entity with values aggregated`() {
        val attribute = Attribute(
            entityId = "urn:ngsi-ld:Beehive:1234".toUri(),
            attributeName = OUTGOING_IRI,
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
            types = listOf(BEEHIVE_IRI),
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
            ),
            NGSILD_TEST_CORE_CONTEXT
        )

        assertJsonPayloadsAreEqual(
            loadSampleData("expectations/beehive_aggregated_outgoing.jsonld"),
            serializeObject(temporalEntity.members),
            setOf(NGSILD_CREATED_AT_IRI, NGSILD_MODIFIED_AT_IRI)
        )
    }
}
