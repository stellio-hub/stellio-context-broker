package com.egm.stellio.search.scope

import com.egm.stellio.search.support.buildDefaultQueryParams
import com.egm.stellio.search.support.buildDefaultTestTemporalQuery
import com.egm.stellio.search.temporal.model.AttributeInstance.TemporalProperty
import com.egm.stellio.search.temporal.model.TemporalEntitiesQueryFromGet
import com.egm.stellio.search.temporal.model.TemporalQuery
import com.egm.stellio.search.temporal.util.TemporalRepresentation
import com.egm.stellio.shared.util.JsonUtils
import com.egm.stellio.shared.util.UriUtils.toUri
import com.egm.stellio.shared.util.assertJsonPayloadsAreEqual
import com.egm.stellio.shared.util.loadSampleData
import org.junit.jupiter.api.Test
import org.springframework.test.context.ActiveProfiles
import java.time.Instant
import java.time.ZoneOffset
import java.time.ZonedDateTime

@ActiveProfiles("test")
class TemporalScopeBuilderTests {

    private val entityId = "urn:ngsi-ld:Beehive:1234".toUri()

    @Test
    fun `it should build an aggregated temporal representation of scopes`() {
        val scopeInstances = listOf(
            AggregatedScopeInstanceResult(
                entityId = entityId,
                listOf(
                    AggregatedScopeInstanceResult.AggregateResult(
                        TemporalQuery.Aggregate.SUM,
                        12,
                        ZonedDateTime.parse("2020-03-25T08:29:17.965206Z"),
                        ZonedDateTime.parse("2020-03-25T10:29:18.965206Z")
                    ),
                    AggregatedScopeInstanceResult.AggregateResult(
                        TemporalQuery.Aggregate.AVG,
                        2,
                        ZonedDateTime.parse("2020-03-25T08:29:17.965206Z"),
                        ZonedDateTime.parse("2020-03-25T10:29:18.965206Z")
                    )
                )
            ),
            AggregatedScopeInstanceResult(
                entityId = entityId,
                listOf(
                    AggregatedScopeInstanceResult.AggregateResult(
                        TemporalQuery.Aggregate.SUM,
                        14,
                        ZonedDateTime.parse("2020-03-25T10:29:18.965206Z"),
                        ZonedDateTime.parse("2020-03-25T12:29:19.965206Z")
                    ),
                    AggregatedScopeInstanceResult.AggregateResult(
                        TemporalQuery.Aggregate.AVG,
                        2.5,
                        ZonedDateTime.parse("2020-03-25T10:29:18.965206Z"),
                        ZonedDateTime.parse("2020-03-25T12:29:19.965206Z")
                    )
                )
            )
        )
        val temporalQuery = buildDefaultTestTemporalQuery(
            timerel = TemporalQuery.Timerel.AFTER,
            timeAt = Instant.now().atZone(ZoneOffset.UTC).minusHours(1),
            aggrPeriodDuration = "P1D",
            aggrMethods = listOf(TemporalQuery.Aggregate.SUM, TemporalQuery.Aggregate.AVG)
        )

        val scopeHistory = TemporalScopeBuilder.buildScopeAttributeInstances(
            scopeInstances,
            TemporalEntitiesQueryFromGet(
                entitiesQuery = buildDefaultQueryParams(),
                temporalQuery = temporalQuery,
                temporalRepresentation = TemporalRepresentation.AGGREGATED_VALUES,
                withAudit = false
            )
        )

        assertJsonPayloadsAreEqual(
            loadSampleData("expectations/scope_aggregated_instances.json"),
            JsonUtils.serializeObject(scopeHistory)
        )
    }

    @Test
    fun `it should build a temporal values representation of scopes`() {
        val scopeInstances = listOf(
            SimplifiedScopeInstanceResult(
                entityId = entityId,
                scopes = listOf("/A/B", "/B/C"),
                time = ZonedDateTime.parse("2020-03-25T08:29:17.965206Z")
            ),
            SimplifiedScopeInstanceResult(
                entityId = entityId,
                scopes = listOf("/B/C", "/D/E"),
                time = ZonedDateTime.parse("2020-03-25T08:30:17.965206Z")
            )
        )
        val temporalQuery = buildDefaultTestTemporalQuery(
            TemporalQuery.Timerel.AFTER,
            Instant.now().atZone(ZoneOffset.UTC).minusHours(1),
        )

        val scopeHistory = TemporalScopeBuilder.buildScopeAttributeInstances(
            scopeInstances,
            TemporalEntitiesQueryFromGet(
                entitiesQuery = buildDefaultQueryParams(),
                temporalQuery = temporalQuery,
                temporalRepresentation = TemporalRepresentation.TEMPORAL_VALUES,
                withAudit = false
            )
        )

        assertJsonPayloadsAreEqual(
            loadSampleData("expectations/scope_temporal_values_instances.json"),
            JsonUtils.serializeObject(scopeHistory)
        )
    }

    @Test
    fun `it should build a full representation of scopes`() {
        val scopeInstances = listOf(
            FullScopeInstanceResult(
                entityId = entityId,
                scopes = listOf("/A/B", "/B/C"),
                time = ZonedDateTime.parse("2020-03-25T08:29:17.965206Z"),
                timeproperty = TemporalProperty.OBSERVED_AT.propertyName
            ),
            FullScopeInstanceResult(
                entityId = entityId,
                scopes = listOf("/B/C", "/D/E"),
                time = ZonedDateTime.parse("2020-03-25T08:30:17.965206Z"),
                timeproperty = TemporalProperty.OBSERVED_AT.propertyName
            )
        )
        val temporalQuery = buildDefaultTestTemporalQuery(
            TemporalQuery.Timerel.AFTER,
            Instant.now().atZone(ZoneOffset.UTC).minusHours(1),
        )

        val scopeHistory = TemporalScopeBuilder.buildScopeAttributeInstances(
            scopeInstances,
            TemporalEntitiesQueryFromGet(
                entitiesQuery = buildDefaultQueryParams(),
                temporalQuery = temporalQuery,
                temporalRepresentation = TemporalRepresentation.NORMALIZED,
                withAudit = false
            )
        )

        assertJsonPayloadsAreEqual(
            loadSampleData("expectations/scope_full_instances.json"),
            JsonUtils.serializeObject(scopeHistory)
        )
    }
}
