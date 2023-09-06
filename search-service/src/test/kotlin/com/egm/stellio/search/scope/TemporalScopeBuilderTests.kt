package com.egm.stellio.search.scope

import com.egm.stellio.search.model.AttributeInstance.TemporalProperty
import com.egm.stellio.search.model.EntityPayload
import com.egm.stellio.search.model.TemporalEntitiesQuery
import com.egm.stellio.search.model.TemporalQuery
import com.egm.stellio.search.support.EMPTY_JSON_PAYLOAD
import com.egm.stellio.shared.util.*
import org.junit.jupiter.api.Test
import org.springframework.test.context.ActiveProfiles
import java.time.Instant
import java.time.ZoneOffset
import java.time.ZonedDateTime

@ActiveProfiles("test")
class TemporalScopeBuilderTests {

    private val now = ngsiLdDateTime()

    private val entityId = "urn:ngsi-ld:Beehive:1234".toUri()

    @Test
    fun `it should build an aggregated temporal representation of scopes`() {
        val entityPayload = gimmeEntityPayload()
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
        val temporalQuery = TemporalQuery(
            timerel = TemporalQuery.Timerel.AFTER,
            timeAt = Instant.now().atZone(ZoneOffset.UTC).minusHours(1),
            aggrPeriodDuration = "P1D",
            aggrMethods = listOf(TemporalQuery.Aggregate.SUM, TemporalQuery.Aggregate.AVG)
        )

        val scopehHistory = TemporalScopeBuilder.buildScopeAttributeInstances(
            entityPayload,
            scopeInstances,
            TemporalEntitiesQuery(
                queryParams = buildDefaultQueryParams(),
                temporalQuery = temporalQuery,
                withTemporalValues = false,
                withAudit = false,
                withAggregatedValues = true
            )
        )

        assertJsonPayloadsAreEqual(
            loadSampleData("expectations/scope_aggregated_instances.json"),
            JsonUtils.serializeObject(scopehHistory)
        )
    }

    @Test
    fun `it should build a temporal values representation of scopes`() {
        val entityPayload = gimmeEntityPayload()
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
        val temporalQuery = TemporalQuery(
            TemporalQuery.Timerel.AFTER,
            Instant.now().atZone(ZoneOffset.UTC).minusHours(1),
        )

        val scopehHistory = TemporalScopeBuilder.buildScopeAttributeInstances(
            entityPayload,
            scopeInstances,
            TemporalEntitiesQuery(
                queryParams = buildDefaultQueryParams(),
                temporalQuery = temporalQuery,
                withTemporalValues = true,
                withAudit = false,
                withAggregatedValues = false
            )
        )

        assertJsonPayloadsAreEqual(
            loadSampleData("expectations/scope_temporal_values_instances.json"),
            JsonUtils.serializeObject(scopehHistory)
        )
    }

    @Test
    fun `it should build a full representation of scopes`() {
        val entityPayload = gimmeEntityPayload()
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
        val temporalQuery = TemporalQuery(
            TemporalQuery.Timerel.AFTER,
            Instant.now().atZone(ZoneOffset.UTC).minusHours(1),
        )

        val scopehHistory = TemporalScopeBuilder.buildScopeAttributeInstances(
            entityPayload,
            scopeInstances,
            TemporalEntitiesQuery(
                queryParams = buildDefaultQueryParams(),
                temporalQuery = temporalQuery,
                withTemporalValues = false,
                withAudit = false,
                withAggregatedValues = false
            )
        )

        assertJsonPayloadsAreEqual(
            loadSampleData("expectations/scope_full_instances.json"),
            JsonUtils.serializeObject(scopehHistory)
        )
    }

    private fun gimmeEntityPayload(): EntityPayload =
        EntityPayload(
            entityId = entityId,
            types = listOf(BEEHIVE_TYPE),
            createdAt = now,
            payload = EMPTY_JSON_PAYLOAD,
            contexts = listOf(APIC_COMPOUND_CONTEXT)
        )
}
