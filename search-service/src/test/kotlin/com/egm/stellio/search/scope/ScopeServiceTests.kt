package com.egm.stellio.search.scope

import com.egm.stellio.search.entity.model.EntitiesQuery
import com.egm.stellio.search.entity.model.Entity
import com.egm.stellio.search.entity.model.OperationType
import com.egm.stellio.search.entity.service.EntityQueryService
import com.egm.stellio.search.entity.service.EntityService
import com.egm.stellio.search.entity.util.toExpandedAttributeInstance
import com.egm.stellio.search.support.WithKafkaContainer
import com.egm.stellio.search.support.WithTimescaleContainer
import com.egm.stellio.search.support.buildDefaultTestTemporalQuery
import com.egm.stellio.search.temporal.model.AttributeInstance.TemporalProperty
import com.egm.stellio.search.temporal.model.TemporalEntitiesQuery
import com.egm.stellio.search.temporal.model.TemporalQuery
import com.egm.stellio.shared.model.PaginationQuery
import com.egm.stellio.shared.model.getScopes
import com.egm.stellio.shared.util.*
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.data.r2dbc.core.delete
import org.springframework.test.context.ActiveProfiles
import java.time.ZonedDateTime
import java.util.stream.Stream

@SpringBootTest
@ActiveProfiles("test")
class ScopeServiceTests : WithTimescaleContainer, WithKafkaContainer {

    @Autowired
    private lateinit var scopeService: ScopeService

    @Autowired
    private lateinit var entityService: EntityService

    @Autowired
    private lateinit var entityQueryService: EntityQueryService

    @Autowired
    private lateinit var r2dbcEntityTemplate: R2dbcEntityTemplate

    private val beehiveTestCId = "urn:ngsi-ld:BeeHive:TESTC".toUri()

    @AfterEach
    fun clearEntityPayloadTable() {
        r2dbcEntityTemplate.delete<Entity>().from("entity_payload").all().block()

        runBlocking {
            scopeService.delete(beehiveTestCId)
        }
    }

    @Suppress("unused")
    private fun generateScopes(): Stream<Arguments> =
        Stream.of(
            Arguments.of(
                "beehive_with_scope.jsonld",
                listOf("/B", "/C"),
                OperationType.APPEND_ATTRIBUTES,
                listOf("/A", "/B", "/C")
            ),
            Arguments.of(
                "beehive_with_scope.jsonld",
                listOf("/B", "/C"),
                OperationType.REPLACE_ATTRIBUTE,
                listOf("/B", "/C")
            ),
            Arguments.of(
                "beehive_with_scope.jsonld",
                listOf("/B", "/C"),
                OperationType.UPDATE_ATTRIBUTES,
                listOf("/B", "/C")
            ),
            Arguments.of(
                "beehive_minimal.jsonld",
                listOf("/B", "/C"),
                OperationType.UPDATE_ATTRIBUTES,
                null
            )
        )

    @ParameterizedTest
    @MethodSource("generateScopes")
    fun `it shoud update scopes as requested by the type of the operation`(
        initialEntity: String,
        inputScopes: List<String>,
        operationType: OperationType,
        expectedScopes: List<String>?
    ) = runTest {
        loadSampleData(initialEntity)
            .sampleDataToNgsiLdEntity()
            .map { entityService.createEntityPayload(it.second, it.first, ngsiLdDateTime()) }

        val expandedAttributes = JsonLdUtils.expandAttributes(
            """
            { 
                "scope": [${inputScopes.joinToString(",") { "\"$it\"" }}]
            }
            """.trimIndent(),
            APIC_COMPOUND_CONTEXTS
        )

        scopeService.update(
            beehiveTestCId,
            expandedAttributes[JsonLdUtils.NGSILD_SCOPE_PROPERTY]!!,
            ngsiLdDateTime(),
            operationType
        ).shouldSucceed()

        entityQueryService.retrieve(beehiveTestCId)
            .shouldSucceedWith {
                assertEquals(expectedScopes, it.scopes)
                val scopesInEntity = it.payload.toExpandedAttributeInstance().getScopes()
                assertEquals(expectedScopes, scopesInEntity)
            }
    }

    private suspend fun createScopeHistory() {
        loadSampleData("beehive_with_scope.jsonld")
            .sampleDataToNgsiLdEntity()
            .map { entityService.createEntityPayload(it.second, it.first, ngsiLdDateTime()) }
        scopeService.addHistoryEntry(
            beehiveTestCId,
            listOf("/A", "/B/C"),
            TemporalProperty.MODIFIED_AT,
            ngsiLdDateTime()
        ).shouldSucceed()
    }

    @Test
    fun `it should retrieve the history of scopes`() = runTest {
        createScopeHistory()

        val scopeHistoryEntries = scopeService.retrieveHistory(
            listOf(beehiveTestCId),
            TemporalEntitiesQuery(
                EntitiesQuery(
                    paginationQuery = PaginationQuery(limit = 100, offset = 0),
                    contexts = APIC_COMPOUND_CONTEXTS
                ),
                buildDefaultTestTemporalQuery(timeproperty = TemporalProperty.MODIFIED_AT),
                withTemporalValues = false,
                withAudit = false,
                withAggregatedValues = false
            )
        ).shouldSucceedAndResult()

        assertEquals(1, scopeHistoryEntries.size)
        assertThat(scopeHistoryEntries).allMatch {
            it as FullScopeInstanceResult
            it.scopes == listOf("/A", "/B/C") &&
                it.timeproperty == TemporalProperty.MODIFIED_AT.propertyName
        }
    }

    @Test
    fun `it should retrieve the history of scopes with a time interval`() = runTest {
        createScopeHistory()

        val scopeHistoryEntries = scopeService.retrieveHistory(
            listOf(beehiveTestCId),
            TemporalEntitiesQuery(
                EntitiesQuery(
                    paginationQuery = PaginationQuery(limit = 100, offset = 0),
                    contexts = APIC_COMPOUND_CONTEXTS
                ),
                buildDefaultTestTemporalQuery(
                    timeproperty = TemporalProperty.MODIFIED_AT,
                    timerel = TemporalQuery.Timerel.BEFORE,
                    timeAt = ngsiLdDateTime()
                ),
                withTemporalValues = false,
                withAudit = false,
                withAggregatedValues = false
            )
        ).shouldSucceedAndResult()

        assertEquals(1, scopeHistoryEntries.size)
        assertThat(scopeHistoryEntries).allMatch {
            it as FullScopeInstanceResult
            it.scopes == listOf("/A", "/B/C") &&
                it.timeproperty == TemporalProperty.MODIFIED_AT.propertyName
        }
    }

    @Test
    fun `it should retrieve the history of scopes with aggregated values`() = runTest {
        createScopeHistory()

        val scopeHistoryEntries = scopeService.retrieveHistory(
            listOf(beehiveTestCId),
            TemporalEntitiesQuery(
                EntitiesQuery(
                    paginationQuery = PaginationQuery(limit = 100, offset = 0),
                    contexts = APIC_COMPOUND_CONTEXTS
                ),
                buildDefaultTestTemporalQuery(
                    timeproperty = TemporalProperty.MODIFIED_AT,
                    timerel = TemporalQuery.Timerel.BEFORE,
                    timeAt = ngsiLdDateTime(),
                    aggrMethods = listOf(TemporalQuery.Aggregate.SUM),
                    aggrPeriodDuration = "PT1S"
                ),
                withTemporalValues = false,
                withAudit = false,
                withAggregatedValues = true
            ),
            ngsiLdDateTime().minusHours(1)
        ).shouldSucceedAndResult()

        assertEquals(1, scopeHistoryEntries.size)
        assertThat(scopeHistoryEntries).allMatch {
            it as AggregatedScopeInstanceResult
            it.values.size == 1 &&
                it.values[0].aggregate == TemporalQuery.Aggregate.SUM &&
                it.values[0].value as Long == 2L
        }
    }

    @Test
    fun `it should retrieve the history of scopes with aggregated values on whole time range`() = runTest {
        createScopeHistory()

        val scopeHistoryEntries = scopeService.retrieveHistory(
            listOf(beehiveTestCId),
            TemporalEntitiesQuery(
                EntitiesQuery(
                    paginationQuery = PaginationQuery(limit = 100, offset = 0),
                    contexts = APIC_COMPOUND_CONTEXTS
                ),
                buildDefaultTestTemporalQuery(
                    timeproperty = TemporalProperty.MODIFIED_AT,
                    timerel = TemporalQuery.Timerel.BEFORE,
                    timeAt = ngsiLdDateTime(),
                    aggrMethods = listOf(TemporalQuery.Aggregate.SUM),
                    aggrPeriodDuration = "PT0S"
                ),
                withTemporalValues = false,
                withAudit = false,
                withAggregatedValues = true
            ),
            ngsiLdDateTime().minusHours(1)
        ).shouldSucceedAndResult()

        assertEquals(1, scopeHistoryEntries.size)
        assertThat(scopeHistoryEntries).allMatch {
            it as AggregatedScopeInstanceResult
            it.values.size == 1 &&
                it.values[0].aggregate == TemporalQuery.Aggregate.SUM &&
                it.values[0].value as Long == 2L
        }
    }

    @Test
    fun `it should retrieve the history of scopes with aggregated values on whole time range without interval`() =
        runTest {
            createScopeHistory()

            val scopeHistoryEntries = scopeService.retrieveHistory(
                listOf(beehiveTestCId),
                TemporalEntitiesQuery(
                    EntitiesQuery(
                        paginationQuery = PaginationQuery(limit = 100, offset = 0),
                        contexts = APIC_COMPOUND_CONTEXTS
                    ),
                    buildDefaultTestTemporalQuery(
                        timeproperty = TemporalProperty.MODIFIED_AT,
                        aggrMethods = listOf(TemporalQuery.Aggregate.SUM),
                        aggrPeriodDuration = "PT0S"
                    ),
                    withTemporalValues = false,
                    withAudit = false,
                    withAggregatedValues = true
                ),
                ngsiLdDateTime().minusHours(1)
            ).shouldSucceedAndResult()

            assertEquals(1, scopeHistoryEntries.size)
            assertThat(scopeHistoryEntries).allMatch {
                it as AggregatedScopeInstanceResult
                it.values.size == 1 &&
                    it.values[0].aggregate == TemporalQuery.Aggregate.SUM &&
                    it.values[0].value as Long == 2L
            }
        }

    @Test
    fun `it should retrieve the last n instances of history of scopes with aggregated values`() = runTest {
        createScopeHistory()

        val scopeHistoryEntries = scopeService.retrieveHistory(
            listOf(beehiveTestCId),
            TemporalEntitiesQuery(
                EntitiesQuery(
                    paginationQuery = PaginationQuery(limit = 100, offset = 0),
                    contexts = APIC_COMPOUND_CONTEXTS
                ),
                buildDefaultTestTemporalQuery(
                    timeproperty = TemporalProperty.MODIFIED_AT,
                    timerel = TemporalQuery.Timerel.BEFORE,
                    timeAt = ngsiLdDateTime(),
                    aggrMethods = listOf(TemporalQuery.Aggregate.SUM),
                    aggrPeriodDuration = "PT1S",
                    instanceLimit = 1,
                    lastN = 1
                ),
                withTemporalValues = false,
                withAudit = false,
                withAggregatedValues = true
            ),
            ngsiLdDateTime().minusHours(1)
        ).shouldSucceedAndResult()

        assertEquals(1, scopeHistoryEntries.size)
        assertThat(scopeHistoryEntries).allMatch {
            it as AggregatedScopeInstanceResult
            it.values.size == 1 &&
                it.values[0].aggregate == TemporalQuery.Aggregate.SUM
        }
    }

    @Test
    fun `it should include lower bound of interval with after timerel`() = runTest {
        loadSampleData("beehive_with_scope.jsonld")
            .sampleDataToNgsiLdEntity()
            .map { entityService.createEntityPayload(it.second, it.first, ngsiLdDateTime()) }
        scopeService.addHistoryEntry(
            beehiveTestCId,
            listOf("/A", "/B/C"),
            TemporalProperty.MODIFIED_AT,
            ZonedDateTime.parse("2024-08-13T00:00:00Z")
        ).shouldSucceed()
        scopeService.addHistoryEntry(
            beehiveTestCId,
            listOf("/B/C"),
            TemporalProperty.MODIFIED_AT,
            ZonedDateTime.parse("2024-08-14T00:00:00Z")
        ).shouldSucceed()

        scopeService.retrieveHistory(
            listOf(beehiveTestCId),
            TemporalEntitiesQuery(
                EntitiesQuery(
                    paginationQuery = PaginationQuery(limit = 100, offset = 0),
                    contexts = APIC_COMPOUND_CONTEXTS
                ),
                buildDefaultTestTemporalQuery(
                    timeproperty = TemporalProperty.MODIFIED_AT,
                    timerel = TemporalQuery.Timerel.AFTER,
                    timeAt = ZonedDateTime.parse("2024-08-13T00:00:00Z"),
                    instanceLimit = 5
                ),
                withTemporalValues = false,
                withAudit = false,
                withAggregatedValues = false
            ),
            ngsiLdDateTime().minusHours(1)
        ).shouldSucceedWith {
            assertEquals(2, it.size)
        }
    }

    @Test
    fun `it should exclude upper bound of interval with between timerel`() = runTest {
        loadSampleData("beehive_with_scope.jsonld")
            .sampleDataToNgsiLdEntity()
            .map { entityService.createEntityPayload(it.second, it.first, ngsiLdDateTime()) }
        scopeService.addHistoryEntry(
            beehiveTestCId,
            listOf("/A", "/B/C"),
            TemporalProperty.MODIFIED_AT,
            ZonedDateTime.parse("2024-08-13T00:00:00Z")
        ).shouldSucceed()
        scopeService.addHistoryEntry(
            beehiveTestCId,
            listOf("/B/C"),
            TemporalProperty.MODIFIED_AT,
            ZonedDateTime.parse("2024-08-14T00:00:00Z")
        ).shouldSucceed()
        scopeService.addHistoryEntry(
            beehiveTestCId,
            listOf("/C/D"),
            TemporalProperty.MODIFIED_AT,
            ZonedDateTime.parse("2024-08-15T00:00:00Z")
        ).shouldSucceed()

        scopeService.retrieveHistory(
            listOf(beehiveTestCId),
            TemporalEntitiesQuery(
                EntitiesQuery(
                    paginationQuery = PaginationQuery(limit = 100, offset = 0),
                    contexts = APIC_COMPOUND_CONTEXTS
                ),
                buildDefaultTestTemporalQuery(
                    timeproperty = TemporalProperty.MODIFIED_AT,
                    timerel = TemporalQuery.Timerel.BETWEEN,
                    timeAt = ZonedDateTime.parse("2024-08-13T00:00:00Z"),
                    endTimeAt = ZonedDateTime.parse("2024-08-15T00:00:00Z"),
                    instanceLimit = 5
                ),
                withTemporalValues = false,
                withAudit = false,
                withAggregatedValues = false
            ),
            ngsiLdDateTime().minusHours(1)
        ).shouldSucceedWith {
            assertEquals(2, it.size)
            (it as List<FullScopeInstanceResult>).forEach { result ->
                assertNotEquals(ZonedDateTime.parse("2024-08-15T00:00:00Z"), result.time)
            }
        }
    }

    @Test
    fun `it should delete scope and its history`() = runTest {
        loadSampleData("beehive_with_scope.jsonld")
            .sampleDataToNgsiLdEntity()
            .map { entityService.createEntityPayload(it.second, it.first, ngsiLdDateTime()) }

        scopeService.delete(beehiveTestCId).shouldSucceed()

        scopeService.retrieve(beehiveTestCId)
            .shouldSucceedWith {
                assertNull(it.first)
                assertNull(it.second.toExpandedAttributeInstance().getScopes())
            }
        val scopeHistoryEntries = scopeService.retrieveHistory(
            listOf(beehiveTestCId),
            TemporalEntitiesQuery(
                EntitiesQuery(
                    paginationQuery = PaginationQuery(limit = 100, offset = 0),
                    contexts = APIC_COMPOUND_CONTEXTS
                ),
                buildDefaultTestTemporalQuery(),
                withTemporalValues = false,
                withAudit = false,
                withAggregatedValues = false
            )
        ).shouldSucceedAndResult()
        assertTrue(scopeHistoryEntries.isEmpty())
    }
}
