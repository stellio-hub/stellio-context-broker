package com.egm.stellio.search.scope

import com.egm.stellio.search.entity.model.EntitiesQueryFromGet
import com.egm.stellio.search.entity.model.Entity
import com.egm.stellio.search.entity.model.OperationType
import com.egm.stellio.search.entity.service.EntityQueryService
import com.egm.stellio.search.entity.service.EntityService
import com.egm.stellio.search.entity.util.toExpandedAttributeInstance
import com.egm.stellio.search.support.WithKafkaContainer
import com.egm.stellio.search.support.WithTimescaleContainer
import com.egm.stellio.search.support.buildDefaultTestTemporalQuery
import com.egm.stellio.search.temporal.model.AttributeInstance.TemporalProperty
import com.egm.stellio.search.temporal.model.TemporalEntitiesQueryFromGet
import com.egm.stellio.search.temporal.model.TemporalQuery
import com.egm.stellio.search.temporal.util.TemporalRepresentation
import com.egm.stellio.shared.model.NGSILD_SCOPE_IRI
import com.egm.stellio.shared.model.getScopes
import com.egm.stellio.shared.queryparameter.PaginationQuery
import com.egm.stellio.shared.util.APIC_COMPOUND_CONTEXTS
import com.egm.stellio.shared.util.JsonLdUtils
import com.egm.stellio.shared.util.loadSampleData
import com.egm.stellio.shared.util.ngsiLdDateTime
import com.egm.stellio.shared.util.sampleDataToNgsiLdEntity
import com.egm.stellio.shared.util.shouldSucceed
import com.egm.stellio.shared.util.shouldSucceedAndResult
import com.egm.stellio.shared.util.shouldSucceedWith
import com.egm.stellio.shared.util.toUri
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
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
class ScopeServiceTests : WithTimescaleContainer, WithKafkaContainer() {

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
    fun `update should persist scopes according to the requested operation type`(
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
            expandedAttributes[NGSILD_SCOPE_IRI]!!,
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
    fun `retrieveHistory should return the history of scopes`() = runTest {
        createScopeHistory()

        val scopeHistoryEntries = scopeService.retrieveHistory(
            listOf(beehiveTestCId),
            TemporalEntitiesQueryFromGet(
                EntitiesQueryFromGet(
                    paginationQuery = PaginationQuery(limit = 100, offset = 0),
                    contexts = APIC_COMPOUND_CONTEXTS
                ),
                temporalQuery = buildDefaultTestTemporalQuery(timeproperty = TemporalProperty.MODIFIED_AT),
                temporalRepresentation = TemporalRepresentation.NORMALIZED,
                withAudit = false
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
    fun `retrieveHistory should return the history of scopes with a time interval`() = runTest {
        createScopeHistory()

        val scopeHistoryEntries = scopeService.retrieveHistory(
            listOf(beehiveTestCId),
            TemporalEntitiesQueryFromGet(
                EntitiesQueryFromGet(
                    paginationQuery = PaginationQuery(limit = 100, offset = 0),
                    contexts = APIC_COMPOUND_CONTEXTS
                ),
                temporalQuery = buildDefaultTestTemporalQuery(
                    timeproperty = TemporalProperty.MODIFIED_AT,
                    timerel = TemporalQuery.Timerel.BEFORE,
                    timeAt = ngsiLdDateTime()
                ),
                temporalRepresentation = TemporalRepresentation.NORMALIZED,
                withAudit = false
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
    fun `retrieveHistory should return the history of scopes with aggregated values`() = runTest {
        createScopeHistory()

        val scopeHistoryEntries = scopeService.retrieveHistory(
            listOf(beehiveTestCId),
            TemporalEntitiesQueryFromGet(
                EntitiesQueryFromGet(
                    paginationQuery = PaginationQuery(limit = 100, offset = 0),
                    contexts = APIC_COMPOUND_CONTEXTS
                ),
                temporalQuery = buildDefaultTestTemporalQuery(
                    timeproperty = TemporalProperty.MODIFIED_AT,
                    timerel = TemporalQuery.Timerel.BEFORE,
                    timeAt = ngsiLdDateTime(),
                    aggrMethods = listOf(TemporalQuery.Aggregate.SUM),
                    aggrPeriodDuration = "PT1S"
                ),
                temporalRepresentation = TemporalRepresentation.AGGREGATED_VALUES,
                withAudit = false
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
    fun `retrieveHistory should return the history of scopes with aggregated values on whole time range`() = runTest {
        createScopeHistory()

        val scopeHistoryEntries = scopeService.retrieveHistory(
            listOf(beehiveTestCId),
            TemporalEntitiesQueryFromGet(
                EntitiesQueryFromGet(
                    paginationQuery = PaginationQuery(limit = 100, offset = 0),
                    contexts = APIC_COMPOUND_CONTEXTS
                ),
                temporalQuery = buildDefaultTestTemporalQuery(
                    timeproperty = TemporalProperty.MODIFIED_AT,
                    timerel = TemporalQuery.Timerel.BEFORE,
                    timeAt = ngsiLdDateTime(),
                    aggrMethods = listOf(TemporalQuery.Aggregate.SUM),
                    aggrPeriodDuration = "PT0S"
                ),
                temporalRepresentation = TemporalRepresentation.AGGREGATED_VALUES,
                withAudit = false
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
    fun `retrieveHistory should return aggregated scopes over the whole time range without interval`() =
        runTest {
            createScopeHistory()

            val scopeHistoryEntries = scopeService.retrieveHistory(
                listOf(beehiveTestCId),
                TemporalEntitiesQueryFromGet(
                    EntitiesQueryFromGet(
                        paginationQuery = PaginationQuery(limit = 100, offset = 0),
                        contexts = APIC_COMPOUND_CONTEXTS
                    ),
                    temporalQuery = buildDefaultTestTemporalQuery(
                        timeproperty = TemporalProperty.MODIFIED_AT,
                        aggrMethods = listOf(TemporalQuery.Aggregate.SUM),
                        aggrPeriodDuration = "PT0S"
                    ),
                    temporalRepresentation = TemporalRepresentation.AGGREGATED_VALUES,
                    withAudit = false
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
    fun `retrieveHistory should return the last n instances of history of scopes with aggregated values`() = runTest {
        createScopeHistory()

        val scopeHistoryEntries = scopeService.retrieveHistory(
            listOf(beehiveTestCId),
            TemporalEntitiesQueryFromGet(
                EntitiesQueryFromGet(
                    paginationQuery = PaginationQuery(limit = 100, offset = 0),
                    contexts = APIC_COMPOUND_CONTEXTS
                ),
                temporalQuery = buildDefaultTestTemporalQuery(
                    timeproperty = TemporalProperty.MODIFIED_AT,
                    timerel = TemporalQuery.Timerel.BEFORE,
                    timeAt = ngsiLdDateTime(),
                    aggrMethods = listOf(TemporalQuery.Aggregate.SUM),
                    aggrPeriodDuration = "PT1S",
                    instanceLimit = 1,
                    lastN = 1
                ),
                temporalRepresentation = TemporalRepresentation.AGGREGATED_VALUES,
                withAudit = false
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
    fun `retrieveHistory should include the lower bound of the interval with the after timerel`() = runTest {
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
            TemporalEntitiesQueryFromGet(
                EntitiesQueryFromGet(
                    paginationQuery = PaginationQuery(limit = 100, offset = 0),
                    contexts = APIC_COMPOUND_CONTEXTS
                ),
                temporalQuery = buildDefaultTestTemporalQuery(
                    timeproperty = TemporalProperty.MODIFIED_AT,
                    timerel = TemporalQuery.Timerel.AFTER,
                    timeAt = ZonedDateTime.parse("2024-08-13T00:00:00Z"),
                    instanceLimit = 5
                ),
                temporalRepresentation = TemporalRepresentation.NORMALIZED,
                withAudit = false
            ),
            ngsiLdDateTime().minusHours(1)
        ).shouldSucceedWith {
            assertEquals(2, it.size)
        }
    }

    @Test
    fun `retrieveHistory should exclude the upper bound of the interval with the between timerel`() = runTest {
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
            TemporalEntitiesQueryFromGet(
                EntitiesQueryFromGet(
                    paginationQuery = PaginationQuery(limit = 100, offset = 0),
                    contexts = APIC_COMPOUND_CONTEXTS
                ),
                temporalQuery = buildDefaultTestTemporalQuery(
                    timeproperty = TemporalProperty.MODIFIED_AT,
                    timerel = TemporalQuery.Timerel.BETWEEN,
                    timeAt = ZonedDateTime.parse("2024-08-13T00:00:00Z"),
                    endTimeAt = ZonedDateTime.parse("2024-08-15T00:00:00Z"),
                    instanceLimit = 5
                ),
                temporalRepresentation = TemporalRepresentation.NORMALIZED,
                withAudit = false
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
    fun `delete should remove the scope and its history`() = runTest {
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
            TemporalEntitiesQueryFromGet(
                EntitiesQueryFromGet(
                    paginationQuery = PaginationQuery(limit = 100, offset = 0),
                    contexts = APIC_COMPOUND_CONTEXTS
                ),
                temporalQuery = buildDefaultTestTemporalQuery(),
                temporalRepresentation = TemporalRepresentation.NORMALIZED,
                withAudit = false
            )
        ).shouldSucceedAndResult()
        assertTrue(scopeHistoryEntries.isEmpty())
    }
}
