package com.egm.stellio.search.service

import com.egm.stellio.search.model.EntityPayload
import com.egm.stellio.search.model.OperationType
import com.egm.stellio.search.support.WithKafkaContainer
import com.egm.stellio.search.support.WithTimescaleContainer
import com.egm.stellio.search.util.deserializeAsMap
import com.egm.stellio.shared.model.getScopes
import com.egm.stellio.shared.util.*
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.test.context.ActiveProfiles
import java.util.stream.Stream

@OptIn(ExperimentalCoroutinesApi::class)
@SpringBootTest
@ActiveProfiles("test")
class ScopeServiceTests : WithTimescaleContainer, WithKafkaContainer {

    @Autowired
    private lateinit var scopeService: ScopeService

    @Autowired
    private lateinit var entityPayloadService: EntityPayloadService

    @Autowired
    private lateinit var r2dbcEntityTemplate: R2dbcEntityTemplate

    private val beehiveTestCId = "urn:ngsi-ld:BeeHive:TESTC".toUri()

    @AfterEach
    fun clearEntityPayloadTable() {
        r2dbcEntityTemplate.delete(EntityPayload::class.java)
            .all()
            .block()
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
            .map { entityPayloadService.createEntityPayload(it.second, ngsiLdDateTime(), it.first) }

        val expandedAttributes = JsonLdUtils.expandAttributes(
            """
            { 
                "scope": [${inputScopes.joinToString(",") { "\"$it\"" } }]
            }
            """.trimIndent(),
            listOf(APIC_COMPOUND_CONTEXT)
        )

        scopeService.update(
            beehiveTestCId,
            expandedAttributes[JsonLdUtils.NGSILD_SCOPE_PROPERTY]!!,
            ngsiLdDateTime(),
            operationType
        ).shouldSucceed()

        entityPayloadService.retrieve(beehiveTestCId)
            .shouldSucceedWith {
                Assertions.assertEquals(expectedScopes, it.scopes)
                val scopesInEntity = (it.payload.deserializeAsMap() as ExpandedAttributeInstance).getScopes()
                Assertions.assertEquals(expectedScopes, scopesInEntity)
            }
    }
}
