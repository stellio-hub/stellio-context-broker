package com.egm.stellio.search.service

import com.egm.stellio.search.model.*
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CORE_CONTEXT
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.util.assertJsonPayloadsAreEqual
import com.egm.stellio.shared.util.loadSampleData
import com.egm.stellio.shared.util.matchContent
import com.egm.stellio.shared.util.toUri
import com.ninjasquad.springmockk.MockkBean
import io.mockk.every
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import reactor.core.publisher.Mono
import java.net.URI
import java.time.Instant
import java.time.ZoneOffset
import java.time.ZonedDateTime

@SpringBootTest
@ActiveProfiles("test")
class TemporalEntityServiceTests {

    @Autowired
    private lateinit var temporalEntityService: TemporalEntityService

    @MockkBean
    private lateinit var temporalEntityAttributeService: TemporalEntityAttributeService

    @MockkBean
    private lateinit var attributeInstanceService: AttributeInstanceService

    @Value("\${application.jsonld.apic_context}")
    val apicContext: String? = null

    @Test
    fun `it should return a temporal entity with an empty array of instances if it has no temporal history`() {
        val temporalEntityAttribute = TemporalEntityAttribute(
            entityId = "urn:ngsi-ld:Subscription:1234".toUri(),
            type = "https://uri.etsi.org/ngsi-ld/Subscription",
            attributeName = "https://uri.etsi.org/ngsi-ld/notification",
            attributeValueType = TemporalEntityAttribute.AttributeValueType.ANY
        )
        val attributeAndResultsMap = mapOf(
            temporalEntityAttribute to emptyList<AttributeInstanceResult>()
        )

        val temporalEntity = temporalEntityService.buildTemporalEntity(
            "urn:ngsi-ld:Subscription:1234".toUri(),
            attributeAndResultsMap,
            TemporalQuery(),
            listOf(NGSILD_CORE_CONTEXT),
            false
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
        expectation: String
    ) {
        val temporalEntity = temporalEntityService.buildTemporalEntity(
            "urn:ngsi-ld:BeeHive:TESTC".toUri(),
            attributeAndResultsMap,
            TemporalQuery(),
            listOf(apicContext!!),
            withTemporalValues
        )
        assertJsonPayloadsAreEqual(expectation, serializeObject(temporalEntity))
    }

    @Test
    fun `it should return a temporal entity with values aggregated`() {
        val temporalEntityAttribute = TemporalEntityAttribute(
            entityId = "urn:ngsi-ld:Subscription:1234".toUri(),
            type = "https://uri.etsi.org/ngsi-ld/Subscription",
            attributeName = "https://uri.etsi.org/ngsi-ld/notification",
            attributeValueType = TemporalEntityAttribute.AttributeValueType.ANY
        )
        val attributeAndResultsMap = mapOf(
            temporalEntityAttribute to listOf(
                SimplifiedAttributeInstanceResult(
                    value = "urn:ngsi-ld:Beehive:1234",
                    observedAt = ZonedDateTime.parse("2020-03-25T08:29:17.965206Z")
                ),
                SimplifiedAttributeInstanceResult(
                    value = "urn:ngsi-ld:Beehive:5678",
                    observedAt = ZonedDateTime.parse("2020-03-25T08:33:17.965206Z")
                )
            )
        )
        val temporalQuery = TemporalQuery(
            emptySet(), TemporalQuery.Timerel.AFTER, Instant.now().atZone(ZoneOffset.UTC).minusHours(1),
            null, "1 day", TemporalQuery.Aggregate.SUM
        )

        val temporalEntity = temporalEntityService.buildTemporalEntity(
            "urn:ngsi-ld:Subscription:1234".toUri(),
            attributeAndResultsMap,
            temporalQuery,
            listOf(NGSILD_CORE_CONTEXT),
            false
        )

        assertJsonPayloadsAreEqual(
            loadSampleData("expectations/subscription_with_notifications_aggregated.jsonld"),
            serializeObject(temporalEntity)
        )
    }

    @ParameterizedTest
    @MethodSource("com.egm.stellio.search.util.QueryParameterizedTests#rawResultsProvider")
    fun `it should correctly build temporal entities`(
        queryResult: List<Pair<URI, TemporalEntityAttributeInstancesResult>>,
        withTemporalValues: Boolean,
        expectation: String
    ) {
        val temporalEntity = temporalEntityService.buildTemporalEntities(
            queryResult,
            TemporalQuery(),
            listOf(apicContext!!),
            withTemporalValues
        )
        assertJsonPayloadsAreEqual(expectation, serializeObject(temporalEntity))
    }

    @Test
    fun `it should delete temporal entity references`() {
        every { temporalEntityAttributeService.deleteEntityPayload(any()) } returns Mono.just(1)
        every { attributeInstanceService.deleteAttributeInstancesOfEntity(any()) } returns Mono.just(2)
        every { temporalEntityAttributeService.deleteTemporalAttributesOfEntity(any()) } returns Mono.just(2)

        val deletedRecords = temporalEntityService.deleteTemporalEntityReferences("urn:ngsi-ld:BeeHive:TESTD".toUri())
            .block()

        assert(deletedRecords == 5)
    }
}
