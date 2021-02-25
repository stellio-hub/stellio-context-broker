package com.egm.stellio.search.service

import com.egm.stellio.search.model.AttributeInstanceResult
import com.egm.stellio.search.model.TemporalEntityAttribute
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CORE_CONTEXT
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.util.buildAttributeInstancePayload
import com.egm.stellio.shared.util.loadSampleData
import com.egm.stellio.shared.util.matchContent
import com.egm.stellio.shared.util.toUri
import junit.framework.Assert.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import java.time.ZonedDateTime

@SpringBootTest
@ActiveProfiles("test")
class TemporalEntityServiceTests {

    @Autowired
    private lateinit var temporalEntityService: TemporalEntityService

    val incomingAttrExpandedName = "https://ontology.eglobalmark.com/apic#incoming"

    @Value("\${application.jsonld.apic_context}")
    val apicContext: String? = null

    @Test
    fun `it should return a temporal entity with numeric values in temporalValues format`() {
        val temporalEntityAttribute = TemporalEntityAttribute(
            entityId = "urn:ngsi-ld:BeeHive:TESTC".toUri(),
            type = "https://ontology.eglobalmark.com/apic#BeeHive",
            attributeName = incomingAttrExpandedName,
            attributeValueType = TemporalEntityAttribute.AttributeValueType.MEASURE
        )
        val attributeAndResultsMap = mapOf(
            temporalEntityAttribute to listOf(
                AttributeInstanceResult(
                    attributeName = incomingAttrExpandedName,
                    value = 550.0,
                    observedAt = ZonedDateTime.parse("2020-03-25T08:29:17.965206Z")
                ),
                AttributeInstanceResult(
                    attributeName = incomingAttrExpandedName,
                    value = 650.0,
                    observedAt = ZonedDateTime.parse("2020-03-25T08:33:00Z")
                )
            )
        )

        val temporalEntity = temporalEntityService.buildTemporalEntity(
            "urn:ngsi-ld:BeeHive:TESTC".toUri(),
            attributeAndResultsMap,
            listOf(apicContext!!),
            true
        )
        assertTrue(
            serializeObject(temporalEntity).matchContent(
                loadSampleData("expectations/beehive_with_incoming_temporal_values.jsonld")
            )
        )
    }

    @Test
    fun `it should return a temporal entity with string values in temporalValues format`() {
        val temporalEntityAttribute = TemporalEntityAttribute(
            entityId = "urn:ngsi-ld:Subscription:1234".toUri(),
            type = "https://uri.etsi.org/ngsi-ld/Subscription",
            attributeName = "https://uri.etsi.org/ngsi-ld/notification",
            attributeValueType = TemporalEntityAttribute.AttributeValueType.ANY
        )
        val attributeAndResultsMap = mapOf(
            temporalEntityAttribute to listOf(
                AttributeInstanceResult(
                    attributeName = "https://uri.etsi.org/ngsi-ld/notification",
                    value = "urn:ngsi-ld:Beehive:1234",
                    observedAt = ZonedDateTime.parse("2020-03-25T08:29:17.965206Z")
                ),
                AttributeInstanceResult(
                    attributeName = "https://uri.etsi.org/ngsi-ld/notification",
                    value = "urn:ngsi-ld:Beehive:5678",
                    observedAt = ZonedDateTime.parse("2020-03-25T08:33:17.965206Z")
                )
            )
        )

        val temporalEntity = temporalEntityService.buildTemporalEntity(
            "urn:ngsi-ld:Subscription:1234".toUri(),
            attributeAndResultsMap,
            listOf(NGSILD_CORE_CONTEXT),
            true
        )
        assertTrue(
            serializeObject(temporalEntity).matchContent(
                loadSampleData("expectations/subscription_with_notifications_temporal_values.jsonld")
            )
        )
    }

    @Test
    fun `it should return a temporal entity with string values in default format`() {
        val temporalEntityAttribute = TemporalEntityAttribute(
            entityId = "urn:ngsi-ld:Subscription:1234".toUri(),
            type = "https://uri.etsi.org/ngsi-ld/Subscription",
            attributeName = "https://uri.etsi.org/ngsi-ld/notification",
            attributeValueType = TemporalEntityAttribute.AttributeValueType.ANY
        )
        val attributeAndResultsMap = mapOf(
            temporalEntityAttribute to listOf(
                AttributeInstanceResult(
                    attributeName = "https://uri.etsi.org/ngsi-ld/notification",
                    value = "urn:ngsi-ld:Beehive:1234",
                    observedAt = ZonedDateTime.parse("2020-03-25T08:29:17.965206Z"),
                    payload = buildAttributeInstancePayload(
                        "urn:ngsi-ld:Beehive:1234",
                        ZonedDateTime.parse("2020-03-25T08:29:17.965206Z"),
                        null,
                        "urn:ngsi-ld:Beehive:notification:1234".toUri()
                    )
                ),
                AttributeInstanceResult(
                    attributeName = "https://uri.etsi.org/ngsi-ld/notification",
                    value = "urn:ngsi-ld:Beehive:5678",
                    observedAt = ZonedDateTime.parse("2020-03-25T08:33:17.965206Z"),
                    payload = buildAttributeInstancePayload(
                        "urn:ngsi-ld:Beehive:5678",
                        ZonedDateTime.parse("2020-03-25T08:33:17.965206Z"),
                        null,
                        "urn:ngsi-ld:Beehive:notification:4567".toUri()
                    )
                )
            )
        )

        val temporalEntity = temporalEntityService.buildTemporalEntity(
            "urn:ngsi-ld:Subscription:1234".toUri(),
            attributeAndResultsMap,
            listOf(NGSILD_CORE_CONTEXT),
            false
        )
        assertTrue(
            serializeObject(temporalEntity).matchContent(
                loadSampleData("expectations/subscription_with_notifications.jsonld")
            )
        )
    }

    @Test
    fun `it should return a temporal entity without temporal attributes if it has no temporal history`() {
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
            listOf(NGSILD_CORE_CONTEXT),
            false
        )
        assertTrue(
            serializeObject(temporalEntity).matchContent(
                loadSampleData("expectations/subscription_without_temporal_attributes.jsonld")
            )
        )
    }

    @ParameterizedTest
    @MethodSource("com.egm.stellio.search.util.ParameterizedTests#rawResultsProvider")
    fun `it should return a temporal entity with two instances property with numeric values`(
        attributeAndResultsMap: Map<TemporalEntityAttribute, List<AttributeInstanceResult>>,
        withTemporalValues: Boolean,
        expectation: String
    ) {
        val temporalEntity = temporalEntityService.buildTemporalEntity(
            "urn:ngsi-ld:BeeHive:TESTC".toUri(),
            attributeAndResultsMap,
            listOf(apicContext!!),
            withTemporalValues
        )
        assertTrue(
            serializeObject(temporalEntity).matchContent(expectation)
        )
    }
}
