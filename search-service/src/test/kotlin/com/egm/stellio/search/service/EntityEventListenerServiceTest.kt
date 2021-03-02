package com.egm.stellio.search.service

import com.egm.stellio.search.model.TemporalEntityAttribute
import com.egm.stellio.shared.model.EventsType
import com.egm.stellio.shared.util.JsonLdUtils.EGM_BASE_CONTEXT_URL
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CORE_CONTEXT
import com.egm.stellio.shared.util.JsonUtils
import com.egm.stellio.shared.util.matchContent
import com.egm.stellio.shared.util.toUri
import com.ninjasquad.springmockk.MockkBean
import io.mockk.Called
import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.verify
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import reactor.core.publisher.Mono
import java.time.ZonedDateTime
import java.util.UUID

@SpringBootTest(classes = [EntityEventListenerService::class])
@ActiveProfiles("test")
class EntityEventListenerServiceTest {

    @Autowired
    private lateinit var entityEventListenerService: EntityEventListenerService

    @MockkBean(relaxed = true)
    private lateinit var temporalEntityAttributeService: TemporalEntityAttributeService

    @MockkBean(relaxed = true)
    private lateinit var attributeInstanceService: AttributeInstanceService

    private val fishContainmentId = "urn:ngsi-ld:FishContainment:1234"
    private val observedAt = "2020-03-12T08:33:38.000Z"

    private val entity =
        """
        {
            \"id\": \"$fishContainmentId\",
            \"type\": \"FishContainment\",
            \"@context\": \"$EGM_BASE_CONTEXT_URL/aquac/jsonld-contexts/aquac-compound.jsonld\"
        }
        """.trimIndent()

    private val updatedEntityTextualValue =
        """
        {
            \"id\": \"$fishContainmentId\",
            \"type\": \"FishContainment\",
            \"totalDissolvedSolids\": {
                \"type\":\"Property\",
                \"value\":\"some textual value\",
                \"observedAt\":\"$observedAt\",
                \"datasetId\": \"urn:ngsi-ld:Dataset:01234\",
                \"observedBy\": {
                     \"type\": \"Relationship\",
                     \"object\": \"urn:ngsi-ld:Sensor:IncomingSensor\"
                }
            },
            \"@context\": \"$EGM_BASE_CONTEXT_URL/aquac/jsonld-contexts/aquac-compound.jsonld\"
        }
        """.trimIndent()

    private val updatedEntityTextualValueDefaultInstance =
        """
        {
            \"id\": \"$fishContainmentId\",
            \"type\": \"FishContainment\",
            \"totalDissolvedSolids\": {
                \"type\":\"Property\",
                \"value\":\"some textual value\",
                \"observedAt\":\"$observedAt\"
            },
            \"@context\": \"$EGM_BASE_CONTEXT_URL/aquac/jsonld-contexts/aquac-compound.jsonld\"
        }
        """.trimIndent()

    private val updatedEntityNumericValue =
        """
        {
            \"id\": \"$fishContainmentId\",
            \"type\": \"FishContainment\",
            \"totalDissolvedSolids\": {
                \"type\":\"Property\",
                \"value\":33869,
                \"observedAt\":\"$observedAt\"
                
            },
            \"@context\": \"$EGM_BASE_CONTEXT_URL/aquac/jsonld-contexts/aquac-compound.jsonld\"
        }
        """.trimIndent()

    @Test
    fun `it should create a temporal entity entry for entityCreate events`() {
        val content =
            """
            {
                "operationType": "ENTITY_CREATE",
                "entityId": "$fishContainmentId",
                "operationPayload": "$entity",
                "contexts": ["$NGSILD_CORE_CONTEXT"]
            }
            """.trimIndent().replace("\n", "")

        every { temporalEntityAttributeService.createEntityTemporalReferences(any(), any()) } returns Mono.just(1)

        entityEventListenerService.processMessage(content)

        verify {
            temporalEntityAttributeService.createEntityTemporalReferences(
                match {
                    it.contains(fishContainmentId)
                },
                listOf(NGSILD_CORE_CONTEXT)
            )
        }
        confirmVerified(temporalEntityAttributeService)
    }

    @Test
    fun `it should create a temporal entry with one attribute instance for attributeAppend events`() {
        val eventPayload =
            """
            {
                \"totalDissolvedSolids\":{
                    \"type\":\"Property\",
                    \"value\":33869,
                    \"observedAt\":\"$observedAt\"
                }
            }
            """.trimIndent()
        val expectedAttributeInstance =
            """
               {
                    "type":"Property",
                    "value":33869,
                    "observedAt":"$observedAt"
               }
            """.trimIndent()
        val content = prepareAttributeEventPayload(EventsType.ATTRIBUTE_APPEND, eventPayload, updatedEntityNumericValue)

        every { temporalEntityAttributeService.create(any()) } returns Mono.just(1)
        every { attributeInstanceService.create(any()) } returns Mono.just(1)
        every { temporalEntityAttributeService.updateEntityPayload(any(), any()) } returns Mono.just(1)

        entityEventListenerService.processMessage(content)

        verify {
            temporalEntityAttributeService.create(
                match {
                    it.entityId == fishContainmentId.toUri() &&
                        it.type == "https://uri.etsi.org/ngsi-ld/default-context/FishContainment" &&
                        it.attributeName == "https://uri.etsi.org/ngsi-ld/default-context/totalDissolvedSolids" &&
                        it.attributeValueType == TemporalEntityAttribute.AttributeValueType.MEASURE &&
                        it.datasetId == null
                }
            )
        }

        verify {
            attributeInstanceService.create(
                match {
                    val payload = JsonUtils.serializeObject(
                        JsonUtils.deserializeObject(it.payload).filterKeys { it != "instanceId" }
                    )
                    it.observedAt == ZonedDateTime.parse("2020-03-12T08:33:38Z") &&
                        it.value == null &&
                        it.measuredValue == 33869.0 &&
                        payload.matchContent(expectedAttributeInstance)
                }
            )
        }

        verify {
            temporalEntityAttributeService.updateEntityPayload(
                eq(fishContainmentId.toUri()),
                match { it.contains(fishContainmentId) }
            )
        }

        confirmVerified(attributeInstanceService, temporalEntityAttributeService)
    }

    @Test
    fun `it should create a temporal entry with attribute instance with a textual value for attributeAppend events`() {
        val eventPayload =
            """
            {
                \"totalDissolvedSolids\":{
                    \"type\":\"Property\",
                    \"value\":\"some textual value\",
                    \"datasetId\":\"urn:ngsi-ld:Dataset:totalDissolvedSolids:01\",
                    \"observedAt\":\"$observedAt\"
                }
            }
            """.trimIndent()
        val content = prepareAttributeEventPayload(EventsType.ATTRIBUTE_APPEND, eventPayload)

        every { temporalEntityAttributeService.create(any()) } returns Mono.just(1)
        every { attributeInstanceService.create(any()) } returns Mono.just(1)
        every { temporalEntityAttributeService.updateEntityPayload(any(), any()) } returns Mono.just(1)

        entityEventListenerService.processMessage(content)

        verify {
            temporalEntityAttributeService.create(
                match {
                    it.entityId == fishContainmentId.toUri() &&
                        it.type == "https://uri.etsi.org/ngsi-ld/default-context/FishContainment" &&
                        it.attributeName == "https://uri.etsi.org/ngsi-ld/default-context/totalDissolvedSolids" &&
                        it.attributeValueType == TemporalEntityAttribute.AttributeValueType.ANY &&
                        it.datasetId == "urn:ngsi-ld:Dataset:totalDissolvedSolids:01".toUri()
                }
            )
        }

        verify {
            attributeInstanceService.create(
                match {
                    it.observedAt == ZonedDateTime.parse("2020-03-12T08:33:38Z") &&
                        it.value == "some textual value" &&
                        it.measuredValue == null
                }
            )
        }

        verify {
            temporalEntityAttributeService.updateEntityPayload(
                eq(fishContainmentId.toUri()),
                match { it.contains(fishContainmentId) }
            )
        }

        confirmVerified(attributeInstanceService, temporalEntityAttributeService)
    }

    @Test
    fun `it should create an attribute instance with a numeric value for attributeReplace events`() {
        val eventPayload =
            """
            {
                \"totalDissolvedSolids\":{
                    \"type\":\"Property\",
                    \"value\":33869,
                    \"observedAt\":\"$observedAt\"
                }
            }
            """.trimIndent()
        val content = prepareAttributeEventPayload(EventsType.ATTRIBUTE_REPLACE, eventPayload)
        val temporalEntityAttributeUuid = UUID.randomUUID()

        every { temporalEntityAttributeService.getForEntityAndAttribute(any(), any()) } returns Mono.just(
            temporalEntityAttributeUuid
        )

        entityEventListenerService.processMessage(content)

        verify {
            attributeInstanceService.create(
                match {
                    it.value == null &&
                        it.measuredValue == 33869.0 &&
                        it.observedAt == ZonedDateTime.parse(observedAt) &&
                        it.temporalEntityAttribute == temporalEntityAttributeUuid
                }
            )
        }

        verifyAndConfirmMockForMeasuredValue(temporalEntityAttributeUuid)
    }

    @Test
    fun `it should create an attribute instance with a textual value for attributeReplace events`() {
        val eventPayload =
            """
            {
                \"totalDissolvedSolids\":{
                    \"type\":\"Property\",
                    \"value\":\"some textual value\",
                    \"observedAt\":\"$observedAt\"
                }
            }
            """.trimIndent()
        val expectedAttributeInstance =
            """
               {
                    "type":"Property",
                    "value":"some textual value",
                    "observedAt":"$observedAt"
               }
            """.trimIndent()
        val content = prepareAttributeEventPayload(
            EventsType.ATTRIBUTE_REPLACE,
            eventPayload,
            updatedEntityTextualValueDefaultInstance
        )
        val temporalEntityAttributeUuid = UUID.randomUUID()

        every { temporalEntityAttributeService.getForEntityAndAttribute(any(), any()) } returns Mono.just(
            temporalEntityAttributeUuid
        )

        entityEventListenerService.processMessage(content)

        verifyAndConfirmMockForValue(temporalEntityAttributeUuid, null, expectedAttributeInstance)
    }

    @Test
    fun `it should create an attribute instance with a datasetId for attributeReplace events`() {
        val datasetId = "urn:ngsi-ld:Dataset:01234"
        val eventPayload =
            """
            {
                \"totalDissolvedSolids\":{
                    \"type\":\"Property\",
                    \"value\":\"some textual value\",
                    \"observedAt\":\"$observedAt\",
                    \"datasetId\": \"$datasetId\",
                    \"observedBy\": {
                      \"type\": \"Relationship\",
                      \"object\": \"urn:ngsi-ld:Sensor:IncomingSensor\"
                    }
                }
            }
            """.trimIndent()
        val expectedAttributeInstance =
            """
               {
                    "type":"Property",
                    "value":"some textual value",
                    "observedAt":"$observedAt",
                    "datasetId": "$datasetId",
                    "observedBy": {
                      "type": "Relationship",
                      "object": "urn:ngsi-ld:Sensor:IncomingSensor"
                    }
               }
            """.trimIndent()
        val content = prepareAttributeEventPayload(EventsType.ATTRIBUTE_REPLACE, eventPayload)
        val temporalEntityAttributeUuid = UUID.randomUUID()

        every { temporalEntityAttributeService.getForEntityAndAttribute(any(), any(), any()) } returns Mono.just(
            temporalEntityAttributeUuid
        )

        entityEventListenerService.processMessage(content)

        verifyAndConfirmMockForValue(temporalEntityAttributeUuid, datasetId, expectedAttributeInstance)
    }

    @Test
    fun `it should ignore an attribute instance without observedAt information for attributeReplace events`() {
        val eventPayload =
            """
            {
                \"totalDissolvedSolids\":{
                    \"type\":\"Property\",
                    \"value\":33869
                }
            }
            """.trimIndent()
        val content = prepareAttributeEventPayload(EventsType.ATTRIBUTE_REPLACE, eventPayload)

        entityEventListenerService.processMessage(content)

        verify {
            temporalEntityAttributeService.getForEntityAndAttribute(any(), any()) wasNot Called
        }
    }

    @Test
    fun `it should handle attributeReplace events with an operation payload containing expanded attribute`() {
        val eventPayload =
            """
            {
                \"https://uri.etsi.org/ngsi-ld/default-context/totalDissolvedSolids\":{
                    \"type\":\"Property\",
                    \"value\":33869,
                    \"observedAt\":\"$observedAt\"
                }
            }
            """.trimIndent()
        val content = prepareAttributeEventPayload(EventsType.ATTRIBUTE_REPLACE, eventPayload)
        val temporalEntityAttributeUuid = UUID.randomUUID()

        every { temporalEntityAttributeService.getForEntityAndAttribute(any(), any()) } returns Mono.just(
            temporalEntityAttributeUuid
        )

        entityEventListenerService.processMessage(content)

        verify {
            attributeInstanceService.create(
                match {
                    it.value == null &&
                        it.measuredValue == 33869.0 &&
                        it.observedAt == ZonedDateTime.parse(observedAt) &&
                        it.temporalEntityAttribute == temporalEntityAttributeUuid
                }
            )
        }

        verifyAndConfirmMockForMeasuredValue(temporalEntityAttributeUuid)
    }

    @Test
    fun `it should create an attribute instance and update entity payload for attributeUpdate events`() {
        val eventPayload =
            """
            {
                \"type\":\"Property\",
                \"value\":33869,
                \"observedAt\":\"$observedAt\"
            }
            """.trimIndent()
        val content = prepareAttributeEventPayload(EventsType.ATTRIBUTE_UPDATE, eventPayload)
        val temporalEntityAttributeUuid = UUID.randomUUID()

        every { temporalEntityAttributeService.getForEntityAndAttribute(any(), any()) } returns Mono.just(
            temporalEntityAttributeUuid
        )

        entityEventListenerService.processMessage(content)

        verifyAndConfirmMockForMeasuredValue(temporalEntityAttributeUuid)
    }

    @Test
    fun `it should create an attribute instance with a datasetId for attributeUpdate events`() {
        val datasetId = "urn:ngsi-ld:Dataset:01234"
        val eventPayload =
            """
            {
                \"type\":\"Property\",
                \"value\":\"some textual value\",
                \"observedAt\":\"$observedAt\",
                \"datasetId\": \"$datasetId\"
            }
            """.trimIndent()
        val expectedAttributeInstance =
            """
               {
                    "type":"Property",
                    "value":"some textual value",
                    "observedAt":"$observedAt",
                    "datasetId": "$datasetId",
                    "observedBy": {
                      "type": "Relationship",
                      "object": "urn:ngsi-ld:Sensor:IncomingSensor"
                    }
               }
            """.trimIndent()
        val content = prepareAttributeEventPayload(EventsType.ATTRIBUTE_UPDATE, eventPayload)
        val temporalEntityAttributeUuid = UUID.randomUUID()

        every { temporalEntityAttributeService.getForEntityAndAttribute(any(), any(), any()) } returns Mono.just(
            temporalEntityAttributeUuid
        )

        entityEventListenerService.processMessage(content)

        verifyAndConfirmMockForValue(temporalEntityAttributeUuid, datasetId, expectedAttributeInstance)
    }

    @Test
    fun `it should ignore an attribute instance without observedAt information for attributeUpdate events`() {
        val eventPayload =
            """
            {
                \"type\":\"Property\",
                \"value\":33869
            }
            """.trimIndent()
        val content = prepareAttributeEventPayload(EventsType.ATTRIBUTE_UPDATE, eventPayload)

        entityEventListenerService.processMessage(content)

        verify {
            temporalEntityAttributeService.getForEntityAndAttribute(any(), any()) wasNot Called
        }
    }

    private fun verifyAndConfirmMockForMeasuredValue(temporalEntityAttributeUuid: UUID) {
        verify {
            temporalEntityAttributeService.getForEntityAndAttribute(
                eq(fishContainmentId.toUri()),
                eq("https://uri.etsi.org/ngsi-ld/default-context/totalDissolvedSolids"),
                isNull()
            )
        }
        verify {
            attributeInstanceService.create(
                match {
                    it.temporalEntityAttribute == temporalEntityAttributeUuid
                }
            )
        }

        verify {
            temporalEntityAttributeService.updateEntityPayload(
                fishContainmentId.toUri(),
                match {
                    it.contains(fishContainmentId)
                }
            )
        }

        confirmVerified(attributeInstanceService, temporalEntityAttributeService)
    }

    private fun verifyAndConfirmMockForValue(
        temporalEntityAttributeUuid: UUID,
        datasetId: String? = null,
        expectedAttributeInstance: String
    ) {
        verify {
            temporalEntityAttributeService.getForEntityAndAttribute(any(), any(), datasetId?.toUri())
        }

        verify {
            attributeInstanceService.create(
                match {
                    val payload = JsonUtils.serializeObject(
                        JsonUtils.deserializeObject(it.payload).filterKeys { it != "instanceId" }
                    )
                    it.value == "some textual value" &&
                        it.measuredValue == null &&
                        it.observedAt == ZonedDateTime.parse(observedAt) &&
                        it.temporalEntityAttribute == temporalEntityAttributeUuid &&
                        payload.matchContent(expectedAttributeInstance)
                }
            )
        }
    }

    private fun prepareAttributeEventPayload(
        operationType: EventsType,
        payload: String,
        updatedEntity: String = updatedEntityTextualValue
    ): String =
        """
            {
                "operationType": "$operationType",
                "entityId": "$fishContainmentId",
                "attributeName": "totalDissolvedSolids",
                "operationPayload": "$payload",
                "updatedEntity": "$updatedEntity",
                "contexts": ["$NGSILD_CORE_CONTEXT"]
            }
        """.trimIndent().replace("\n", "")
}
