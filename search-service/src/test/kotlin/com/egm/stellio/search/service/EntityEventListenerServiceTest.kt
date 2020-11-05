package com.egm.stellio.search.service

import com.egm.stellio.shared.util.JsonLdUtils.EGM_BASE_CONTEXT_URL
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CORE_CONTEXT
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

    @Test
    fun `it should create a temporal entity entry`() {
        val content =
            """
            {
                "operationType": "ENTITY_CREATE",
                "entityId": "$fishContainmentId",
                "operationPayload": "$entity"
            }
            """.trimIndent().replace("\n", "")

        every { temporalEntityAttributeService.createEntityTemporalReferences(any()) } returns Mono.just(1)

        entityEventListenerService.processMessage(content)

        verify {
            temporalEntityAttributeService.createEntityTemporalReferences(
                match {
                    it.contains(fishContainmentId)
                }
            )
        }
        confirmVerified(temporalEntityAttributeService)
    }

    @Test
    fun `it should create an attribute instance and update entity payload`() {
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
        val content = prepareUpdateEventPayload(eventPayload)

        val temporalEntityAttributeUuid = UUID.randomUUID()
        every { temporalEntityAttributeService.getForEntityAndAttribute(any(), any()) } returns Mono.just(
            temporalEntityAttributeUuid
        )

        entityEventListenerService.processMessage(content)

        verify {
            temporalEntityAttributeService.getForEntityAndAttribute(
                eq(fishContainmentId.toUri()),
                eq("https://ontology.eglobalmark.com/aquac#totalDissolvedSolids"),
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

    @Test
    fun `it should create an attribute instance with a numeric value`() {
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
        val content = prepareUpdateEventPayload(eventPayload)

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

        confirmVerified(attributeInstanceService)
    }

    @Test
    fun `it should create an attribute instance with a textual value`() {
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
        val content = prepareUpdateEventPayload(eventPayload)

        val temporalEntityAttributeUuid = UUID.randomUUID()
        every { temporalEntityAttributeService.getForEntityAndAttribute(any(), any()) } returns Mono.just(
            temporalEntityAttributeUuid
        )

        entityEventListenerService.processMessage(content)

        verify {
            attributeInstanceService.create(
                match {
                    it.value == "some textual value" &&
                        it.measuredValue == null &&
                        it.observedAt == ZonedDateTime.parse(observedAt) &&
                        it.temporalEntityAttribute == temporalEntityAttributeUuid
                }
            )
        }

        confirmVerified(attributeInstanceService)
    }

    @Test
    fun `it should create an attribute instance for a datasetId`() {
        val eventPayload =
            """
            {
                \"totalDissolvedSolids\":{
                    \"type\":\"Property\",
                    \"value\":\"some textual value\",
                    \"observedAt\":\"$observedAt\",
                    \"datasetId\": \"urn:ngsi-ld:Dataset:01234\"
                }
            }
            """.trimIndent()
        val content = prepareUpdateEventPayload(eventPayload)

        val temporalEntityAttributeUuid = UUID.randomUUID()
        every { temporalEntityAttributeService.getForEntityAndAttribute(any(), any(), any()) } returns Mono.just(
            temporalEntityAttributeUuid
        )

        entityEventListenerService.processMessage(content)

        verify {
            temporalEntityAttributeService.getForEntityAndAttribute(any(), any(), "urn:ngsi-ld:Dataset:01234")
        }

        verify {
            attributeInstanceService.create(
                match {
                    it.value == "some textual value" &&
                        it.measuredValue == null &&
                        it.observedAt == ZonedDateTime.parse(observedAt) &&
                        it.temporalEntityAttribute == temporalEntityAttributeUuid
                }
            )
        }
    }

    @Test
    fun `it should ignore an attribute instance without observedAt information`() {
        val eventPayload =
            """
            {
                \"totalDissolvedSolids\":{
                    \"type\":\"Property\",
                    \"value\":33869
                }
            }
            """.trimIndent()
        val content = prepareUpdateEventPayload(eventPayload)

        entityEventListenerService.processMessage(content)

        verify {
            temporalEntityAttributeService.getForEntityAndAttribute(any(), any()) wasNot Called
        }
    }

    private fun prepareUpdateEventPayload(payload: String): String =
        """
            {
                "operationType": "ATTRIBUTE_REPLACE",
                "entityId": "$fishContainmentId",
                "attributeName": "totalDissolvedSolids",
                "operationPayload": "$payload",
                "updatedEntity": "$entity",
                "contexts": ["$NGSILD_CORE_CONTEXT"]
            }
        """.trimIndent().replace("\n", "")
}
