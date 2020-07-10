package com.egm.stellio.search.listener

import com.egm.stellio.search.service.AttributeInstanceService
import com.egm.stellio.search.service.TemporalEntityAttributeService
import com.ninjasquad.springmockk.MockkBean
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

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = [EntityListener::class])
@ActiveProfiles("test")
class EntityListenerTest {

    @Autowired
    private lateinit var entityListener: EntityListener

    @MockkBean(relaxed = true)
    private lateinit var temporalEntityAttributeService: TemporalEntityAttributeService

    @MockkBean(relaxed = true)
    private lateinit var attributeInstanceService: AttributeInstanceService

    private val entity = """
        {
            \"id\": \"urn:ngsi-ld:FishContainment:1234\",
            \"type\": \"FishContainment\",
            \"@context\": \"https://raw.githubusercontent.com/easy-global-market/ngsild-api-data-models/master/aquac/jsonld-contexts/aquac-compound.jsonld\"
        }
    """.trimIndent()

    @Test
    fun `it should create a temporal entity entry`() {
        val content = """
            {
                "operationType": "CREATE",
                "entityId": "urn:ngsi-ld:FishContainment:1234",
                "entityType": "FishContainment",
                "payload": "$entity"
            }
        """.trimIndent().replace("\n", "")

        every { temporalEntityAttributeService.createEntityTemporalReferences(any()) } returns Mono.just(1)

        entityListener.processMessage(content)

        verify {
            temporalEntityAttributeService.createEntityTemporalReferences(match {
                it.contains("urn:ngsi-ld:FishContainment:1234")
            })
        }
        confirmVerified(temporalEntityAttributeService)
    }

    @Test
    fun `it should create an attribute instance and update entity payload`() {
        val eventPayload = """
            {
                \"totalDissolvedSolids\":{
                    \"type\":\"Property\",
                    \"value\":33869,
                    \"observedAt\":\"2020-03-12T08:33:38.000Z\"
                }
            }
        """.trimIndent()
        val content = prepareUpdateEventPayload(eventPayload)

        val temporalEntityAttributeUuid = UUID.randomUUID()
        every { temporalEntityAttributeService.getForEntityAndAttribute(any(), any()) } returns Mono.just(
            temporalEntityAttributeUuid
        )

        entityListener.processMessage(content)

        verify {
            temporalEntityAttributeService.getForEntityAndAttribute(
                eq("urn:ngsi-ld:FishContainment:1234"),
                eq("https://ontology.eglobalmark.com/aquac#totalDissolvedSolids")
            )
        }
        verify {
            attributeInstanceService.create(match {
                it.temporalEntityAttribute == temporalEntityAttributeUuid
            })
        }

        verify {
            temporalEntityAttributeService.addEntityPayload(temporalEntityAttributeUuid, match {
                it.contains("urn:ngsi-ld:FishContainment:1234")
            })
        }

        confirmVerified(attributeInstanceService)
        confirmVerified(temporalEntityAttributeService)
    }

    @Test
    fun `it should create an attribute instance with a numeric value`() {
        val eventPayload = """
            {
                \"totalDissolvedSolids\":{
                    \"type\":\"Property\",
                    \"value\":33869,
                    \"observedAt\":\"2020-03-12T08:33:38.000Z\"
                }
            }
        """.trimIndent()
        val content = prepareUpdateEventPayload(eventPayload)

        val temporalEntityAttributeUuid = UUID.randomUUID()
        every { temporalEntityAttributeService.getForEntityAndAttribute(any(), any()) } returns Mono.just(
            temporalEntityAttributeUuid
        )

        entityListener.processMessage(content)

        verify {
            attributeInstanceService.create(match {
                it.value == null &&
                    it.measuredValue == 33869.0 &&
                    it.observedAt == ZonedDateTime.parse("2020-03-12T08:33:38.000Z") &&
                    it.temporalEntityAttribute == temporalEntityAttributeUuid
            })
        }

        confirmVerified(attributeInstanceService)
    }

    @Test
    fun `it should create an attribute instance with a textual value`() {
        val eventPayload = """
            {
                \"totalDissolvedSolids\":{
                    \"type\":\"Property\",
                    \"value\":\"some textual value\",
                    \"observedAt\":\"2020-03-12T08:33:38.000Z\"
                }
            }
        """.trimIndent()
        val content = prepareUpdateEventPayload(eventPayload)

        val temporalEntityAttributeUuid = UUID.randomUUID()
        every { temporalEntityAttributeService.getForEntityAndAttribute(any(), any()) } returns Mono.just(
            temporalEntityAttributeUuid
        )

        entityListener.processMessage(content)

        verify {
            attributeInstanceService.create(match {
                it.value == "some textual value" &&
                        it.measuredValue == null &&
                        it.observedAt == ZonedDateTime.parse("2020-03-12T08:33:38.000Z") &&
                        it.temporalEntityAttribute == temporalEntityAttributeUuid
            })
        }

        confirmVerified(attributeInstanceService)
    }

    private fun prepareUpdateEventPayload(payload: String): String =
        """
            {
                "operationType": "UPDATE",
                "entityId": "urn:ngsi-ld:FishContainment:1234",
                "entityType": "FishContainment",
                "payload": "$payload",
                "updatedEntity": "$entity"
            }
        """.trimIndent().replace("\n", "")
}
