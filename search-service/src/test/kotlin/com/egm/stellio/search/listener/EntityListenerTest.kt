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

    @MockkBean
    private lateinit var temporalEntityAttributeService: TemporalEntityAttributeService

    @MockkBean
    private lateinit var attributeInstanceService: AttributeInstanceService

    @Test
    fun `it should ask to create a temporal entity entry`() {
        val content = """
            {
                "operationType": "CREATE",
                "entityId": "urn:ngsi-ld:FishContainment:1234",
                "entityType": "FishContainment",
                "payload": "{ 
                    \"id\": \"urn:ngsi-ld:FishContainment:1234\",
                    \"type\": \"FishContainment\",
                    \"@context\": \"https://raw.githubusercontent.com/easy-global-market/ngsild-api-data-models/master/aquac/jsonld-contexts/aquac-compound.jsonld\"
                }"
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
    fun `it should ask to create an attribute instance`() {
        val entity = """
            {
                \"id\": \"urn:ngsi-ld:FishContainment:1234\",
                \"type\": \"FishContainment\",
                \"@context\": \"https://raw.githubusercontent.com/easy-global-market/ngsild-api-data-models/master/aquac/jsonld-contexts/aquac-compound.jsonld\"
            }
        """.trimIndent()
        val eventPayload = """
            {
                \"totalDissolvedSolids\":{
                    \"type\":\"Property\",
                    \"observedBy\": {
                        \"type\":\"Relationship\",
                        \"object\":\"urn:ngsi-ld:Sensor:HCMR-AQUABOX1totalDissolvedSolids\"
                    },
                    \"value\":33869,
                    \"observedAt\":\"2020-03-12T08:33:38.000Z\",
                    \"unitCode\":\"G42\"
                }
            }
        """.trimIndent()
        val content = """
            {
                "operationType": "UPDATE",
                "entityId": "urn:ngsi-ld:FishContainment:1234",
                "entityType": "FishContainment",
                "payload": "$eventPayload",
                "updatedEntity": "$entity"
            }
        """.trimIndent().replace("\n", "")

        val temporalEntityAttributeUuid = UUID.randomUUID()
        every { temporalEntityAttributeService.getForEntityAndAttribute(any(), any()) } returns Mono.just(
            temporalEntityAttributeUuid
        )
        every { attributeInstanceService.create(any()) } returns Mono.just(1)
        every { temporalEntityAttributeService.addEntityPayload(any(), any()) } returns Mono.just(1)

        entityListener.processMessage(content)

        verify {
            temporalEntityAttributeService.getForEntityAndAttribute(
                eq("urn:ngsi-ld:FishContainment:1234"),
                eq("https://ontology.eglobalmark.com/aquac#totalDissolvedSolids")
            )
        }
        verify {
            attributeInstanceService.create(match {
                it.value == null &&
                    it.measuredValue == 33869.0 &&
                    it.observedAt == ZonedDateTime.parse("2020-03-12T08:33:38.000Z") &&
                    it.temporalEntityAttribute == temporalEntityAttributeUuid
            })
        }

        verify {
            temporalEntityAttributeService.addEntityPayload(temporalEntityAttributeUuid, match {
                it.contains("urn:ngsi-ld:FishContainment:1234")
            })
        }
        confirmVerified(temporalEntityAttributeService)
    }
}
