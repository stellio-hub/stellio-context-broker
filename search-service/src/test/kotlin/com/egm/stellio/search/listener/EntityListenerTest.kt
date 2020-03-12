package com.egm.stellio.search.listener

import com.egm.stellio.search.service.EntityService
import com.egm.stellio.search.service.ObservationService
import com.ninjasquad.springmockk.MockkBean
import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.verify
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import reactor.core.publisher.Mono
import java.time.OffsetDateTime

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = [ EntityListener::class ])
@ActiveProfiles("test")
class EntityListenerTest {

    @Autowired
    private lateinit var entityListener: EntityListener

    @MockkBean
    private lateinit var entityService: EntityService

    @MockkBean
    private lateinit var observationService: ObservationService

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

        every { entityService.createEntityTemporalReferences(any()) } returns Mono.just(1)

        entityListener.processMessage(content)

        verify { entityService.createEntityTemporalReferences(match {
            it.first.containsKey("@id") &&
                it.first["@id"] == "urn:ngsi-ld:FishContainment:1234"
                it.first.containsKey("@type") &&
                it.first["@type"] is List<*> &&
                (it.first["@type"] as List<String>)[0] == "https://ontology.eglobalmark.com/aquac#FishContainment" &&
                it.second.size == 1 &&
                it.second[0] == "https://raw.githubusercontent.com/easy-global-market/ngsild-api-data-models/master/aquac/jsonld-contexts/aquac-compound.jsonld"
        }) }
        confirmVerified(entityService)
    }

    @Test
    fun `it should ask to create an observation`() {
        val content = """
            {
                "operationType": "UPDATE",
                "entityId": "urn:ngsi-ld:FishContainment:1234",
                "entityType": "FishContainment",
                "payload": "{
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
                }"
            }
        """.trimIndent().replace("\n", "")

        every { observationService.create(any()) } returns Mono.just(1)

        entityListener.processMessage(content)

        verify { observationService.create(match {
            it.attributeName == "totalDissolvedSolids" &&
                it.observedBy == "urn:ngsi-ld:Sensor:HCMR-AQUABOX1totalDissolvedSolids" &&
                it.unitCode == "G42" &&
                it.value == 33869.0 &&
                it.observedAt == OffsetDateTime.parse("2020-03-12T08:33:38.000Z")
        }) }
        confirmVerified(entityService)
    }
}