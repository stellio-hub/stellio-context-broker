package com.egm.stellio.entity.service

import com.egm.stellio.shared.model.ExpandedEntity
import com.egm.stellio.shared.util.NgsiLdParsingUtils
import com.ninjasquad.springmockk.MockkBean
import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.core.io.ClassPathResource
import org.springframework.test.context.ActiveProfiles
import java.net.URI

@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.NONE,
    classes = [EntitiesListener::class]
)
@ActiveProfiles("test")
class EntitiesListenerTests {

    @Autowired
    private lateinit var entitiesListener: EntitiesListener

    @MockkBean(relaxed = true)
    private lateinit var entityService: EntityService

    @MockkBean
    private lateinit var ngsiLdParsingUtils: NgsiLdParsingUtils

    @Test
    fun `it should parse and transmit user creation event`() {
        val userCreateEvent = ClassPathResource("/ngsild/authorization/UserCreateEvent.json")

        entitiesListener.processMessage(userCreateEvent.inputStream.readBytes().toString(Charsets.UTF_8))

        verify {
            entityService.createEntity(match {
                it.id == "urn:ngsi-ld:User:6ad19fe0-fc11-4024-85f2-931c6fa6f7e0"
            })
        }
        confirmVerified()
    }

    @Test
    fun `it should parse and transmit user deletion event`() {
        val userCreateEvent = ClassPathResource("/ngsild/authorization/UserDeleteEvent.json")

        entitiesListener.processMessage(userCreateEvent.inputStream.readBytes().toString(Charsets.UTF_8))

        verify { entityService.deleteEntity("urn:ngsi-ld:User:6ad19fe0-fc11-4024-85f2-931c6fa6f7e0") }
        confirmVerified()
    }

    @Test
    fun `it should parse and transmit Group membership append event`() {
        val groupMembershipAppendEvent = ClassPathResource("/ngsild/authorization/GroupMembershipAppendEvent.json")

        entitiesListener.processMessage(
            groupMembershipAppendEvent.inputStream.readBytes().toString(Charsets.UTF_8)
        )


        verify {
            entityService.appendEntityAttributes(
                "urn:ngsi-ld:User:96e1f1e9-d798-48d7-820e-59f5a9a2abf5",
                match {
                    it.keys.contains("https://ontology.eglobalmark.com/authorization#isMemberOf")
                },
                false
            )
        }
        confirmVerified()
    }

    @Test
    fun `it should parse and transmit group membership deletion event`() {
        val userCreateEvent = ClassPathResource("/ngsild/authorization/GroupMembershipDeleteEvent.json")

        entitiesListener.processMessage(userCreateEvent.inputStream.readBytes().toString(Charsets.UTF_8))

        verify {
            entityService.deleteEntityAttributeInstance(
                "urn:ngsi-ld:User:6ad19fe0-fc11-4024-85f2-931c6fa6f7e0",
                "isMemberOf",
                URI.create("urn:ngsi-ld:Dataset:isMemberOf:7cdad168-96ee-4649-b768-a060ac2ef435"),
                "https://raw.githubusercontent.com/easy-global-market/ngsild-api-data-models/master/authorization/jsonld-contexts/authorization.jsonld"
            )
        }
    }
}
