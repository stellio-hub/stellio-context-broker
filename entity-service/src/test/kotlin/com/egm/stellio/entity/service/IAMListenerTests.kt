package com.egm.stellio.entity.service

import com.egm.stellio.shared.model.NgsiLdProperty
import com.egm.stellio.shared.model.NgsiLdRelationship
import com.egm.stellio.shared.util.loadSampleData
import com.ninjasquad.springmockk.MockkBean
import io.mockk.confirmVerified
import io.mockk.verify
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import java.net.URI

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = [IAMListener::class])
@ActiveProfiles("test")
class IAMListenerTests {

    @Autowired
    private lateinit var iamListener: IAMListener

    @MockkBean(relaxed = true)
    private lateinit var entityService: EntityService

    @Test
    fun `it should parse and transmit user creation event`() {
        val userCreateEvent = loadSampleData("authorization/UserCreateEvent.json")

        iamListener.processMessage(userCreateEvent)

        verify {
            entityService.createEntity(
                match {
                    it.id == "urn:ngsi-ld:User:6ad19fe0-fc11-4024-85f2-931c6fa6f7e0" &&
                        it.properties.size == 1 &&
                        it.properties[0].compactName == "username" &&
                        it.properties[0].instances.size == 1 &&
                        it.properties[0].instances[0].value == "stellio"
                }
            )
        }
        confirmVerified()
    }

    @Test
    fun `it should parse and transmit user deletion event`() {
        val userDeleteEvent = loadSampleData("authorization/UserDeleteEvent.json")

        iamListener.processMessage(userDeleteEvent)

        verify { entityService.deleteEntity("urn:ngsi-ld:User:6ad19fe0-fc11-4024-85f2-931c6fa6f7e0") }
        confirmVerified()
    }

    @Test
    fun `it should parse and transmit group creation event`() {
        val groupCreateEvent = loadSampleData("authorization/GroupCreateEvent.json")

        iamListener.processMessage(groupCreateEvent)

        verify {
            entityService.createEntity(
                match {
                    it.id == "urn:ngsi-ld:Group:ab67edf3-238c-4f50-83f4-617c620c62eb"
                }
            )
        }
        confirmVerified()
    }

    @Test
    fun `it should parse and transmit group update event`() {
        val groupUpdateEvent = loadSampleData("authorization/GroupUpdateEvent.json")

        iamListener.processMessage(groupUpdateEvent)

        verify {
            entityService.updateEntityAttributes(
                "urn:ngsi-ld:Group:ab67edf3-238c-4f50-83f4-617c620c62eb",
                match {
                    it.size == 1 &&
                        it[0].compactName == "name" &&
                        it[0] is NgsiLdProperty &&
                        (it[0] as NgsiLdProperty).instances.size == 1 &&
                        (it[0] as NgsiLdProperty).instances[0].value == "EGM Team"
                }
            )
        }
        confirmVerified()
    }

    @Test
    fun `it should parse and transmit group deletion event`() {
        val groupDeleteEvent = loadSampleData("authorization/GroupDeleteEvent.json")

        iamListener.processMessage(groupDeleteEvent)

        verify { entityService.deleteEntity("urn:ngsi-ld:Group:a11c00f9-43bc-47a8-9d23-13d67696bdb8") }
        confirmVerified()
    }

    @Test
    fun `it should parse and transmit client creation event`() {
        val clientCreateEvent = loadSampleData("authorization/ClientCreateEvent.json")

        iamListener.processMessage(clientCreateEvent)

        verify {
            entityService.createEntity(
                match {
                    it.id == "urn:ngsi-ld:Client:191a6f0d-df07-4697-afde-da9d8a91d954" &&
                        it.properties.size == 1 &&
                        it.properties[0].compactName == "clientId" &&
                        it.properties[0].instances.size == 1 &&
                        it.properties[0].instances[0].value == "stellio-client"
                }
            )
        }
        confirmVerified()
    }

    @Test
    fun `it should parse and transmit client deletion event`() {
        val clientDeleteEvent = loadSampleData("authorization/ClientDeleteEvent.json")

        iamListener.processMessage(clientDeleteEvent)

        verify { entityService.deleteEntity("urn:ngsi-ld:Client:6ad19fe0-fc11-4024-85f2-931c6fa6f7e0") }
        confirmVerified()
    }

    @Test
    fun `it should parse and transmit group membership append event`() {
        val groupMembershipAppendEvent = loadSampleData("authorization/GroupMembershipAppendEvent.json")

        iamListener.processMessage(groupMembershipAppendEvent)

        verify {
            entityService.appendEntityAttributes(
                "urn:ngsi-ld:User:96e1f1e9-d798-48d7-820e-59f5a9a2abf5",
                match {
                    it.size == 1 &&
                        it[0].name == "https://ontology.eglobalmark.com/authorization#isMemberOf" &&
                        it[0] is NgsiLdRelationship &&
                        (it[0] as NgsiLdRelationship).instances[0].datasetId ==
                        URI.create("7cdad168-96ee-4649-b768-a060ac2ef435") &&
                        (it[0] as NgsiLdRelationship).instances[0].objectId ==
                        "urn:ngsi-ld:Group:7cdad168-96ee-4649-b768-a060ac2ef435"
                },
                false
            )
        }
        confirmVerified()
    }

    @Test
    fun `it should parse and transmit group membership deletion event`() {
        val groupMembershipDeleteEvent = loadSampleData("authorization/GroupMembershipDeleteEvent.json")

        iamListener.processMessage(groupMembershipDeleteEvent)

        verify {
            entityService.deleteEntityAttributeInstance(
                "urn:ngsi-ld:User:6ad19fe0-fc11-4024-85f2-931c6fa6f7e0",
                "https://ontology.eglobalmark.com/authorization#isMemberOf",
                URI.create("urn:ngsi-ld:Dataset:isMemberOf:7cdad168-96ee-4649-b768-a060ac2ef435")
            )
        }
    }

    @Test
    fun `it should parse and transmit role update event with two roles`() {
        val roleAppendEvent = loadSampleData("authorization/RealmRoleAppendEventTwoRoles.json")

        iamListener.processMessage(roleAppendEvent)

        verify {
            entityService.appendEntityAttributes(
                "urn:ngsi-ld:Group:ab67edf3-238c-4f50-83f4-617c620c62eb",
                match {
                    it.size == 1 &&
                        it[0].compactName == "roles" &&
                        it[0] is NgsiLdProperty &&
                        (it[0] as NgsiLdProperty).instances.size == 1 &&
                        (it[0] as NgsiLdProperty).instances[0].value is List<*> &&
                        ((it[0] as NgsiLdProperty).instances[0].value as List<*>)
                            .containsAll(setOf("stellio-admin", "stellio-creator"))
                },
                false
            )
        }
        confirmVerified()
    }

    @Test
    fun `it should parse and transmit role update event with one role`() {
        val roleAppendEvent = loadSampleData("authorization/RealmRoleAppendEventOneRole.json")

        iamListener.processMessage(roleAppendEvent)

        verify {
            entityService.appendEntityAttributes(
                "urn:ngsi-ld:Group:ab67edf3-238c-4f50-83f4-617c620c62eb",
                match {
                    it.size == 1 &&
                        it[0].compactName == "roles" &&
                        it[0] is NgsiLdProperty &&
                        (it[0] as NgsiLdProperty).instances.size == 1 &&
                        (it[0] as NgsiLdProperty).instances[0].value is String &&
                        (it[0] as NgsiLdProperty).instances[0].value == "stellio-admin"
                },
                false
            )
        }
        confirmVerified()
    }

    @Test
    fun `it should parse and transmit role update event with no roles`() {
        val roleAppendEvent = loadSampleData("authorization/RealmRoleAppendEventNoRole.json")

        iamListener.processMessage(roleAppendEvent)

        verify {
            entityService.appendEntityAttributes(
                "urn:ngsi-ld:Group:ab67edf3-238c-4f50-83f4-617c620c62eb",
                match {
                    it.size == 1 &&
                        it[0].compactName == "roles" &&
                        it[0] is NgsiLdProperty &&
                        (it[0] as NgsiLdProperty).instances.size == 1 &&
                        (it[0] as NgsiLdProperty).instances[0].value is List<*> &&
                        ((it[0] as NgsiLdProperty).instances[0].value as List<*>).isEmpty()
                },
                false
            )
        }
        confirmVerified()
    }

    @Test
    fun `it should parse and transmit role update event for a client`() {
        val roleAppendEvent = loadSampleData("authorization/RealmRoleAppendToClient.json")

        iamListener.processMessage(roleAppendEvent)

        verify {
            entityService.appendEntityAttributes(
                "urn:ngsi-ld:Client:ab67edf3-238c-4f50-83f4-617c620c62eb",
                match {
                    it.size == 2 &&
                        it.map { it.compactName }.containsAll(listOf("roles", "serviceAccountId"))
                },
                false
            )
        }
        confirmVerified()
    }
}
