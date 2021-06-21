package com.egm.stellio.search.service

import com.egm.stellio.search.config.TimescaleBasedTests
import com.egm.stellio.search.model.SubjectAccessRights
import com.egm.stellio.shared.util.toUri
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.data.r2dbc.core.DatabaseClient
import org.springframework.test.context.ActiveProfiles
import reactor.test.StepVerifier

@SpringBootTest
@ActiveProfiles("test")
class SubjectAccessRightsServiceTests : TimescaleBasedTests() {

    @Autowired
    private lateinit var subjectAccessRightsService: SubjectAccessRightsService

    @Autowired
    private lateinit var databaseClient: DatabaseClient

    private val userUri = "urn:ngsi-ld:User:0123".toUri()

    @AfterEach
    fun clearUsersAccessRightsTable() {
        databaseClient.delete()
            .from("subject_access_rights")
            .fetch()
            .rowsUpdated()
            .block()
    }

    @Test
    fun `it should persist an user access right`() {
        val userAccessRights = SubjectAccessRights(
            subjectId = userUri,
            subjectType = SubjectAccessRights.SubjectType.USER,
            globalRole = "stellio-admin",
            allowedReadEntities = arrayOf("urn:ngsi-ld:Entity:1234", "urn:ngsi-ld:Entity:5678"),
            allowedWriteEntities = arrayOf("urn:ngsi-ld:Entity:6666", "urn:ngsi-ld:Entity:0000")
        )

        StepVerifier.create(
            subjectAccessRightsService.create(userAccessRights)
        )
            .expectNextMatches { it == 1 }
            .expectComplete()
            .verify()
    }

    @Test
    fun `it should retrieve an user access right`() {
        val allowedReadEntities = arrayOf("urn:ngsi-ld:Entity:1234", "urn:ngsi-ld:Entity:5678")
        val allowedWriteEntities = arrayOf("urn:ngsi-ld:Entity:6666", "urn:ngsi-ld:Entity:0000")
        val userAccessRights = SubjectAccessRights(
            subjectId = userUri,
            subjectType = SubjectAccessRights.SubjectType.USER,
            globalRole = "stellio-admin",
            allowedReadEntities = allowedReadEntities,
            allowedWriteEntities = allowedWriteEntities
        )

        subjectAccessRightsService.create(userAccessRights).block()

        StepVerifier.create(
            subjectAccessRightsService.retrieve(userUri)
        )
            .expectNextMatches {
                it.subjectId == userUri &&
                    it.subjectType == SubjectAccessRights.SubjectType.USER &&
                    it.globalRole == "stellio-admin" &&
                    it.allowedReadEntities?.contentEquals(allowedReadEntities) == true &&
                    it.allowedWriteEntities?.contentEquals(allowedWriteEntities) == true
            }
            .expectComplete()
            .verify()
    }

    @Test
    fun `it should add a new entity in the allowed list of read entities`() {
        val userAccessRights = SubjectAccessRights(
            subjectId = userUri,
            subjectType = SubjectAccessRights.SubjectType.USER,
            globalRole = "stellio-admin",
            allowedReadEntities = arrayOf("urn:ngsi-ld:Entity:1234", "urn:ngsi-ld:Entity:5678")
        )

        subjectAccessRightsService.create(userAccessRights).block()

        StepVerifier.create(
            subjectAccessRightsService.addReadRoleOnEntity(userUri, "urn:ngsi-ld:Entity:1111".toUri())
        )
            .expectNextMatches { it == 1 }
            .expectComplete()
            .verify()

        StepVerifier.create(
            subjectAccessRightsService.hasReadRoleOnEntity(userUri, "urn:ngsi-ld:Entity:1111".toUri())
        )
            .expectNextMatches { it == true }
            .expectComplete()
            .verify()
    }

    @Test
    fun `it should update the global role of a subject`() {
        val userAccessRights = SubjectAccessRights(
            subjectId = userUri,
            subjectType = SubjectAccessRights.SubjectType.USER,
            allowedReadEntities = arrayOf("urn:ngsi-ld:Entity:1234", "urn:ngsi-ld:Entity:5678"),
            allowedWriteEntities = arrayOf("urn:ngsi-ld:Entity:6666")
        )

        subjectAccessRightsService.create(userAccessRights).block()

        StepVerifier.create(
            subjectAccessRightsService.addAdminGlobalRole(userUri)
        )
            .expectNextMatches { it == 1 }
            .expectComplete()
            .verify()

        StepVerifier.create(
            subjectAccessRightsService.retrieve(userUri)
        )
            .expectNextMatches {
                it.globalRole == "stellio-admin"
            }
            .expectComplete()
            .verify()

        StepVerifier.create(
            subjectAccessRightsService.removeAdminGlobalRole(userUri)
        )
            .expectNextMatches { it == 1 }
            .expectComplete()
            .verify()

        StepVerifier.create(
            subjectAccessRightsService.retrieve(userUri)
        )
            .expectNextMatches {
                it.globalRole == null
            }
            .expectComplete()
            .verify()
    }

    @Test
    fun `it should find if an user has a read role on a entity`() {
        val userAccessRights = SubjectAccessRights(
            subjectId = userUri,
            subjectType = SubjectAccessRights.SubjectType.USER,
            globalRole = "stellio-admin",
            allowedReadEntities = arrayOf("urn:ngsi-ld:Entity:1234", "urn:ngsi-ld:Entity:5678"),
            allowedWriteEntities = arrayOf("urn:ngsi-ld:Entity:6666")
        )

        subjectAccessRightsService.create(userAccessRights).block()

        StepVerifier.create(
            subjectAccessRightsService.hasReadRoleOnEntity(userUri, "urn:ngsi-ld:Entity:1234".toUri())
        )
            .expectNextMatches { it == true }
            .expectComplete()
            .verify()

        StepVerifier.create(
            subjectAccessRightsService.hasReadRoleOnEntity(userUri, "urn:ngsi-ld:Entity:1111".toUri())
        )
            .expectNextMatches { it == false }
            .expectComplete()
            .verify()

        StepVerifier.create(
            subjectAccessRightsService.hasReadRoleOnEntity(userUri, "urn:ngsi-ld:Entity:6666".toUri())
        )
            .expectNextMatches { it == true }
            .expectComplete()
            .verify()
    }

    @Test
    fun `it should delete an user access right`() {
        val userAccessRights = SubjectAccessRights(
            subjectId = userUri,
            subjectType = SubjectAccessRights.SubjectType.USER,
            globalRole = "stellio-admin",
            allowedReadEntities = arrayOf("urn:ngsi-ld:Entity:1234", "urn:ngsi-ld:Entity:5678"),
            allowedWriteEntities = arrayOf("urn:ngsi-ld:Entity:6666", "urn:ngsi-ld:Entity:0000")
        )

        subjectAccessRightsService.create(userAccessRights).block()

        StepVerifier.create(
            subjectAccessRightsService.delete(userUri)
        )
            .expectNextMatches { it == 1 }
            .expectComplete()
            .verify()

        StepVerifier.create(
            subjectAccessRightsService.retrieve(userUri)
        )
            .expectComplete()
            .verify()
    }
}
