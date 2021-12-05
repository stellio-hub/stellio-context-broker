package com.egm.stellio.search.service

import com.egm.stellio.search.model.SubjectAccessRights
import com.egm.stellio.search.support.WithTimescaleContainer
import com.egm.stellio.shared.util.ADMIN_ROLE_LABEL
import com.egm.stellio.shared.util.SubjectType
import com.egm.stellio.shared.util.toUri
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.test.context.ActiveProfiles
import reactor.test.StepVerifier
import java.util.UUID

@SpringBootTest
@ActiveProfiles("test")
class SubjectAccessRightsServiceTests : WithTimescaleContainer {

    @Autowired
    private lateinit var subjectAccessRightsService: SubjectAccessRightsService

    @Autowired
    private lateinit var r2dbcEntityTemplate: R2dbcEntityTemplate

    private val subjectUuid = UUID.fromString("0768A6D5-D87B-4209-9A22-8C40A8961A79")

    @AfterEach
    fun clearUsersAccessRightsTable() {
        r2dbcEntityTemplate.delete(SubjectAccessRights::class.java)
            .all()
            .block()
    }

    @Test
    fun `it should persist an user access right`() {
        val userAccessRights = SubjectAccessRights(
            subjectId = subjectUuid,
            subjectType = SubjectType.USER,
            globalRole = ADMIN_ROLE_LABEL,
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
            subjectId = subjectUuid,
            subjectType = SubjectType.USER,
            globalRole = ADMIN_ROLE_LABEL,
            allowedReadEntities = allowedReadEntities,
            allowedWriteEntities = allowedWriteEntities
        )

        subjectAccessRightsService.create(userAccessRights).block()

        StepVerifier.create(
            subjectAccessRightsService.retrieve(subjectUuid)
        )
            .expectNextMatches {
                it.subjectId == subjectUuid &&
                    it.subjectType == SubjectType.USER &&
                    it.globalRole == ADMIN_ROLE_LABEL &&
                    it.allowedReadEntities?.contentEquals(allowedReadEntities) == true &&
                    it.allowedWriteEntities?.contentEquals(allowedWriteEntities) == true
            }
            .expectComplete()
            .verify()
    }

    @Test
    fun `it should add a new entity in the allowed list of read entities`() {
        val userAccessRights = SubjectAccessRights(
            subjectId = subjectUuid,
            subjectType = SubjectType.USER,
            globalRole = ADMIN_ROLE_LABEL,
            allowedReadEntities = arrayOf("urn:ngsi-ld:Entity:1234", "urn:ngsi-ld:Entity:5678")
        )

        subjectAccessRightsService.create(userAccessRights).block()

        StepVerifier.create(
            subjectAccessRightsService.addReadRoleOnEntity(subjectUuid, "urn:ngsi-ld:Entity:1111".toUri())
        )
            .expectNextMatches { it == 1 }
            .expectComplete()
            .verify()

        StepVerifier.create(
            subjectAccessRightsService.hasReadRoleOnEntity(subjectUuid, "urn:ngsi-ld:Entity:1111".toUri())
        )
            .expectNextMatches { it == true }
            .expectComplete()
            .verify()
    }

    @Test
    fun `it should remove an entity from the allowed list of read entities`() {
        val userAccessRights = SubjectAccessRights(
            subjectId = subjectUuid,
            subjectType = SubjectType.USER,
            globalRole = ADMIN_ROLE_LABEL,
            allowedReadEntities = arrayOf("urn:ngsi-ld:Entity:1234", "urn:ngsi-ld:Entity:5678")
        )

        subjectAccessRightsService.create(userAccessRights).block()

        StepVerifier.create(
            subjectAccessRightsService.removeRoleOnEntity(subjectUuid, "urn:ngsi-ld:Entity:1234".toUri())
        )
            .expectNextMatches { it == 1 }
            .expectComplete()
            .verify()

        StepVerifier.create(
            subjectAccessRightsService.hasReadRoleOnEntity(subjectUuid, "urn:ngsi-ld:Entity:1234".toUri())
        )
            .expectNextMatches { it == false }
            .expectComplete()
            .verify()
    }

    @Test
    fun `it should update the global role of a subject`() {
        val userAccessRights = SubjectAccessRights(
            subjectId = subjectUuid,
            subjectType = SubjectType.USER,
            allowedReadEntities = arrayOf("urn:ngsi-ld:Entity:1234", "urn:ngsi-ld:Entity:5678"),
            allowedWriteEntities = arrayOf("urn:ngsi-ld:Entity:6666")
        )

        subjectAccessRightsService.create(userAccessRights).block()

        StepVerifier.create(
            subjectAccessRightsService.addAdminGlobalRole(subjectUuid)
        )
            .expectNextMatches { it == 1 }
            .expectComplete()
            .verify()

        StepVerifier.create(
            subjectAccessRightsService.retrieve(subjectUuid)
        )
            .expectNextMatches {
                it.globalRole == ADMIN_ROLE_LABEL
            }
            .expectComplete()
            .verify()

        StepVerifier.create(
            subjectAccessRightsService.removeAdminGlobalRole(subjectUuid)
        )
            .expectNextMatches { it == 1 }
            .expectComplete()
            .verify()

        StepVerifier.create(
            subjectAccessRightsService.retrieve(subjectUuid)
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
            subjectId = subjectUuid,
            subjectType = SubjectType.USER,
            globalRole = ADMIN_ROLE_LABEL,
            allowedReadEntities = arrayOf("urn:ngsi-ld:Entity:1234", "urn:ngsi-ld:Entity:5678"),
            allowedWriteEntities = arrayOf("urn:ngsi-ld:Entity:6666")
        )

        subjectAccessRightsService.create(userAccessRights).block()

        StepVerifier.create(
            subjectAccessRightsService.hasReadRoleOnEntity(subjectUuid, "urn:ngsi-ld:Entity:1234".toUri())
        )
            .expectNextMatches { it == true }
            .expectComplete()
            .verify()

        StepVerifier.create(
            subjectAccessRightsService.hasReadRoleOnEntity(subjectUuid, "urn:ngsi-ld:Entity:1111".toUri())
        )
            .expectNextMatches { it == false }
            .expectComplete()
            .verify()

        StepVerifier.create(
            subjectAccessRightsService.hasReadRoleOnEntity(subjectUuid, "urn:ngsi-ld:Entity:6666".toUri())
        )
            .expectNextMatches { it == true }
            .expectComplete()
            .verify()
    }

    @Test
    fun `it should delete an user access right`() {
        val userAccessRights = SubjectAccessRights(
            subjectId = subjectUuid,
            subjectType = SubjectType.USER,
            globalRole = ADMIN_ROLE_LABEL,
            allowedReadEntities = arrayOf("urn:ngsi-ld:Entity:1234", "urn:ngsi-ld:Entity:5678"),
            allowedWriteEntities = arrayOf("urn:ngsi-ld:Entity:6666", "urn:ngsi-ld:Entity:0000")
        )

        subjectAccessRightsService.create(userAccessRights).block()

        StepVerifier.create(
            subjectAccessRightsService.delete(subjectUuid)
        )
            .expectNextMatches { it == 1 }
            .expectComplete()
            .verify()

        StepVerifier.create(
            subjectAccessRightsService.retrieve(subjectUuid)
        )
            .expectComplete()
            .verify()
    }
}
