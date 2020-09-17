package com.egm.stellio.entity.authorization

import com.egm.stellio.entity.authorization.AuthorizationService.Companion.ADMIN_ROLE_LABEL
import com.egm.stellio.entity.authorization.AuthorizationService.Companion.READ_RIGHT
import com.egm.stellio.shared.util.toListOfUri
import com.egm.stellio.shared.util.toUri
import com.ninjasquad.springmockk.MockkBean
import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.verify
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import java.net.URI

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = [Neo4jAuthorizationService::class])
@ActiveProfiles("test")
class Neo4jAuthorizationServiceTest {

    @Autowired
    private lateinit var neo4jAuthorizationService: Neo4jAuthorizationService

    @MockkBean
    private lateinit var neo4jAuthorizationRepository: Neo4jAuthorizationRepository

    @Test
    fun `it should find user has read right on entity`() {
        assertUserHasRightOnEntity(neo4jAuthorizationService::userCanReadEntity)
    }

    @Test
    fun `it should find user has not read right on entity`() {
        assertUserHasNotRightOnEntity(neo4jAuthorizationService::userCanReadEntity)
    }

    @Test
    fun `it should find user has write right on entity`() {
        assertUserHasRightOnEntity(neo4jAuthorizationService::userCanUpdateEntity)
    }

    @Test
    fun `it should find user has not write right on entity`() {
        assertUserHasNotRightOnEntity(neo4jAuthorizationService::userCanUpdateEntity)
    }

    @Test
    fun `it should find user has admin right on entity`() {
        assertUserHasRightOnEntity(neo4jAuthorizationService::userIsAdminOfEntity)
    }

    @Test
    fun `it should find user has not admin right on entity`() {
        assertUserHasNotRightOnEntity(neo4jAuthorizationService::userIsAdminOfEntity)
    }

    @Test
    fun `it should find admin user has admin, read or write right entity`() {
        every {
            neo4jAuthorizationRepository.getUserRoles("urn:ngsi-ld:User:mock-user".toUri())
        } returns setOf(ADMIN_ROLE_LABEL)

        assert(neo4jAuthorizationService.userIsAdminOfEntity("entityId".toUri(), "mock-user".toUri()))
        assert(neo4jAuthorizationService.userCanReadEntity("entityId".toUri(), "mock-user".toUri()))
        assert(neo4jAuthorizationService.userCanUpdateEntity("entityId".toUri(), "mock-user".toUri()))
    }

    @Test
    fun `it should filter entities which user has read right`() {
        val entitiesId = listOf("entityId", "entityId2", "entityId3", "entityId4", "entityId5").toListOfUri()

        every { neo4jAuthorizationRepository.getUserRoles("urn:ngsi-ld:User:mock-user".toUri()) } returns emptySet()
        every {
            neo4jAuthorizationRepository.filterEntitiesUserHasOneOfGivenRights(
                "urn:ngsi-ld:User:mock-user".toUri(),
                entitiesId,
                READ_RIGHT
            )
        } returns listOf("entityId", "entityId3", "entityId4").toListOfUri()

        assert(
            neo4jAuthorizationService.filterEntitiesUserCanRead(entitiesId, "mock-user".toUri())
                == listOf("entityId", "entityId3", "entityId4").toListOfUri()
        )
    }

    @Test
    fun `it should keep all entities if user has admin rights`() {
        every {
            neo4jAuthorizationRepository.getUserRoles("urn:ngsi-ld:User:mock-user".toUri())
        } returns setOf(ADMIN_ROLE_LABEL)

        assert(
            neo4jAuthorizationService.filterEntitiesUserCanRead(listOf("entityId").toListOfUri(), "mock-user".toUri())
                == listOf("entityId").toListOfUri()
        )
    }

    @Test
    fun `it should create an admin link to an entity`() {
        val userId = "mock-user".toUri()
        val entityId = "urn:ngsi-ld:Apiary:01".toUri()

        every { neo4jAuthorizationRepository.createAdminLinks(any(), any(), any()) } returns listOf("relId")
            .toListOfUri()

        neo4jAuthorizationService.createAdminLink(entityId, userId)

        verify {
            neo4jAuthorizationRepository.createAdminLinks(
                "urn:ngsi-ld:User:$userId".toUri(),
                match {
                    it.size == 1 &&
                        it[0].type == listOf(AuthorizationService.R_CAN_ADMIN) &&
                        it[0].datasetId == "urn:ngsi-ld:Dataset:rCanAdmin:$entityId".toUri()
                },
                listOf(entityId)
            )
        }
        confirmVerified()
    }

    private fun assertUserHasRightOnEntity(userHasRightOnEntity: (URI, URI) -> Boolean) {
        assertUserHasRightOnEntity(userHasRightOnEntity, true)
    }

    private fun assertUserHasNotRightOnEntity(userHasRightOnEntity: (URI, URI) -> Boolean) {
        assertUserHasRightOnEntity(userHasRightOnEntity, false)
    }

    private fun assertUserHasRightOnEntity(
        userHasRightOnEntity: (URI, URI) -> Boolean,
        can: Boolean
    ) {
        every { neo4jAuthorizationRepository.getUserRoles("urn:ngsi-ld:User:mock-user".toUri()) } returns emptySet()
        every {
            neo4jAuthorizationRepository.filterEntitiesUserHasOneOfGivenRights(
                "urn:ngsi-ld:User:mock-user".toUri(),
                listOf("entityId").toListOfUri(),
                any()
            )
        } returns if (can) listOf("entityId").toListOfUri() else emptyList()

        assert(userHasRightOnEntity("entityId".toUri(), "mock-user".toUri()) == can)
    }
}
