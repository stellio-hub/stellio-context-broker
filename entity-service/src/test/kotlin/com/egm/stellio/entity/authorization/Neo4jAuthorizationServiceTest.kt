package com.egm.stellio.entity.authorization

import com.egm.stellio.entity.authorization.AuthorizationService.Companion.READ_RIGHT
import com.egm.stellio.entity.repository.Neo4jRepository
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

    @MockkBean(relaxed = true)
    private lateinit var neo4jRepository: Neo4jRepository

    @Test
    fun `it should find user has read right on entity`() {
        assertUserHasRightOnEntity(neo4jAuthorizationService::userCanReadEntity)
    }

    @Test
    fun `it should find user has not read  right on entity`() {
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
        every { neo4jAuthorizationRepository.getUserRoles("urn:ngsi-ld:User:mock-user") } returns setOf("admin")

        assert(neo4jAuthorizationService.userIsAdminOfEntity("entityId", "mock-user"))
        assert(neo4jAuthorizationService.userCanReadEntity("entityId", "mock-user"))
        assert(neo4jAuthorizationService.userCanUpdateEntity("entityId", "mock-user"))
    }

    @Test
    fun `it shoud filter entities which user has read right`() {
        val entitiesId = listOf("entityId", "entityId2", "entityId3", "entityId4", "entityId5")

        every { neo4jAuthorizationRepository.getUserRoles("urn:ngsi-ld:User:mock-user") } returns emptySet()
        every {
            neo4jAuthorizationRepository.filterEntitiesUserHasOneOfGivenRights(
                "urn:ngsi-ld:User:mock-user",
                entitiesId,
                READ_RIGHT
            )
        } returns listOf("entityId", "entityId3", "entityId4")

        assert(
            neo4jAuthorizationService.filterEntitiesUserCanRead(entitiesId, "mock-user") == listOf(
                "entityId",
                "entityId3",
                "entityId4"
            )
        )
    }

    @Test
    fun `it should keep all entities if user has admin rights`() {
        every { neo4jAuthorizationRepository.getUserRoles("urn:ngsi-ld:User:mock-user") } returns setOf("admin")

        assert(
            neo4jAuthorizationService.filterEntitiesUserCanRead(listOf("entityId"), "mock-user") == listOf(
                "entityId"
            )
        )
    }

    @Test
    fun `it should create an admin link to an entity`() {
        val userId = "mock-user"
        val entityId = "urn:ngsi-ld:Apiary:01"

        neo4jAuthorizationService.createAdminLink(entityId, userId)

        verify {
            neo4jRepository.createRelationshipOfSubject(
                match { it.id == "urn:ngsi-ld:User:$userId" },
                match {
                    it.type == listOf(AuthorizationService.R_CAN_ADMIN) &&
                        it.datasetId == URI.create("urn:ngsi-ld:Dataset:rCanAdmin:$entityId")
                },
                eq(entityId)
            )
        }
        confirmVerified()
    }

    private fun assertUserHasRightOnEntity(userHasRightOnEntity: (String, String) -> Boolean) {
        assertUserHasRightOnEntity(userHasRightOnEntity, true)
    }

    private fun assertUserHasNotRightOnEntity(userHasRightOnEntity: (String, String) -> Boolean) {
        assertUserHasRightOnEntity(userHasRightOnEntity, false)
    }

    private fun assertUserHasRightOnEntity(
        userHasRightOnEntity: (String, String) -> Boolean,
        can: Boolean
    ) {
        every { neo4jAuthorizationRepository.getUserRoles("urn:ngsi-ld:User:mock-user") } returns emptySet()
        every {
            neo4jAuthorizationRepository.filterEntitiesUserHasOneOfGivenRights(
                "urn:ngsi-ld:User:mock-user",
                listOf("entityId"),
                any()
            )
        } returns if (can) listOf("entityId") else emptyList()

        assert(userHasRightOnEntity("entityId", "mock-user") == can)
    }
}
