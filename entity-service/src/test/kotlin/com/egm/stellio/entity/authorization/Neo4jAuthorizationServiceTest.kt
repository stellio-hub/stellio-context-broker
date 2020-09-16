package com.egm.stellio.entity.authorization

import com.egm.stellio.entity.authorization.AuthorizationService.Companion.ADMIN_ROLE_LABEL
import com.egm.stellio.entity.authorization.AuthorizationService.Companion.READ_RIGHT
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
            neo4jAuthorizationRepository.getUserRoles(URI.create("urn:ngsi-ld:User:mock-user"))
        } returns setOf(ADMIN_ROLE_LABEL)

        assert(neo4jAuthorizationService.userIsAdminOfEntity(URI.create("entityId"), URI.create("mock-user")))
        assert(neo4jAuthorizationService.userCanReadEntity(URI.create("entityId"), URI.create("mock-user")))
        assert(neo4jAuthorizationService.userCanUpdateEntity(URI.create("entityId"), URI.create("mock-user")))
    }

    @Test
    fun `it should filter entities which user has read right`() {
        val entitiesId = listOf(
            URI.create("entityId"),
            URI.create("entityId2"),
            URI.create("entityId3"),
            URI.create("entityId4"),
            URI.create("entityId5")
        )

        every { neo4jAuthorizationRepository.getUserRoles(URI.create("urn:ngsi-ld:User:mock-user")) } returns emptySet()
        every {
            neo4jAuthorizationRepository.filterEntitiesUserHasOneOfGivenRights(
                URI.create("urn:ngsi-ld:User:mock-user"),
                entitiesId,
                READ_RIGHT
            )
        } returns listOf(URI.create("entityId"), URI.create("entityId3"), URI.create("entityId4"))

        assert(
            neo4jAuthorizationService.filterEntitiesUserCanRead(entitiesId, URI.create("mock-user")) == listOf(
                URI.create("entityId"),
                URI.create("entityId3"),
                URI.create("entityId4")
            )
        )
    }

    @Test
    fun `it should keep all entities if user has admin rights`() {
        every {
            neo4jAuthorizationRepository.getUserRoles(URI.create("urn:ngsi-ld:User:mock-user"))
        } returns setOf(ADMIN_ROLE_LABEL)

        assert(
            neo4jAuthorizationService.filterEntitiesUserCanRead(listOf(URI.create("entityId")), URI.create("mock-user"))
                == listOf(URI.create("entityId"))
        )
    }

    @Test
    fun `it should create an admin link to an entity`() {
        val userId = URI.create("mock-user")
        val entityId = URI.create("urn:ngsi-ld:Apiary:01")

        every { neo4jAuthorizationRepository.createAdminLinks(any(), any(), any()) } returns listOf(URI.create("relId"))

        neo4jAuthorizationService.createAdminLink(entityId, userId)

        verify {
            neo4jAuthorizationRepository.createAdminLinks(
                URI.create("urn:ngsi-ld:User:$userId"),
                match {
                    it.size == 1 &&
                        it[0].type == listOf(AuthorizationService.R_CAN_ADMIN) &&
                        it[0].datasetId == URI.create("urn:ngsi-ld:Dataset:rCanAdmin:$entityId")
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
        every { neo4jAuthorizationRepository.getUserRoles(URI.create("urn:ngsi-ld:User:mock-user")) } returns emptySet()
        every {
            neo4jAuthorizationRepository.filterEntitiesUserHasOneOfGivenRights(
                URI.create("urn:ngsi-ld:User:mock-user"),
                listOf(URI.create("entityId")),
                any()
            )
        } returns if (can) listOf(URI.create("entityId")) else emptyList()

        assert(userHasRightOnEntity(URI.create("entityId"), URI.create("mock-user")) == can)
    }
}
