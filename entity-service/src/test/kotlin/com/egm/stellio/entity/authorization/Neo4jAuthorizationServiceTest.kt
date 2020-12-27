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

    private val mockUserSub = "mock-user"
    private val mockUserUri = "urn:ngsi-ld:User:$mockUserSub".toUri()

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
            neo4jAuthorizationRepository.getUserRoles(mockUserUri)
        } returns setOf(ADMIN_ROLE_LABEL)

        assert(neo4jAuthorizationService.userIsAdminOfEntity("urn:ngsi-ld:Entity:01".toUri(), mockUserSub))
        assert(neo4jAuthorizationService.userCanReadEntity("urn:ngsi-ld:Entity:01".toUri(), mockUserSub))
        assert(neo4jAuthorizationService.userCanUpdateEntity("urn:ngsi-ld:Entity:01".toUri(), mockUserSub))
    }

    @Test
    fun `it should filter entities which user has read right`() {
        val entitiesId = (1..5).map { "urn:ngsi-ld:Entity:$it" }.toListOfUri()

        every { neo4jAuthorizationRepository.getUserRoles(mockUserUri) } returns emptySet()
        every {
            neo4jAuthorizationRepository.filterEntitiesUserHasOneOfGivenRights(
                mockUserUri,
                entitiesId,
                READ_RIGHT
            )
        } returns listOf("urn:ngsi-ld:Entity:1", "urn:ngsi-ld:Entity:3", "urn:ngsi-ld:Entity:4").toListOfUri()

        assert(
            neo4jAuthorizationService.filterEntitiesUserCanRead(entitiesId, mockUserSub)
                == listOf("urn:ngsi-ld:Entity:1", "urn:ngsi-ld:Entity:3", "urn:ngsi-ld:Entity:4").toListOfUri()
        )
    }

    @Test
    fun `it should keep all entities if user has admin rights`() {
        every {
            neo4jAuthorizationRepository.getUserRoles(mockUserUri)
        } returns setOf(ADMIN_ROLE_LABEL)

        val entitiesIds = listOf("urn:ngsi-ld:Entity:01").toListOfUri()
        assert(
            neo4jAuthorizationService.filterEntitiesUserCanRead(entitiesIds, mockUserSub) == entitiesIds
        )
    }

    @Test
    fun `it should create an admin link to an entity`() {
        val entityId = "urn:ngsi-ld:Apiary:01".toUri()

        every {
            neo4jAuthorizationRepository.createAdminLinks(any(), any(), any())
        } returns listOf("urn:ngsi-ld:Relationship:01").toListOfUri()

        neo4jAuthorizationService.createAdminLink(entityId, mockUserSub)

        verify {
            neo4jAuthorizationRepository.createAdminLinks(
                mockUserUri,
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

    private fun assertUserHasRightOnEntity(userHasRightOnEntity: (URI, String) -> Boolean) {
        assertUserHasRightOnEntity(userHasRightOnEntity, true)
    }

    private fun assertUserHasNotRightOnEntity(userHasRightOnEntity: (URI, String) -> Boolean) {
        assertUserHasRightOnEntity(userHasRightOnEntity, false)
    }

    private fun assertUserHasRightOnEntity(
        userHasRightOnEntity: (URI, String) -> Boolean,
        can: Boolean
    ) {
        every { neo4jAuthorizationRepository.getUserRoles(mockUserUri) } returns emptySet()
        every {
            neo4jAuthorizationRepository.filterEntitiesUserHasOneOfGivenRights(
                mockUserUri,
                listOf("urn:ngsi-ld:Entity:01").toListOfUri(),
                any()
            )
        } returns if (can) listOf("urn:ngsi-ld:Entity:01").toListOfUri() else emptyList()

        assert(userHasRightOnEntity("urn:ngsi-ld:Entity:01".toUri(), mockUserSub) == can)
    }
}
