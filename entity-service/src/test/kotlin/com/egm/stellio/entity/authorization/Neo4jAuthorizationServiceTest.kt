package com.egm.stellio.entity.authorization

import com.egm.stellio.entity.authorization.AuthorizationService.Companion.AUTHORIZATION_ONTOLOGY
import com.egm.stellio.entity.repository.Neo4jRepository
import com.ninjasquad.springmockk.MockkBean
import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.verify
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
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

    @ParameterizedTest
    @ValueSource(strings = ["rCanRead", "rCanWrite", "rCanAdmin"])
    fun `it should find user has read right on entity`(right: String) {
        assertUserHasRightOnEntity(right, neo4jAuthorizationService::userHasReadRightsOnEntity)
    }

    @Test
    fun `it should find user has not read  right on entity`() {
        assertUserHasNotRightOnEntity("", neo4jAuthorizationService::userHasReadRightsOnEntity)
    }

    @ParameterizedTest
    @ValueSource(strings = ["rCanWrite", "rCanAdmin"])
    fun `it should find user has write right on entity`(right: String) {
        assertUserHasRightOnEntity(right, neo4jAuthorizationService::userHasWriteRightsOnEntity)
    }

    @ParameterizedTest
    @ValueSource(strings = ["rCanRead", ""])
    fun `it should find user has not write right on entity`(right: String) {
        assertUserHasNotRightOnEntity(right, neo4jAuthorizationService::userHasWriteRightsOnEntity)
    }

    @Test
    fun `it should find user has admin right on entity`() {
        assertUserHasRightOnEntity("rCanAdmin", neo4jAuthorizationService::userHasAdminRightsOnEntity)
    }

    @ParameterizedTest
    @ValueSource(strings = ["rCanRead", "rCanWrite", ""])
    fun `it should find user has not admin right on entity`(right: String) {
        assertUserHasNotRightOnEntity(right, neo4jAuthorizationService::userHasAdminRightsOnEntity)
    }

    @Test
    fun `it should find admin user has admin, read or write right entity`() {
        every { neo4jAuthorizationRepository.getUserRoles("urn:ngsi-ld:User:mock-user") } returns listOf("admin")
        every {
            neo4jAuthorizationRepository.getAvailableRightsForEntities(
                "urn:ngsi-ld:User:mock-user",
                listOf("entityId")
            )
        } returns listOf(
            Neo4jAuthorizationRepository.AvailableRightsForEntity(
                targetEntityId = "entityId"
            )
        )

        assert(neo4jAuthorizationService.userHasAdminRightsOnEntity("entityId", "mock-user"))
        assert(neo4jAuthorizationService.userHasReadRightsOnEntity("entityId", "mock-user"))
        assert(neo4jAuthorizationService.userHasWriteRightsOnEntity("entityId", "mock-user"))
    }

    @Test
    fun `it shoud filter entities which user has read right`() {
        val entitiesId = listOf("entityId", "entityId2", "entityId3", "entityId4", "entityId5")

        every { neo4jAuthorizationRepository.getUserRoles("urn:ngsi-ld:User:mock-user") } returns listOf()
        every {
            neo4jAuthorizationRepository.getAvailableRightsForEntities("urn:ngsi-ld:User:mock-user", entitiesId)
        } returns listOf(
            Neo4jAuthorizationRepository.AvailableRightsForEntity(
                targetEntityId = "entityId",
                rights = listOf(AUTHORIZATION_ONTOLOGY + "rCanRead")
            ),
            Neo4jAuthorizationRepository.AvailableRightsForEntity(targetEntityId = "entityId2"),
            Neo4jAuthorizationRepository.AvailableRightsForEntity(
                targetEntityId = "entityId3",
                rights = listOf(AUTHORIZATION_ONTOLOGY + "rCanAdmin")
            ),
            Neo4jAuthorizationRepository.AvailableRightsForEntity(
                targetEntityId = "entityId4",
                rights = listOf(AUTHORIZATION_ONTOLOGY + "rCanRead", AUTHORIZATION_ONTOLOGY + "rCanWrite")
            ),
            Neo4jAuthorizationRepository.AvailableRightsForEntity(targetEntityId = "entityId5")
        )

        assert(
            neo4jAuthorizationService.filterEntitiesUserHasReadRight(entitiesId, "mock-user") == listOf(
                "entityId",
                "entityId3",
                "entityId4"
            )
        )
    }

    @Test
    fun `it should keep all entities if user has admin rights`() {
        every { neo4jAuthorizationRepository.getUserRoles("urn:ngsi-ld:User:mock-user") } returns listOf("admin")

        assert(
            neo4jAuthorizationService.filterEntitiesUserHasReadRight(listOf("entityId"), "mock-user") == listOf(
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

    private fun assertUserHasRightOnEntity(right: String, userHasRightOnEntity: (String, String) -> Boolean) {
        assertUserHasRightOnEntity(right, userHasRightOnEntity, true)
    }

    private fun assertUserHasNotRightOnEntity(right: String, userHasRightOnEntity: (String, String) -> Boolean) {
        assertUserHasRightOnEntity(right, userHasRightOnEntity, false)
    }

    private fun assertUserHasRightOnEntity(
        right: String,
        userHasRightOnEntity: (String, String) -> Boolean,
        can: Boolean
    ) {
        every { neo4jAuthorizationRepository.getUserRoles("urn:ngsi-ld:User:mock-user") } returns listOf()
        every {
            neo4jAuthorizationRepository.getAvailableRightsForEntities(
                "urn:ngsi-ld:User:mock-user",
                listOf("entityId")
            )
        } returns listOf(
            Neo4jAuthorizationRepository.AvailableRightsForEntity(
                targetEntityId = "entityId",
                rights = if (right != "") listOf(AUTHORIZATION_ONTOLOGY + right) else emptyList()
            )
        )

        assert(userHasRightOnEntity("entityId", "mock-user") == can)
    }
}
