package com.egm.stellio.entity.authorization

import com.egm.stellio.entity.authorization.AuthorizationService.Companion.AUTHORIZATION_ONTOLOGY
import com.egm.stellio.entity.model.Relationship
import com.egm.stellio.entity.repository.Neo4jRepository
import com.ninjasquad.springmockk.MockkBean
import io.mockk.every
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = [Neo4jAuthorizationService::class])
@ActiveProfiles("test")
class Neo4jAuthorizationServiceTest {

    @Autowired
    private lateinit var neo4jAuthorizationService: Neo4jAuthorizationService

    @MockkBean
    private lateinit var neo4jAuthorizationRepository: Neo4jAuthorizationRepository

    @MockkBean
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
                right = Relationship(type = listOf(AUTHORIZATION_ONTOLOGY + "rCanRead"))
            ),
            Neo4jAuthorizationRepository.AvailableRightsForEntity(targetEntityId = "entityId2"),
            Neo4jAuthorizationRepository.AvailableRightsForEntity(
                targetEntityId = "entityId3",
                grpRight = Relationship(type = listOf(AUTHORIZATION_ONTOLOGY + "rCanAdmin"))
            ),
            Neo4jAuthorizationRepository.AvailableRightsForEntity(
                targetEntityId = "entityId4",
                right = Relationship(type = listOf(AUTHORIZATION_ONTOLOGY + "rCanRead")),
                grpRight = Relationship(type = listOf(AUTHORIZATION_ONTOLOGY + "rCanWrite"))
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
    fun `it shoud filter entities if user's group has read right`() {
        every { neo4jAuthorizationRepository.getUserRoles("urn:ngsi-ld:User:mock-user") } returns listOf()
        every {
            neo4jAuthorizationRepository.getAvailableRightsForEntities(
                "urn:ngsi-ld:User:mock-user",
                listOf("entityId")
            )
        } returns listOf(
            Neo4jAuthorizationRepository.AvailableRightsForEntity(
                targetEntityId = "entityId",
                right = Relationship(type = listOf(AUTHORIZATION_ONTOLOGY + "rCanRead"))
            )
        )

        assert(
            neo4jAuthorizationService.filterEntitiesUserHasReadRight(listOf("entityId"), "mock-user") == listOf(
                "entityId"
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
                right = if (right != "") Relationship(type = listOf(AUTHORIZATION_ONTOLOGY + right)) else null
            )
        )

        assert(userHasRightOnEntity("entityId", "mock-user") == can)
    }
}
