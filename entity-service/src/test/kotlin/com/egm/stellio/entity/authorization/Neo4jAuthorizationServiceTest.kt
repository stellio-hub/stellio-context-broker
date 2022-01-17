package com.egm.stellio.entity.authorization

import arrow.core.Option
import arrow.core.Some
import com.egm.stellio.shared.util.AuthContextModel.AUTH_REL_CAN_ADMIN
import com.egm.stellio.shared.util.AuthContextModel.READ_RIGHTS
import com.egm.stellio.shared.util.AuthContextModel.SpecificAccessPolicy
import com.egm.stellio.shared.util.AuthContextModel.USER_PREFIX
import com.egm.stellio.shared.util.AuthContextModel.WRITE_RIGHTS
import com.egm.stellio.shared.util.GlobalRole
import com.egm.stellio.shared.util.Sub
import com.egm.stellio.shared.util.toListOfUri
import com.egm.stellio.shared.util.toUri
import com.ninjasquad.springmockk.MockkBean
import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.verify
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import java.net.URI
import java.util.UUID

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = [Neo4jAuthorizationService::class])
@ActiveProfiles("test")
class Neo4jAuthorizationServiceTest {

    @Autowired
    private lateinit var neo4jAuthorizationService: Neo4jAuthorizationService

    @MockkBean
    private lateinit var neo4jAuthorizationRepository: Neo4jAuthorizationRepository

    private val mockUserSub = Some(UUID.randomUUID().toString())
    private val mockUserUri = (USER_PREFIX + mockUserSub.value).toUri()
    private val entityUri = "urn:ngsi-ld:Entity:01".toUri()

    @BeforeEach
    fun createGlobalMockResponses() {
        every { neo4jAuthorizationRepository.getSubjectUri(mockUserUri) } returns mockUserUri
    }

    @Test
    fun `it should find user has read right on entity`() {
        assertUserHasRightOnEntity(
            neo4jAuthorizationService::userCanReadEntity,
            hasGrantedAccess = true,
            hasSpecificPolicyAccess = false
        )
    }

    @Test
    fun `it should find user has not read right on entity if no policies match`() {
        assertUserHasRightOnEntity(
            neo4jAuthorizationService::userCanReadEntity,
            hasGrantedAccess = false,
            hasSpecificPolicyAccess = false
        )
    }

    @Test
    fun `it should find user has read right on entity with auth read policy`() {
        assertUserHasRightOnEntity(
            neo4jAuthorizationService::userCanReadEntity,
            hasGrantedAccess = false,
            hasSpecificPolicyAccess = true
        )
    }

    @Test
    fun `it should find user has write right on entity`() {
        assertUserHasRightOnEntity(
            neo4jAuthorizationService::userCanUpdateEntity,
            hasGrantedAccess = true,
            hasSpecificPolicyAccess = false
        )
    }

    @Test
    fun `it should find user has not write right on entity if no policies match`() {
        assertUserHasRightOnEntity(
            neo4jAuthorizationService::userCanUpdateEntity,
            hasGrantedAccess = false,
            hasSpecificPolicyAccess = false
        )
    }

    @Test
    fun `it should find user has write right on entity with auth write policy`() {
        assertUserHasRightOnEntity(
            neo4jAuthorizationService::userCanUpdateEntity,
            hasGrantedAccess = false,
            hasSpecificPolicyAccess = true
        )
    }

    @Test
    fun `it should find user has admin right on entity`() {
        assertUserHasRightOnEntity(
            neo4jAuthorizationService::userIsAdminOfEntity,
            hasGrantedAccess = true,
            hasSpecificPolicyAccess = false
        )
    }

    @Test
    fun `it should find user has not admin right on entity`() {
        assertUserHasRightOnEntity(
            neo4jAuthorizationService::userIsAdminOfEntity,
            hasGrantedAccess = false,
            hasSpecificPolicyAccess = false
        )
    }

    @Test
    fun `it should find admin user has admin, read or write right entity`() {
        every {
            neo4jAuthorizationRepository.getSubjectRoles(mockUserUri)
        } returns setOf(GlobalRole.STELLIO_ADMIN.key)

        assert(neo4jAuthorizationService.userIsAdminOfEntity(entityUri, mockUserSub))
        assert(neo4jAuthorizationService.userCanReadEntity(entityUri, mockUserSub))
        assert(neo4jAuthorizationService.userCanUpdateEntity(entityUri, mockUserSub))
    }

    @Test
    fun `it should filter entities which user has read right`() {
        val entitiesId = (1..5).map { "urn:ngsi-ld:Entity:$it" }.toListOfUri()

        every {
            neo4jAuthorizationRepository.filterEntitiesWithSpecificAccessPolicy(
                entitiesId,
                listOf(SpecificAccessPolicy.AUTH_WRITE.name, SpecificAccessPolicy.AUTH_READ.name)
            )
        } returns emptyList()
        every { neo4jAuthorizationRepository.getSubjectRoles(mockUserUri) } returns emptySet()
        every {
            neo4jAuthorizationRepository.filterEntitiesUserHasOneOfGivenRights(
                mockUserUri,
                entitiesId,
                READ_RIGHTS
            )
        } returns listOf("urn:ngsi-ld:Entity:1", "urn:ngsi-ld:Entity:3", "urn:ngsi-ld:Entity:4").toListOfUri()

        assert(
            neo4jAuthorizationService.filterEntitiesUserCanRead(entitiesId, mockUserSub)
                == listOf("urn:ngsi-ld:Entity:1", "urn:ngsi-ld:Entity:3", "urn:ngsi-ld:Entity:4").toListOfUri()
        )
    }

    @Test
    fun `it should filter entities which have an access policy set to auth read`() {
        val entitiesId = (1..5).map { "urn:ngsi-ld:Entity:$it" }.toListOfUri()

        every {
            neo4jAuthorizationRepository.filterEntitiesWithSpecificAccessPolicy(
                entitiesId,
                listOf(SpecificAccessPolicy.AUTH_WRITE.name, SpecificAccessPolicy.AUTH_READ.name)
            )
        } returns listOf("urn:ngsi-ld:Entity:1", "urn:ngsi-ld:Entity:4").toListOfUri()
        every { neo4jAuthorizationRepository.getSubjectRoles(mockUserUri) } returns emptySet()
        every {
            neo4jAuthorizationRepository.filterEntitiesUserHasOneOfGivenRights(
                mockUserUri,
                listOf("urn:ngsi-ld:Entity:2", "urn:ngsi-ld:Entity:3", "urn:ngsi-ld:Entity:5").toListOfUri(),
                READ_RIGHTS
            )
        } returns emptyList()

        assert(
            neo4jAuthorizationService.filterEntitiesUserCanRead(entitiesId, mockUserSub)
                == listOf("urn:ngsi-ld:Entity:1", "urn:ngsi-ld:Entity:4").toListOfUri()
        )
    }

    @Test
    fun `it should filter entities mixed with write right and auth write access policy`() {
        val entitiesId = (1..5).map { "urn:ngsi-ld:Entity:$it" }.toListOfUri()

        every {
            neo4jAuthorizationRepository.filterEntitiesWithSpecificAccessPolicy(
                entitiesId,
                listOf(SpecificAccessPolicy.AUTH_WRITE.name)
            )
        } returns listOf("urn:ngsi-ld:Entity:1", "urn:ngsi-ld:Entity:4").toListOfUri()
        every { neo4jAuthorizationRepository.getSubjectRoles(mockUserUri) } returns emptySet()
        every {
            neo4jAuthorizationRepository.filterEntitiesUserHasOneOfGivenRights(
                mockUserUri,
                listOf("urn:ngsi-ld:Entity:2", "urn:ngsi-ld:Entity:3", "urn:ngsi-ld:Entity:5").toListOfUri(),
                WRITE_RIGHTS
            )
        } returns listOf("urn:ngsi-ld:Entity:3").toListOfUri()

        assert(
            neo4jAuthorizationService.filterEntitiesUserCanUpdate(entitiesId, mockUserSub)
                == listOf("urn:ngsi-ld:Entity:1", "urn:ngsi-ld:Entity:4", "urn:ngsi-ld:Entity:3").toListOfUri()
        )
    }

    @Test
    fun `it should keep all entities if user has admin rights`() {
        val entitiesIds = listOf(entityUri)

        every {
            neo4jAuthorizationRepository.filterEntitiesWithSpecificAccessPolicy(
                entitiesIds,
                listOf(SpecificAccessPolicy.AUTH_WRITE.name, SpecificAccessPolicy.AUTH_READ.name)
            )
        } returns emptyList()
        every {
            neo4jAuthorizationRepository.getSubjectRoles(mockUserUri)
        } returns setOf(GlobalRole.STELLIO_ADMIN.key)

        assert(
            neo4jAuthorizationService.filterEntitiesUserCanRead(entitiesIds, mockUserSub) == entitiesIds
        )
    }

    @Test
    fun `it should create an admin link to an entity`() {
        every {
            neo4jAuthorizationRepository.createAdminLinks(any(), any(), any())
        } returns listOf("urn:ngsi-ld:Relationship:01").toListOfUri()

        neo4jAuthorizationService.createAdminLink(entityUri, mockUserSub)

        verify {
            neo4jAuthorizationRepository.createAdminLinks(
                mockUserUri,
                match {
                    it.size == 1 &&
                        it[0].type == listOf(AUTH_REL_CAN_ADMIN) &&
                        it[0].datasetId == "urn:ngsi-ld:Dataset:rCanAdmin:$entityUri".toUri()
                },
                listOf(entityUri)
            )
        }
        confirmVerified()
    }

    private fun assertUserHasRightOnEntity(
        userHasRightOnEntity: (URI, Option<Sub>) -> Boolean,
        hasGrantedAccess: Boolean,
        hasSpecificPolicyAccess: Boolean
    ) {
        every { neo4jAuthorizationRepository.getSubjectRoles(mockUserUri) } returns emptySet()
        every {
            neo4jAuthorizationRepository.filterEntitiesUserHasOneOfGivenRights(
                mockUserUri,
                listOf(entityUri),
                any()
            )
        } returns if (hasGrantedAccess) listOf(entityUri) else emptyList()
        every {
            neo4jAuthorizationRepository.filterEntitiesWithSpecificAccessPolicy(
                any(),
                any()
            )
        } returns if (hasSpecificPolicyAccess) listOf(entityUri) else emptyList()

        assert(userHasRightOnEntity(entityUri, mockUserSub) == (hasGrantedAccess || hasSpecificPolicyAccess))
    }
}
