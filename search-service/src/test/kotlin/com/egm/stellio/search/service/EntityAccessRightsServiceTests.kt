package com.egm.stellio.search.service

import arrow.core.Some
import com.egm.stellio.search.model.EntityAccessRights
import com.egm.stellio.search.support.WithTimescaleContainer
import com.egm.stellio.shared.model.AccessDeniedException
import com.egm.stellio.shared.util.toUri
import com.ninjasquad.springmockk.MockkBean
import io.mockk.Called
import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.verify
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.test.context.ActiveProfiles
import reactor.core.publisher.Mono
import reactor.test.StepVerifier

@SpringBootTest
@ActiveProfiles("test")
class EntityAccessRightsServiceTests : WithTimescaleContainer {

    @Autowired
    private lateinit var entityAccessRightsService: EntityAccessRightsService

    @Autowired
    private lateinit var r2dbcEntityTemplate: R2dbcEntityTemplate

    @MockkBean(relaxed = true)
    private lateinit var subjectReferentialService: SubjectReferentialService

    @MockkBean
    private lateinit var temporalEntityAttributeService: TemporalEntityAttributeService

    private val subjectUuid = "0768A6D5-D87B-4209-9A22-8C40A8961A79"
    private val groupUuid = "220FC854-3609-404B-BC77-F2DFE332B27B"
    private val entityId = "urn:ngsi-ld:Entity:1111".toUri()

    @BeforeEach
    fun setDefaultBehaviorOnSubjectReferential() {
        every { subjectReferentialService.hasStellioAdminRole(Some(subjectUuid)) } answers { Mono.just(false) }
        every {
            subjectReferentialService.getSubjectAndGroupsUUID(Some(subjectUuid))
        } answers { Mono.just(listOf(subjectUuid)) }
    }

    @AfterEach
    fun clearEntityAccessRightsTable() {
        r2dbcEntityTemplate.delete(EntityAccessRights::class.java)
            .all()
            .block()
    }

    @Test
    fun `it should add a new entity in the allowed list of read entities`() {
        StepVerifier
            .create(entityAccessRightsService.setReadRoleOnEntity(subjectUuid, entityId))
            .expectNextMatches { it == 1 }
            .expectComplete()
            .verify()

        every { temporalEntityAttributeService.hasSpecificAccessPolicies(any(), any()) } returns Mono.just(true)

        runBlocking {
            entityAccessRightsService.canReadEntity(Some(subjectUuid), "urn:ngsi-ld:Entity:1111".toUri()).fold(
                { "it should have read right on entity" },
                {}
            )
        }
    }

    @Test
    fun `it should remove an entity from the allowed list of read entities`() {
        every { temporalEntityAttributeService.hasSpecificAccessPolicies(any(), any()) } returns Mono.just(true)

        StepVerifier
            .create(entityAccessRightsService.setReadRoleOnEntity(subjectUuid, entityId))
            .expectNextMatches { it == 1 }
            .expectComplete()
            .verify()

        StepVerifier
            .create(entityAccessRightsService.removeRoleOnEntity(subjectUuid, entityId))
            .expectNextMatches { it == 1 }
            .expectComplete()
            .verify()

        runBlocking {
            entityAccessRightsService.canReadEntity(Some(subjectUuid), entityId).fold(
                { assertEquals(AccessDeniedException("User forbidden read access to entity $entityId"), it.message) },
                { "it should have not read right on entity" }
            )
        }
    }

    @Test
    fun `it should remove an entity from the list of known entities`() {
        every { temporalEntityAttributeService.hasSpecificAccessPolicies(any(), any()) } returns Mono.just(true)

        StepVerifier
            .create(entityAccessRightsService.setAdminRoleOnEntity(subjectUuid, entityId))
            .expectNextMatches { it == 1 }
            .expectComplete()
            .verify()

        runBlocking {
            entityAccessRightsService.canReadEntity(Some(subjectUuid), entityId).fold(
                { "it should have read right on entity" },
                {}
            )
        }

        StepVerifier
            .create(entityAccessRightsService.removeRolesOnEntity(entityId))
            .expectNextMatches { it == 1 }
            .expectComplete()
            .verify()

        runBlocking {
            entityAccessRightsService.canReadEntity(Some(subjectUuid), entityId).fold(
                { assertEquals(AccessDeniedException("User forbidden read access to entity $entityId"), it.message) },
                { "it should have not read right on entity" }
            )
        }
    }

    @Test
    fun `it should allow an user having a direct read role on a entity`() {
        every { temporalEntityAttributeService.hasSpecificAccessPolicies(any(), any()) } returns Mono.just(true)

        StepVerifier
            .create(entityAccessRightsService.setReadRoleOnEntity(subjectUuid, entityId))
            .expectNextMatches { it == 1 }
            .expectComplete()
            .verify()

        StepVerifier
            .create(entityAccessRightsService.setWriteRoleOnEntity(subjectUuid, "urn:ngsi-ld:Entity:6666".toUri()))
            .expectNextMatches { it == 1 }
            .expectComplete()
            .verify()

        runBlocking {
            entityAccessRightsService.canReadEntity(Some(subjectUuid), entityId).fold(
                { "it should have read right on entity" },
                {}
            )
        }
        runBlocking {
            entityAccessRightsService.canReadEntity(Some(subjectUuid), "urn:ngsi-ld:Entity:2222".toUri()).fold(
                {
                    assertEquals(
                        AccessDeniedException("User forbidden read access to entity urn:ngsi-ld:Entity:2222"),
                        it.message
                    )
                },
                { "it should have not read right on entity" }
            )
        }
        runBlocking {
            entityAccessRightsService.canWriteEntity(Some(subjectUuid), "urn:ngsi-ld:Entity:6666".toUri()).fold(
                { "it should have write right on entity" },
                {}
            )
        }
    }

    @Test
    fun `it should allow an user having a read role on a entity via a group membership`() {
        every { temporalEntityAttributeService.hasSpecificAccessPolicies(any(), any()) } returns Mono.just(true)

        every {
            subjectReferentialService.getSubjectAndGroupsUUID(Some(subjectUuid))
        } answers { Mono.just(listOf(groupUuid, subjectUuid)) }

        entityAccessRightsService.setReadRoleOnEntity(groupUuid, entityId).block()

        runBlocking {
            entityAccessRightsService.canReadEntity(Some(subjectUuid), entityId).fold(
                { "it should have read right on entity" },
                {}
            )
        }
        runBlocking {
            entityAccessRightsService.canReadEntity(Some(subjectUuid), "urn:ngsi-ld:Entity:2222".toUri()).fold(
                {
                    assertEquals(
                        AccessDeniedException("User forbidden read access to entity urn:ngsi-ld:Entity:2222"),
                        it.message
                    )
                },
                { "it should have not read right on entity" }
            )
        }
    }

    @Test
    fun `it should allow an user having a read role on a entity both directly and via a group membership`() {
        every { temporalEntityAttributeService.hasSpecificAccessPolicies(any(), any()) } returns Mono.just(true)
        every {
            subjectReferentialService.getSubjectAndGroupsUUID(Some(subjectUuid))
        } answers { Mono.just(listOf(groupUuid, subjectUuid)) }

        entityAccessRightsService.setReadRoleOnEntity(groupUuid, entityId).block()
        entityAccessRightsService.setReadRoleOnEntity(subjectUuid, entityId).block()

        runBlocking {
            entityAccessRightsService.canReadEntity(Some(subjectUuid), entityId).fold(
                { "it should have read right on entity" },
                {}
            )
        }
        runBlocking {
            entityAccessRightsService.canReadEntity(Some(subjectUuid), "urn:ngsi-ld:Entity:2222".toUri()).fold(
                {
                    assertEquals(
                        AccessDeniedException("User forbidden read access to entity urn:ngsi-ld:Entity:2222"),
                        it.message
                    )
                },
                { "it should have not read right on entity" }
            )
        }
    }

    @Test
    fun `it should allow an user having the stellio-admin role to read any entity`() {
        every { subjectReferentialService.hasStellioAdminRole(Some(subjectUuid)) } answers { Mono.just(true) }

        runBlocking {
            entityAccessRightsService.canReadEntity(Some(subjectUuid), entityId).fold(
                { "it should have read right on entity" },
                {}
            )
        }
        runBlocking {
            entityAccessRightsService.canReadEntity(Some(subjectUuid), "urn:ngsi-ld:Entity:2222".toUri()).fold(
                { "it should have read right on entity" },
                {}
            )
        }

        verify {
            subjectReferentialService.hasStellioAdminRole(Some(subjectUuid))
            subjectReferentialService.retrieve(eq(subjectUuid)) wasNot Called
        }
        confirmVerified(subjectReferentialService)
    }

    @Test
    fun `it should return a null filter is user has the stellio-admin role`() {
        every { subjectReferentialService.hasStellioAdminRole(Some(subjectUuid)) } answers { Mono.just(true) }

        runBlocking {
            val accessRightFilter = entityAccessRightsService.computeAccessRightFilter(Some(subjectUuid))
            assertNull(accessRightFilter())
        }
    }

    @Test
    fun `it should return a valid entity filter if user does not have the stellio-admin role`() {
        every { subjectReferentialService.hasStellioAdminRole(Some(subjectUuid)) } answers { Mono.just(false) }
        every { subjectReferentialService.getSubjectAndGroupsUUID(Some(subjectUuid)) } answers {
            Mono.just(listOf(subjectUuid, groupUuid))
        }

        runBlocking {
            val accessRightFilter = entityAccessRightsService.computeAccessRightFilter(Some(subjectUuid))
            assertEquals(
                """
                ( 
                    (specific_access_policy = 'AUTH_READ' OR specific_access_policy = 'AUTH_WRITE')
                    OR
                    (entity_id IN (
                        SELECT entity_id
                        FROM entity_access_rights
                        WHERE subject_id IN ('$subjectUuid','$groupUuid')
                    ))
                )
                """.trimIndent(),
                accessRightFilter()
            )
        }
    }

    @Test
    fun `it should delete entity access rights associated to an user`() {
        entityAccessRightsService.setReadRoleOnEntity(subjectUuid, entityId).block()

        StepVerifier
            .create(entityAccessRightsService.delete(subjectUuid))
            .expectNextMatches { it == 1 }
            .expectComplete()
            .verify()
    }

    @Test
    fun `it should allow user who have read right on entity`() {
        entityAccessRightsService.setReadRoleOnEntity(subjectUuid, entityId).block()

        every { temporalEntityAttributeService.hasSpecificAccessPolicies(any(), any()) } returns Mono.just(false)

        runBlocking {
            entityAccessRightsService.canReadEntity(Some(subjectUuid), entityId).fold(
                { "it should have read right on entity" },
                {}
            )
        }
    }

    @Test
    fun `it should allow user who have write right on entity`() {
        entityAccessRightsService.setWriteRoleOnEntity(subjectUuid, entityId).block()

        every { temporalEntityAttributeService.hasSpecificAccessPolicies(any(), any()) } returns Mono.just(false)

        runBlocking {
            entityAccessRightsService.canWriteEntity(Some(subjectUuid), entityId).fold(
                { "it should have write right on entity" },
                {}
            )
        }
    }
    @Test
    fun `it should not allow user who have not read right on entity and entity has not a specific access policy`() {
        every { temporalEntityAttributeService.hasSpecificAccessPolicies(any(), any()) } returns Mono.just(false)

        runBlocking {
            entityAccessRightsService.canReadEntity(Some(subjectUuid), entityId).fold(
                { assertEquals("User forbidden read access to entity $entityId", it.message) },
                { fail("it should have not read right on entity") }
            )
        }
    }

    @Test
    fun `it should not allow user who have not write right on entity and entity has not a specific access policy`() {
        every { temporalEntityAttributeService.hasSpecificAccessPolicies(any(), any()) } returns Mono.just(false)

        runBlocking {
            entityAccessRightsService.canWriteEntity(Some(subjectUuid), entityId).fold(
                { assertEquals("User forbidden write access to entity $entityId", it.message) },
                { fail("it should have not write right on entity") }
            )
        }
    }
    @Test
    fun `it should allow user it has no right on entity but entity has a specific access policy`() {
        every { temporalEntityAttributeService.hasSpecificAccessPolicies(any(), any()) } returns Mono.just(true)

        runBlocking {
            entityAccessRightsService.canWriteEntity(Some(subjectUuid), entityId).fold(
                { fail("it should have write right on entity") },
                { }
            )
        }

        runBlocking {
            entityAccessRightsService.canReadEntity(Some(subjectUuid), entityId).fold(
                { fail("it should have read right on entity") },
                { }
            )
        }

        verify { subjectReferentialService.getSubjectAndGroupsUUID(Some(subjectUuid)) wasNot Called }
    }
}
