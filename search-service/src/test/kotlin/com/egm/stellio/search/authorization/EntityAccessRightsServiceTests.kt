package com.egm.stellio.search.authorization

import arrow.core.Some
import arrow.core.right
import com.egm.stellio.search.model.EntityAccessRights
import com.egm.stellio.search.service.TemporalEntityAttributeService
import com.egm.stellio.search.support.WithTimescaleContainer
import com.egm.stellio.shared.model.AccessDeniedException
import com.egm.stellio.shared.util.shouldSucceed
import com.egm.stellio.shared.util.toUri
import com.ninjasquad.springmockk.MockkBean
import io.mockk.Called
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.confirmVerified
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.fail
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.test.context.ActiveProfiles

@OptIn(ExperimentalCoroutinesApi::class)
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
        coEvery { subjectReferentialService.hasStellioAdminRole(Some(subjectUuid)) } answers { false.right() }
        coEvery {
            subjectReferentialService.getSubjectAndGroupsUUID(Some(subjectUuid))
        } answers { listOf(subjectUuid).right() }
    }

    @AfterEach
    fun clearEntityAccessRightsTable() {
        r2dbcEntityTemplate.delete(EntityAccessRights::class.java)
            .all()
            .block()
    }

    @Test
    fun `it should add a new entity in the allowed list of read entities`() = runTest {
        coEvery { temporalEntityAttributeService.hasSpecificAccessPolicies(any(), any()) } returns true.right()

        entityAccessRightsService.setReadRoleOnEntity(subjectUuid, entityId)

        entityAccessRightsService.canReadEntity(Some(subjectUuid), "urn:ngsi-ld:Entity:1111".toUri())
            .shouldSucceed()
    }

    @Test
    fun `it should remove an entity from the allowed list of read entities`() = runTest {
        coEvery { temporalEntityAttributeService.hasSpecificAccessPolicies(any(), any()) } returns false.right()

        entityAccessRightsService.setReadRoleOnEntity(subjectUuid, entityId)
        entityAccessRightsService.removeRoleOnEntity(subjectUuid, entityId)

        entityAccessRightsService.canReadEntity(Some(subjectUuid), entityId).fold(
            { assertEquals(AccessDeniedException("User forbidden read access to entity $entityId"), it) },
            { fail("it should have not read right on entity") }
        )
    }

    @Test
    fun `it should remove an entity from the list of known entities`() = runTest {
        coEvery { temporalEntityAttributeService.hasSpecificAccessPolicies(any(), any()) } returns false.right()

        entityAccessRightsService.setAdminRoleOnEntity(subjectUuid, entityId)

        entityAccessRightsService.canReadEntity(Some(subjectUuid), entityId).shouldSucceed()

        entityAccessRightsService.removeRolesOnEntity(entityId)

        entityAccessRightsService.canReadEntity(Some(subjectUuid), entityId).fold(
            { assertEquals(AccessDeniedException("User forbidden read access to entity $entityId"), it) },
            { fail("it should have not read right on entity") }
        )
    }

    @Test
    fun `it should allow an user having a direct read role on a entity`() = runTest {
        coEvery { temporalEntityAttributeService.hasSpecificAccessPolicies(any(), any()) } returns false.right()

        entityAccessRightsService.setReadRoleOnEntity(subjectUuid, entityId)
        entityAccessRightsService.setWriteRoleOnEntity(subjectUuid, "urn:ngsi-ld:Entity:6666".toUri())

        entityAccessRightsService.canReadEntity(Some(subjectUuid), entityId).shouldSucceed()
        entityAccessRightsService.canReadEntity(Some(subjectUuid), "urn:ngsi-ld:Entity:2222".toUri()).fold(
            {
                assertEquals(
                    AccessDeniedException("User forbidden read access to entity urn:ngsi-ld:Entity:2222"),
                    it
                )
            },
            { fail("it should have not read right on entity") }
        )
        entityAccessRightsService.canWriteEntity(Some(subjectUuid), "urn:ngsi-ld:Entity:6666".toUri()).shouldSucceed()
    }

    @Test
    fun `it should allow an user having a read role on a entity via a group membership`() = runTest {
        coEvery { temporalEntityAttributeService.hasSpecificAccessPolicies(any(), any()) } returns false.right()
        coEvery {
            subjectReferentialService.getSubjectAndGroupsUUID(Some(subjectUuid))
        } returns listOf(groupUuid, subjectUuid).right()

        entityAccessRightsService.setReadRoleOnEntity(groupUuid, entityId)

        entityAccessRightsService.canReadEntity(Some(subjectUuid), entityId).shouldSucceed()
        entityAccessRightsService.canReadEntity(Some(subjectUuid), "urn:ngsi-ld:Entity:2222".toUri()).fold(
            {
                assertEquals(
                    AccessDeniedException("User forbidden read access to entity urn:ngsi-ld:Entity:2222"),
                    it
                )
            },
            { fail("it should not have read right on entity") }
        )
    }

    @Test
    fun `it should allow an user having a read role on a entity both directly and via a group membership`() = runTest {
        coEvery { temporalEntityAttributeService.hasSpecificAccessPolicies(any(), any()) } returns false.right()
        coEvery {
            subjectReferentialService.getSubjectAndGroupsUUID(Some(subjectUuid))
        } answers { listOf(groupUuid, subjectUuid).right() }

        entityAccessRightsService.setReadRoleOnEntity(groupUuid, entityId)
        entityAccessRightsService.setReadRoleOnEntity(subjectUuid, entityId)

        entityAccessRightsService.canReadEntity(Some(subjectUuid), entityId).shouldSucceed()
        entityAccessRightsService.canReadEntity(Some(subjectUuid), "urn:ngsi-ld:Entity:2222".toUri()).fold(
            {
                assertEquals(
                    AccessDeniedException("User forbidden read access to entity urn:ngsi-ld:Entity:2222"),
                    it
                )
            },
            { fail("it should have not read right on entity") }
        )
    }

    @Test
    fun `it should allow an user having the stellio-admin role to read any entity`() = runTest {
        coEvery { subjectReferentialService.hasStellioAdminRole(Some(subjectUuid)) } returns true.right()

        entityAccessRightsService.canReadEntity(Some(subjectUuid), entityId).shouldSucceed()
        entityAccessRightsService.canReadEntity(Some(subjectUuid), "urn:ngsi-ld:Entity:2222".toUri()).shouldSucceed()

        coVerify {
            subjectReferentialService.hasStellioAdminRole(Some(subjectUuid))
            subjectReferentialService.retrieve(eq(subjectUuid)) wasNot Called
        }
        confirmVerified(subjectReferentialService)
    }

    @Test
    fun `it should delete entity access rights associated to an user`() = runTest {
        entityAccessRightsService.setReadRoleOnEntity(subjectUuid, entityId)

        entityAccessRightsService.delete(subjectUuid).shouldSucceed()
    }

    @Test
    fun `it should allow user who have read right on entity`() = runTest {
        coEvery { temporalEntityAttributeService.hasSpecificAccessPolicies(any(), any()) } returns false.right()

        entityAccessRightsService.setReadRoleOnEntity(subjectUuid, entityId)

        entityAccessRightsService.canReadEntity(Some(subjectUuid), entityId).shouldSucceed()
    }

    @Test
    fun `it should allow user who have write right on entity`() = runTest {
        coEvery { temporalEntityAttributeService.hasSpecificAccessPolicies(any(), any()) } returns false.right()

        entityAccessRightsService.setWriteRoleOnEntity(subjectUuid, entityId)

        entityAccessRightsService.canWriteEntity(Some(subjectUuid), entityId).shouldSucceed()
    }

    @Test
    fun `it should not allow user who have not read right on entity and entity has not a specific access policy`() =
        runTest {
            coEvery { temporalEntityAttributeService.hasSpecificAccessPolicies(any(), any()) } returns false.right()

            runBlocking {
                entityAccessRightsService.canReadEntity(Some(subjectUuid), entityId).fold(
                    { assertEquals("User forbidden read access to entity $entityId", it.message) },
                    { fail("it should have not read right on entity") }
                )
            }
        }

    @Test
    fun `it should not allow user who have not write right on entity and entity has not a specific access policy`() =
        runTest {
            coEvery { temporalEntityAttributeService.hasSpecificAccessPolicies(any(), any()) } returns false.right()

            entityAccessRightsService.canWriteEntity(Some(subjectUuid), entityId).fold(
                { assertEquals("User forbidden write access to entity $entityId", it.message) },
                { fail("it should have not write right on entity") }
            )
        }

    @Test
    fun `it should allow user it has no right on entity but entity has a specific access policy`() = runTest {
        coEvery { temporalEntityAttributeService.hasSpecificAccessPolicies(any(), any()) } returns true.right()

        entityAccessRightsService.canWriteEntity(Some(subjectUuid), entityId).shouldSucceed()
        entityAccessRightsService.canReadEntity(Some(subjectUuid), entityId).shouldSucceed()

        coVerify { subjectReferentialService.getSubjectAndGroupsUUID(Some(subjectUuid)) wasNot Called }
    }
}
