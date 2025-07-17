package com.egm.stellio.search.authorization.permission.service

import arrow.core.right
import com.egm.stellio.search.authorization.permission.model.Action
import com.egm.stellio.search.authorization.permission.model.Permission
import com.egm.stellio.search.authorization.permission.model.Permission.Companion.notFoundMessage
import com.egm.stellio.search.authorization.permission.model.PermissionFilters
import com.egm.stellio.search.authorization.permission.model.PermissionFilters.Companion.OnlyGetPermission
import com.egm.stellio.search.authorization.permission.model.TargetAsset
import com.egm.stellio.search.authorization.subject.USER_UUID
import com.egm.stellio.search.authorization.subject.service.SubjectReferentialService
import com.egm.stellio.search.entity.model.Entity
import com.egm.stellio.search.entity.service.EntityService
import com.egm.stellio.search.support.WithKafkaContainer
import com.egm.stellio.search.support.WithTimescaleContainer
import com.egm.stellio.shared.WithMockCustomUser
import com.egm.stellio.shared.model.AlreadyExistsException
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.util.APIC_COMPOUND_CONTEXTS
import com.egm.stellio.shared.util.BEEHIVE_IRI
import com.egm.stellio.shared.util.BEEKEEPER_IRI
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import com.egm.stellio.shared.util.Sub
import com.egm.stellio.shared.util.loadAndPrepareSampleData
import com.egm.stellio.shared.util.loadSampleData
import com.egm.stellio.shared.util.shouldFailWith
import com.egm.stellio.shared.util.shouldSucceed
import com.egm.stellio.shared.util.shouldSucceedAndResult
import com.egm.stellio.shared.util.shouldSucceedWith
import com.egm.stellio.shared.util.toUri
import com.ninjasquad.springmockk.MockkBean
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.slot
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.data.r2dbc.core.delete
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.TestPropertySource
import java.util.*

@SpringBootTest
@ActiveProfiles("test")
@TestPropertySource(properties = ["application.authentication.enabled=true"])
class PermissionServiceTests : WithTimescaleContainer, WithKafkaContainer() {

    @Autowired
    private lateinit var entityService: EntityService

    @Autowired
    private lateinit var permissionService: PermissionService

    @Autowired
    private lateinit var r2dbcEntityTemplate: R2dbcEntityTemplate

    @MockkBean
    private lateinit var subjectReferentialService: SubjectReferentialService

    private val userUuid = "55e64faf-4bda-41cc-98b0-195874cefd29"
    private val groupUuid = UUID.randomUUID().toString()
    private val entityId = "urn:ngsi-ld:Entity:01".toUri()
    val minimalPermission = loadAndDeserializePermission("permission/permission_minimal.json")

    @BeforeEach
    fun setDefaultBehaviorOnSubjectReferential() {
        coEvery {
            subjectReferentialService.getSubjectAndGroupsUUID()
        } answers { listOf(userUuid).right() }
        val capturedSub = slot<Sub>()
        coEvery { subjectReferentialService.hasStellioAdminRole(any()) } returns false.right()
        coEvery { subjectReferentialService.getSubjectAndGroupsUUID(capture(capturedSub)) } answers {
            listOfNotNull(capturedSub.captured).right()
        }
    }

    @AfterEach
    fun deletePermissions() {
        r2dbcEntityTemplate.delete(Permission::class.java).all().block()
        r2dbcEntityTemplate.delete<Entity>().from("entity_payload").all().block()
    }

    final fun loadAndDeserializePermission(
        filename: String,
        contexts: List<String> = APIC_COMPOUND_CONTEXTS
    ): Permission {
        val permissionPayload = loadSampleData(filename)
        return deserializePermission(permissionPayload, contexts)
    }

    fun deserializePermission(
        permissionPayload: String,
        contexts: List<String> = APIC_COMPOUND_CONTEXTS
    ): Permission =
        Permission.deserialize(permissionPayload.deserializeAsMap(), contexts)
            .shouldSucceedAndResult()

    @Test
    fun `create a second Permission with the same id should return an AlreadyExist error`() = runTest {
        permissionService.create(minimalPermission).shouldSucceed()
        permissionService.create(minimalPermission).shouldFailWith {
            it is AlreadyExistsException
        }
    }

    @Test
    fun `create a second Permission with the same target and assignee should return an error`() = runTest {
        permissionService.create(minimalPermission).shouldSucceed()
        assertTrue(
            runCatching {
                permissionService.create(minimalPermission.copy(id = "urn:ngsi-ld:Permission:differentId".toUri()))
            }.isFailure
        )
    }

    @Test
    fun `get a minimal Permission should return the created Permission`() = runTest {
        permissionService.create(minimalPermission).shouldSucceed()

        permissionService.getById(
            minimalPermission.id
        ).shouldSucceedWith {
            assertEquals(minimalPermission, it)
        }
    }

    @Test
    fun `query Permission on entities ids should return Permissions matching this id`() = runTest {
        permissionService.create(minimalPermission).shouldSucceed()

        val matchingPermissions = permissionService.getPermissions(
            PermissionFilters(
                ids = setOf("urn:ngsi-ld:BeeHive:A456".toUri()),
                onlyGetPermission = OnlyGetPermission.ASSIGNED
            )
        )
        assertTrue(matchingPermissions.isRight())
        assertThat(matchingPermissions.getOrNull()).hasSize(1)
    }

    @Test
    fun `query Permission on entities ids should return an empty list if no Permission matches`() = runTest {
        permissionService.create(minimalPermission).shouldSucceed()

        val matchingPermissions = permissionService.getPermissions(
            PermissionFilters(ids = setOf("urn:ngsi-ld:Vehicle:A457".toUri()))
        )
        assertTrue(matchingPermissions.isRight())
        assertThat(matchingPermissions.getOrNull()).isEmpty()
    }

    @Test
    fun `query Permission on action should return all Permission matching the action`() = runTest {
        val readPermission = loadAndDeserializePermission("permission/permission_minimal.json")
            .copy(id = "urn:readPermission".toUri(), action = Action.READ)
        permissionService.create(readPermission).shouldSucceed()

        val writePermission = loadAndDeserializePermission("permission/permission_minimal.json")
            .copy(id = "urn:writePermission".toUri(), action = Action.WRITE)
        permissionService.create(writePermission).shouldSucceed()

        val adminPermission = loadAndDeserializePermission("permission/permission_minimal.json")
            .copy(id = "urn:adminPermission".toUri(), action = Action.ADMIN)
        permissionService.create(adminPermission).shouldSucceed()

        val ownPermission = loadAndDeserializePermission("permission/permission_minimal.json")
            .copy(id = "urn:ownPermission".toUri(), action = Action.OWN)
        permissionService.create(ownPermission).shouldSucceed()

        val readFilterAnswer = permissionService.getPermissions(
            PermissionFilters(action = Action.READ)
        )
        assertTrue(readFilterAnswer.isRight())
        assertThat(readFilterAnswer.getOrNull()).hasSize(4)

        val adminFilterAnswer = permissionService.getPermissions(
            PermissionFilters(action = Action.ADMIN)
        )
        assertTrue(adminFilterAnswer.isRight())
        assertThat(adminFilterAnswer.getOrNull()).hasSize(2)
    }

    @Test
    fun `query on Permission assignee should filter the result`() = runTest {
        val permission = minimalPermission.copy(assignee = userUuid)
        permissionService.create(permission).shouldSucceed()

        val matchingPermissions = permissionService.getPermissions(
            PermissionFilters(assignee = userUuid, onlyGetPermission = OnlyGetPermission.ASSIGNED)
        )

        assertTrue(matchingPermissions.isRight())
        assertThat(matchingPermissions.getOrNull()).hasSize(1)

        val notMatchingPermission = permissionService.getPermissions(
            PermissionFilters(assignee = "i:am:not:matching", onlyGetPermission = OnlyGetPermission.ASSIGNED)
        )
        assertTrue(notMatchingPermission.isRight())
        assertThat(notMatchingPermission.getOrNull()).isEmpty()
    }

    @Test
    fun `query on Permission assigner should filter the result`() = runTest {
        val permission = minimalPermission.copy(assigner = userUuid)
        permissionService.create(permission).shouldSucceed()

        val matchingPermissions = permissionService.getPermissions(
            PermissionFilters(assigner = userUuid, onlyGetPermission = OnlyGetPermission.ASSIGNED)
        )

        assertTrue(matchingPermissions.isRight())
        assertThat(matchingPermissions.getOrNull()).hasSize(1)

        val notMatchingPermission = permissionService.getPermissions(
            PermissionFilters(assigner = "i:am:not:matching", onlyGetPermission = OnlyGetPermission.ASSIGNED)
        )
        assertTrue(notMatchingPermission.isRight())
        assertThat(notMatchingPermission.getOrNull()).isEmpty()
    }

    @Test
    fun `query on Permissions should return nothing if subject is nor assignee nor administrator of the target`() =
        runTest {
            val permission = minimalPermission
                .copy(assignee = "not-the-subject", assigner = userUuid)
            permissionService.create(permission).shouldSucceed()

            val matchingPermissions = permissionService.getPermissions(
                PermissionFilters(assigner = userUuid)
            )

            assertTrue(matchingPermissions.isRight())
            assertThat(matchingPermissions.getOrNull()).isEmpty()
        }

    @Test
    @WithMockCustomUser(sub = USER_UUID, name = "Mock User")
    fun `query Permission on target entities type should return an empty list if no Permission matches`() = runTest {
        coEvery {
            subjectReferentialService.hasOneOfGlobalRoles(any(), any())
        } returns true.right() // allow entity creation

        val (expandedEntity, ngsiLdEntity) =
            loadAndPrepareSampleData("beehive_minimal.jsonld").shouldSucceedAndResult()
        val entityId = "urn:ngsi-ld:BeeHive:TESTC".toUri()
        entityService.createEntity(
            ngsiLdEntity,
            expandedEntity
        ).shouldSucceed()

        val permission = minimalPermission.copy(target = TargetAsset(id = entityId))
        permissionService.create(permission).shouldSucceed()

        val matchingPermissions = permissionService.getPermissions(
            PermissionFilters(targetTypeSelection = "$BEEKEEPER_IRI", onlyGetPermission = OnlyGetPermission.ASSIGNED)
        )
        assertTrue(matchingPermissions.isRight())
        assertThat(matchingPermissions.getOrNull()).isEmpty()
    }

    @Test
    @WithMockCustomUser(sub = USER_UUID, name = "Mock User")
    fun `query Permission on target entities type should return Permissions matching this type`() = runTest {
        coEvery {
            subjectReferentialService.hasOneOfGlobalRoles(any(), any())
        } returns true.right() // allow entity creation

        coEvery {
            subjectReferentialService.getSubjectAndGroupsUUID()
        } answers { listOf(USER_UUID).right() }

        val (expandedEntity, ngsiLdEntity) =
            loadAndPrepareSampleData("beehive_minimal.jsonld").shouldSucceedAndResult()
        val entityId = "urn:ngsi-ld:BeeHive:TESTC".toUri()
        entityService.createEntity(
            ngsiLdEntity,
            expandedEntity
        ).shouldSucceed()

        coEvery {
            subjectReferentialService.getSubjectAndGroupsUUID(USER_UUID)
        } returns listOf(USER_UUID).right()

        val permission = minimalPermission.copy(assignee = USER_UUID, target = TargetAsset(id = entityId))
        permissionService.create(permission).shouldSucceed()

        val matchingPermissions = permissionService.getPermissions(
            PermissionFilters(targetTypeSelection = BEEHIVE_IRI, onlyGetPermission = OnlyGetPermission.ASSIGNED)
        )
        assertTrue(matchingPermissions.isRight())
        assertThat(matchingPermissions.getOrNull()).hasSize(2) // created permission and owner permission
    }

    @Test
    fun `count on Permission should apply the filter`() = runTest {
        val permission = minimalPermission.copy(assignee = userUuid)
        val invalidUser = "INVALID"
        permissionService.create(permission).shouldSucceed()

        coEvery {
            subjectReferentialService.getSubjectAndGroupsUUID(userUuid)
        } returns listOf(userUuid).right()

        val count = permissionService.getPermissionsCount(
            PermissionFilters(assignee = userUuid, onlyGetPermission = OnlyGetPermission.ASSIGNED),
        )
        assertEquals(1, count.getOrNull())

        coEvery {
            subjectReferentialService.getSubjectAndGroupsUUID(invalidUser)
        } returns listOf(invalidUser).right()

        val countEmpty = permissionService.getPermissionsCount(
            filters = PermissionFilters(assignee = invalidUser, onlyGetPermission = OnlyGetPermission.ASSIGNED)
        )
        assertEquals(0, countEmpty.getOrNull())
    }

    @Test
    fun `delete an existing Permission should succeed`() = runTest {
        permissionService.create(minimalPermission).shouldSucceed()

        permissionService.delete(minimalPermission.id).shouldSucceed()

        permissionService.getById(minimalPermission.id).shouldFailWith {
            it is ResourceNotFoundException &&
                it.message == notFoundMessage(minimalPermission.id)
        }
    }

    @Test
    fun `delete a non existing Permission should return a RessourceNotFound error`() = runTest {
        val id = "urn:ngsi-ld:Permission:UnknownPermission".toUri()
        permissionService.delete(
            id
        ).shouldFailWith {
            it is ResourceNotFoundException &&
                it.message == notFoundMessage(id)
        }
    }

    @Test
    fun `removePermissionsOnEntity should remove all permissions on entity`() = runTest {
        permissionService.create(
            Permission(
                assignee = userUuid,
                target = TargetAsset(entityId),
                action = Action.READ,
                assigner = userUuid
            )
        )

        permissionService.removePermissionsOnEntity(entityId)

        val permissions = permissionService.getPermissions(PermissionFilters(ids = setOf(entityId)))

        assertTrue(permissions.isRight())
        assertThat(permissions.getOrNull()).isEmpty()
    }

    @Test
    fun `hasPermissionOnEntity should not allow a user having no permission`() = runTest {
        coEvery { subjectReferentialService.hasStellioAdminRole(listOf(userUuid)) } returns false.right()

        coEvery {
            subjectReferentialService.getSubjectAndGroupsUUID()
        } returns listOf(userUuid).right()

        permissionService.checkHasPermissionOnEntity(entityId, Action.READ)
            .shouldSucceedWith { assertFalse(it) }

        coVerify {
            subjectReferentialService.hasStellioAdminRole(listOf(userUuid))
        }
    }

    @Test
    fun `hasPermissionOnEntity should allow a user having a direct permission on a entity`() = runTest {
        coEvery { subjectReferentialService.hasStellioAdminRole(listOf(userUuid)) } returns false.right()

        coEvery {
            subjectReferentialService.getSubjectAndGroupsUUID()
        } returns listOf(userUuid).right()

        permissionService.create(
            Permission(
                assignee = userUuid,
                target = TargetAsset(entityId),
                action = Action.READ,
                assigner = userUuid
            )
        )

        permissionService.checkHasPermissionOnEntity(entityId, Action.READ)
            .shouldSucceedWith { assertTrue(it) }
    }

    @Test
    fun `hasPermissionOnEntity should allow a user to read if it has a write permission`() = runTest {
        coEvery { subjectReferentialService.hasStellioAdminRole(listOf(userUuid)) } returns false.right()

        coEvery {
            subjectReferentialService.getSubjectAndGroupsUUID()
        } returns listOf(userUuid).right()

        permissionService.create(
            Permission(
                assignee = userUuid,
                target = TargetAsset(entityId),
                action = Action.WRITE,
                assigner = userUuid
            )
        )

        permissionService.checkHasPermissionOnEntity(entityId, Action.READ)
            .shouldSucceedWith { assertTrue(it) }
    }

    @Test
    fun `hasPermissionOnEntity should allow a user having permission via a group membership`() = runTest {
        coEvery { subjectReferentialService.hasStellioAdminRole(any()) } returns false.right()

        coEvery {
            subjectReferentialService.getSubjectAndGroupsUUID()
        } returns listOf(groupUuid, userUuid).right()

        permissionService.create(
            Permission(
                assignee = groupUuid,
                target = TargetAsset(entityId),
                action = Action.READ,
                assigner = userUuid
            )
        )

        permissionService.checkHasPermissionOnEntity(entityId, Action.READ)
            .shouldSucceedWith { assertTrue(it) }
    }

    @Test
    fun `hasPermissionOnEntity should allow a user having the stellio-admin role`() = runTest {
        coEvery { subjectReferentialService.hasStellioAdminRole(listOf(userUuid)) } returns true.right()

        coEvery {
            subjectReferentialService.getSubjectAndGroupsUUID()
        } returns listOf(userUuid).right()

        permissionService.checkHasPermissionOnEntity(entityId, Action.READ)
            .shouldSucceedWith {
                assertTrue(it)
            }

        coVerify {
            subjectReferentialService.hasStellioAdminRole(listOf(userUuid))
        }
        coVerify(exactly = 0) {
            subjectReferentialService.retrieve(eq(userUuid))
        }
    }
}
