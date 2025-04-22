package com.egm.stellio.search.authorization.permission.service

import com.egm.stellio.search.authorization.permission.model.Action
import com.egm.stellio.search.authorization.permission.model.Permission
import com.egm.stellio.search.authorization.permission.model.Permission.Companion.notFoundMessage
import com.egm.stellio.search.authorization.permission.model.PermissionFilters
import com.egm.stellio.search.authorization.permission.model.TargetAsset
import com.egm.stellio.search.support.WithKafkaContainer
import com.egm.stellio.search.support.WithTimescaleContainer
import com.egm.stellio.shared.model.AlreadyExistsException
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.util.APIC_COMPOUND_CONTEXTS
import com.egm.stellio.shared.util.BEEHIVE_TYPE
import com.egm.stellio.shared.util.BEEKEEPER_TYPE
import com.egm.stellio.shared.util.DEVICE_TYPE
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import com.egm.stellio.shared.util.loadSampleData
import com.egm.stellio.shared.util.shouldFailWith
import com.egm.stellio.shared.util.shouldSucceed
import com.egm.stellio.shared.util.shouldSucceedAndResult
import com.egm.stellio.shared.util.shouldSucceedWith
import com.egm.stellio.shared.util.toUri
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.TestPropertySource
import java.util.UUID

@SpringBootTest
@ActiveProfiles("test")
@TestPropertySource(properties = ["application.authentication.enabled=false"])
class PermissionServiceTests : WithTimescaleContainer, WithKafkaContainer() {

    @Autowired
    private lateinit var permissionService: PermissionService

    @Autowired
    private lateinit var r2dbcEntityTemplate: R2dbcEntityTemplate

    private val userSub = UUID.randomUUID().toString()

    @AfterEach
    fun deletePermissions() {
        r2dbcEntityTemplate.delete(Permission::class.java)
            .all()
            .block()
    }

    fun loadAndDeserializePermission(
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
        val permission =
            loadAndDeserializePermission("permission/permission_minimal_entities.json")
        permissionService.create(permission).shouldSucceed()
        permissionService.create(permission).shouldFailWith {
            it is AlreadyExistsException
        }
    }

    @Test
    fun `get a minimal Permission should return the created Permission`() = runTest {
        val permission =
            loadAndDeserializePermission("permission/permission_minimal_entities.json")
        permissionService.create(permission).shouldSucceed()

        permissionService.getById(
            permission.id
        ).shouldSucceedWith {
            assertEquals(permission, it)
        }
    }

    @Test
    fun `get a full Permission should return the created Permission`() = runTest {
        val permission =
            loadAndDeserializePermission("permission/permission_full.json")
        permissionService.create(permission).shouldSucceed()

        permissionService.getById(
            permission.id
        ).shouldSucceedWith {
            assertEquals(permission, it)
        }
    }

    @Test
    fun `query Permission on entities ids should return Permissions matching this id`() = runTest {
        val permission =
            loadAndDeserializePermission("permission/permission_minimal_entities.json")
        permissionService.create(permission).shouldSucceed()

        val matchingPermissions = permissionService.getPermissions(
            PermissionFilters(ids = setOf("urn:ngsi-ld:BeeHive:A456".toUri()))
        )
        assertTrue(matchingPermissions.isRight())
        assertThat(matchingPermissions.getOrNull()).hasSize(1)
    }

    @Test
    fun `query Permission on entities ids should return an empty list if no Permission matches`() = runTest {
        val permission =
            loadAndDeserializePermission("permission/permission_minimal_entities.json")
        permissionService.create(permission).shouldSucceed()

        val matchingPermissions = permissionService.getPermissions(
            PermissionFilters(ids = setOf("urn:ngsi-ld:Vehicle:A457".toUri()))
        )
        assertTrue(matchingPermissions.isRight())
        assertThat(matchingPermissions.getOrNull()).isEmpty()
    }

    @Test
    fun `query Permission on action should return all Permission matching the right`() = runTest {
        val readPermission = loadAndDeserializePermission("permission/permission_minimal_entities.json")
            .copy(id = "urn:readPermission".toUri(), action = Action.READ)
        permissionService.create(readPermission).shouldSucceed()

        val writePermission = loadAndDeserializePermission("permission/permission_minimal_entities.json")
            .copy(id = "urn:writePermission".toUri(), action = Action.WRITE)
        permissionService.create(writePermission).shouldSucceed()

        val adminPermission = loadAndDeserializePermission("permission/permission_minimal_entities.json")
            .copy(id = "urn:adminPermission".toUri(), action = Action.ADMIN)
        permissionService.create(adminPermission).shouldSucceed()

        val ownPermission = loadAndDeserializePermission("permission/permission_minimal_entities.json")
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
        val permission =
            loadAndDeserializePermission("permission/permission_minimal_entities.json")
                .copy(assignee = userSub)
        permissionService.create(permission).shouldSucceed()

        val matchingPermissions = permissionService.getPermissions(
            PermissionFilters(assignee = userSub)
        )

        assertTrue(matchingPermissions.isRight())
        assertThat(matchingPermissions.getOrNull()).hasSize(1)

        val notMatchingPermission = permissionService.getPermissions(
            PermissionFilters(assignee = "i:am:not:matching")
        )
        assertTrue(notMatchingPermission.isRight())
        assertThat(notMatchingPermission.getOrNull()).isEmpty()
    }

    @Test
    fun `query on Permission assigner should filter the result`() = runTest {
        val permission =
            loadAndDeserializePermission("permission/permission_minimal_entities.json")
                .copy(assigner = userSub)
        permissionService.create(permission).shouldSucceed()

        val matchingPermissions = permissionService.getPermissions(
            PermissionFilters(assigner = userSub)
        )

        assertTrue(matchingPermissions.isRight())
        assertThat(matchingPermissions.getOrNull()).hasSize(1)

        val notMatchingPermission = permissionService.getPermissions(
            PermissionFilters(assigner = "i:am:not:matching")
        )
        assertTrue(notMatchingPermission.isRight())
        assertThat(notMatchingPermission.getOrNull()).isEmpty()
    }

    @Test
    fun `query on Permission target types should filter the result`() = runTest {
        val permission =
            loadAndDeserializePermission("permission/permission_minimal_entities.json")
                .copy(target = TargetAsset(types = listOf(BEEHIVE_TYPE, BEEKEEPER_TYPE)))

        permissionService.create(permission).shouldSucceed()
        val onePermissionMatching = permissionService.getPermissions(
            PermissionFilters(typeSelection = BEEHIVE_TYPE)
        )
        assertEquals(listOf(permission), onePermissionMatching.getOrNull())

        val multipleTypesOnePermissionMatching = permissionService.getPermissions(
            PermissionFilters(typeSelection = "$BEEHIVE_TYPE|$DEVICE_TYPE")
        )
        assertEquals(listOf(permission), multipleTypesOnePermissionMatching.getOrNull())

        val notMatchingPermission = permissionService.getPermissions(
            PermissionFilters(typeSelection = DEVICE_TYPE)
        )
        assertTrue(notMatchingPermission.isRight())
        assertThat(notMatchingPermission.getOrNull()).isEmpty()
    }

    @Test
    fun `query on Permission target scopes should filter the result`() = runTest {
        val permission =
            loadAndDeserializePermission("permission/permission_minimal_entities.json")
                .copy(target = TargetAsset(scope = "/my/scope"))

        permissionService.create(permission).shouldSucceed()
        val permissionMatching = permissionService.getPermissions(
            PermissionFilters(scopeSelection = "/my/scope")
        )
        assertEquals(listOf(permission), permissionMatching.getOrNull())

        val permissionmatchingWithWildcard = permissionService.getPermissions(
            PermissionFilters(typeSelection = "/my/#")
        )
        assertEquals(listOf(permission), permissionmatchingWithWildcard.getOrNull())

        val notMatchingPermission = permissionService.getPermissions(
            PermissionFilters(typeSelection = "/not/my/scope")
        )
        assertTrue(notMatchingPermission.isRight())
        assertThat(notMatchingPermission.getOrNull()).isEmpty()
    }

    @Test
    fun `count should apply the filter`() = runTest {
        val permission =
            loadAndDeserializePermission("permission/permission_minimal_entities.json")
        permissionService.create(permission).shouldSucceed()

        val count = permissionService.getPermissionsCount(
            PermissionFilters(typeSelection = "*")
        )
        assertEquals(1, count.getOrNull())

        val countEmpty = permissionService.getPermissionsCount(
            PermissionFilters(typeSelection = "INVALID")
        )
        assertEquals(0, countEmpty.getOrNull())
    }

    @Test
    fun `delete an existing Permission should succeed`() = runTest {
        val permission =
            loadAndDeserializePermission("permission/permission_minimal_entities.json")
        permissionService.create(permission).shouldSucceed()

        permissionService.delete(permission.id).shouldSucceed()

        permissionService.getById(permission.id).shouldFailWith {
            it is ResourceNotFoundException &&
                it.message == notFoundMessage(permission.id)
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
}
