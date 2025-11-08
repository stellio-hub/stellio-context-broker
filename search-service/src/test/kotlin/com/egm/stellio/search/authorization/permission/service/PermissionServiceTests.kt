package com.egm.stellio.search.authorization.permission.service

import arrow.core.right
import com.egm.stellio.search.authorization.permission.model.Action
import com.egm.stellio.search.authorization.permission.model.Permission
import com.egm.stellio.search.authorization.permission.model.Permission.Companion.notFoundMessage
import com.egm.stellio.search.authorization.permission.model.PermissionFilters
import com.egm.stellio.search.authorization.permission.model.PermissionFilters.Companion.PermissionKind
import com.egm.stellio.search.authorization.permission.model.TargetAsset
import com.egm.stellio.search.authorization.permission.service.PermissionServiceTests.PermissionId.beehiveType
import com.egm.stellio.search.authorization.permission.service.PermissionServiceTests.PermissionId.beehiveTypeAndScopeA
import com.egm.stellio.search.authorization.permission.service.PermissionServiceTests.PermissionId.beehiveWithScope
import com.egm.stellio.search.authorization.permission.service.PermissionServiceTests.PermissionId.beekeeper
import com.egm.stellio.search.authorization.permission.service.PermissionServiceTests.PermissionId.scopeA
import com.egm.stellio.search.authorization.permission.support.PermissionUtils.gimmeRawPermission
import com.egm.stellio.search.authorization.subject.USER_UUID
import com.egm.stellio.search.authorization.subject.service.SubjectReferentialService
import com.egm.stellio.search.entity.model.Entity
import com.egm.stellio.search.entity.service.EntityService
import com.egm.stellio.search.support.WithKafkaContainer
import com.egm.stellio.search.support.WithTimescaleContainer
import com.egm.stellio.shared.WithMockCustomUser
import com.egm.stellio.shared.model.AlreadyExistsException
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.model.SeeOtherException
import com.egm.stellio.shared.util.APIARY_IRI
import com.egm.stellio.shared.util.APIC_COMPOUND_CONTEXTS
import com.egm.stellio.shared.util.BEEHIVE_IRI
import com.egm.stellio.shared.util.BEEKEEPER_IRI
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import com.egm.stellio.shared.util.Sub
import com.egm.stellio.shared.util.loadAndPrepareSampleData
import com.egm.stellio.shared.util.loadSampleData
import com.egm.stellio.shared.util.ngsiLdDateTime
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
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.data.r2dbc.core.delete
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.TestPropertySource
import java.util.UUID

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
    private val entityId = "urn:ngsi-ld:BeeHive:TESTC".toUri()
    val minimalPermission = loadAndDeserializePermission("permission/permission_minimal.json")
    val beehiveScope = "/A"

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
        permissionService.create(minimalPermission.copy(id = "urn:ngsi-ld:Permission:differentId".toUri()))
            .shouldFailWith { it is SeeOtherException }
    }

    @ParameterizedTest
    @CsvSource(
        // scopes  , types                         , new scopes, new types                                ,shouldSucceed
        "  '/1,/2' , '$BEEHIVE_IRI,$BEEKEEPER_IRI' , '/1,/2,/3', '$BEEHIVE_IRI'                           ,true",
        "  '/1,/2' , '$BEEHIVE_IRI,$BEEKEEPER_IRI' , '/3'      , '$BEEHIVE_IRI'                           ,true",
        "  '/1,/2' , '$BEEHIVE_IRI,$BEEKEEPER_IRI' , null      , '$BEEHIVE_IRI'                           ,true",
        "  '/1,/2' , '$BEEHIVE_IRI,$BEEKEEPER_IRI' , '/1'      , '$BEEHIVE_IRI,$BEEKEEPER_IRI,$APIARY_IRI',true",
        "  '/1,/2' , '$BEEHIVE_IRI,$BEEKEEPER_IRI' , '/1'      , '$APIARY_IRI'                            ,true",
        "  '/1,/2' , '$BEEHIVE_IRI,$BEEKEEPER_IRI' , '/2'      , null                                     ,true",
        "  '/1,/2' , '$BEEHIVE_IRI,$BEEKEEPER_IRI' , '/1,/2'   , '$BEEHIVE_IRI,$BEEKEEPER_IRI'            ,false",
        "  '/1,/2' , '$BEEHIVE_IRI,$BEEKEEPER_IRI' , '/1'      , '$BEEHIVE_IRI,$BEEKEEPER_IRI'            ,false",
        "  '/1,/2' , '$BEEHIVE_IRI,$BEEKEEPER_IRI' , '/1,/2'   , '$BEEHIVE_IRI'                           ,false",
        "  null    , '$BEEHIVE_IRI,$BEEKEEPER_IRI' , null      , '$APIARY_IRI'                            ,true",
        "  null    , '$BEEHIVE_IRI,$BEEKEEPER_IRI' , '/1'      , '$APIARY_IRI'                            ,true",
        "  null    , '$BEEHIVE_IRI,$BEEKEEPER_IRI' , '/1'      , '$BEEHIVE_IRI'                           ,false",
        "  null    , '$BEEHIVE_IRI,$BEEKEEPER_IRI' , null      , '$BEEHIVE_IRI'                           ,false",
        "  '/1,/2' , null                          , '/3'      , null                                     ,true",
        "  '/1,/2' , null                          , '/3'      , '$BEEHIVE_IRI'                           ,true",
        "  '/1,/2' , null                          , '/1'      , '$BEEHIVE_IRI'                           ,false",
        "  '/1,/2' , null                          , '/1'      ,  null                                    ,false",
        "  '/1,/2' , null                          , null      ,  null                                    ,true",
        nullValues = ["null"]
    )
    fun `checkDuplicate should fail only if all the scopes and all the types are included in an existing permission`(
        createdScopes: String?,
        createdTypes: String?,
        newScopes: String?,
        newTypes: String?,
        shouldSucceed: Boolean
    ) = runTest {
        val permission = Permission(
            assignee = userUuid,
            target = TargetAsset(scopes = createdScopes?.split(','), types = createdTypes?.split(',')),
            action = Action.READ,
            assigner = userUuid
        )
        permissionService.create(permission).shouldSucceed()

        permissionService.checkDuplicate(
            permission.copy(
                id = "urn:ngsi-ld:Permission:differentId".toUri(),
                target = TargetAsset(types = newTypes?.split(','), scopes = newScopes?.split(','))
            )
        ).let { if (shouldSucceed) it.shouldSucceed() else it.shouldFailWith { it is SeeOtherException } }
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
    fun `get a Permission with types and scopes should return the created Permission`() = runTest {
        val permision = Permission(
            assignee = userUuid,
            target = TargetAsset(scopes = listOf("/1"), types = listOf(BEEHIVE_IRI)),
            action = Action.READ,
            assigner = userUuid
        )
        permissionService.create(permision).shouldSucceed()

        permissionService.getById(
            permision.id
        ).shouldSucceedWith {
            assertEquals(permision, it)
        }
    }

    @Test
    fun `query Permission on entities ids should return Permissions matching this id`() = runTest {
        permissionService.create(minimalPermission).shouldSucceed()

        val matchingPermissions = permissionService.getPermissions(
            PermissionFilters(
                ids = setOf("urn:ngsi-ld:BeeHive:A456".toUri()),
                kind = PermissionKind.ASSIGNED
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

        coEvery {
            subjectReferentialService.hasStellioAdminRole(any())
        } returns true.right()

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
            PermissionFilters(assignee = userUuid, kind = PermissionKind.ASSIGNED)
        )

        assertTrue(matchingPermissions.isRight())
        assertThat(matchingPermissions.getOrNull()).hasSize(1)

        val notMatchingPermission = permissionService.getPermissions(
            PermissionFilters(assignee = "i:am:not:matching", kind = PermissionKind.ASSIGNED)
        )
        assertTrue(notMatchingPermission.isRight())
        assertThat(notMatchingPermission.getOrNull()).isEmpty()
    }

    @Test
    fun `query on Permission assigner should filter the result`() = runTest {
        val permission = minimalPermission.copy(assigner = userUuid)
        permissionService.create(permission).shouldSucceed()

        val matchingPermissions = permissionService.getPermissions(
            PermissionFilters(assigner = userUuid, kind = PermissionKind.ASSIGNED)
        )

        assertTrue(matchingPermissions.isRight())
        assertThat(matchingPermissions.getOrNull()).hasSize(1)

        val notMatchingPermission = permissionService.getPermissions(
            PermissionFilters(assigner = "i:am:not:matching", kind = PermissionKind.ASSIGNED)
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

        entityService.createEntity(
            ngsiLdEntity,
            expandedEntity
        ).shouldSucceed()

        val permission = minimalPermission.copy(target = TargetAsset(id = entityId))
        permissionService.create(permission).shouldSucceed()

        val matchingPermissions = permissionService.getPermissions(
            PermissionFilters(targetTypeSelection = BEEKEEPER_IRI, kind = PermissionKind.ASSIGNED)
        )
        assertTrue(matchingPermissions.isRight())
        assertThat(matchingPermissions.getOrNull()).isEmpty()
    }

    private object PermissionId {
        const val beekeeper = "urn:ngsi-ld:Permission:1"
        const val beehiveWithScope = "urn:ngsi-ld:Permission:2"
        const val beehiveTypeAndScopeA = "urn:ngsi-ld:Permission:3"
        const val beehiveType = "urn:ngsi-ld:Permission:4"
        const val scopeA = "urn:ngsi-ld:Permission:5"
    }

    private suspend fun createRequestedPermissions() {
        coEvery {
            subjectReferentialService.hasOneOfGlobalRoles(any(), any())
        } returns true.right()

        val (beekeeperExpandedEntity, beekeeperNgsiLdEntity) =
            loadAndPrepareSampleData("beekeeper.jsonld").shouldSucceedAndResult()
        // avoid creating OWN permission
        entityService.createEntityPayload(beekeeperNgsiLdEntity, beekeeperExpandedEntity, ngsiLdDateTime())
            .shouldSucceed()

        val permissionOnMinimalEntity = gimmeRawPermission(
            id = beekeeper.toUri(),
            assignee = USER_UUID,
            target = TargetAsset(id = beekeeperNgsiLdEntity.id)
        )
        permissionService.create(permissionOnMinimalEntity).shouldSucceed()

        val (beehiveExpandedEntity, beehiveNgsiLdEntity) =
            loadAndPrepareSampleData("beehive_with_scope.jsonld").shouldSucceedAndResult()
        // avoid creating OWN permission
        entityService.createEntityPayload(beehiveNgsiLdEntity, beehiveExpandedEntity, ngsiLdDateTime()).shouldSucceed()

        val permissionOnEntityWithScope = gimmeRawPermission(
            id = beehiveWithScope.toUri(),
            assignee = USER_UUID,
            target = TargetAsset(id = beehiveNgsiLdEntity.id),
        )
        permissionService.create(permissionOnEntityWithScope).shouldSucceed()

        val permissionOnTypeAndScope = gimmeRawPermission(
            id = beehiveTypeAndScopeA.toUri(),
            assignee = USER_UUID,
            target = TargetAsset(types = listOf(BEEHIVE_IRI), scopes = listOf(beehiveScope))
        )
        permissionService.create(permissionOnTypeAndScope).shouldSucceed()

        val permissionOnScope = gimmeRawPermission(
            id = scopeA.toUri(),
            assignee = USER_UUID,
            target = TargetAsset(scopes = listOf(beehiveScope))
        )
        permissionService.create(permissionOnScope).shouldSucceed()

        val permissionOnType = gimmeRawPermission(
            id = beehiveType.toUri(),
            assignee = USER_UUID,
            target = TargetAsset(types = listOf(BEEHIVE_IRI))
        )
        permissionService.create(permissionOnType).shouldSucceed()
    }

    // with typeQ and scopeQ we return all permissions that can impact something inside the typeQ/scopeQ combination
    @ParameterizedTest
    @CsvSource(
        // typeQ                        ,  scopeQ  , expectedIds , nonExpectedIds
        "  '$BEEHIVE_IRI,$BEEKEEPER_IRI', '/A,/B'  , '$beehiveWithScope,$scopeA,$beehiveType,$beehiveTypeAndScopeA', '$beekeeper'",
        "  '$BEEHIVE_IRI,$BEEKEEPER_IRI',  null    , '$beekeeper,$beehiveWithScope,$scopeA,$beehiveType,$beehiveTypeAndScopeA', null",
        "  '$BEEKEEPER_IRI'             ,  null    , '$beekeeper,$scopeA', '$beehiveWithScope,$beehiveType,$beehiveTypeAndScopeA'",
        "  null                         , '/A,/B'  , '$beehiveWithScope,$scopeA,$beehiveType,$beehiveTypeAndScopeA', '$beekeeper'",
        "  null                         , '/B'     , '$beehiveType', '$beekeeper,$beehiveWithScope,$scopeA,$beehiveTypeAndScopeA'",
        "  '$BEEKEEPER_IRI'             , '/B'     , null, '$beehiveType,$beekeeper,$beehiveWithScope,$scopeA,$beehiveTypeAndScopeA'",
        nullValues = ["null"]
    )
    @WithMockCustomUser(sub = USER_UUID, name = "Mock User")
    fun `query Permissions should return Permissions that affect the filtered typeQ and scopeQ`(
        typeQ: String?,
        scopeQ: String?,
        expectedIds: String?,
        nonExpectedIds: String?
    ) = runTest {
        coEvery {
            subjectReferentialService.getSubjectAndGroupsUUID()
        } answers { listOf(USER_UUID).right() }

        createRequestedPermissions()

        val permissions = permissionService.getPermissions(
            PermissionFilters(
                targetScopeSelection = scopeQ,
                targetTypeSelection = typeQ,
                kind = PermissionKind.ASSIGNED
            )
        )
        assertTrue(permissions.isRight())
        expectedIds?.split(',')?.forEach { expectedId ->
            assertThat(permissions.getOrNull()).anyMatch { it.id == expectedId.toUri() }
        }
        nonExpectedIds?.split(',')?.forEach { nonExpectedId ->
            assertThat(permissions.getOrNull()).allMatch { it.id != nonExpectedId.toUri() }
        }
    }

    // for the admin filter we only return permissions that are included in an admin permission target.
    @ParameterizedTest
    @CsvSource(
        // adminTypes                   ,  adminScope   , expectedIds, nonExpectedIds
        "  '$BEEHIVE_IRI,$BEEKEEPER_IRI', '/A,/B'       , '$beehiveWithScope,$beehiveTypeAndScopeA' , '$beekeeper,$scopeA,$beehiveType'",
        "  '$BEEHIVE_IRI,$BEEKEEPER_IRI',  null         , '$beekeeper,$beehiveWithScope,$beehiveType,$beehiveTypeAndScopeA', '$scopeA'",
        "  '$BEEKEEPER_IRI'             ,  null         , '$beekeeper', '$beehiveWithScope,$beehiveType,$beehiveTypeAndScopeA,$scopeA'",
        "  null                         , '/A,/B'       , '$beehiveWithScope,$scopeA,$beehiveTypeAndScopeA', '$beekeeper,$beehiveType'",
        "  null                         , '/B'          , null, '$beehiveType,$beekeeper,$beehiveWithScope,$scopeA,$beehiveTypeAndScopeA'",
        "  '$BEEKEEPER_IRI'             , '/B'          , null, '$beehiveType,$beekeeper,$beehiveWithScope,$scopeA,$beehiveTypeAndScopeA'",
        nullValues = ["null"]
    )
    @WithMockCustomUser(sub = USER_UUID, name = "Mock User")
    fun `query Permission with kind Admin should return Permissions with target included in permission you administer`(
        adminTypes: String?,
        adminScopes: String?,
        expectedIds: String?,
        nonExpectedIds: String?
    ) = runTest {
        coEvery {
            subjectReferentialService.getSubjectAndGroupsUUID()
        } answers { listOf(USER_UUID).right() }

        createRequestedPermissions()

        val permissionOnType = gimmeRawPermission(
            id = "urn:ngsild:GiveAdminRight".toUri(),
            assignee = USER_UUID,
            target = TargetAsset(
                types = adminTypes?.split(','),
                scopes = adminScopes?.split(',')
            ),
            action = Action.ADMIN
        )
        permissionService.create(permissionOnType).shouldSucceed()

        val permissions = permissionService.getPermissions(
            PermissionFilters(kind = PermissionKind.ADMIN)
        )
        assertTrue(permissions.isRight())
        expectedIds?.split(',')?.forEach { expectedId ->
            assertThat(permissions.getOrNull()).anyMatch { it.id == expectedId.toUri() }
        }
        nonExpectedIds?.split(',')?.forEach { nonExpectedId ->
            assertThat(permissions.getOrNull()).allMatch { it.id != nonExpectedId.toUri() }
        }
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
            PermissionFilters(assignee = userUuid, kind = PermissionKind.ASSIGNED),
        )
        assertEquals(1, count.getOrNull())

        coEvery {
            subjectReferentialService.getSubjectAndGroupsUUID(invalidUser)
        } returns listOf(invalidUser).right()

        val countEmpty = permissionService.getPermissionsCount(
            filters = PermissionFilters(assignee = invalidUser, kind = PermissionKind.ASSIGNED)
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
    fun `hasPermissionOnEntity should not allow a subject having no permission`() = runTest {
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
    fun `hasPermissionOnEntity should allow a subject having a direct permission on a entity`() = runTest {
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
    fun `hasPermissionOnEntity should allow a subject to read if it has a write permission`() = runTest {
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
    fun `hasPermissionOnEntity should allow a subject having permission via a group membership`() = runTest {
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
        ).shouldSucceed()

        permissionService.checkHasPermissionOnEntity(entityId, Action.READ)
            .shouldSucceedWith { assertTrue(it) }
    }

    @Test
    fun `hasPermissionOnEntity should allow a subject having the stellio-admin role`() = runTest {
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

    @Test
    fun `hasPermissionOnEntity should allow a subject having a scope-based permission on an entity`() = runTest {
        coEvery { subjectReferentialService.hasStellioAdminRole(listOf(userUuid)) } returns false.right()

        coEvery {
            subjectReferentialService.getSubjectAndGroupsUUID()
        } returns listOf(userUuid).right()

        val (expandedEntity, ngsiLdEntity) =
            loadAndPrepareSampleData("beehive_with_scope.jsonld").shouldSucceedAndResult()

        entityService.createEntityPayload(
            ngsiLdEntity,
            expandedEntity,
            ngsiLdDateTime()
        ).shouldSucceed()

        permissionService.create(
            Permission(
                assignee = userUuid,
                target = TargetAsset(scopes = listOf(beehiveScope)),
                action = Action.READ,
                assigner = userUuid
            )
        ).shouldSucceed()

        permissionService.checkHasPermissionOnEntity(entityId, Action.READ)
            .shouldSucceedWith { assertTrue(it) }
    }

    @Test
    fun `hasPermissionOnEntity should allow a subject having a type-based permission on an entity`() = runTest {
        coEvery { subjectReferentialService.hasStellioAdminRole(listOf(userUuid)) } returns false.right()

        coEvery {
            subjectReferentialService.getSubjectAndGroupsUUID()
        } returns listOf(userUuid).right()

        val (expandedEntity, ngsiLdEntity) =
            loadAndPrepareSampleData("beehive_with_scope.jsonld").shouldSucceedAndResult()

        entityService.createEntityPayload(
            ngsiLdEntity,
            expandedEntity,
            ngsiLdDateTime()
        ).shouldSucceed()

        permissionService.create(
            Permission(
                assignee = userUuid,
                target = TargetAsset(types = listOf(BEEHIVE_IRI)),
                action = Action.READ,
                assigner = userUuid
            )
        ).shouldSucceed()

        permissionService.checkHasPermissionOnEntity(entityId, Action.READ)
            .shouldSucceedWith { assertTrue(it) }
    }

    @Test
    fun `hasPermissionOnEntity should allow a subject having both type and scope-based permission on an entity`() = runTest {
        coEvery { subjectReferentialService.hasStellioAdminRole(listOf(userUuid)) } returns false.right()

        coEvery {
            subjectReferentialService.getSubjectAndGroupsUUID()
        } returns listOf(userUuid).right()

        val (expandedEntity, ngsiLdEntity) =
            loadAndPrepareSampleData("beehive_with_scope.jsonld").shouldSucceedAndResult()

        entityService.createEntityPayload(
            ngsiLdEntity,
            expandedEntity,
            ngsiLdDateTime()
        ).shouldSucceed()

        permissionService.create(
            Permission(
                assignee = userUuid,
                target = TargetAsset(scopes = listOf(beehiveScope), types = listOf(BEEHIVE_IRI)),
                action = Action.READ,
                assigner = userUuid
            )
        ).shouldSucceed()

        permissionService.checkHasPermissionOnEntity(entityId, Action.READ)
            .shouldSucceedWith { assertTrue(it) }
    }

    @Test
    fun `hasPermissionOnEntity should not allow a subject having both type and scope-based permission on an entity with only type matching`() = runTest {
        coEvery { subjectReferentialService.hasStellioAdminRole(listOf(userUuid)) } returns false.right()

        coEvery {
            subjectReferentialService.getSubjectAndGroupsUUID()
        } returns listOf(userUuid).right()

        val matchingType = BEEHIVE_IRI
        val nonMatchingType = BEEKEEPER_IRI
        val nonMatchingScope = "/non/matching/scope"
        val matchingScope = beehiveScope

        val (expandedEntity, ngsiLdEntity) =
            loadAndPrepareSampleData("beehive_with_scope.jsonld").shouldSucceedAndResult()

        entityService.createEntityPayload(
            ngsiLdEntity,
            expandedEntity,
            ngsiLdDateTime()
        ).shouldSucceed()

        permissionService.create(
            Permission(
                assignee = userUuid,
                target = TargetAsset(scopes = listOf(nonMatchingScope), types = listOf(matchingType)),
                action = Action.READ,
                assigner = userUuid
            )
        ).shouldSucceed()

        permissionService.checkHasPermissionOnEntity(entityId, Action.READ)
            .shouldSucceedWith { assertFalse(it) }

        permissionService.create(
            Permission(
                assignee = userUuid,
                target = TargetAsset(scopes = listOf(matchingScope), types = listOf(nonMatchingType)),
                action = Action.READ,
                assigner = userUuid
            )
        ).shouldSucceed()

        permissionService.checkHasPermissionOnEntity(entityId, Action.READ)
            .shouldSucceedWith { assertFalse(it) }
    }
}
