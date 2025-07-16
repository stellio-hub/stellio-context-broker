package com.egm.stellio.search.authorization.permission.web

import arrow.core.left
import arrow.core.right
import com.egm.stellio.search.authorization.permission.model.Action
import com.egm.stellio.search.authorization.permission.model.Permission
import com.egm.stellio.search.authorization.permission.model.PermissionFilters.Companion.OnlyGetPermission
import com.egm.stellio.search.authorization.permission.model.TargetAsset
import com.egm.stellio.search.authorization.permission.service.AuthorizationService
import com.egm.stellio.search.authorization.permission.service.PermissionService
import com.egm.stellio.search.authorization.subject.model.SubjectReferential
import com.egm.stellio.search.authorization.subject.service.SubjectReferentialService
import com.egm.stellio.search.common.config.SearchProperties
import com.egm.stellio.search.entity.service.EntityQueryService
import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.model.AccessDeniedException
import com.egm.stellio.shared.model.AlreadyExistsException
import com.egm.stellio.shared.model.DEFAULT_DETAIL
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.util.APIC_COMPOUND_CONTEXTS
import com.egm.stellio.shared.util.APIC_HEADER_LINK
import com.egm.stellio.shared.util.AQUAC_HEADER_LINK
import com.egm.stellio.shared.util.BEEHIVE_IRI
import com.egm.stellio.shared.util.BEEHIVE_TERM
import com.egm.stellio.shared.util.JSON_LD_MEDIA_TYPE
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import com.egm.stellio.shared.util.MOCK_USER_SUB
import com.egm.stellio.shared.util.RESULTS_COUNT_HEADER
import com.egm.stellio.shared.util.Sub
import com.egm.stellio.shared.util.SubjectType
import com.egm.stellio.shared.util.expandJsonLdEntity
import com.egm.stellio.shared.util.toUri
import com.ninjasquad.springmockk.MockkBean
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.confirmVerified
import io.r2dbc.postgresql.codec.Json
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest
import org.springframework.core.io.ClassPathResource
import org.springframework.http.HttpHeaders
import org.springframework.http.MediaType
import org.springframework.security.test.web.reactive.server.SecurityMockServerConfigurers.csrf
import org.springframework.security.test.web.reactive.server.SecurityMockServerConfigurers.mockJwt
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.json.JsonCompareMode
import org.springframework.test.web.reactive.server.WebTestClient
import java.net.URI

@ActiveProfiles("test")
@EnableConfigurationProperties(ApplicationProperties::class, SearchProperties::class)
@AutoConfigureWebTestClient(timeout = "30000")
@WebFluxTest(PermissionHandler::class)
class PermissionHandlerTests {

    @MockkBean
    private lateinit var permissionService: PermissionService

    @Autowired
    private lateinit var webClient: WebTestClient

    @MockkBean
    private lateinit var subjectReferentialService: SubjectReferentialService

    @MockkBean
    private lateinit var entityQueryService: EntityQueryService

    @MockkBean
    private lateinit var authorizationService: AuthorizationService

    private val permissionUri = "/ngsi-ld/v1/auth/permissions"
    private val id = "urn:ngsi-ld:Permission:1".toUri()
    private val jwt = mockJwt().jwt { it.subject(MOCK_USER_SUB) }
    private val userUuid = "55e64faf-4bda-41cc-98b0-195874cefd29"
    private val entity = """
            {
                "id": "my:id",
                "type": "Beehive",
                "attr1": {
                    "type": "Property",
                    "value": "some value 1"
                },
                "attr2": {
                    "type": "Property",
                    "value": "some value 2"
                },
                "@context" : [
                     "http://localhost:8093/jsonld-contexts/apic-compound.jsonld"
                ]
            }
    """.trimIndent()

    @BeforeAll
    fun configureWebClientDefaults() {
        webClient = webClient.mutate()
            .apply(jwt)
            .apply(csrf())
            .defaultHeaders {
                it.accept = listOf(JSON_LD_MEDIA_TYPE)
                it.contentType = JSON_LD_MEDIA_TYPE
            }
            .build()
    }

    private fun gimmeRawPermission(
        id: URI = "urn:ngsi-ld:Permission:1".toUri(),
        target: TargetAsset = TargetAsset(id = "my:id".toUri()),
        assignee: Sub? = MOCK_USER_SUB,
        action: Action = Action.READ,
        assigner: Sub = MOCK_USER_SUB,
    ) = Permission(id, target = target, assignee = assignee, action = action, assigner = assigner)

    @Test
    fun `get Permission by id should return 200 when it exists`() = runTest {
        val permission = gimmeRawPermission()

        coEvery { permissionService.isAdminOf(any()) } returns true.right()
        coEvery { permissionService.getById(any()) } returns permission.right()
        coEvery { subjectReferentialService.getSubjectAndGroupsUUID() } returns listOf(userUuid).right()

        webClient.get()
            .uri("$permissionUri/$id")
            .exchange()
            .expectStatus().isOk
            .expectBody()
            .jsonPath("$.createdAt").doesNotExist()
            .jsonPath("$.modifiedAt").doesNotExist()

        coVerify { permissionService.getById(id) }
    }

    @Test
    fun `get Permission by id should return 200 with sysAttrs when options query param specify it`() = runTest {
        val permission = gimmeRawPermission()

        coEvery { permissionService.isAdminOf(any()) } returns true.right()
        coEvery { permissionService.getById(any()) } returns permission.right()
        coEvery { subjectReferentialService.getSubjectAndGroupsUUID() } returns listOf(userUuid).right()

        webClient.get()
            .uri("$permissionUri/$id?options=sysAttrs")
            .exchange()
            .expectStatus().isOk
            .expectBody()
            .jsonPath("$.createdAt").exists()

        coVerify { permissionService.getById(id) }
    }

    @Test
    fun `get Permission by id should return the errors from the service`() = runTest {
        val permission = gimmeRawPermission()

        coEvery { permissionService.isAdminOf(any()) } returns true.right()
        coEvery { permissionService.getById(any()) } returns ResourceNotFoundException("").left()
        coEvery { subjectReferentialService.getSubjectAndGroupsUUID() } returns listOf(userUuid).right()

        webClient.get()
            .uri("$permissionUri/$id")
            .exchange()
            .expectStatus().isNotFound

        coVerify { permissionService.getById(permission.id) }
    }

    @Test
    fun `get Permission by id should return the permission if the subject is assigned to the permission`() = runTest {
        val permission = gimmeRawPermission(assignee = userUuid)

        coEvery { permissionService.isAdminOf(any()) } returns false.right()
        coEvery { permissionService.getById(any()) } returns permission.right()
        coEvery { subjectReferentialService.getSubjectAndGroupsUUID() } returns listOf(userUuid).right()

        webClient.get()
            .uri("$permissionUri/$id")
            .exchange()
            .expectStatus().isOk

        coVerify { permissionService.getById(id) }
    }

    @Test
    fun `get Permission by id should return 403 if the subject is nor the assignee nor the admin of target entity`() =
        runTest {
            val permission = gimmeRawPermission(assignee = "not-matching-sub")

            coEvery { permissionService.isAdminOf(any()) } returns false.right()
            coEvery { permissionService.getById(any()) } returns permission.right()
            coEvery { subjectReferentialService.getSubjectAndGroupsUUID() } returns listOf(userUuid).right()

            webClient.get()
                .uri("$permissionUri/$id")
                .exchange()
                .expectStatus().isForbidden

            coVerify { permissionService.getById(id) }
        }

    @Test
    fun `query Permissions should return 200 whether a Permission exists or not`() = runTest {
        val permission = gimmeRawPermission()

        coEvery {
            permissionService.getPermissions(any(), any(), any())
        } returns listOf(permission).right()

        coEvery { permissionService.getPermissionsCount(any()) } returns 1.right()

        webClient.get()
            .uri("$permissionUri?id=$id")
            .exchange()
            .expectStatus().isOk
            .expectBody()

        coVerify { permissionService.getPermissions(any(), any()) }
    }

    @Test
    fun `query Permissions with details=true should return entire entity and subject information`() = runTest {
        val permission = gimmeRawPermission()
        val expandedEntity = expandJsonLdEntity(entity)

        coEvery { permissionService.isAdminOf(any()) } returns true.right()
        coEvery { permissionService.getPermissions(any(), any(), any()) } returns listOf(permission).right()
        coEvery { entityQueryService.queryEntity(any()) } returns expandedEntity.right()
        coEvery { subjectReferentialService.retrieve(any()) } returns SubjectReferential(
            userUuid, SubjectType.GROUP,
            Json.of(
                """
                    {"type": "Property", "value": {"kind": "Group", "name": "Stellio Team"}}
                """.trimIndent()
            )
        ).right()

        coEvery { permissionService.getPermissionsCount(any()) } returns 1.right()

        webClient.get()
            .uri("$permissionUri?id=$id&details=true")
            .accept(MediaType.APPLICATION_JSON)
            .exchange()
            .expectStatus().isOk
            .expectBody()
            .json(
                """
                    [
                      {
                        "id": "${permission.id}",
                        "type": "Permission",
                        "action" : "read",
                        "assignee" : {
                            "subjectId":"55e64faf-4bda-41cc-98b0-195874cefd29",
                            "subjectType":"GROUP",
                            "subjectInfo":{"kind":"Group","name":"Stellio Team"}
                        },
                        "assigner" : {
                            "subjectId":"55e64faf-4bda-41cc-98b0-195874cefd29",
                            "subjectType":"GROUP",
                            "subjectInfo":{"kind":"Group","name":"Stellio Team"}
                        },
                        "target" : {
                            "id": "my:id",
                            "type": "Beehive",
                            "attr1": {
                                "type": "Property",
                                "value": "some value 1"
                            },
                            "attr2": {
                                "type": "Property",
                                "value": "some value 2"
                            }                               
                        }
                      },
                    ]                
                """.trimIndent(),
                JsonCompareMode.STRICT
            )
            .returnResult().responseBody?.toString(Charsets.UTF_8)

        coVerify { permissionService.getPermissions(any(), any()) }
        coVerify { entityQueryService.queryEntity(any()) }
        coVerify { subjectReferentialService.retrieve(any()) }
    }

    @Test
    fun `query Permissions with details=true and pickDetails should only returned the picked attributes`() = runTest {
        val permission = gimmeRawPermission()
        val expandedEntity = expandJsonLdEntity(entity)

        coEvery { permissionService.isAdminOf(any()) } returns true.right()
        coEvery { permissionService.getPermissions(any(), any(), any()) } returns listOf(permission).right()
        coEvery { entityQueryService.queryEntity(any()) } returns expandedEntity.right()
        coEvery { subjectReferentialService.retrieve(any()) } returns SubjectReferential(
            userUuid, SubjectType.GROUP,
            Json.of(
                """
                    {"type": "Property", "value": {"kind": "Group", "name": "Stellio Team"}}
                """.trimIndent()
            )
        ).right()

        coEvery { permissionService.getPermissionsCount(any()) } returns 1.right()

        webClient.get()
            .uri("$permissionUri?id=$id&details=true&detailsPick=attr1")
            .accept(MediaType.APPLICATION_JSON)
            .exchange()
            .expectStatus().isOk
            .expectBody()
            .json(
                """
                    [
                      {
                        "id": "${permission.id}",
                        "type": "Permission",
                        "action" : "read",
                        "assignee" : {
                            "subjectId":"55e64faf-4bda-41cc-98b0-195874cefd29",
                            "subjectType":"GROUP",
                            "subjectInfo":{"kind":"Group","name":"Stellio Team"}
                        },
                        "assigner" : {
                            "subjectId":"55e64faf-4bda-41cc-98b0-195874cefd29",
                            "subjectType":"GROUP",
                            "subjectInfo":{"kind":"Group","name":"Stellio Team"}
                        },
                        "target" : {
                            "id": "my:id",
                            "type": "Beehive",
                            "attr1": {
                                "type": "Property",
                                "value": "some value 1"
                            },
                        }
                      },
                    ]                
                """.trimIndent(),
                JsonCompareMode.STRICT
            )
            .returnResult().responseBody?.toString(Charsets.UTF_8)

        coVerify { permissionService.getPermissions(any(), any()) }
        coVerify { entityQueryService.queryEntity(any()) }
        coVerify { subjectReferentialService.retrieve(any()) }
    }

    @Test
    fun `query Permission should return the count if it was asked`() = runTest {
        val permission = gimmeRawPermission()

        coEvery { permissionService.isAdminOf(any()) } returns true.right()
        coEvery {
            permissionService.getPermissions(any(), any(), any())
        } returns listOf(permission).right()

        coEvery { permissionService.getPermissionsCount(any()) } returns 1.right()

        webClient.get()
            .uri("$permissionUri?id=$id&count=true")
            .exchange()
            .expectStatus().isOk
            .expectHeader().exists(RESULTS_COUNT_HEADER)
            .expectBody()

        coVerify { permissionService.getPermissions(any(), any()) }
    }

    @Test
    fun `query Permissions instantiate filter based on query parameters`() = runTest {
        val permission = gimmeRawPermission()

        coEvery {
            permissionService.getPermissions(any(), any(), any())
        } returns listOf(permission).right()

        coEvery { permissionService.getPermissionsCount(any()) } returns 1.right()

        webClient.get()
            .uri(
                "$permissionUri?id=$id&action=${Action.OWN.value}&assignee=assigneeId&assigner=assignerId&type=${BEEHIVE_TERM}"
            )
            .header(HttpHeaders.LINK, APIC_HEADER_LINK)
            .exchange()
            .expectStatus().isOk
            .expectBody()

        coVerify {
            permissionService.getPermissions(
                match {
                    it.ids == listOf(id)
                    it.action == Action.OWN
                    it.assignee == "assigneeId"
                    it.assigner == "assignerId"
                    it.targetTypeSelection == BEEHIVE_IRI
                },
                any()
            )
        }
    }

    @Test
    fun `query base Permissions endpoint should only ask for permission you admin`() = runTest {
        val permission = gimmeRawPermission()

        coEvery {
            permissionService.getPermissions(any(), any(), any())
        } returns listOf(permission).right()

        coEvery { permissionService.getPermissionsCount(any()) } returns 1.right()

        webClient.get()
            .uri("$permissionUri?id=$id")
            .exchange()
            .expectStatus().isOk
            .expectBody()

        coVerify {
            permissionService.getPermissions(
                match {
                    it.onlyGetPermission == OnlyGetPermission.ADMIN
                },
                any()
            )
        }
    }

    @Test
    fun `query assigned Permissions endpoint should only ask for permission assigned to you`() = runTest {
        val permission = gimmeRawPermission()

        coEvery {
            permissionService.getPermissions(any(), any(), any())
        } returns listOf(permission).right()

        coEvery { permissionService.getPermissionsCount(any()) } returns 1.right()

        webClient.get()
            .uri("$permissionUri/assigned?id=$id")
            .exchange()
            .expectStatus().isOk
            .expectBody()

        coVerify {
            permissionService.getPermissions(
                match {
                    it.onlyGetPermission == OnlyGetPermission.ASSIGNED
                },
                any()
            )
        }
    }

    @Test
    fun `delete Permission should return the errors from the service`() = runTest {
        coEvery { permissionService.getById(any()) } returns gimmeRawPermission().right()
        coEvery { permissionService.checkHasPermissionOnEntity(any(), any()) } returns true.right()
        coEvery { permissionService.delete(any()) } returns ResourceNotFoundException("").left()

        webClient.delete()
            .uri("$permissionUri/$id")
            .exchange()
            .expectStatus().isNotFound

        coVerify { permissionService.delete(eq(id)) }
    }

    @Test
    fun `delete Permission should return a 204 if the deletion succeeded`() = runTest {
        coEvery { permissionService.getById(any()) } returns gimmeRawPermission().right()
        coEvery { permissionService.checkHasPermissionOnEntity(any(), any()) } returns true.right()
        coEvery { permissionService.delete(any()) } returns Unit.right()

        webClient.delete()
            .uri("$permissionUri/$id")
            .exchange()
            .expectStatus().isNoContent

        coVerify { permissionService.delete(eq(id)) }
    }

    @Test
    fun `delete Permission should refuse to delete permission if the subject is not admin of the target`() = runTest {
        coEvery { permissionService.getById(any()) } returns gimmeRawPermission().right()
        coEvery { permissionService.checkHasPermissionOnEntity(any(), any()) } returns false.right()
        coEvery { permissionService.delete(any()) } returns Unit.right()

        webClient.delete()
            .uri("$permissionUri/$id")
            .exchange()
            .expectStatus().isForbidden

        coVerify(exactly = 0) { permissionService.delete(eq(id)) }
    }

    @Test
    fun `delete Permission should refuse to delete an owning permission`() = runTest {
        coEvery { permissionService.getById(any()) } returns gimmeRawPermission(action = Action.OWN).right()
        coEvery { permissionService.checkHasPermissionOnEntity(any(), any()) } returns true.right()
        coEvery { permissionService.delete(any()) } returns Unit.right()

        webClient.delete()
            .uri("$permissionUri/$id")
            .exchange()
            .expectStatus().isBadRequest

        coVerify(exactly = 0) { permissionService.delete(eq(id)) }
    }

    @Test
    fun `create Permission should return the errors from the service`() = runTest {
        val jsonLdFile = ClassPathResource("/ngsild/permission/permission_minimal.json")

        coEvery { permissionService.create(any()) } returns AlreadyExistsException("").left()
        coEvery {
            permissionService.checkHasPermissionOnEntity(any(), any())
        } returns true.right()

        webClient.post()
            .uri(permissionUri)
            .headers {
                it.accept = listOf(MediaType.APPLICATION_JSON)
                it.contentType = MediaType.APPLICATION_JSON
                it.add(HttpHeaders.LINK, AQUAC_HEADER_LINK)
            }
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isEqualTo(409)

        coVerify(exactly = 1) { permissionService.create(any()) }
    }

    @Test
    fun `create Permission should return a 204 if the creation succeeded`() = runTest {
        val jsonLdFile = ClassPathResource("/ngsild/permission/permission_minimal.json")

        coEvery { permissionService.create(any()) } returns Unit.right()
        coEvery { permissionService.checkHasPermissionOnEntity(any(), any()) } returns true.right()

        webClient.post()
            .uri(permissionUri)
            .headers {
                it.accept = listOf(MediaType.APPLICATION_JSON)
                it.contentType = MediaType.APPLICATION_JSON
                it.add(HttpHeaders.LINK, AQUAC_HEADER_LINK)
            }
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isCreated
            .expectHeader().exists("Location")

        coVerify(exactly = 1) { permissionService.create(any()) }
    }

    @Test
    fun `create Permission with JSON-LD payload should succeed`() = runTest {
        val jsonLdFile = ClassPathResource("/ngsild/permission/permission.jsonld")

        coEvery { permissionService.create(any()) } returns Unit.right()
        coEvery { permissionService.checkHasPermissionOnEntity(any(), any()) } returns true.right()

        webClient.post()
            .uri(permissionUri)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isCreated
            .expectHeader().exists("Location")

        coVerify(exactly = 1) { permissionService.create(any()) }
    }

    @Test
    fun `create Permission should refuse to create permission if the subject is not administrator`() = runTest {
        val jsonLdFile = ClassPathResource("/ngsild/permission/permission.jsonld")

        coEvery { permissionService.create(any()) } returns Unit.right()
        coEvery { permissionService.checkHasPermissionOnEntity(any(), any()) } returns false.right()

        webClient.post()
            .uri(permissionUri)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isForbidden

        coVerify(exactly = 0) { permissionService.create(any()) }
    }

    @Test
    fun `update permission should return a 500 if update in DB failed`() = runTest {
        val jsonLdFile = ClassPathResource("/ngsild/permission/permission_update.jsonld")
        val permissionId = id
        val parsedPermission = jsonLdFile.inputStream.readBytes().toString(Charsets.UTF_8).deserializeAsMap()

        coEvery { permissionService.isAdminOf(any()) } returns true.right()
        coEvery { authorizationService.userCanAdminEntity(any()) } returns Unit.right()
        coEvery { permissionService.getById(any()) } returns gimmeRawPermission().right()
        coEvery { permissionService.update(any(), any(), any()) } throws RuntimeException("Update failed")

        webClient.patch()
            .uri("/ngsi-ld/v1/auth/permissions/$permissionId")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().is5xxServerError
            .expectBody().json(
                """
                    {
                        "type": "https://uri.etsi.org/ngsi-ld/errors/InternalError",
                        "title": "java.lang.RuntimeException: Update failed",
                        "detail": "$DEFAULT_DETAIL"
                    }
                    """
            )

        coVerify { permissionService.isAdminOf(permissionId) }
        coVerify { permissionService.update(eq(permissionId), parsedPermission, APIC_COMPOUND_CONTEXTS) }
        coVerify { permissionService.getById(permissionId) }

        confirmVerified(permissionService)
    }

    @Test
    fun `update permission should return a 204 if update succeeded`() = runTest {
        val jsonLdFile = ClassPathResource("/ngsild/permission/permission_update.jsonld")
        val permissionId = id
        val parsedPermission = jsonLdFile.inputStream.readBytes().toString(Charsets.UTF_8).deserializeAsMap()

        coEvery { permissionService.isAdminOf(any()) } returns true.right()
        coEvery { authorizationService.userCanAdminEntity(any()) } returns Unit.right()
        coEvery { permissionService.update(any(), any(), any()) } returns Unit.right()
        coEvery { permissionService.getById(any()) } returns gimmeRawPermission().right()

        webClient.patch()
            .uri("/ngsi-ld/v1/auth/permissions/$permissionId")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isNoContent

        coVerify { permissionService.isAdminOf(permissionId) }
        coVerify { permissionService.update(eq(permissionId), parsedPermission, APIC_COMPOUND_CONTEXTS) }
        coVerify { permissionService.getById(permissionId) }

        confirmVerified(permissionService)
    }

    @Test
    fun `update permission should return a 400 if JSON-LD context is not correct`() = runTest {
        val jsonLdFile = ClassPathResource("/ngsild/permission/permission_update.json")

        coEvery { permissionService.isAdminOf(any()) } returns true.right()
        coEvery { authorizationService.userCanAdminEntity(any()) } returns Unit.right()
        coEvery { permissionService.getById(any()) } returns gimmeRawPermission().right()

        webClient.patch()
            .uri("/ngsi-ld/v1/auth/permissions/$id")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isBadRequest
            .expectBody().json(
                """
                {
                    "type": "https://uri.etsi.org/ngsi-ld/errors/BadRequestData",
                    "title": "Request payload must contain @context term for a request having an application/ld+json content type",
                    "detail": "$DEFAULT_DETAIL"
                } 
                """.trimIndent()
            )

        coVerify { permissionService.isAdminOf(id) }
    }

    @Test
    fun `update permission should return a 204 if JSON-LD context is provided in header`() = runTest {
        val jsonLdFile = ClassPathResource("/ngsild/permission/permission_update.json")

        coEvery { permissionService.isAdminOf(any()) } returns true.right()
        coEvery { authorizationService.userCanAdminEntity(any()) } returns Unit.right()
        coEvery { permissionService.update(any(), any(), any()) } returns Unit.right()
        coEvery { permissionService.getById(any()) } returns gimmeRawPermission().right()

        webClient.patch()
            .uri("/ngsi-ld/v1/auth/permissions/$id")
            .headers {
                it.accept = listOf(MediaType.APPLICATION_JSON)
                it.contentType = MediaType.APPLICATION_JSON
                it.add(HttpHeaders.LINK, AQUAC_HEADER_LINK)
            }
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isNoContent
    }

    @Test
    fun `update permission should refuse to modify a permission if the subject does not administrate it`() = runTest {
        val jsonLdFile = ClassPathResource("/ngsild/permission/permission_update.jsonld")
        val permissionId = id

        coEvery { permissionService.isAdminOf(any()) } returns false.right()
        coEvery { permissionService.update(any(), any(), any()) } returns Unit.right()

        webClient.patch()
            .uri("/ngsi-ld/v1/auth/permissions/$permissionId")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isForbidden

        coVerify(exactly = 0) { permissionService.update(any(), any(), any()) }
    }

    @Test
    fun `update permission should refuse to edit the target to an entity the subject does not administrate`() = runTest {
        val jsonLdFile = ClassPathResource("/ngsild/permission/permission_update.json")
        val newId = "urn:ngsi-ld:Vehicle:A456".toUri()
        val permissionId = id

        coEvery { permissionService.isAdminOf(any()) } returns true.right()
        coEvery { permissionService.update(any(), any(), any()) } returns Unit.right()
        coEvery { permissionService.getById(permissionId) } returns gimmeRawPermission().right()
        coEvery { authorizationService.userCanAdminEntity(newId) } returns
            AccessDeniedException("errorMessage").left()

        webClient.patch()
            .uri("/ngsi-ld/v1/auth/permissions/$permissionId")
            .headers {
                it.accept = listOf(MediaType.APPLICATION_JSON)
                it.contentType = MediaType.APPLICATION_JSON
                it.add(HttpHeaders.LINK, AQUAC_HEADER_LINK)
            }
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isForbidden

        coVerify(exactly = 0) { permissionService.update(any(), any(), any()) }
    }

    @Test
    fun `update permission should refuse to upgrade action to admin if permission is assigne to everyone`() = runTest {
        val jsonLdFile = ClassPathResource("/ngsild/permission/permission_update_action_to_admin.json")
        val permissionId = id

        coEvery { permissionService.isAdminOf(any()) } returns true.right()
        coEvery { permissionService.update(any(), any(), any()) } returns Unit.right()
        coEvery { permissionService.getById(permissionId) } returns gimmeRawPermission(assignee = null).right()

        webClient.patch()
            .uri("/ngsi-ld/v1/auth/permissions/$permissionId")
            .headers {
                it.accept = listOf(MediaType.APPLICATION_JSON)
                it.contentType = MediaType.APPLICATION_JSON
                it.add(HttpHeaders.LINK, AQUAC_HEADER_LINK)
            }
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isBadRequest

        coVerify(exactly = 0) { permissionService.update(any(), any(), any()) }
    }

    @Test
    fun `update permission should refuse to edit an owner permission`() = runTest {
        val jsonLdFile = ClassPathResource("/ngsild/permission/permission_update.json")
        val permissionId = id

        coEvery { permissionService.isAdminOf(any()) } returns true.right()
        coEvery { permissionService.update(any(), any(), any()) } returns Unit.right()
        coEvery { permissionService.getById(permissionId) } returns gimmeRawPermission(action = Action.OWN).right()

        webClient.patch()
            .uri("/ngsi-ld/v1/auth/permissions/$permissionId")
            .headers {
                it.accept = listOf(MediaType.APPLICATION_JSON)
                it.contentType = MediaType.APPLICATION_JSON
                it.add(HttpHeaders.LINK, AQUAC_HEADER_LINK)
            }
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isBadRequest

        coVerify(exactly = 0) { permissionService.update(any(), any(), any()) }
    }
}
