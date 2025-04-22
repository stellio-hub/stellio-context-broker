package com.egm.stellio.search.authorization.permission.web

import arrow.core.left
import arrow.core.right
import com.egm.stellio.search.authorization.permission.model.Action
import com.egm.stellio.search.authorization.permission.model.Permission
import com.egm.stellio.search.authorization.permission.model.TargetAsset
import com.egm.stellio.search.authorization.permission.service.PermissionService
import com.egm.stellio.search.common.config.SearchProperties
import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.model.AlreadyExistsException
import com.egm.stellio.shared.model.DEFAULT_DETAIL
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.util.APIC_COMPOUND_CONTEXTS
import com.egm.stellio.shared.util.AQUAC_HEADER_LINK
import com.egm.stellio.shared.util.AuthContextModel.AUTH_PERMISSION_TERM
import com.egm.stellio.shared.util.JSON_LD_MEDIA_TYPE
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import com.egm.stellio.shared.util.MOCK_USER_SUB
import com.egm.stellio.shared.util.RESULTS_COUNT_HEADER
import com.egm.stellio.shared.util.Sub
import com.egm.stellio.shared.util.ngsiLdDateTime
import com.egm.stellio.shared.util.sub
import com.egm.stellio.shared.util.toUri
import com.ninjasquad.springmockk.MockkBean
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.confirmVerified
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
import org.springframework.test.web.reactive.server.WebTestClient
import java.net.URI
import java.time.ZonedDateTime

@ActiveProfiles("test")
@EnableConfigurationProperties(ApplicationProperties::class, SearchProperties::class)
@AutoConfigureWebTestClient(timeout = "30000")
@WebFluxTest(PermissionHandler::class)
class PermissionHandlerTests {

    @MockkBean
    private lateinit var permissionService: PermissionService

    @Autowired
    private lateinit var webClient: WebTestClient

    private val permissionUri = "/ngsi-ld/v1/auth/permissions"
    private val id = "urn:ngsi-ld:Permission:1".toUri()
    private val jwt = mockJwt().jwt { it.subject(MOCK_USER_SUB) }

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

    @SuppressWarnings("LongParameterList")
    fun giveMeRawPermission(
        id: URI = "urn:ngsi-ld:Permission".toUri(),
        type: String = AUTH_PERMISSION_TERM,
        target: TargetAsset = TargetAsset(id = "my:id".toUri()),
        assignee: Sub = MOCK_USER_SUB,
        action: Action = Action.READ,
        createdAt: ZonedDateTime = ngsiLdDateTime(),
        modifiedAt: ZonedDateTime = createdAt,
        assigner: Sub = MOCK_USER_SUB,
    ) = Permission(id, type, target, assignee, action, createdAt, modifiedAt, assigner)

    @Test
    fun `get Permission by id should return 200 when it exists`() = runTest {
        val permission = giveMeRawPermission(id = id)

        coEvery { permissionService.isCreatorOf(any(), any()) } returns true.right()
        coEvery { permissionService.getById(any()) } returns permission.right()

        webClient.get()
            .uri("$permissionUri/$id")
            .exchange()
            .expectStatus().isOk
            .expectBody()
            .jsonPath("$..createdAt").doesNotExist()
            .jsonPath("$..modifiedAt").doesNotExist()

        coVerify { permissionService.getById(id) }
    }

    @Test
    fun `get Permission by id should return 200 with sysAttrs when options query param specify it`() = runTest {
        val permission = giveMeRawPermission(id = id)

        coEvery { permissionService.isCreatorOf(any(), any()) } returns true.right()
        coEvery { permissionService.getById(any()) } returns permission.right()

        webClient.get()
            .uri("$permissionUri/$id?options=sysAttrs")
            .exchange()
            .expectStatus().isOk
            .expectBody()
            .jsonPath("$..createdAt").exists()

        coVerify { permissionService.getById(id) }
    }

    @Test
    fun `get Permission by id should return the errors from the service`() = runTest {
        val permission = giveMeRawPermission(id = id)

        coEvery { permissionService.isCreatorOf(any(), any()) } returns true.right()
        coEvery { permissionService.getById(any()) } returns ResourceNotFoundException("").left()

        webClient.get()
            .uri("$permissionUri/$id")
            .exchange()
            .expectStatus().isNotFound

        coVerify { permissionService.getById(permission.id) }
    }

    @Test
    fun `query Permission should return 200 whether a Permission exists or not`() = runTest {
        val permission = giveMeRawPermission(id = id)

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
    fun `query Permission should return the count if it was asked`() = runTest {
        val permission = giveMeRawPermission(id = id)

        coEvery { permissionService.isCreatorOf(any(), any()) } returns true.right()
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
    fun `delete Permission should return the errors from the service`() = runTest {
        coEvery { permissionService.isCreatorOf(any(), any()) } returns true.right()
        coEvery { permissionService.delete(any()) } returns ResourceNotFoundException("").left()

        webClient.delete()
            .uri("$permissionUri/$id")
            .exchange()
            .expectStatus().isNotFound

        coVerify { permissionService.delete(eq(id)) }
    }

    @Test
    fun `delete Permission should return a 204 if the deletion succeeded`() = runTest {
        coEvery { permissionService.isCreatorOf(any(), any()) } returns true.right()
        coEvery { permissionService.delete(any()) } returns Unit.right()

        webClient.delete()
            .uri("$permissionUri/$id")
            .exchange()
            .expectStatus().isNoContent

        coVerify { permissionService.delete(eq(id)) }
    }

    @Test
    fun `create Permission should return the errors from the service`() = runTest {
        val jsonLdFile = ClassPathResource("/ngsild/permission/permission_minimal_entities.json")

        coEvery { permissionService.create(any()) } returns AlreadyExistsException("").left()

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
        val jsonLdFile = ClassPathResource("/ngsild/permission/permission_minimal_entities.json")

        coEvery { permissionService.create(any()) } returns Unit.right()

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
    fun `create Permission with context in payload should succeed`() = runTest {
        val jsonLdFile = ClassPathResource("/ngsild/permission/permission.jsonld")

        coEvery { permissionService.create(any()) } returns Unit.right()

        webClient.post()
            .uri(permissionUri)
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isCreated
            .expectHeader().exists("Location")

        coVerify(exactly = 1) { permissionService.create(any()) }
    }

    @Test
    fun `update permission should return a 500 if update in DB failed`() = runTest {
        val jsonLdFile = ClassPathResource("/ngsild/permission/permission_update.jsonld")
        val permissionId = id
        val parsedPermission = jsonLdFile.inputStream.readBytes().toString(Charsets.UTF_8).deserializeAsMap()

        coEvery { permissionService.isCreatorOf(any(), any()) } returns true.right()
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

        coVerify { permissionService.isCreatorOf(permissionId, sub) }
        coVerify { permissionService.update(eq(permissionId), parsedPermission, APIC_COMPOUND_CONTEXTS) }

        confirmVerified(permissionService)
    }

    @Test
    fun `update permission should return a 204 if JSON-LD payload is correct`() = runTest {
        val jsonLdFile = ClassPathResource("/ngsild/permission/permission_update.jsonld")
        val permissionId = id
        val parsedPermission = jsonLdFile.inputStream.readBytes().toString(Charsets.UTF_8).deserializeAsMap()

        coEvery { permissionService.isCreatorOf(any(), any()) } returns true.right()
        coEvery { permissionService.update(any(), any(), any()) } returns Unit.right()

        webClient.patch()
            .uri("/ngsi-ld/v1/auth/permissions/$permissionId")
            .bodyValue(jsonLdFile)
            .exchange()
            .expectStatus().isNoContent

        coVerify { permissionService.isCreatorOf(permissionId, sub) }
        coVerify { permissionService.update(eq(permissionId), parsedPermission, APIC_COMPOUND_CONTEXTS) }

        confirmVerified(permissionService)
    }

    @Test
    fun `update permission should return a 400 if JSON-LD context is not correct`() = runTest {
        val jsonLdFile = ClassPathResource("/ngsild/permission/permission_update.json")

        coEvery { permissionService.isCreatorOf(any(), any()) } returns true.right()

        @Suppress("MaxLineLength")
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

        coVerify { permissionService.isCreatorOf(id, sub) }
    }

    @Test
    fun `update permission should return a 204 if JSON-LD context is provided in header`() = runTest {
        val jsonLdFile = ClassPathResource("/ngsild/permission/permission_update.json")

        coEvery { permissionService.isCreatorOf(any(), any()) } returns true.right()
        coEvery { permissionService.update(any(), any(), any()) } returns Unit.right()

        @Suppress("MaxLineLength")
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
}
