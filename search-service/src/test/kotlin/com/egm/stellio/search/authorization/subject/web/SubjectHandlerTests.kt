package com.egm.stellio.search.authorization.subject.web

import arrow.core.left
import arrow.core.right
import com.egm.stellio.search.authorization.permission.service.AuthorizationService
import com.egm.stellio.search.authorization.subject.model.User
import com.egm.stellio.search.common.config.SearchProperties
import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.model.AccessDeniedException
import com.egm.stellio.shared.model.ExpandedEntity
import com.egm.stellio.shared.util.AUTHZ_HEADER_LINK
import com.egm.stellio.shared.util.AUTHZ_TEST_COMPOUND_CONTEXTS
import com.egm.stellio.shared.util.AUTHZ_TEST_CONTEXT
import com.egm.stellio.shared.util.AuthContextModel
import com.egm.stellio.shared.util.AuthContextModel.AUTH_TERM_FAMILY_NAME
import com.egm.stellio.shared.util.AuthContextModel.AUTH_TERM_GIVEN_NAME
import com.egm.stellio.shared.util.AuthContextModel.AUTH_TERM_SUBJECT_INFO
import com.egm.stellio.shared.util.AuthContextModel.AUTH_TERM_USERNAME
import com.egm.stellio.shared.util.AuthContextModel.GROUP_COMPACT_TYPE
import com.egm.stellio.shared.util.AuthContextModel.GROUP_TYPE
import com.egm.stellio.shared.util.AuthContextModel.USER_COMPACT_TYPE
import com.egm.stellio.shared.util.AuthContextModel.USER_TYPE
import com.egm.stellio.shared.util.JSON_LD_MEDIA_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.buildExpandedPropertyValue
import com.egm.stellio.shared.util.MOCK_USER_SUB
import com.egm.stellio.shared.util.NAME_IRI
import com.egm.stellio.shared.util.RESULTS_COUNT_HEADER
import com.ninjasquad.springmockk.MockkBean
import io.mockk.coEvery
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest
import org.springframework.http.HttpHeaders
import org.springframework.http.MediaType
import org.springframework.security.test.web.reactive.server.SecurityMockServerConfigurers.csrf
import org.springframework.security.test.web.reactive.server.SecurityMockServerConfigurers.mockJwt
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.web.reactive.server.WebTestClient
import java.time.Duration

@ActiveProfiles("test")
@WebFluxTest(SubjectHandler::class)
@EnableConfigurationProperties(ApplicationProperties::class, SearchProperties::class)
class SubjectHandlerTests {

    @Autowired
    private lateinit var webClient: WebTestClient

    @Autowired
    private lateinit var applicationProperties: ApplicationProperties

    @MockkBean(relaxed = true)
    private lateinit var authorizationService: AuthorizationService

    @BeforeAll
    fun configureWebClientDefaults() {
        webClient = webClient.mutate()
            .apply(mockJwt().jwt { it.subject(MOCK_USER_SUB) })
            .apply(csrf())
            .defaultHeaders {
                it.accept = listOf(JSON_LD_MEDIA_TYPE)
                it.contentType = JSON_LD_MEDIA_TYPE
            }
            .responseTimeout(Duration.ofMinutes(5))
            .build()
    }

    @Test
    fun `get groups memberships should return 200 and the number of results if requested limit is 0`() {
        coEvery {
            authorizationService.getGroupsMemberships(any(), any(), any(), any())
        } returns Pair(3, emptyList<ExpandedEntity>()).right()

        webClient.get()
            .uri("/ngsi-ld/v1/auth/subjects/groups?&limit=0&offset=1&count=true")
            .exchange()
            .expectStatus().isOk
            .expectHeader().valueEquals(RESULTS_COUNT_HEADER, "3")
            .expectBody().json("[]")
    }

    @Test
    fun `get groups memberships should return groups I am member of`() {
        coEvery {
            authorizationService.getGroupsMemberships(any(), any(), any(), any())
        } returns Pair(
            1,
            listOf(
                ExpandedEntity(
                    mapOf(
                        "@id" to "urn:ngsi-ld:group:1",
                        "@type" to listOf(GROUP_TYPE),
                        NAME_IRI to buildExpandedPropertyValue("egm")
                    )
                )
            )
        ).right()

        webClient.get()
            .uri("/ngsi-ld/v1/auth/subjects/groups?count=true")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isOk
            .expectHeader().valueEquals(RESULTS_COUNT_HEADER, "1")
            .expectBody().json(
                """
                [
                    {
                        "id": "urn:ngsi-ld:group:1",
                        "type": "$GROUP_COMPACT_TYPE",
                        "name" : {"type":"Property", "value": "egm"},
                        "@context": "${applicationProperties.contexts.authzCompound}"
                    }
                ]
                """.trimMargin()
            )
            .jsonPath("[0].createdAt").doesNotExist()
            .jsonPath("[0].modifiedAt").doesNotExist()
    }

    @Test
    fun `get groups memberships should return groups I am member of with authorization context`() {
        coEvery {
            authorizationService.getGroupsMemberships(any(), any(), any(), any())
        } returns Pair(
            1,
            listOf(
                ExpandedEntity(
                    mapOf(
                        "@id" to "urn:ngsi-ld:group:01",
                        "@type" to listOf(GROUP_TYPE),
                        NAME_IRI to buildExpandedPropertyValue("egm"),
                        AuthContextModel.AUTH_REL_IS_MEMBER_OF to buildExpandedPropertyValue("true")
                    )
                )
            )
        ).right()

        webClient.get()
            .uri("/ngsi-ld/v1/auth/subjects/groups?count=true")
            .header(HttpHeaders.LINK, AUTHZ_HEADER_LINK)
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isOk
            .expectHeader().valueEquals(RESULTS_COUNT_HEADER, "1")
            .expectBody().json(
                """
                [
                    {
                        "id": "urn:ngsi-ld:group:01",
                        "type": "Group",
                        "name": {"type":"Property", "value": "egm"},
                        "isMemberOf": {"type":"Property", "value": "true"},
                        "@context": [
                          "$AUTHZ_TEST_CONTEXT",
                          "${applicationProperties.contexts.authzCompound}"
                        ]
                    }
                ]
                """.trimMargin()
            )
            .jsonPath("[0].createdAt").doesNotExist()
            .jsonPath("[0].modifiedAt").doesNotExist()
    }

    @Test
    fun `get groups memberships should return 204 if authentication is not enabled`() {
        coEvery {
            authorizationService.getGroupsMemberships(any(), any(), any(), any())
        } returns Pair(-1, emptyList<ExpandedEntity>()).right()

        webClient.get()
            .uri("/ngsi-ld/v1/auth/subjects/groups")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isNoContent
    }

    @Test
    fun `get users should return an access denied error if user is not a stellio admin`() {
        coEvery {
            authorizationService.userIsAdmin(any())
        } returns AccessDeniedException("Access denied").left()

        webClient.get()
            .uri("/ngsi-ld/v1/auth/subjects/users")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isForbidden
    }

    @Test
    fun `get users should return 200 and the number of results if requested limit is 0`() {
        coEvery { authorizationService.userIsAdmin(any()) } returns Unit.right()
        coEvery {
            authorizationService.getUsers(any(), any(), any())
        } returns Pair(3, emptyList<ExpandedEntity>()).right()

        webClient.get()
            .uri("/ngsi-ld/v1/auth/subjects/users?&limit=0&offset=1&count=true")
            .exchange()
            .expectStatus().isOk
            .expectHeader().valueEquals(RESULTS_COUNT_HEADER, "3")
            .expectBody().json("[]")
    }

    @Test
    fun `get users should return 204 if authentication is not enabled`() {
        coEvery { authorizationService.userIsAdmin(any()) } returns Unit.right()
        coEvery {
            authorizationService.getUsers(any(), any(), any())
        } returns Pair(-1, emptyList<ExpandedEntity>()).right()

        webClient.get()
            .uri("/ngsi-ld/v1/auth/subjects/users")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isNoContent
    }

    @Test
    fun `get users should return users if user is a stellio admin`() = runTest {
        coEvery { authorizationService.userIsAdmin(any()) } returns Unit.right()
        coEvery {
            authorizationService.getUsers(any(), any(), any())
        } returns Pair(
            1,
            listOf(
                ExpandedEntity(
                    User(
                        "1",
                        USER_TYPE,
                        "username",
                        "givenName",
                        "familyName",
                        mapOf("profile" to "stellio-user", "username" to "username")
                    ).serializeProperties(AUTHZ_TEST_COMPOUND_CONTEXTS)
                )
            )
        ).right()

        webClient.get()
            .uri("/ngsi-ld/v1/auth/subjects/users?count=true")
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .exchange()
            .expectStatus().isOk
            .expectHeader().valueEquals(RESULTS_COUNT_HEADER, "1")
            .expectBody().json(
                """
                [
                    {
                        "id": "urn:ngsi-ld:User:1",
                        "type": "$USER_COMPACT_TYPE",
                        "$AUTH_TERM_USERNAME" : { "type":"Property", "value": "username" },
                        "$AUTH_TERM_GIVEN_NAME" : { "type":"Property", "value": "givenName" },
                        "$AUTH_TERM_FAMILY_NAME" : { "type":"Property", "value": "familyName" },
                        "$AUTH_TERM_SUBJECT_INFO": { "type":"Property","value":{ "profile": "stellio-user" } },
                        "@context": "${applicationProperties.contexts.authzCompound}"
                    }
                ]
                """.trimMargin()
            )
            .jsonPath("[0].createdAt").doesNotExist()
            .jsonPath("[0].modifiedAt").doesNotExist()
    }
}
