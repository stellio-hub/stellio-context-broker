package com.egm.stellio.search.authorization.service

import com.egm.stellio.search.entity.model.EntitiesQueryFromGet
import com.egm.stellio.shared.config.ApplicationProperties
import com.egm.stellio.shared.queryparameter.PaginationQuery
import com.egm.stellio.shared.util.AUTHZ_TEST_COMPOUND_CONTEXTS
import com.egm.stellio.shared.util.shouldSucceedWith
import com.egm.stellio.shared.util.toUri
import io.mockk.every
import io.mockk.mockk
import io.mockk.spyk
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.fail

class AuthorizationServiceTests {

    private val authorizationService = spyk(DisabledAuthorizationService())

    private val entityUri = "urn:ngsi-ld:Entity:01".toUri()

    private val applicationProperties = mockk<ApplicationProperties> {
        every { contexts.core } returns "http://localhost:8093/jsonld-contexts/ngsi-ld-core-context-v1.8.jsonld"
    }

    @Test
    fun `it should authorize access to read`() = runTest {
        authorizationService.userCanReadEntity(entityUri)
            .mapLeft {
                fail("it should not have failed")
            }
    }

    @Test
    fun `get authorized entities should return a count of -1 if authentication is not enabled`() = runTest {
        authorizationService.getAuthorizedEntities(
            EntitiesQueryFromGet(
                paginationQuery = PaginationQuery(limit = 0, offset = 0),
                contexts = listOf(applicationProperties.contexts.core)
            ),
            false,
            listOf(applicationProperties.contexts.core)
        ).shouldSucceedWith {
            assertEquals(-1, it.first)
            assertEquals(0, it.second.size)
        }
    }

    @Test
    fun `get groups memberships should return a count of -1 if authentication is not enabled`() = runTest {
        authorizationService.getGroupsMemberships(
            0,
            0,
            AUTHZ_TEST_COMPOUND_CONTEXTS
        ).shouldSucceedWith {
            assertEquals(-1, it.first)
            assertEquals(0, it.second.size)
        }
    }

    @Test
    fun `get users should return a count of -1 if authentication is not enabled`() = runTest {
        authorizationService.getUsers(
            0,
            0,
            AUTHZ_TEST_COMPOUND_CONTEXTS
        ).shouldSucceedWith {
            assertEquals(-1, it.first)
            assertEquals(0, it.second.size)
        }
    }
}
