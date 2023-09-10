package com.egm.stellio.search.authorization

import arrow.core.None
import com.egm.stellio.shared.model.QueryParams
import com.egm.stellio.shared.util.AuthContextModel.AUTHORIZATION_COMPOUND_CONTEXT
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CORE_CONTEXT
import com.egm.stellio.shared.util.shouldSucceedWith
import com.egm.stellio.shared.util.toUri
import io.mockk.spyk
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.fail

@OptIn(ExperimentalCoroutinesApi::class)
class AuthorizationServiceTests {

    private val authorizationService = spyk(DisabledAuthorizationService())

    private val entityUri = "urn:ngsi-ld:Entity:01".toUri()

    @Test
    fun `it should authorize access to read`() = runTest {
        authorizationService.userCanReadEntity(entityUri, None)
            .mapLeft {
                fail("it should not have failed")
            }
    }

    @Test
    fun `get authorized entities should return a count of -1 if authentication is not enabled`() = runTest {
        authorizationService.getAuthorizedEntities(
            QueryParams(limit = 0, offset = 0, context = NGSILD_CORE_CONTEXT),
            NGSILD_CORE_CONTEXT,
            None
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
            AUTHORIZATION_COMPOUND_CONTEXT,
            None
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
            AUTHORIZATION_COMPOUND_CONTEXT
        ).shouldSucceedWith {
            assertEquals(-1, it.first)
            assertEquals(0, it.second.size)
        }
    }
}
