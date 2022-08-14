package com.egm.stellio.search.authorization

import arrow.core.None
import arrow.core.left
import com.egm.stellio.shared.model.AccessDeniedException
import com.egm.stellio.shared.model.NgsiLdAttribute
import com.egm.stellio.shared.model.NgsiLdEntity
import com.egm.stellio.shared.util.AuthContextModel.AUTH_PROP_SAP
import com.egm.stellio.shared.util.AuthContextModel.USER_TYPE
import com.egm.stellio.shared.util.BEEHIVE_TYPE
import com.egm.stellio.shared.util.INCOMING_PROPERTY
import com.egm.stellio.shared.util.OUTGOING_PROPERTY
import com.egm.stellio.shared.util.toUri
import io.mockk.*
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
    fun `it should authorize an entity type that is not related to authorization`() {
        authorizationService.checkEntityTypesAreAuthorized(listOf(BEEHIVE_TYPE))
            .fold({
                fail("it should have allowed Beehive type")
            }, {})
    }

    @Test
    fun `it should not authorize one of the entity type related to authorization`() {
        authorizationService.checkEntityTypesAreAuthorized(listOf(BEEHIVE_TYPE, USER_TYPE))
            .fold({
                assertEquals("Entity type(s) [$USER_TYPE] cannot be managed via normal entity API", it.message)
            }, {
                fail("it should not have allowed User type")
            })
    }

    @Test
    fun `it should not authorize an entity type related to authorization`() {
        authorizationService.checkEntityTypesAreAuthorized(listOf(USER_TYPE))
            .fold({
                assertEquals("Entity type(s) [$USER_TYPE] cannot be managed via normal entity API", it.message)
            }, {
                fail("it should not have allowed User type")
            })
    }

    @Test
    fun `it should authorize all attributes that are not specific access policy`() {
        val mockedIncomingProperty = mockkClass(NgsiLdAttribute::class) {
            every { name } returns INCOMING_PROPERTY
        }
        val mockedOutgoingProperty = mockkClass(NgsiLdAttribute::class) {
            every { name } returns OUTGOING_PROPERTY
        }

        authorizationService.checkAttributesAreAuthorized(listOf(mockedIncomingProperty, mockedOutgoingProperty))
            .mapLeft { fail("it should not have failed as all attributes are authorized") }
    }

    @Test
    fun `it should not authorize if one of attributes is specific access policy`() {
        val mockedIncomingProperty = mockkClass(NgsiLdAttribute::class) {
            every { name } returns INCOMING_PROPERTY
        }
        val mockedSapProperty = mockkClass(NgsiLdAttribute::class) {
            every { name } returns AUTH_PROP_SAP
        }

        authorizationService.checkAttributesAreAuthorized(listOf(mockedIncomingProperty, mockedSapProperty))
            .map { fail("it should have failed as one of the attributes is not authorized") }
    }

    @Test
    fun `it should authorize an attribute that is not specific access policy`() {
        authorizationService.checkAttributeIsAuthorized(INCOMING_PROPERTY)
            .mapLeft { fail("it should not have failed as attribute name is authorized") }
    }

    @Test
    fun `it should not authorize an attribute thaht is specific access policy`() {
        authorizationService.checkAttributeIsAuthorized(AUTH_PROP_SAP)
            .fold(
                {
                    assertEquals(
                        "Specific access policy cannot be updated as a normal property, use " +
                            "/ngsi-ld/v1/entityAccessControl/{entityId}/attrs/specificAccessPolicy endpoint instead",
                        it.message
                    )
                },
                { fail("it should not have authorized specific access policy attribute") }
            )
    }

    @Test
    fun `it should authorize access as admin`() = runTest {
        authorizationService.checkAdminAuthorized(entityUri, listOf(BEEHIVE_TYPE), None)
            .mapLeft { fail("it should not have failed as entity type is allowed") }
    }

    @Test
    fun `it should not authorize access as admin if user is not admin`() = runTest {
        coEvery {
            authorizationService.userIsAdminOfEntity(any(), any())
        } returns AccessDeniedException("User forbidden admin access to entity urn:ngsi-ld:Entity:01").left()

        authorizationService.checkAdminAuthorized(entityUri, listOf(BEEHIVE_TYPE), None)
            .fold({
                assertEquals(
                    "User forbidden admin access to entity urn:ngsi-ld:Entity:01",
                    it.message
                )
            }, {
                fail("it should have failed as user is not admin of the entity")
            })
    }

    @Test
    fun `it should not authorize access as admin if entity type is related to authorization`() = runTest {
        authorizationService.checkAdminAuthorized(entityUri, listOf(USER_TYPE), None)
            .fold({
                assertEquals(
                    "Entity type(s) [$USER_TYPE] cannot be managed via normal entity API",
                    it.message
                )
            }, {
                fail("it should have failed as entity type is not allowed")
            })
    }

    @Test
    fun `it should not authorize access as admin if one of the entity types is related to authorization`() = runTest {
        authorizationService.checkAdminAuthorized(entityUri, listOf(USER_TYPE, BEEHIVE_TYPE), None)
            .fold({
                assertEquals(
                    "Entity type(s) [$USER_TYPE] cannot be managed via normal entity API",
                    it.message
                )
            }, {
                fail("it should have failed as entity type is not allowed")
            })
    }

    @Test
    fun `it should authorize access as creator`() = runTest {
        val mockedEntity = mockkClass(NgsiLdEntity::class) {
            every { types } returns listOf(BEEHIVE_TYPE)
        }

        authorizationService.checkCreationAuthorized(mockedEntity, None)
            .mapLeft { fail("it should not have failed as entity type is allowed") }
    }

    @Test
    fun `it should not authorize access as creator if user does not have the creator role`() = runTest {
        val mockedEntity = mockkClass(NgsiLdEntity::class) {
            every { types } returns listOf(BEEHIVE_TYPE)
        }
        coEvery {
            authorizationService.userCanCreateEntities(any())
        } returns AccessDeniedException("User forbidden to create entities").left()

        authorizationService.checkCreationAuthorized(mockedEntity, None)
            .fold({
                assertEquals(
                    "User forbidden to create entities",
                    it.message
                )
            }, {
                fail("it should have failed as user does not have the creator role")
            })

        clearMocks(authorizationService)
    }

    @Test
    fun `it should not authorize access as creator if entity type is related to authorization`() = runTest {
        val mockedEntity = mockkClass(NgsiLdEntity::class) {
            every { types } returns listOf(USER_TYPE)
        }

        authorizationService.checkCreationAuthorized(mockedEntity, None)
            .fold({
                assertEquals(
                    "Entity type(s) [$USER_TYPE] cannot be managed via normal entity API",
                    it.message
                )
            }, {
                fail("it should have failed as entity type is not allowed")
            })
    }

    @Test
    fun `it should authorize access to update for a single attribute`() = runTest {
        authorizationService.checkUpdateAuthorized(entityUri, listOf(BEEHIVE_TYPE), INCOMING_PROPERTY, None)
            .mapLeft {
                fail("it should not have failed as entity type and attribute are allowed")
            }
    }

    @Test
    fun `it should not authorize access to update for a single attribute if user has not enough rights`() = runTest {
        coEvery {
            authorizationService.userCanUpdateEntity(any(), any())
        } returns AccessDeniedException("User forbidden write access to entity urn:ngsi-ld:Entity:01").left()

        authorizationService.checkUpdateAuthorized(entityUri, listOf(BEEHIVE_TYPE), INCOMING_PROPERTY, None)
            .fold({
                assertEquals("User forbidden write access to entity urn:ngsi-ld:Entity:01", it.message)
            }, {
                fail("it should have failed as user is not allowed to update entity")
            })

        clearMocks(authorizationService)
    }

    @Test
    fun `it should not authorize access to update if the attribute is related to authorization`() = runTest {
        authorizationService.checkUpdateAuthorized(entityUri, listOf(BEEHIVE_TYPE), AUTH_PROP_SAP, None)
            .fold({
                assertEquals(
                    "Specific access policy cannot be updated as a normal property, " +
                        "use /ngsi-ld/v1/entityAccessControl/{entityId}/attrs/specificAccessPolicy endpoint instead",
                    it.message
                )
            }, {
                fail("it should have failed as user is not allowed to update entity")
            })
    }

    @Test
    fun `it should not authorize access to update if the entity type is related to authorization`() = runTest {
        authorizationService.checkUpdateAuthorized(entityUri, listOf(USER_TYPE), INCOMING_PROPERTY, None)
            .fold({
                assertEquals("Entity type(s) [$USER_TYPE] cannot be managed via normal entity API", it.message)
            }, {
                fail("it should have failed as user is not allowed to update entity")
            })
    }

    @Test
    fun `it should authorize access to update for a list of NGSI-LD attributes`() = runTest {
        val mockedIncomingProperty = mockkClass(NgsiLdAttribute::class) {
            every { name } returns INCOMING_PROPERTY
        }
        val mockedOutgoingProperty = mockkClass(NgsiLdAttribute::class) {
            every { name } returns OUTGOING_PROPERTY
        }
        val ngsiLdAttributes = listOf(mockedIncomingProperty, mockedOutgoingProperty)

        authorizationService.checkUpdateAuthorized(entityUri, listOf(BEEHIVE_TYPE), ngsiLdAttributes, None)
            .mapLeft {
                fail("it should not have failed as entity type and attributes are allowed")
            }
    }

    @Test
    fun `it should not authorize access to update if one of the NGSI-LD attributes is related to authorization`() =
        runTest {
            val mockedIncomingProperty = mockkClass(NgsiLdAttribute::class) {
                every { name } returns INCOMING_PROPERTY
            }
            val mockedOutgoingProperty = mockkClass(NgsiLdAttribute::class) {
                every { name } returns AUTH_PROP_SAP
            }
            val ngsiLdAttributes = listOf(mockedIncomingProperty, mockedOutgoingProperty)

            authorizationService.checkUpdateAuthorized(entityUri, listOf(BEEHIVE_TYPE), ngsiLdAttributes, None)
                .fold({}, {
                    fail("it should have failed as one of the attributes is not allowed")
                })
        }

    @Test
    fun `it should authorize access to read`() = runTest {
        authorizationService.checkReadAuthorized(entityUri, listOf(BEEHIVE_TYPE), None)
            .mapLeft {
                fail("it should not have failed as entity type is allowed")
            }
    }

    @Test
    fun `it should not authorize access to read if user has not enough rigts`() = runTest {
        coEvery { authorizationService.userCanReadEntity(any(), any()) } returns AccessDeniedException("").left()

        authorizationService.checkReadAuthorized(entityUri, listOf(BEEHIVE_TYPE), None)
            .fold({}, {
                fail("it should have failed as user has not enough rights to read the entity")
            })

        clearMocks(authorizationService)
    }

    @Test
    fun `it should not authorize access to read if entity type is related to authorization`() = runTest {
        authorizationService.checkReadAuthorized(entityUri, listOf(USER_TYPE), None)
            .fold({}, {
                fail("it should have failed as entity type is related to authorization")
            })
    }
}
