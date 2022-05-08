package com.egm.stellio.entity.authorization

import arrow.core.None
import com.egm.stellio.shared.model.NgsiLdAttribute
import com.egm.stellio.shared.model.NgsiLdEntity
import com.egm.stellio.shared.model.QueryParams
import com.egm.stellio.shared.util.AuthContextModel.AUTH_PROP_SAP
import com.egm.stellio.shared.util.AuthContextModel.USER_TYPE
import com.egm.stellio.shared.util.BEEHIVE_TYPE
import com.egm.stellio.shared.util.INCOMING_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CORE_CONTEXT
import com.egm.stellio.shared.util.OUTGOING_PROPERTY
import com.egm.stellio.shared.util.toUri
import io.mockk.clearMocks
import io.mockk.every
import io.mockk.mockkClass
import io.mockk.spyk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.fail

class AuthorizationServiceTests {

    private val authorizationService = spyk(StandaloneAuthorizationService())

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
    fun `it should authorize access as admin`() {
        authorizationService.isAdminAuthorized(entityUri, listOf(BEEHIVE_TYPE), None)
            .mapLeft { fail("it should not have failed as entity type is allowed") }
    }

    @Test
    fun `it should not authorize access as admin if user is not admin`() {
        every { authorizationService.userIsAdminOfEntity(any(), any()) } returns false

        authorizationService.isAdminAuthorized(entityUri, listOf(BEEHIVE_TYPE), None)
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
    fun `it should not authorize access as admin if entity type is related to authorization`() {
        authorizationService.isAdminAuthorized(entityUri, listOf(USER_TYPE), None)
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
    fun `it should not authorize access as admin if one of the entity types is related to authorization`() {
        authorizationService.isAdminAuthorized(entityUri, listOf(USER_TYPE, BEEHIVE_TYPE), None)
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
    fun `it should authorize access as creator`() {
        val mockedEntity = mockkClass(NgsiLdEntity::class) {
            every { types } returns listOf(BEEHIVE_TYPE)
        }

        authorizationService.isCreationAuthorized(mockedEntity, None)
            .mapLeft { fail("it should not have failed as entity type is allowed") }
    }

    @Test
    fun `it should not authorize access as creator if user does not have the creator role`() {
        val mockedEntity = mockkClass(NgsiLdEntity::class) {
            every { types } returns listOf(BEEHIVE_TYPE)
        }
        every { authorizationService.userCanCreateEntities(any()) } returns false

        authorizationService.isCreationAuthorized(mockedEntity, None)
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
    fun `it should not authorize access as creator if entity type is related to authorization`() {
        val mockedEntity = mockkClass(NgsiLdEntity::class) {
            every { types } returns listOf(USER_TYPE)
        }

        authorizationService.isCreationAuthorized(mockedEntity, None)
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
    fun `it should authorize access to update for a single attribute`() {
        authorizationService.isUpdateAuthorized(entityUri, listOf(BEEHIVE_TYPE), INCOMING_PROPERTY, None)
            .mapLeft {
                fail("it should not have failed as entity type and attribute are allowed")
            }
    }

    @Test
    fun `it should not authorize access to update for a single attribute if user has not enough rights`() {
        every { authorizationService.userCanUpdateEntity(any(), any()) } returns false

        authorizationService.isUpdateAuthorized(entityUri, listOf(BEEHIVE_TYPE), INCOMING_PROPERTY, None)
            .fold({
                assertEquals("User forbidden write access to entity urn:ngsi-ld:Entity:01", it.message)
            }, {
                fail("it should have failed as user is not allowed to update entity")
            })

        clearMocks(authorizationService)
    }

    @Test
    fun `it should not authorize access to update if the attribute is related to authorization`() {
        authorizationService.isUpdateAuthorized(entityUri, listOf(BEEHIVE_TYPE), AUTH_PROP_SAP, None)
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
    fun `it should not authorize access to update if the entity type is related to authorization`() {
        authorizationService.isUpdateAuthorized(entityUri, listOf(USER_TYPE), INCOMING_PROPERTY, None)
            .fold({
                assertEquals("Entity type(s) [$USER_TYPE] cannot be managed via normal entity API", it.message)
            }, {
                fail("it should have failed as user is not allowed to update entity")
            })
    }

    @Test
    fun `it should authorize access to update for a list of NGSI-LD attributes`() {
        val mockedIncomingProperty = mockkClass(NgsiLdAttribute::class) {
            every { name } returns INCOMING_PROPERTY
        }
        val mockedOutgoingProperty = mockkClass(NgsiLdAttribute::class) {
            every { name } returns OUTGOING_PROPERTY
        }
        val ngsiLdAttributes = listOf(mockedIncomingProperty, mockedOutgoingProperty)

        authorizationService.isUpdateAuthorized(entityUri, listOf(BEEHIVE_TYPE), ngsiLdAttributes, None)
            .mapLeft {
                fail("it should not have failed as entity type and attributes are allowed")
            }
    }

    @Test
    fun `it should not authorize access to update if one of the NGSI-LD attributes is related to authorization`() {
        val mockedIncomingProperty = mockkClass(NgsiLdAttribute::class) {
            every { name } returns INCOMING_PROPERTY
        }
        val mockedOutgoingProperty = mockkClass(NgsiLdAttribute::class) {
            every { name } returns AUTH_PROP_SAP
        }
        val ngsiLdAttributes = listOf(mockedIncomingProperty, mockedOutgoingProperty)

        authorizationService.isUpdateAuthorized(entityUri, listOf(BEEHIVE_TYPE), ngsiLdAttributes, None)
            .fold({}, {
                fail("it should have failed as one of the attributes is not allowed")
            })
    }

    @Test
    fun `it should authorize access to read`() {
        authorizationService.isReadAuthorized(entityUri, listOf(BEEHIVE_TYPE), None)
            .mapLeft {
                fail("it should not have failed as entity type is allowed")
            }
    }

    @Test
    fun `it should not authorize access to read if user has not enough rigts`() {
        every { authorizationService.userCanReadEntity(any(), any()) } returns false

        authorizationService.isReadAuthorized(entityUri, listOf(BEEHIVE_TYPE), None)
            .fold({}, {
                fail("it should have failed as user has not enough rights to read the entity")
            })

        clearMocks(authorizationService)
    }

    @Test
    fun `it should not authorize access to read if entity type is related to authorization`() {
        authorizationService.isReadAuthorized(entityUri, listOf(USER_TYPE), None)
            .fold({}, {
                fail("it should have failed as entity type is related to authorization")
            })
    }

    @Test
    fun `get authorized entities should return a count of -1 if authentication is not enabled`() {
        val authorizedEntities = authorizationService.getAuthorizedEntities(
            QueryParams(offset = 0, limit = 0),
            None,
            NGSILD_CORE_CONTEXT
        )
        assertEquals(-1, authorizedEntities.first)
        assertEquals(0, authorizedEntities.second.size)
    }

    @Test
    fun `get groups memberships should return a count of -1 if authentication is not enabled`() {
        val authorizedEntities = authorizationService.getGroupsMemberships(
            None,
            0,
            0,
            NGSILD_CORE_CONTEXT
        )
        assertEquals(-1, authorizedEntities.first)
        assertEquals(0, authorizedEntities.second.size)
    }
}
