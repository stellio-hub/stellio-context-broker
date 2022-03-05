package com.egm.stellio.entity.authorization

import com.egm.stellio.shared.util.AuthContextModel.AUTH_PROP_SAP
import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.assertEquals

class AuthorizationServiceTests {

    private val authorizationService = StandaloneAuthorizationService()

    @Test
    fun `it should authorize an attribute that is not specific access policy`() {
        authorizationService.checkAttributeIsAuthorized("someAttribute")
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
}
