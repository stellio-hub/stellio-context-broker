package com.egm.stellio.entity.authorization

import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.util.AuthContextModel.AUTH_PROP_SAP
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.assertThrows

class AuthorizationServiceTests {

    private val authorizationService = StandaloneAuthorizationService()

    @Test
    fun `it should authorize an attribute that is not specific access policy`() {
        assertDoesNotThrow {
            authorizationService.checkAttributeIsAuthorized("someAttribute")
        }
    }

    @Test
    fun `it should throw a bad request exception if attribute is specific access policy`() {
        val exception = assertThrows<BadRequestDataException> {
            authorizationService.checkAttributeIsAuthorized(AUTH_PROP_SAP)
        }
        Assertions.assertEquals(
            "Specific access policy cannot be updated as a normal property, " +
                "use /ngsi-ld/v1/entityAccessControl/{entityId}/attrs/specificAccessPolicy endpoint instead",
            exception.message
        )
    }
}
