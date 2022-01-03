package com.egm.stellio.shared.util

import arrow.core.None
import arrow.core.Some
import com.egm.stellio.shared.util.AuthContextModel.AUTH_TERM_SID
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.springframework.test.context.ActiveProfiles
import java.net.URI
import java.util.UUID

@ActiveProfiles("test")
class AuthUtilsTests {

    @Test
    fun `it should extract UUID from an entity URI`() {
        assertEquals(
            UUID.fromString("3693C62A-D5B2-4F9E-9D3A-F82814984D5C"),
            URI.create("urn:ngsi-ld:Entity:3693C62A-D5B2-4F9E-9D3A-F82814984D5C").extractSubjectUuid()
        )
    }

    @Test
    fun `it should extract UUID from a string version of an entity URI`() {
        assertEquals(
            UUID.fromString("3693C62A-D5B2-4F9E-9D3A-F82814984D5C"),
            "urn:ngsi-ld:Entity:3693C62A-D5B2-4F9E-9D3A-F82814984D5C".extractSubjectUuid()
        )
    }

    @Test
    fun `it should extract the compact form of an authorization term`() {
        assertEquals("serviceAccountId", AUTH_TERM_SID.toCompactTerm())
    }

    @Test
    fun `it should find the global role with a given key`() {
        assertEquals(Some(GlobalRole.STELLIO_ADMIN), GlobalRole.forKey("stellio-admin"))
        assertEquals(Some(GlobalRole.STELLIO_CREATOR), GlobalRole.forKey("stellio-creator"))
    }

    @Test
    fun `it should not find the global role for an unknown key`() {
        assertEquals(None, GlobalRole.forKey("unknown-role"))
    }

    @Test
    fun `it should find the access right with a given key`() {
        assertEquals(Some(AccessRight.R_CAN_READ), AccessRight.forAttributeName("rCanRead"))
        assertEquals(Some(AccessRight.R_CAN_WRITE), AccessRight.forAttributeName("rCanWrite"))
        assertEquals(Some(AccessRight.R_CAN_ADMIN), AccessRight.forAttributeName("rCanAdmin"))
    }

    @Test
    fun `it should not find the access right for an unknown key`() {
        assertEquals(None, AccessRight.forAttributeName("unknown-access-right"))
    }
}
