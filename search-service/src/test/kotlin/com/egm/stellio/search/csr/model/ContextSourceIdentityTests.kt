package com.egm.stellio.search.csr.model

import com.egm.stellio.shared.util.DataTypes.serialize
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import com.egm.stellio.shared.util.toUri
import com.egm.stellio.shared.web.DEFAULT_TENANT_NAME
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.ZonedDateTime

class ContextSourceIdentityTests {

    @Test
    fun `forTenant should use the alias as is for the default tenant`() {
        val identity = ContextSourceIdentity.forTenant("stellio", DEFAULT_TENANT_NAME, emptyMap())

        assertEquals("stellio", identity.contextSourceAlias)
        assertEquals("urn:ngsi-ld:ContextSource:stellio".toUri(), identity.id)
        assertEquals(NGSILD_CONTEXT_SOURCE_IDENTITY_TERM, identity.type)
    }

    @Test
    fun `forTenant should suffix the alias with the last segment of the tenant name`() {
        val identity = ContextSourceIdentity.forTenant("stellio", "urn:ngsi-ld:tenant:city", emptyMap())

        assertEquals("stellio-city", identity.contextSourceAlias)
        assertEquals("urn:ngsi-ld:ContextSource:stellio-city".toUri(), identity.id)
    }

    @Test
    fun `forTenant should replace characters not allowed in a token when building the alias`() {
        val identity = ContextSourceIdentity.forTenant("stellio", "urn:ngsi-ld:tenant:my city/north", emptyMap())

        assertEquals("stellio-my-city-north", identity.contextSourceAlias)
    }

    @Test
    fun `forTenant should return an ISO 8601 uptime`() {
        val identity = ContextSourceIdentity.forTenant("stellio", DEFAULT_TENANT_NAME, emptyMap())

        assertTrue(Duration.parse(identity.contextSourceUptime) > Duration.ZERO)
    }

    @Test
    fun `serialize should return the time at the Context Source as a DateTime`() {
        val identity = ContextSourceIdentity.forTenant("stellio", DEFAULT_TENANT_NAME, emptyMap())

        val serialized = serialize(identity).deserializeAsMap()
        ZonedDateTime.parse(serialized["contextSourceTimeAt"] as String)
    }

    @Test
    fun `serialize should not return contextSourceExtras when there are no extras`() {
        val identity = ContextSourceIdentity.forTenant("stellio", DEFAULT_TENANT_NAME, emptyMap())

        assertFalse(serialize(identity).deserializeAsMap().containsKey("contextSourceExtras"))
    }

    @Test
    fun `serialize should return contextSourceExtras when there are extras`() {
        val extras = mapOf("deployment" to "test", "contact" to mapOf("email" to "ops@example.org"))
        val identity = ContextSourceIdentity.forTenant("stellio", DEFAULT_TENANT_NAME, extras)

        assertEquals(extras, serialize(identity).deserializeAsMap()["contextSourceExtras"])
    }
}
