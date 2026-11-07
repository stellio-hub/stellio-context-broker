package com.egm.stellio.search.csr.model

import com.egm.stellio.shared.util.ngsiLdDateTime
import com.egm.stellio.shared.util.toUri
import com.egm.stellio.shared.web.DEFAULT_TENANT_NAME
import com.fasterxml.jackson.annotation.JsonInclude
import java.lang.management.ManagementFactory
import java.net.URI
import java.time.Duration
import java.time.ZonedDateTime

const val NGSILD_CONTEXT_SOURCE_IDENTITY_TERM = "ContextSourceIdentity"

// characters not allowed in an RFC 7230 token (the syntax of a Via pseudonym)
private val nonTokenChars = Regex("[^A-Za-z0-9!#$%&'*+.^_`|~-]")

/**
 * ContextSourceIdentity type as defined in TS 104 175 §5.2.6.5.6
 */
data class ContextSourceIdentity(
    val id: URI,
    val type: String = NGSILD_CONTEXT_SOURCE_IDENTITY_TERM,
    val contextSourceAlias: String,
    val contextSourceUptime: String,
    val contextSourceTimeAt: ZonedDateTime,
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    val contextSourceExtras: Map<String, Any> = emptyMap()
) {
    companion object {
        fun forTenant(alias: String, tenantName: String, extras: Map<String, Any>): ContextSourceIdentity {
            val tenantAlias =
                if (tenantName == DEFAULT_TENANT_NAME) alias
                else "$alias-${tenantName.substringAfterLast(':').replace(nonTokenChars, "-")}"

            return ContextSourceIdentity(
                id = "urn:ngsi-ld:ContextSource:$tenantAlias".toUri(),
                contextSourceAlias = tenantAlias,
                // JVM uptime, i.e. uptime of this replica
                contextSourceUptime = Duration.ofMillis(ManagementFactory.getRuntimeMXBean().uptime).toString(),
                contextSourceTimeAt = ngsiLdDateTime(),
                contextSourceExtras = extras
            )
        }
    }
}
