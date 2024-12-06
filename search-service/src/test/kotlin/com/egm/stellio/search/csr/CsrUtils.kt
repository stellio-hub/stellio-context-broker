package com.egm.stellio.search.csr

import com.egm.stellio.search.csr.model.ContextSourceRegistration
import com.egm.stellio.search.csr.model.Operation
import com.egm.stellio.shared.util.ngsiLdDateTime
import com.egm.stellio.shared.util.toUri

object CsrUtils {
    fun gimmeRawCSR() = ContextSourceRegistration(
        id = "urn:ngsi-ld:ContextSourceRegistration:test".toUri(),
        endpoint = "http://localhost:8089".toUri(),
        information = emptyList(),
        operations = listOf(Operation.FEDERATION_OPS),
        createdAt = ngsiLdDateTime(),
    )
}
