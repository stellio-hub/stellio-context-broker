package com.egm.stellio.search.csr

import com.egm.stellio.search.csr.model.ContextSourceInfo
import com.egm.stellio.search.csr.model.ContextSourceRegistration
import com.egm.stellio.search.csr.model.EntityInfo
import com.egm.stellio.search.csr.model.Mode
import com.egm.stellio.search.csr.model.Operation
import com.egm.stellio.search.csr.model.RegistrationInfo
import com.egm.stellio.shared.util.APIARY_IRI
import com.egm.stellio.shared.util.ngsiLdDateTime
import com.egm.stellio.shared.util.toUri
import java.net.URI

object CsrUtils {
    fun gimmeRawCSR(
        id: URI = "urn:ngsi-ld:ContextSourceRegistration:test".toUri(),
        endpoint: URI = "http://localhost:8089".toUri(),
        information: List<RegistrationInfo> = listOf(
            RegistrationInfo(
                listOf(EntityInfo(types = listOf(APIARY_IRI)))
            )
        ),
        operations: List<Operation> = listOf(Operation.FEDERATION_OPS),
        mode: Mode = Mode.INCLUSIVE,
        contextSourceInfo: List<ContextSourceInfo>? = null,
        tenant: String? = null
    ) = ContextSourceRegistration(
        id = id,
        endpoint = endpoint,
        information = information,
        operations = operations,
        createdAt = ngsiLdDateTime(),
        mode = mode,
        contextSourceInfo = contextSourceInfo,
        tenant = tenant
    )
}
