package com.egm.stellio.search.csr

import com.egm.stellio.search.csr.model.ContextSourceRegistration
import com.egm.stellio.search.csr.model.EntityInfo
import com.egm.stellio.search.csr.model.Mode
import com.egm.stellio.search.csr.model.Operation
import com.egm.stellio.search.csr.model.RegistrationInfo
import com.egm.stellio.shared.util.APIARY_IRI
import com.egm.stellio.shared.util.DateUtils.ngsiLdDateTime
import com.egm.stellio.shared.util.UriUtils.toUri
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
        mode: Mode = Mode.INCLUSIVE
    ) = ContextSourceRegistration(
        id = id,
        endpoint = endpoint,
        information = information,
        operations = operations,
        createdAt = ngsiLdDateTime(),
        mode = mode
    )
}
