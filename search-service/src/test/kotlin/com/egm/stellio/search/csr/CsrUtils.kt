package com.egm.stellio.search.csr

import com.egm.stellio.search.csr.model.ContextSourceRegistration
import com.egm.stellio.search.csr.model.Mode
import com.egm.stellio.search.csr.model.Operation
import com.egm.stellio.shared.util.APIARY_TYPE
import com.egm.stellio.shared.util.ngsiLdDateTime
import com.egm.stellio.shared.util.toUri
import java.net.URI

object CsrUtils {
    fun gimmeRawCSR(
        id: URI = "urn:ngsi-ld:ContextSourceRegistration:test".toUri(),
        endpoint: URI = "http://localhost:8089".toUri(),
        information: List<ContextSourceRegistration.RegistrationInfo> = listOf(
            ContextSourceRegistration.RegistrationInfo(
                listOf(ContextSourceRegistration.EntityInfo(types = listOf(APIARY_TYPE)))
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
