package com.egm.stellio.search.service.registration.model

import arrow.core.Either
import arrow.core.left
import arrow.core.raise.either
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.EntityTypeSelection
import com.egm.stellio.shared.queryparameter.QueryParameter
import com.egm.stellio.shared.util.ErrorMessages.ServiceRegistration.SERVICE_REGISTRATION_QUERY_REQUIRED_MESSAGE
import com.egm.stellio.shared.util.expandTypeSelection
import com.egm.stellio.shared.util.toListOfUri
import org.springframework.util.MultiValueMap
import java.net.URI

data class ServiceRegistrationFilters(
    val ids: Set<URI>,
    val typeSelection: EntityTypeSelection
) {
    companion object {
        fun fromQueryParameters(
            queryParams: MultiValueMap<String, String>,
            contexts: List<String>
        ): Either<APIException, ServiceRegistrationFilters> = either {
            val rawIds = queryParams.getFirst(QueryParameter.ID.key)
            val rawType = queryParams.getFirst(QueryParameter.TYPE.key)

            if (rawIds.isNullOrBlank() || rawType.isNullOrBlank()) {
                BadRequestDataException(SERVICE_REGISTRATION_QUERY_REQUIRED_MESSAGE)
                    .left()
                    .bind<ServiceRegistrationFilters>()
            }

            ServiceRegistrationFilters(
                ids = rawIds!!.split(",").toListOfUri().toSet(),
                typeSelection = expandTypeSelection(rawType!!, contexts)!!
            )
        }
    }
}
