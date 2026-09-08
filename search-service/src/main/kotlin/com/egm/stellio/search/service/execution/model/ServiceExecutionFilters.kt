package com.egm.stellio.search.service.execution.model

import arrow.core.Either
import arrow.core.raise.either
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.queryparameter.QueryParameter
import com.egm.stellio.shared.util.parseQueryParameter
import com.egm.stellio.shared.util.toUri
import org.springframework.util.MultiValueMap
import java.net.URI

data class ServiceExecutionFilters(
    val ids: Set<URI>? = null,
    val serviceIds: Set<URI>? = null,
    val entityIds: Set<URI>? = null,
    val executionStatuses: Set<ServiceExecutionStatus>? = null,
) {
    companion object {
        fun fromQueryParameters(
            queryParams: MultiValueMap<String, String>
        ): Either<APIException, ServiceExecutionFilters> = either {
            val ids = parseQueryParameter(queryParams.getFirst(QueryParameter.ID.key)).map { it.toUri() }.toSet()
            val serviceIds = parseQueryParameter(queryParams.getFirst(QueryParameter.SERVICE_ID.key))
                .map { it.toUri() }.toSet()
            val entityIds = parseQueryParameter(queryParams.getFirst(QueryParameter.ENTITY_ID.key))
                .map { it.toUri() }.toSet()
            val executionStatuses = parseQueryParameter(queryParams.getFirst(QueryParameter.EXECUTION_STATUS.key))
                .map { ServiceExecutionStatus.fromString(it).bind() }.toSet()

            ServiceExecutionFilters(
                ids = ids,
                serviceIds = serviceIds,
                entityIds = entityIds,
                executionStatuses = executionStatuses
            )
        }
    }
}
