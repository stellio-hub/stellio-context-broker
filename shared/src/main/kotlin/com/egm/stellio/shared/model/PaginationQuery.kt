package com.egm.stellio.shared.model

import arrow.core.Either
import arrow.core.left
import arrow.core.right
import com.egm.stellio.shared.model.parameter.PaginationParameter.COUNT
import com.egm.stellio.shared.model.parameter.PaginationParameter.LIMIT
import com.egm.stellio.shared.model.parameter.PaginationParameter.OFFSET
import org.springframework.util.MultiValueMap

data class PaginationQuery(
    val offset: Int,
    val limit: Int,
    val count: Boolean = false
) {
    companion object {
        fun create(
            params: MultiValueMap<String, String>,
            limitDefault: Int,
            limitMax: Int
        ): Either<APIException, PaginationQuery> {
            val count = params.getFirst(COUNT.key)?.toBoolean() ?: false
            val offset = params.getFirst(OFFSET.key)?.toIntOrNull() ?: 0
            val limit = params.getFirst(LIMIT.key)?.toIntOrNull() ?: limitDefault
            if (!count && (limit <= 0 || offset < 0))
                return BadRequestDataException(
                    "Offset must be greater than zero and limit must be strictly greater than zero"
                ).left()
            if (count && (limit < 0 || offset < 0))
                return BadRequestDataException("Offset and limit must be greater than zero").left()
            if (limit > limitMax)
                return TooManyResultsException(
                    "You asked for $limit results, but the supported maximum limit is $limitMax"
                ).left()
            return PaginationQuery(offset, limit, count).right()
        }
    }
}
