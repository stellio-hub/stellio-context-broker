package com.egm.stellio.shared.queryparameter

import arrow.core.Either
import arrow.core.left
import arrow.core.right
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.TooManyResultsException
import com.egm.stellio.shared.util.QueryParameterErrorMessages.OFFSET_AND_LIMIT_MUST_BE_POSITIVE_MESSAGE
import com.egm.stellio.shared.util.QueryParameterErrorMessages.OFFSET_AND_LIMIT_MUST_BE_POSITIVE_NO_COUNT_MESSAGE
import com.egm.stellio.shared.util.QueryParameterErrorMessages.tooHighLimitMessage
import org.springframework.util.MultiValueMap

data class PaginationQuery(
    val offset: Int,
    val limit: Int,
    val count: Boolean = false
) {
    companion object {

        fun parsePaginationParameters(
            queryParams: MultiValueMap<String, String>,
            limitDefault: Int,
            limitMax: Int
        ): Either<APIException, PaginationQuery> {
            val count = queryParams.getFirst(QueryParameter.COUNT.key)?.toBoolean() == true
            val offset = queryParams.getFirst(QueryParameter.OFFSET.key)?.toIntOrNull() ?: 0
            val limit = queryParams.getFirst(QueryParameter.LIMIT.key)?.toIntOrNull() ?: limitDefault
            if (!count && (limit <= 0 || offset < 0))
                return BadRequestDataException(OFFSET_AND_LIMIT_MUST_BE_POSITIVE_NO_COUNT_MESSAGE).left()
            if (count && (limit < 0 || offset < 0))
                return BadRequestDataException(OFFSET_AND_LIMIT_MUST_BE_POSITIVE_MESSAGE).left()
            if (limit > limitMax)
                return TooManyResultsException(tooHighLimitMessage(limit, limitMax)).left()
            return PaginationQuery(offset, limit, count).right()
        }
    }
}
