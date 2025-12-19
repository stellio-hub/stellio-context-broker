package com.egm.stellio.search.common.model

import arrow.core.Either
import arrow.core.left
import arrow.core.raise.either
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.InvalidRequestException
import com.egm.stellio.shared.queryparameter.AttributePath

data class OrderBy(
    val param: String,
    val contexts: List<String>
) {
    val direction: Direction = Direction.fromString(param.substringAfter(';')).fold({ Direction.ASC }, { it })
    val attributePath: AttributePath = AttributePath(param.substringBefore(';', param), contexts)

    fun buildSql(): String {
        val attributeSql = attributePath.buildSqlOrderClause()
        val directionSql = when (direction) {
            Direction.ASC -> "ASC"
            Direction.DESC -> "DESC"
            Direction.DIST_ASC -> "ASC"
            Direction.DIST_DESC -> "DESC"
        }
        return "$attributeSql $directionSql"
    }

    enum class Direction(val value: String) {

        ASC("asc"),
        DESC("desc"),
        DIST_ASC("dist-asc"),
        DIST_DESC("dist-desc");
        companion object {
            fun fromString(key: String): Either<APIException, Direction> = either {
                Direction.entries.find { it.value == key }
                    ?: return InvalidRequestException("'$key' is not a valid ordering direction query parameter").left()
            }
        }
    }
}
