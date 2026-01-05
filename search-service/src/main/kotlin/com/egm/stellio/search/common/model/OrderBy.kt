package com.egm.stellio.search.common.model

import arrow.core.Either
import arrow.core.left
import arrow.core.raise.either
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.queryparameter.AttributePath

data class OrderBy(
    val direction: Direction,
    val attributePath: AttributePath
) {

    fun buildSql(): String {
        val attributeSql = attributePath.buildSqlOrderClause()
        val directionSql = when (direction) {
            Direction.ASC -> "ASC"
            Direction.DESC -> "DESC"
            Direction.DIST_ASC -> "ASC"
            Direction.DIST_DESC -> "DESC"
        }
        return "$attributeSql $directionSql NULLS LAST"
    }
    companion object {
        fun fromParam(
            param: String,
            contexts: List<String>
        ): Either<APIException, OrderBy> = either {
            OrderBy(
                Direction.fromString(param.substringAfter(';', Direction.ASC.value)).bind(),
                AttributePath(param.substringBefore(';', param), contexts)
            )
        }
    }

    enum class Direction(val value: String) {
        ASC("asc"),
        DESC("desc"),
        DIST_ASC("dist-asc"),
        DIST_DESC("dist-desc");

        companion object {
            fun fromString(key: String): Either<APIException, Direction> = either {
                Direction.entries.find { it.value == key }
                    ?: return BadRequestDataException("'$key' is not a valid ordering direction parameter").left()
            }
        }
    }
}
