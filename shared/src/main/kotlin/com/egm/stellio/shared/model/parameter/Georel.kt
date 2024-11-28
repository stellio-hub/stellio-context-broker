package com.egm.stellio.shared.model.parameter

import arrow.core.Either
import arrow.core.left
import arrow.core.right
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.BadRequestDataException

enum class Georel(val key: String) {
    NEAR("near"),
    WITHIN("within"),
    CONTAINS("contains"),
    INTERSECTS("intersects"),
    EQUALS("equals"),
    DISJOINT("disjoint"),
    OVERLAPS("overlaps");
    companion object {
        val ALL = entries.map { it.key }

        const val NEAR_DISTANCE_MODIFIER = "distance"
        const val NEAR_MAXDISTANCE_MODIFIER = "maxDistance"
        private val nearRegex = "^near;(?:minDistance|maxDistance)==\\d+$".toRegex()

        fun verify(georel: String): Either<APIException, Unit> {
            if (georel.startsWith(NEAR.key)) {
                if (!georel.matches(nearRegex))
                    return BadRequestDataException("Invalid expression for 'near' georel: $georel").left()
                return Unit.right()
            } else if (ALL.any { georel == it })
                return Unit.right()
            else return BadRequestDataException("Invalid 'georel' parameter provided: $georel").left()
        }

        fun prepareQuery(georel: String): Triple<String, String?, String?> =
            if (georel.startsWith(NEAR.key)) {
                val comparisonParams = georel.split(";")[1].split("==")
                if (comparisonParams[0] == NEAR_MAXDISTANCE_MODIFIER)
                    Triple(NEAR_DISTANCE_MODIFIER, "<=", comparisonParams[1])
                else Triple(NEAR_DISTANCE_MODIFIER, ">=", comparisonParams[1])
            } else Triple(georel, null, null)
    }
}
