package com.egm.stellio.search.model

import arrow.core.Either
import arrow.core.left
import arrow.core.raise.either
import arrow.core.raise.ensure
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.EntitySelector
import com.egm.stellio.shared.util.JsonUtils

/**
 * A Query data type as defined in 5.2.23.
 *
 * It represents the raw data as received in the API. To have a consistent validation handling, even mandatory
 * parameters are declared as optional here, validation is performed later in #{EntitiesQueryUtils}.
 */
data class Query private constructor(
    val type: String,
    val entities: List<EntitySelector>? = null,
    val attrs: List<String>? = null,
    val q: String? = null,
    val geoQ: UnparsedGeoQuery? = null,
    val temporalQ: UnparsedTemporalQuery? = null,
    val scopeQ: String? = null,
    val lang: String? = null,
    val datasetId: List<String>? = null,
) {
    companion object {
        operator fun invoke(queryBody: String): Either<APIException, Query> = either {
            runCatching {
                JsonUtils.deserializeDataTypeAs<Query>(queryBody)
            }.fold(
                {
                    ensure(it.type == "Query") {
                        BadRequestDataException("The type parameter should be equals to 'Query'")
                    }
                    it
                },
                {
                    BadRequestDataException(
                        "The supplied query could not be parsed: ${it.message}"
                    ).left().bind<Query>()
                }
            )
        }
    }
}

data class UnparsedTemporalQuery(
    val timerel: String? = null,
    val timeAt: String? = null,
    val endTimeAt: String? = null,
    val aggrPeriodDuration: String? = null,
    val aggrMethods: List<String>? = null,
    val lastN: Int? = null,
    val timeproperty: String = "observedAt"
)

data class UnparsedGeoQuery(
    val geometry: String,
    val coordinates: List<Any>,
    val georel: String,
    val geoproperty: String
)
