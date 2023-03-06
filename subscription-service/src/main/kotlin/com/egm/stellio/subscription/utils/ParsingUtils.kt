package com.egm.stellio.subscription.utils

import arrow.core.Either
import arrow.core.continuations.either
import arrow.core.left
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.toAPIException
import com.egm.stellio.shared.util.JsonLdUtils
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_CONTEXT
import com.egm.stellio.shared.util.mapper
import com.egm.stellio.shared.util.parseAndExpandTypeSelection
import com.egm.stellio.subscription.model.EndpointInfo
import com.egm.stellio.subscription.model.EntityTypeSelector
import com.egm.stellio.subscription.model.GeoQuery
import com.egm.stellio.subscription.model.Subscription

object ParsingUtils {

    suspend fun parseSubscription(input: Map<String, Any>, contexts: List<String>): Either<APIException, Subscription> =
        try {
            either {
                val subscription = mapper.convertValue(
                    input.plus(JSONLD_CONTEXT to contexts),
                    Subscription::class.java
                )
                subscription.expand(contexts)
                subscription
            }
        } catch (e: Exception) {
            e.toAPIException().left()
        }

    fun parseEntityTypeSelector(input: Map<String, Any>, contexts: List<String>): EntityTypeSelector {
        val entityTypeSelector = mapper.convertValue(input, EntityTypeSelector::class.java)
        entityTypeSelector.type = parseAndExpandTypeSelection(entityTypeSelector.type, contexts)!!
        return entityTypeSelector
    }

    fun parseEndpointInfo(input: String?): List<EndpointInfo>? {
        input?.let {
            return mapper.readValue(
                input,
                mapper.typeFactory.constructCollectionType(List::class.java, EndpointInfo::class.java)
            )
        }
        return null
    }

    fun parseGeoQuery(input: Map<String, Any>, contexts: List<String>): GeoQuery {
        val geoQuery = mapper.convertValue(input, GeoQuery::class.java)
        geoQuery?.geoproperty = geoQuery.geoproperty?.let { JsonLdUtils.expandJsonLdTerm(it, contexts) }
        return geoQuery
    }

    fun endpointInfoToString(input: List<EndpointInfo>?): String =
        mapper.writeValueAsString(input)

    fun endpointInfoMapToString(input: List<Map<String, String>>?): String =
        mapper.writeValueAsString(input)

    fun String.toSqlColumnName(): String =
        this.map {
            if (it.isUpperCase()) "_${it.lowercase()}"
            else it
        }.joinToString("")

    fun Any.toSqlValue(columnName: String): Any? =
        when (columnName) {
            "watchedAttributes", "contexts" -> {
                val valueAsArrayList = this as ArrayList<String>
                if (valueAsArrayList.isEmpty())
                    null
                else
                    valueAsArrayList.joinToString(separator = ",")
            }
            else -> this
        }
}
