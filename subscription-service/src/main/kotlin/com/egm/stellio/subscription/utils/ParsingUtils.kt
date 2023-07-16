package com.egm.stellio.subscription.utils

import arrow.core.Either
import arrow.core.left
import arrow.core.raise.either
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.toAPIException
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_CONTEXT
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdTerm
import com.egm.stellio.shared.util.mapper
import com.egm.stellio.subscription.model.EndpointInfo
import com.egm.stellio.subscription.model.EntityInfo
import com.egm.stellio.subscription.model.Subscription

object ParsingUtils {

    fun parseSubscription(input: Map<String, Any>, contexts: List<String>): Either<APIException, Subscription> =
        try {
            either {
                mapper.convertValue(
                    input.plus(JSONLD_CONTEXT to contexts),
                    Subscription::class.java
                ).expand(contexts)
            }
        } catch (e: Exception) {
            e.toAPIException().left()
        }

    fun parseEntityInfo(input: Map<String, Any>, contexts: List<String>): EntityInfo {
        val entityInfo = mapper.convertValue(input, EntityInfo::class.java)
        return entityInfo.copy(
            type = expandJsonLdTerm(entityInfo.type, contexts)
        )
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
