package com.egm.stellio.subscription.utils

import arrow.core.Either
import arrow.core.left
import arrow.core.right
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.EntitySelector
import com.egm.stellio.shared.model.toAPIException
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_CONTEXT
import com.egm.stellio.shared.util.JsonUtils.deserializeAs
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.util.expandTypeSelection
import com.egm.stellio.subscription.model.EndpointInfo
import com.egm.stellio.subscription.model.Subscription
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.module.kotlin.convertValue
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper

object ParsingUtils {

    private val mapper: ObjectMapper =
        jacksonObjectMapper()
            .setSerializationInclusion(JsonInclude.Include.NON_NULL)
            .findAndRegisterModules()
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

    fun parseSubscription(input: Map<String, Any>, contexts: List<String>): Either<APIException, Subscription> =
        runCatching {
            deserializeAs<Subscription>(serializeObject(input.plus(JSONLD_CONTEXT to contexts)))
                .expand(contexts)
        }.fold(
            { it.right() },
            { it.toAPIException("Failed to parse subscription").left() }
        )

    fun convertToMap(input: Any): Map<String, Any> =
        mapper.convertValue(input)

    fun serializeSubscription(input: Any): String =
        mapper.writeValueAsString(input)

    fun parseEntitySelector(input: Map<String, Any>, contexts: List<String>): EntitySelector {
        val entitySelector = mapper.convertValue(input, EntitySelector::class.java)
        return entitySelector.copy(
            typeSelection = expandTypeSelection(entitySelector.typeSelection, contexts)!!
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
            "notificationTrigger" -> (this as ArrayList<String>).toTypedArray()
            else -> this
        }
}
