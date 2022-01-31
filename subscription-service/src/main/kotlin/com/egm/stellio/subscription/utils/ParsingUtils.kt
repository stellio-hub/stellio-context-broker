package com.egm.stellio.subscription.utils

import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.util.JsonLdUtils
import com.egm.stellio.shared.util.mapper
import com.egm.stellio.subscription.model.EndpointInfo
import com.egm.stellio.subscription.model.EntityInfo
import com.egm.stellio.subscription.model.Subscription
import com.fasterxml.jackson.databind.node.ObjectNode
import org.slf4j.LoggerFactory

object ParsingUtils {

    private val logger = LoggerFactory.getLogger(javaClass)

    fun parseSubscription(input: String, context: List<String>): Subscription {
        val rawParsedData = mapper.readTree(input) as ObjectNode
        if (rawParsedData.get("@context") != null)
            rawParsedData.remove("@context")

        try {
            val subscription = mapper.readValue(rawParsedData.toString(), Subscription::class.java)
            subscription.expandTypes(context)
            return subscription
        } catch (e: Exception) {
            logger.error("Error while parsing a subscription: ${e.message}", e)
            throw BadRequestDataException(e.message ?: "Failed to parse subscription")
        }
    }

    fun parseSubscriptionUpdate(input: String, context: List<String>): Pair<Map<String, Any>, List<String>> {
        val parsedSubscription: Map<String, List<Any>> = mapper.readValue(
            input,
            mapper.typeFactory.constructMapLikeType(
                Map::class.java, String::class.java, Any::class.java
            )
        )

        return Pair(parsedSubscription, context)
    }

    fun parseEntityInfo(input: Map<String, Any>, contexts: List<String>?): EntityInfo {
        val entityInfo = mapper.convertValue(input, EntityInfo::class.java)
        entityInfo.type = JsonLdUtils.expandJsonLdKey(entityInfo.type, contexts!!)!!
        return entityInfo
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

    fun endpointInfoToString(input: List<EndpointInfo>?): String {
        return mapper.writeValueAsString(input)
    }

    fun endpointInfoMapToString(input: List<Map<String, String>>?): String {
        return mapper.writeValueAsString(input)
    }

    fun String.toSqlColumnName(): String =
        this.map {
            if (it.isUpperCase()) "_${it.lowercase()}"
            else it
        }.joinToString("")

    fun Any.toSqlValue(columnName: String): Any? =
        when (columnName) {
            "watchedAttributes" -> {
                val valueAsArrayList = this as ArrayList<String>
                if (valueAsArrayList.isEmpty())
                    null
                else
                    valueAsArrayList.joinToString(separator = ",")
            }
            else -> this
        }
}
