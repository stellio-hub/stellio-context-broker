package com.egm.stellio.subscription.utils

import arrow.core.Either
import arrow.core.computations.either
import arrow.core.left
import arrow.core.right
import com.egm.stellio.shared.model.APiException
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.util.JsonLdUtils
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_CONTEXT
import com.egm.stellio.shared.util.mapper
import com.egm.stellio.subscription.model.EndpointInfo
import com.egm.stellio.subscription.model.EntityInfo
import com.egm.stellio.subscription.model.Subscription
import org.slf4j.LoggerFactory

object ParsingUtils {

    private val logger = LoggerFactory.getLogger(javaClass)

    suspend fun parseSubscription(input: Map<String, Any>, context: List<String>): Either<APiException, Subscription> =
        try {
            either {
                val subscription = mapper.convertValue(input.minus(JSONLD_CONTEXT), Subscription::class.java)
                subscription.expandTypes(context)

                checkTimeIntervalGreaterThanZero(subscription).bind()
                checkSubscriptionValidity(subscription).bind()
            }
        } catch (e: Exception) {
            logger.error("Error while parsing a subscription: ${e.message}", e)
            BadRequestDataException(e.message ?: "Failed to parse subscription").left()
        }

    fun parseEntityInfo(input: Map<String, Any>, contexts: List<String>?): EntityInfo {
        val entityInfo = mapper.convertValue(input, EntityInfo::class.java)
        entityInfo.type = JsonLdUtils.expandJsonLdTerm(entityInfo.type, contexts!!)!!
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

    fun checkSubscriptionValidity(subscription: Subscription): Either<APiException, Subscription> =
        if (subscription.watchedAttributes != null && subscription.timeInterval != null)
            BadRequestDataException(
                "You can't use 'timeInterval' with 'watchedAttributes' in conjunction"
            )
                .left()
        else subscription.right()

    fun checkTimeIntervalGreaterThanZero(subscription: Subscription): Either<APiException, Subscription> =
        if ((subscription.timeInterval != null) && (subscription.timeInterval < 1))
            BadRequestDataException("The value of 'timeInterval' must be greater than zero (int)").left()
        else subscription.right()
}
