package com.egm.stellio.subscription.utils

import arrow.core.Either
import arrow.core.left
import arrow.core.right
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.EntitySelector
import com.egm.stellio.shared.model.JSONLD_CONTEXT_KW
import com.egm.stellio.shared.model.toAPIException
import com.egm.stellio.shared.util.DataTypes
import com.egm.stellio.shared.util.DataTypes.deserializeAs
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.util.expandTypeSelection
import com.egm.stellio.subscription.model.EndpointInfo
import com.egm.stellio.subscription.model.Subscription

object ParsingUtils {

    fun parseSubscription(input: Map<String, Any>, contexts: List<String>): Either<APIException, Subscription> =
        runCatching {
            deserializeAs<Subscription>(serializeObject(input.plus(JSONLD_CONTEXT_KW to contexts)))
                .expand(contexts)
        }.fold(
            { it.right() },
            { it.toAPIException("Failed to parse subscription: ${it.message}").left() }
        )

    fun parseEntitySelector(input: Map<String, Any>, contexts: List<String>): EntitySelector {
        val entitySelector = DataTypes.convertTo<EntitySelector>(input)
        return entitySelector.copy(
            typeSelection = expandTypeSelection(entitySelector.typeSelection, contexts)!!
        )
    }

    fun parseEndpointInfo(input: String?): List<EndpointInfo>? {
        return if (input != null && input != "null")
            DataTypes.convertToList(input)
        else null
    }
}
