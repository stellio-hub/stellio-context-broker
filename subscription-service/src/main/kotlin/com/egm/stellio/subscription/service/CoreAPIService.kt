package com.egm.stellio.subscription.service

import com.egm.stellio.shared.model.CompactedEntity
import com.egm.stellio.shared.queryparameter.OptionsValue
import com.egm.stellio.shared.queryparameter.QueryParameter
import com.egm.stellio.shared.util.JsonUtils
import com.egm.stellio.shared.util.encode
import com.egm.stellio.shared.web.NGSILD_TENANT_HEADER
import com.egm.stellio.subscription.model.NotificationParams
import kotlinx.coroutines.reactive.awaitFirst
import org.springframework.http.HttpHeaders
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.client.WebClient
import java.net.URI

@Component
class CoreAPIService(
    private val webClient: WebClient
) {
    suspend fun getEntities(
        tenantName: String,
        paramRequest: String,
        contextLink: String
    ): List<CompactedEntity> =
        webClient.get()
            .uri("/ngsi-ld/v1/entities$paramRequest")
            .header(HttpHeaders.LINK, contextLink)
            .header(NGSILD_TENANT_HEADER, tenantName)
            .retrieve()
            .bodyToMono(String::class.java)
            .map { JsonUtils.deserializeListOfObjects(it) }
            .awaitFirst()

    suspend fun retrieveLinkedEntities(
        tenantName: String,
        entityId: URI,
        notificationParams: NotificationParams,
        contextLink: String
    ): List<CompactedEntity> {
        val queryParams = prepareQueryParamsFromNotificationParams(notificationParams)
        return webClient.get()
            .uri("/ngsi-ld/v1/entities/$entityId?$queryParams")
            .header(HttpHeaders.LINK, contextLink)
            .header(HttpHeaders.ACCEPT, notificationParams.endpoint.accept.accept)
            .header(NGSILD_TENANT_HEADER, tenantName)
            .retrieve()
            .bodyToMono(String::class.java)
            .map { JsonUtils.deserializeListOfObjects(it) }
            .awaitFirst()
    }

    private fun prepareQueryParamsFromNotificationParams(
        notificationParams: NotificationParams
    ): String {
        val params = StringBuilder()

        if (!notificationParams.attributes.isNullOrEmpty()) {
            val attrsParam = notificationParams.attributes!!.joinToString(",") { it.encode() }
            params.append("&${QueryParameter.ATTRS.key}=$attrsParam")
        }

        val optionsParam =
            if (notificationParams.sysAttrs)
                "${OptionsValue.SYS_ATTRS.value},${notificationParams.format.format}"
            else
                notificationParams.format.format
        params.append("&${QueryParameter.OPTIONS.key}=$optionsParam")

        if (notificationParams.join != null && notificationParams.join != NotificationParams.JoinType.NONE) {
            params.append("&${QueryParameter.JOIN.key}=${notificationParams.join}")
            notificationParams.joinLevel?.let {
                params.append("&${QueryParameter.JOIN_LEVEL.key}=$it")
            }
        }

        return if (params.isEmpty()) "" else "${params.substring(1)}"
    }
}
