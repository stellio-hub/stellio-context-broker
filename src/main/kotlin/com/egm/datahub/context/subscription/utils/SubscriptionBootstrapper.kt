package com.egm.datahub.context.subscription.utils

import com.egm.datahub.context.subscription.model.Endpoint
import com.egm.datahub.context.subscription.model.EntityInfo
import com.egm.datahub.context.subscription.model.NotificationParams
import com.egm.datahub.context.subscription.model.Subscription
import com.egm.datahub.context.subscription.service.SubscriptionService
import org.slf4j.LoggerFactory
import org.springframework.boot.CommandLineRunner
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Component
import java.net.URI

@Component
@Profile("dev")
class SubscriptionBootstrapper(
    private val subscriptionService: SubscriptionService
) : CommandLineRunner {

    private val logger = LoggerFactory.getLogger(javaClass)

    override fun run(vararg args: String) {

        val subscription = Subscription(name = "My Subscription",
            entities = setOf(
                EntityInfo(id = null, idPattern = null, type = "Beehive"),
                EntityInfo(id = "urn:ngsi-ld:Beehive:1234567890", idPattern = null, type = "Beehive"),
                EntityInfo(id = null, idPattern = "urn:ngsi-ld:Beehive:1234*", type = "Beehive")
            ),
            notification = NotificationParams(
                attributes = listOf("incoming"),
                format = NotificationParams.FormatType.NORMALIZED,
                endpoint = Endpoint(
                    uri = URI.create("http://localhost:8084"),
                    accept = Endpoint.AcceptType.JSON
                ),
                status = null,
                lastNotification = null,
                lastFailure = null,
                lastSuccess = null
            ))
    }
}