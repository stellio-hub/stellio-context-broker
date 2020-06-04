package com.egm.stellio.subscription.utils

import com.egm.stellio.subscription.model.Endpoint
import com.egm.stellio.subscription.model.EntityInfo
import com.egm.stellio.subscription.model.GeoQuery
import com.egm.stellio.subscription.model.NotificationParams
import com.egm.stellio.subscription.model.Subscription
import com.egm.stellio.subscription.service.SubscriptionService
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

        val subscription = Subscription(
            name = "My Subscription",
            entities = setOf(
                EntityInfo(id = null, idPattern = null, type = "https://ontology.eglobalmark.com/aquac#FeedingService"),
                EntityInfo(
                    id = "urn:ngsi-ld:FeedingService:018z59",
                    idPattern = null,
                    type = "https://ontology.eglobalmark.com/aquac#FeedingService"
                ),
                EntityInfo(
                    id = null,
                    idPattern = "urn:ngsi-ld:FeedingService:018*",
                    type = "https://ontology.eglobalmark.com/aquac#FeedingService"
                )
            ),
            geoQ = GeoQuery(
                georel = "within",
                geometry = GeoQuery.GeometryType.Polygon,
                coordinates = "[[100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0]]"
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
            )
        )

        subscriptionService.create(subscription, "subscription-bootstrapper")
            .thenEmpty {
                logger.debug("Created subscription !")
            }
            .subscribe()
    }
}
