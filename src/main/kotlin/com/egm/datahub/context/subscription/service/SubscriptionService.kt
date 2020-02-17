package com.egm.datahub.context.subscription.service

import com.egm.datahub.context.subscription.model.*
import io.r2dbc.spi.Row
import org.springframework.data.r2dbc.core.DatabaseClient
import org.springframework.data.r2dbc.core.bind
import org.springframework.data.r2dbc.query.Criteria
import org.springframework.data.r2dbc.query.Update
import org.springframework.stereotype.Component
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.net.URI
import java.time.OffsetDateTime

@Component
class SubscriptionService(
    private val databaseClient: DatabaseClient
) {

    fun create(subscription: Subscription): Mono<Void> {
        val insertStatement = """
            INSERT INTO subscription (id, type, name, description, notif_attributes, notif_format, endpoint_uri, endpoint_accept, times_sent) 
            VALUES(:id, :type, :name, :description, :notif_attributes, :notif_format, :endpoint_uri, :endpoint_accept, :times_sent)
        """.trimIndent()
        return databaseClient.execute(insertStatement)
            .bind("id", subscription.id)
            .bind("type", subscription.type)
            .bind("name", subscription.name)
            .bind("description", subscription.description)
            .bind("notif_attributes", subscription.notification.attributes.joinToString(separator = ","))
            .bind("notif_format", subscription.notification.format.name)
            .bind("endpoint_uri", subscription.notification.endpoint.uri)
            .bind("endpoint_accept", subscription.notification.endpoint.accept.name)
            .bind("times_sent", subscription.notification.timesSent)
            .fetch()
            .rowsUpdated()
            .flatMapIterable {
                subscription.entities
            }
            .flatMap {
                databaseClient.execute("INSERT INTO entity_info (id, id_pattern, type, subscription_id) VALUES (:id, :id_pattern, :type, :subscription_id)")
                    .bind("id", it.id)
                    .bind("id_pattern", it.idPattern)
                    .bind("type", it.type)
                    .bind("subscription_id", subscription.id)
                    .then()
            }
            .then()
    }

    fun getById(id: String): Mono<Subscription> {
        val selectStatement = """
            SELECT subscription.id as sub_id, subscription.type as sub_type, name, description,
                   notif_attributes, notif_format, endpoint_uri, endpoint_accept,
                   status, times_sent, last_notification, last_failure, last_success,
                   entity_info.id as entity_id, id_pattern, entity_info.type as entity_type
            FROM subscription 
            LEFT JOIN entity_info ON entity_info.subscription_id = :id 
            WHERE subscription.id = :id
        """.trimIndent()

        return databaseClient.execute(selectStatement)
            .bind("id", id)
            .map(rowToSubscription)
            .all()
            .reduce { t: Subscription, u: Subscription ->
                t.copy(entities = t.entities.plus(u.entities))
            }
    }

    fun getMatchingSubscriptions(id: String, type: String): Flux<Subscription> {
        val selectStatement = """
            SELECT subscription.id as sub_id, subscription.type as sub_type, name, description,
                   notif_attributes, notif_format, endpoint_uri, endpoint_accept
            FROM subscription 
            WHERE id IN (
                SELECT subscription_id
                FROM entity_info
                WHERE entity_info.type = :type
                AND (entity_info.id IS NULL OR entity_info.id = :id)
                AND (entity_info.id_pattern IS NULL OR :id ~ entity_info.id_pattern)
            )
        """.trimIndent()

        return databaseClient.execute(selectStatement)
            .bind("id", id)
            .bind("type", type)
            .map(rowToRawSubscription)
            .all()
    }

    fun updateSubscriptionNotification(subscription: Subscription, notification: Notification, success: Boolean): Mono<Int> {
        val subscriptionStatus = if (success) NotificationParams.StatusType.OK.name else NotificationParams.StatusType.FAILED.name
        val lastStatusName = if (success) "last_success" else "last_failure"
        val updateStatement = Update.update("status", subscriptionStatus)
            .set("times_sent", subscription.notification.timesSent + 1)
            .set("last_notification", notification.notifiedAt)
            .set(lastStatusName, notification.notifiedAt)

        return databaseClient.update()
            .table("subscription")
            .using(updateStatement)
            .matching(Criteria.where("id").`is`(subscription.id))
            .fetch()
            .rowsUpdated()
    }

    private var rowToSubscription: ((Row) -> Subscription) = { row ->
        Subscription(
            id = row.get("sub_id", String::class.java)!!,
            type = row.get("sub_type", String::class.java)!!,
            name = row.get("name", String::class.java),
            description = row.get("description", String::class.java),
            entities = setOf(EntityInfo(
                id = row.get("entity_id", String::class.java),
                idPattern = row.get("id_pattern", String::class.java),
                type = row.get("entity_type", String::class.java)!!
            )),
            notification = NotificationParams(
                attributes = row.get("notif_attributes", String::class.java)?.split(",").orEmpty(),
                format = NotificationParams.FormatType.valueOf(row.get("notif_format", String::class.java)!!),
                endpoint = Endpoint(
                    uri = URI(row.get("endpoint_uri", String::class.java)!!),
                    accept = Endpoint.AcceptType.valueOf(row.get("endpoint_accept", String::class.java)!!)
                ),
                status = row.get("status", String::class.java)?.let { NotificationParams.StatusType.valueOf(it) },
                timesSent = row.get("times_sent", Integer::class.java)!!.toInt(),
                lastNotification = row.get("last_notification", OffsetDateTime::class.java),
                lastFailure = row.get("last_failure", OffsetDateTime::class.java),
                lastSuccess = row.get("last_success", OffsetDateTime::class.java)
            )
        )
    }

    private var rowToRawSubscription: ((Row) -> Subscription) = { row ->
        Subscription(
            id = row.get("sub_id", String::class.java)!!,
            type = row.get("sub_type", String::class.java)!!,
            name = row.get("name", String::class.java),
            description = row.get("description", String::class.java),
            entities = emptySet(),
            notification = NotificationParams(
                attributes = row.get("notif_attributes", String::class.java)?.split(",").orEmpty(),
                format = NotificationParams.FormatType.valueOf(row.get("notif_format", String::class.java)!!),
                endpoint = Endpoint(
                    uri = URI(row.get("endpoint_uri", String::class.java)!!),
                    accept = Endpoint.AcceptType.valueOf(row.get("endpoint_accept", String::class.java)!!)
                ),
                status = null,
                lastNotification = null,
                lastFailure = null,
                lastSuccess = null
            )
        )
    }
}