package com.egm.datahub.context.subscription.service

import com.egm.datahub.context.subscription.model.*
import com.egm.datahub.context.subscription.repository.SubscriptionRepository
import com.egm.datahub.context.subscription.utils.QueryUtils
import com.egm.datahub.context.subscription.utils.QueryUtils.createGeoQueryStatement
import com.egm.datahub.context.subscription.web.ResourceNotFoundException
import io.r2dbc.spi.Row
import org.slf4j.LoggerFactory
import org.springframework.data.r2dbc.core.DatabaseClient
import org.springframework.data.r2dbc.core.bind
import org.springframework.data.r2dbc.query.Criteria
import org.springframework.data.r2dbc.query.Update
import org.springframework.stereotype.Component
import org.springframework.transaction.annotation.Transactional
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.net.URI
import java.time.OffsetDateTime
import com.jayway.jsonpath.JsonPath.read

@Component
class SubscriptionService(
    private val databaseClient: DatabaseClient,
    private val subscriptionRepository: SubscriptionRepository
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    @Transactional
    fun create(subscription: Subscription): Mono<Void> {
        val insertStatement = """
            INSERT INTO subscription (id, type, name, description, q, notif_attributes, notif_format, endpoint_uri, endpoint_accept, times_sent) 
            VALUES(:id, :type, :name, :description, :q, :notif_attributes, :notif_format, :endpoint_uri, :endpoint_accept, :times_sent)
        """.trimIndent()
        return databaseClient.execute(insertStatement)
            .bind("id", subscription.id)
            .bind("type", subscription.type)
            .bind("name", subscription.name)
            .bind("description", subscription.description)
            .bind("q", subscription.q)
            .bind("notif_attributes", subscription.notification.attributes.joinToString(separator = ","))
            .bind("notif_format", subscription.notification.format.name)
            .bind("endpoint_uri", subscription.notification.endpoint.uri)
            .bind("endpoint_accept", subscription.notification.endpoint.accept.name)
            .bind("times_sent", subscription.notification.timesSent)
            .fetch()
            .rowsUpdated()
            .flatMap {
                createGeometryQuery(subscription.geoQ, subscription.id)
            }
            .flatMapIterable {
                subscription.entities
            }
            .flatMap {
                createEntityInfo(it, subscription.id)
            }
            .then()
    }

    fun exists(subscriptionId: String): Mono<Boolean> {
        return subscriptionRepository.existsById(subscriptionId)
    }

    private fun createEntityInfo(entityInfo: EntityInfo, subscriptionId: String): Mono<Int> =
        databaseClient.execute("INSERT INTO entity_info (id, id_pattern, type, subscription_id) VALUES (:id, :id_pattern, :type, :subscription_id)")
            .bind("id", entityInfo.id)
            .bind("id_pattern", entityInfo.idPattern)
            .bind("type", entityInfo.type)
            .bind("subscription_id", subscriptionId)
            .fetch()
            .rowsUpdated()

    private fun createGeometryQuery(geoQuery: GeoQuery?, subscriptionId: String): Mono<Int> =
        if (geoQuery != null)
            databaseClient.execute("INSERT INTO geometry_query (georel, geometry, coordinates, subscription_id) VALUES (:georel, :geometry, :coordinates, :subscription_id)")
                .bind("georel", geoQuery.georel)
                .bind("geometry", geoQuery.geometry.name)
                .bind("coordinates", geoQuery.coordinates)
                .bind("subscription_id", subscriptionId)
                .fetch()
                .rowsUpdated()
        else
            Mono.just(0)

    fun getById(id: String): Mono<Subscription> {
        return exists(id)
            .map {
                if (!it)
                    throw ResourceNotFoundException("Subscription Not Found")
            }
            .flatMap {
                val selectStatement = """
                SELECT subscription.id as sub_id, subscription.type as sub_type, name, description, q,
                       notif_attributes, notif_format, endpoint_uri, endpoint_accept,
                       status, times_sent, last_notification, last_failure, last_success,
                       entity_info.id as entity_id, id_pattern, entity_info.type as entity_type,
                       georel, geometry, coordinates, geoproperty
                FROM subscription 
                LEFT JOIN entity_info ON entity_info.subscription_id = :id
                LEFT JOIN geometry_query ON geometry_query.subscription_id = :id 
                WHERE subscription.id = :id
            """.trimIndent()

                databaseClient.execute(selectStatement)
                    .bind("id", id)
                    .map(rowToSubscription)
                    .all()
                    .reduce { t: Subscription, u: Subscription ->
                        t.copy(entities = t.entities.plus(u.entities))
                    }
            }
    }

    fun deleteSubscription(subscriptionId: String): Mono<Int> {
        val deleteStatement = """
            DELETE FROM subscription 
            WHERE subscription.id = :id
        """.trimIndent()

        return databaseClient.execute(deleteStatement)
                .bind("id", subscriptionId)
                .fetch()
                .rowsUpdated()
    }

    fun getMatchingSubscriptions(id: String, type: String): Flux<Subscription> {
        val selectStatement = """
            SELECT subscription.id as sub_id, subscription.type as sub_type, name, description, q,
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

    fun isMatchingQuery(query: String?, entity: String): Boolean {
        // TODO Add support for REGEX
        return if (query == null)
            true
        else {
            try {
                runQuery(query, entity)
            } catch (e: Exception) {
                logger.warn(e.message)
                false
            }
        }
    }

    fun runQuery(query: String, entity: String): Boolean {
        val jsonPathQuery = QueryUtils.createQueryStatement(query, entity)
        val res: List<String> = read(entity, "$[?($jsonPathQuery)]")
        return res.isNotEmpty()
    }

    fun isMatchingGeoQuery(subscriptionId: String, targetGeometry: Map<String, Any>): Mono<Boolean> {
        return getMatchingGeoQuery(subscriptionId)
                .map {
                    createGeoQueryStatement(it, targetGeometry)
                }.flatMap {
                    runGeoQueryStatement(it)
                }
                .switchIfEmpty(Mono.just(true))
    }

    fun getMatchingGeoQuery(subscriptionId: String): Mono<GeoQuery?> {
        val selectStatement = """
            SELECT *
            FROM geometry_query 
            WHERE subscription_id = :sub_id
        """.trimIndent()
        return databaseClient.execute(selectStatement)
                .bind("sub_id", subscriptionId)
                .map(rowToGeoQuery)
                .first()
    }

    fun runGeoQueryStatement(geoQueryStatement: String): Mono<Boolean> {
        return databaseClient.execute(geoQueryStatement.trimIndent())
                    .map(matchesGeoQuery)
                    .first()
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
                q = row.get("q", String::class.java),
                entities = setOf(EntityInfo(
                        id = row.get("entity_id", String::class.java),
                        idPattern = row.get("id_pattern", String::class.java),
                        type = row.get("entity_type", String::class.java)!!
                )),
                geoQ = rowToGeoQuery(row),
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
            q = row.get("q", String::class.java),
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

    private var rowToGeoQuery: ((Row) -> GeoQuery?) = { row ->
        if (row.get("georel", String::class.java) != null)
            GeoQuery(
                    georel = row.get("georel", String::class.java)!!,
                    geometry = GeoQuery.GeometryType.valueOf(row.get("geometry", String::class.java)!!),
                    coordinates = row.get("coordinates", String::class.java)!!
            )
        else
            null
    }

    private var matchesGeoQuery: ((Row) -> Boolean) = { row ->
        row.get("geoquery_result", Object::class.java).toString() == "true"
    }
}