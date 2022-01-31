package com.egm.stellio.subscription.service

import arrow.core.Option
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.NgsiLdGeoProperty
import com.egm.stellio.shared.model.NotImplementedException
import com.egm.stellio.shared.model.Notification
import com.egm.stellio.shared.util.JsonLdUtils
import com.egm.stellio.shared.util.Sub
import com.egm.stellio.shared.util.toStringValue
import com.egm.stellio.shared.util.toUri
import com.egm.stellio.subscription.model.Endpoint
import com.egm.stellio.subscription.model.EntityInfo
import com.egm.stellio.subscription.model.GeoQuery
import com.egm.stellio.subscription.model.NotificationParams
import com.egm.stellio.subscription.model.Subscription
import com.egm.stellio.subscription.repository.SubscriptionRepository
import com.egm.stellio.subscription.utils.ParsingUtils.endpointInfoMapToString
import com.egm.stellio.subscription.utils.ParsingUtils.endpointInfoToString
import com.egm.stellio.subscription.utils.ParsingUtils.parseEndpointInfo
import com.egm.stellio.subscription.utils.ParsingUtils.parseEntityInfo
import com.egm.stellio.subscription.utils.ParsingUtils.toSqlColumnName
import com.egm.stellio.subscription.utils.ParsingUtils.toSqlValue
import com.egm.stellio.subscription.utils.QueryUtils
import com.egm.stellio.subscription.utils.QueryUtils.createGeoQueryStatement
import com.jayway.jsonpath.JsonPath.read
import io.r2dbc.postgresql.codec.Json
import io.r2dbc.spi.Row
import org.slf4j.LoggerFactory
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.data.relational.core.query.Criteria.where
import org.springframework.data.relational.core.query.Query.query
import org.springframework.data.relational.core.query.Update
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.r2dbc.core.bind
import org.springframework.stereotype.Component
import org.springframework.transaction.annotation.Transactional
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.kotlin.core.publisher.toFlux
import java.net.URI
import java.time.Instant
import java.time.ZoneOffset
import java.time.ZonedDateTime

@Component
class SubscriptionService(
    private val databaseClient: DatabaseClient,
    private val r2dbcEntityTemplate: R2dbcEntityTemplate,
    private val subscriptionRepository: SubscriptionRepository
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    @Transactional
    fun create(subscription: Subscription, sub: Option<Sub>): Mono<Int> {
        val insertStatement =
            """
        INSERT INTO subscription(id, type, name, created_at, description, watched_attributes, q, notif_attributes,
            notif_format, endpoint_uri, endpoint_accept, endpoint_info, times_sent, is_active, expires_at, sub)
        VALUES(:id, :type, :name, :created_at, :description, :watched_attributes, :q, :notif_attributes, :notif_format,
            :endpoint_uri, :endpoint_accept, :endpoint_info, :times_sent, :is_active, :expires_at, :sub)
            """.trimIndent()

        return databaseClient.sql(insertStatement)
            .bind("id", subscription.id)
            .bind("type", subscription.type)
            .bind("name", subscription.name)
            .bind("created_at", subscription.createdAt)
            .bind("description", subscription.description)
            .bind("watched_attributes", subscription.watchedAttributes?.joinToString(separator = ","))
            .bind("q", subscription.q)
            .bind("notif_attributes", subscription.notification.attributes?.joinToString(separator = ","))
            .bind("notif_format", subscription.notification.format.name)
            .bind("endpoint_uri", subscription.notification.endpoint.uri)
            .bind("endpoint_accept", subscription.notification.endpoint.accept.name)
            .bind("endpoint_info", Json.of(endpointInfoToString(subscription.notification.endpoint.info)))
            .bind("times_sent", subscription.notification.timesSent)
            .bind("is_active", subscription.isActive)
            .bind("expires_at", subscription.expiresAt)
            .bind("sub", sub.toStringValue())
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
            .collectList()
            .map {
                it.size
            }
    }

    fun exists(subscriptionId: URI): Mono<Boolean> {
        return subscriptionRepository.existsById(subscriptionId.toString())
    }

    private fun createEntityInfo(entityInfo: EntityInfo, subscriptionId: URI): Mono<Int> =
        databaseClient.sql(
            """
            INSERT INTO entity_info (id, id_pattern, type, subscription_id) 
            VALUES (:id, :id_pattern, :type, :subscription_id)
            """.trimIndent()
        )
            .bind("id", entityInfo.id)
            .bind("id_pattern", entityInfo.idPattern)
            .bind("type", entityInfo.type)
            .bind("subscription_id", subscriptionId)
            .fetch()
            .rowsUpdated()

    private fun createGeometryQuery(geoQuery: GeoQuery?, subscriptionId: URI): Mono<Int> =
        if (geoQuery != null) {
            val storedCoordinates =
                when (geoQuery.coordinates) {
                    is String -> geoQuery.coordinates
                    is List<*> -> geoQuery.coordinates.toString()
                    else -> geoQuery.coordinates
                }

            databaseClient.sql(
                """
                INSERT INTO geometry_query (georel, geometry, coordinates, subscription_id) 
                VALUES (:georel, :geometry, :coordinates, :subscription_id)
                """
            )
                .bind("georel", geoQuery.georel)
                .bind("geometry", geoQuery.geometry.name)
                .bind("coordinates", storedCoordinates)
                .bind("subscription_id", subscriptionId)
                .fetch()
                .rowsUpdated()
        } else
            Mono.just(0)

    fun getById(id: URI): Mono<Subscription> {
        val selectStatement =
            """
            SELECT subscription.id as sub_id, subscription.type as sub_type, name, created_at, modified_at, description,
                   watched_attributes, q, notif_attributes, notif_format, endpoint_uri, endpoint_accept, endpoint_info,
                   status, times_sent, is_active, last_notification, last_failure, last_success,
                   entity_info.id as entity_id, id_pattern, entity_info.type as entity_type,
                   georel, geometry, coordinates, geoproperty, expires_at
            FROM subscription 
            LEFT JOIN entity_info ON entity_info.subscription_id = :id
            LEFT JOIN geometry_query ON geometry_query.subscription_id = :id 
            WHERE subscription.id = :id
            """.trimIndent()

        return databaseClient.sql(selectStatement)
            .bind("id", id)
            .map(rowToSubscription)
            .all()
            .reduce { t: Subscription, u: Subscription ->
                t.copy(entities = t.entities.plus(u.entities))
            }
    }

    fun isCreatorOf(subscriptionId: URI, sub: Option<Sub>): Mono<Boolean> {
        val selectStatement =
            """
            SELECT sub
            FROM subscription
            WHERE id = :id
            """.trimIndent()

        return databaseClient.sql(selectStatement)
            .bind("id", subscriptionId)
            .map(rowToSub)
            .first()
            .map { it == sub.toStringValue() }
    }

    @Transactional
    fun update(subscriptionId: URI, parsedInput: Pair<Map<String, Any>, List<String>>): Mono<Int> {
        val contexts = parsedInput.second
        val updates = mutableListOf<Mono<Int>>()

        val subscriptionInputWithModifiedAt = parsedInput.first
            .plus("modifiedAt" to Instant.now().atZone(ZoneOffset.UTC))

        subscriptionInputWithModifiedAt.filterKeys {
            it !in JsonLdUtils.JSONLD_COMPACTED_ENTITY_MANDATORY_FIELDS
        }.forEach {
            when {
                it.key == "geoQ" -> {
                    val geoQuery = it.value as Map<String, Any>
                    updates.add(updateGeometryQuery(subscriptionId, geoQuery))
                }
                it.key == "notification" -> {
                    val notification = it.value as Map<String, Any>
                    updates.add(updateNotification(subscriptionId, notification, contexts))
                }
                it.key == "entities" -> {
                    val entities = it.value as List<Map<String, Any>>
                    updates.add(updateEntities(subscriptionId, entities, contexts))
                }
                listOf(
                    "name", "description", "watchedAttributes", "q", "isActive", "modifiedAt",
                    "expiresAt"
                ).contains(it.key) -> {
                    val columnName = it.key.toSqlColumnName()
                    val value = it.value.toSqlValue(it.key)
                    updates.add(updateSubscriptionAttribute(subscriptionId, it.key, columnName, value))
                }
                listOf("timeInterval", "csf", "throttling", "temporalQ").contains(it.key) -> {
                    logger.warn("Subscription $subscriptionId has unsupported attribute: ${it.key}")
                    throw NotImplementedException("Subscription $subscriptionId has unsupported attribute: ${it.key}")
                }
                else -> {
                    logger.warn("Subscription $subscriptionId has invalid attribute: ${it.key}")
                    throw BadRequestDataException("Subscription $subscriptionId has invalid attribute: ${it.key}")
                }
            }
        }

        return updates.toFlux()
            .flatMap { updateOperation ->
                updateOperation.map { it }
            }
            .collectList()
            .map { it.size }
    }

    private fun updateSubscriptionAttribute(
        subscriptionId: URI,
        attributeName: String,
        columnName: String,
        value: Any?
    ): Mono<Int> {
        val updateStatement = Update.update(columnName, value)
        return r2dbcEntityTemplate.update(Subscription::class.java)
            .matching(query(where("id").`is`(subscriptionId)))
            .apply(updateStatement)
            .doOnError { e ->
                throw BadRequestDataException(
                    e.message ?: "Could not update attribute $attributeName"
                )
            }
    }

    fun updateGeometryQuery(subscriptionId: URI, geoQuery: Map<String, Any>): Mono<Int> {
        try {
            val firstValue = geoQuery.entries.iterator().next()
            var updateStatement = Update.update(firstValue.key, firstValue.value.toString())
            geoQuery.filterKeys { it != firstValue.key }.forEach {
                updateStatement = updateStatement.set(it.key, it.value.toString())
            }

            return r2dbcEntityTemplate.update(
                query(where("subscription_id").`is`(subscriptionId)),
                updateStatement,
                GeoQuery::class.java
            )
                .doOnError { e ->
                    throw BadRequestDataException(e.message ?: "Could not update the Geometry query")
                }
        } catch (e: Exception) {
            throw BadRequestDataException(e.message ?: "No values provided for the Geometry query attribute")
        }
    }

    fun updateNotification(subscriptionId: URI, notification: Map<String, Any>, contexts: List<String>?): Mono<Int> {
        try {
            val firstValue = notification.entries.iterator().next()
            val updateParams = extractParamsFromNotificationAttribute(firstValue, contexts)
            var updateStatement = Update.update(updateParams[0].first, updateParams[0].second)
            if (updateParams.size > 1) {
                updateParams.drop(1).forEach {
                    updateStatement = updateStatement.set(it.first, it.second)
                }
            }

            notification.filterKeys { it != firstValue.key }.forEach {
                extractParamsFromNotificationAttribute(it, contexts).forEach {
                    updateStatement = updateStatement.set(it.first, it.second)
                }
            }

            return r2dbcEntityTemplate.update(
                query(where("id").`is`(subscriptionId)),
                updateStatement,
                Subscription::class.java
            )
                .doOnError { e ->
                    throw BadRequestDataException(e.message ?: "Could not update the Notification")
                }
        } catch (e: Exception) {
            throw BadRequestDataException(e.message ?: "No values provided for the Notification attribute")
        }
    }

    private fun extractParamsFromNotificationAttribute(
        attribute: Map.Entry<String, Any>,
        contexts: List<String>?
    ): List<Pair<String, Any?>> {
        return when (attribute.key) {
            "attributes" -> {
                var valueList = attribute.value as List<String>
                valueList = valueList.map {
                    JsonLdUtils.expandJsonLdKey(it, contexts!!)!!
                }
                listOf(Pair("notif_attributes", valueList.joinToString(separator = ",")))
            }
            "format" -> {
                val format =
                    if (attribute.value == "keyValues")
                        NotificationParams.FormatType.KEY_VALUES.name
                    else
                        NotificationParams.FormatType.NORMALIZED.name
                listOf(Pair("notif_format", format))
            }
            "endpoint" -> {
                val endpoint = attribute.value as Map<String, Any>
                val accept =
                    if (endpoint["accept"] == "application/json")
                        Endpoint.AcceptType.JSON.name
                    else
                        Endpoint.AcceptType.JSONLD.name
                val endpointInfo = endpoint["info"] as List<Map<String, String>>?

                listOf(
                    Pair("endpoint_uri", endpoint["uri"]),
                    Pair("endpoint_accept", accept),
                    Pair("endpoint_info", Json.of(endpointInfoMapToString(endpointInfo)))
                )
            }
            else -> {
                throw BadRequestDataException("Could not update attribute ${attribute.key}")
            }
        }
    }

    fun updateEntities(subscriptionId: URI, entities: List<Map<String, Any>>, contexts: List<String>?): Mono<Int> {
        return deleteEntityInfo(subscriptionId).doOnNext {
            entities.forEach {
                createEntityInfo(parseEntityInfo(it, contexts), subscriptionId).subscribe {}
            }
        }
    }

    fun delete(subscriptionId: URI): Mono<Int> =
        r2dbcEntityTemplate.delete(
            query(where("id").`is`(subscriptionId)),
            Subscription::class.java
        )

    fun deleteEntityInfo(subscriptionId: URI): Mono<Int> =
        r2dbcEntityTemplate.delete(
            query(where("subscription_id").`is`(subscriptionId)),
            EntityInfo::class.java
        )

    fun getSubscriptions(limit: Int, offset: Int, sub: Option<Sub>): Flux<Subscription> {
        val selectStatement =
            """
            SELECT subscription.id as sub_id, subscription.type as sub_type, name, created_at, modified_At, description,
                   watched_attributes, q, notif_attributes, notif_format, endpoint_uri, endpoint_accept, endpoint_info,
                   status, times_sent, is_active, last_notification, last_failure, last_success,
                   entity_info.id as entity_id, id_pattern, entity_info.type as entity_type,
                   georel, geometry, coordinates, geoproperty, expires_at
            FROM subscription 
            LEFT JOIN entity_info ON entity_info.subscription_id = subscription.id
            LEFT JOIN geometry_query ON geometry_query.subscription_id = subscription.id

            WHERE subscription.id in (
                SELECT subscription.id as sub_id
                from subscription
                WHERE subscription.sub = :sub 
                ORDER BY sub_id
                limit :limit
                offset :offset)
            """.trimIndent()
        return databaseClient.sql(selectStatement)
            .bind("limit", limit)
            .bind("offset", offset)
            .bind("sub", sub.toStringValue())
            .map(rowToSubscription)
            .all()
            .groupBy { t: Subscription ->
                t.id
            }
            .flatMap { grouped ->
                grouped.reduce { t: Subscription, u: Subscription ->
                    t.copy(entities = t.entities.plus(u.entities))
                }
            }
    }

    fun getSubscriptionsCount(sub: Option<Sub>): Mono<Int> {
        val selectStatement =
            """
            SELECT count(*) from subscription
            WHERE subscription.sub = :sub
            """.trimIndent()
        return databaseClient.sql(selectStatement)
            .bind("sub", sub.toStringValue())
            .map(rowToSubscriptionCount)
            .first()
    }

    fun getMatchingSubscriptions(id: URI, type: String, updatedAttributes: String): Flux<Subscription> {
        val selectStatement =
            """
            SELECT subscription.id as sub_id, subscription.type as sub_type, name, description, q,
                   notif_attributes, notif_format, endpoint_uri, endpoint_accept, times_sent, endpoint_info
            FROM subscription 
            WHERE is_active
            AND ( expires_at is null OR expires_at >= :date )
            AND ( string_to_array(watched_attributes, ',') && string_to_array(:updatedAttributes, ',') OR watched_attributes IS NULL )
            AND id IN (
                SELECT subscription_id
                FROM entity_info
                WHERE entity_info.type = :type
                AND (entity_info.id IS NULL OR entity_info.id = :id)
                AND (entity_info.id_pattern IS NULL OR :id ~ entity_info.id_pattern)
            )
            """.trimIndent()
        return databaseClient.sql(selectStatement)
            .bind("id", id)
            .bind("type", type)
            .bind("updatedAttributes", updatedAttributes)
            .bind("date", Instant.now().atZone(ZoneOffset.UTC))
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
                false
            }
        }
    }

    fun runQuery(query: String, entity: String): Boolean {
        val jsonPathQuery = QueryUtils.createQueryStatement(query, entity)
        val res: List<String> = read(entity, "$[?($jsonPathQuery)]")
        return res.isNotEmpty()
    }

    fun isMatchingGeoQuery(subscriptionId: URI, location: NgsiLdGeoProperty?): Mono<Boolean> {
        return if (location == null)
            Mono.just(true)
        else {
            getMatchingGeoQuery(subscriptionId)
                .map {
                    createGeoQueryStatement(it, location)
                }.flatMap {
                    runGeoQueryStatement(it)
                }
                .switchIfEmpty(Mono.just(true))
        }
    }

    fun getMatchingGeoQuery(subscriptionId: URI): Mono<GeoQuery?> {
        val selectStatement =
            """
            SELECT *
            FROM geometry_query 
            WHERE subscription_id = :sub_id
            """.trimIndent()
        return databaseClient.sql(selectStatement)
            .bind("sub_id", subscriptionId)
            .map(rowToGeoQuery)
            .first()
    }

    fun runGeoQueryStatement(geoQueryStatement: String): Mono<Boolean> {
        return databaseClient.sql(geoQueryStatement.trimIndent())
            .map(matchesGeoQuery)
            .first()
    }

    fun updateSubscriptionNotification(
        subscription: Subscription,
        notification: Notification,
        success: Boolean
    ): Mono<Int> {
        val subscriptionStatus =
            if (success) NotificationParams.StatusType.OK.name else NotificationParams.StatusType.FAILED.name
        val lastStatusName = if (success) "last_success" else "last_failure"
        val updateStatement = Update.update("status", subscriptionStatus)
            .set("times_sent", subscription.notification.timesSent + 1)
            .set("last_notification", notification.notifiedAt)
            .set(lastStatusName, notification.notifiedAt)

        return r2dbcEntityTemplate.update(
            query(where("id").`is`(subscription.id)),
            updateStatement,
            Subscription::class.java
        )
    }

    private var rowToSubscription: ((Row) -> Subscription) = { row ->
        Subscription(
            id = row.get("sub_id", String::class.java)!!.toUri(),
            type = row.get("sub_type", String::class.java)!!,
            name = row.get("name", String::class.java),
            createdAt = row.get("created_at", ZonedDateTime::class.java)!!.toInstant().atZone(ZoneOffset.UTC),
            modifiedAt = row.get("modified_at", ZonedDateTime::class.java)?.toInstant()?.atZone(ZoneOffset.UTC),
            expiresAt = row.get("expires_at", ZonedDateTime::class.java)?.toInstant()?.atZone(ZoneOffset.UTC),
            description = row.get("description", String::class.java),
            watchedAttributes = row.get("watched_attributes", String::class.java)?.split(","),
            q = row.get("q", String::class.java),
            entities = setOf(
                EntityInfo(
                    id = row.get("entity_id", String::class.java)?.toUri(),
                    idPattern = row.get("id_pattern", String::class.java),
                    type = row.get("entity_type", String::class.java)!!
                )
            ),
            geoQ = rowToGeoQuery(row),
            notification = NotificationParams(
                attributes = row.get("notif_attributes", String::class.java)?.split(",").orEmpty(),
                format = NotificationParams.FormatType.valueOf(row.get("notif_format", String::class.java)!!),
                endpoint = Endpoint(
                    uri = URI(row.get("endpoint_uri", String::class.java)!!),
                    accept = Endpoint.AcceptType.valueOf(row.get("endpoint_accept", String::class.java)!!),
                    info = parseEndpointInfo(row.get("endpoint_info", String::class.java))
                ),
                status = row.get("status", String::class.java)?.let { NotificationParams.StatusType.valueOf(it) },
                timesSent = row.get("times_sent", Integer::class.java)!!.toInt(),
                lastNotification = row.get("last_notification", ZonedDateTime::class.java)?.toInstant()
                    ?.atZone(ZoneOffset.UTC),
                lastFailure = row.get("last_failure", ZonedDateTime::class.java)?.toInstant()?.atZone(ZoneOffset.UTC),
                lastSuccess = row.get("last_success", ZonedDateTime::class.java)?.toInstant()?.atZone(ZoneOffset.UTC)
            ),
            isActive = row.get("is_active", Object::class.java).toString() == "true"
        )
    }

    private var rowToRawSubscription: ((Row) -> Subscription) = { row ->
        Subscription(
            id = row.get("sub_id", String::class.java)!!.toUri(),
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
                    accept = Endpoint.AcceptType.valueOf(row.get("endpoint_accept", String::class.java)!!),
                    info = parseEndpointInfo(row.get("endpoint_info", String::class.java))
                ),
                status = null,
                timesSent = row.get("times_sent", Integer::class.java)!!.toInt(),
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

    private var rowToSubscriptionCount: ((Row) -> Int) = { row ->
        row.get("count", Integer::class.java)!!.toInt()
    }

    private var rowToSub: (Row) -> String = { row ->
        row.get("sub", String::class.java)!!
    }
}
