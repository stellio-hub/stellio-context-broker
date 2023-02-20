package com.egm.stellio.subscription.service

import arrow.core.Either
import arrow.core.Option
import arrow.core.continuations.either
import arrow.core.left
import arrow.core.right
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_TYPE_TERM
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_LOCATION_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_SUBSCRIPTION_TERM
import com.egm.stellio.subscription.model.*
import com.egm.stellio.subscription.model.GeoQuery
import com.egm.stellio.subscription.model.Subscription
import com.egm.stellio.subscription.repository.SubscriptionRepository
import com.egm.stellio.subscription.utils.ParsingUtils
import com.egm.stellio.subscription.utils.ParsingUtils.endpointInfoMapToString
import com.egm.stellio.subscription.utils.ParsingUtils.endpointInfoToString
import com.egm.stellio.subscription.utils.ParsingUtils.parseEndpointInfo
import com.egm.stellio.subscription.utils.ParsingUtils.parseEntityInfo
import com.egm.stellio.subscription.utils.ParsingUtils.toSqlColumnName
import com.egm.stellio.subscription.utils.ParsingUtils.toSqlValue
import com.egm.stellio.subscription.utils.QueryUtils
import com.egm.stellio.subscription.utils.QueryUtils.createGeoQueryStatement
import com.egm.stellio.subscription.utils.execute
import com.jayway.jsonpath.JsonPath.read
import io.r2dbc.postgresql.codec.Json
import io.r2dbc.spi.Row
import io.r2dbc.spi.RowMetadata
import kotlinx.coroutines.reactive.awaitFirst
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
import java.net.URI
import java.time.Instant
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.util.regex.Pattern

@Component
class SubscriptionService(
    private val databaseClient: DatabaseClient,
    private val r2dbcEntityTemplate: R2dbcEntityTemplate,
    private val subscriptionRepository: SubscriptionRepository
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    suspend fun validateNewSubscription(subscription: Subscription): Either<APIException, Unit> {
        return either {
            checkTypeIsSubscription(subscription).bind()
            checkIdIsValid(subscription).bind()
            checkTimeIntervalGreaterThanZero(subscription).bind()
            checkSubscriptionValidity(subscription).bind()
            checkExpiresAtInTheFuture(subscription).bind()
            checkIdPatternIsValid(subscription).bind()
        }
    }

    private fun checkIdIsValid(subscription: Subscription): Either<APIException, Unit> =
        if (!subscription.id.isAbsolute)
            BadRequestDataException(
                "The supplied identifier was expected to be an URI but it is not: ${subscription.id}"
            ).left()
        else Unit.right()

    private fun checkTypeIsSubscription(subscription: Subscription): Either<APIException, Unit> =
        if (subscription.type != NGSILD_SUBSCRIPTION_TERM)
            BadRequestDataException("type attribute must be equal to 'Subscription'").left()
        else Unit.right()

    private fun checkSubscriptionValidity(subscription: Subscription): Either<APIException, Unit> =
        if (subscription.watchedAttributes != null && subscription.timeInterval != null)
            BadRequestDataException(
                "You can't use 'timeInterval' with 'watchedAttributes' in conjunction"
            )
                .left()
        else Unit.right()

    private fun checkTimeIntervalGreaterThanZero(subscription: Subscription): Either<APIException, Unit> =
        if (subscription.timeInterval != null && subscription.timeInterval < 1)
            BadRequestDataException("The value of 'timeInterval' must be greater than zero (int)").left()
        else Unit.right()

    private fun checkExpiresAtInTheFuture(subscription: Subscription): Either<BadRequestDataException, Unit> =
        if (subscription.expiresAt != null && subscription.expiresAt.isBefore(ZonedDateTime.now()))
            BadRequestDataException("'expiresAt' must be in the future").left()
        else Unit.right()

    private fun checkExpiresAtInTheFuture(expiresAt: String): Either<BadRequestDataException, ZonedDateTime> =
        runCatching { ZonedDateTime.parse(expiresAt) }.fold({
            if (it.isBefore(ZonedDateTime.now()))
                BadRequestDataException("'expiresAt' must be in the future").left()
            else it.right()
        }, {
            BadRequestDataException("Unable to parse 'expiresAt' value: $expiresAt").left()
        })

    private fun checkIdPatternIsValid(subscription: Subscription): Either<BadRequestDataException, Unit> =
        subscription.entities.forEach { endpoint ->
            runCatching {
                endpoint.idPattern?.let { Pattern.compile(it) }
            }.onFailure {
                return BadRequestDataException("Invalid value for idPattern: ${endpoint.idPattern}").left()
            }
        }.right()

    @Transactional
    suspend fun create(subscription: Subscription, sub: Option<Sub>): Either<APIException, Unit> {
        return either {
            validateNewSubscription(subscription).bind()

            val insertStatement =
                """
                INSERT INTO subscription(id, type, subscription_name, created_at, description, watched_attributes,
                    time_interval, q, notif_attributes, notif_format, endpoint_uri, endpoint_accept, 
                    endpoint_info, times_sent, is_active, expires_at, sub)
                VALUES(:id, :type, :subscription_name, :created_at, :description, :watched_attributes, 
                    :time_interval, :q, :notif_attributes, :notif_format, :endpoint_uri, :endpoint_accept, 
                    :endpoint_info, :times_sent, :is_active, :expires_at, :sub)
                """.trimIndent()

            databaseClient.sql(insertStatement)
                .bind("id", subscription.id)
                .bind("type", subscription.type)
                .bind("subscription_name", subscription.subscriptionName)
                .bind("created_at", subscription.createdAt)
                .bind("description", subscription.description)
                .bind("watched_attributes", subscription.watchedAttributes?.joinToString(separator = ","))
                .bind("time_interval", subscription.timeInterval)
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
                .execute().bind()

            createGeometryQuery(subscription.geoQ, subscription.id).bind()

            subscription.entities.forEach {
                createEntityInfo(it, subscription.id).bind()
            }
        }
    }

    fun exists(subscriptionId: URI): Mono<Boolean> =
        subscriptionRepository.existsById(subscriptionId.toString())

    private suspend fun createEntityInfo(entityInfo: EntityInfo, subscriptionId: URI): Either<APIException, Unit> =
        either {
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
                .execute()
        }

    private suspend fun createGeometryQuery(geoQuery: GeoQuery?, subscriptionId: URI): Either<APIException, Unit> =
        either {
            if (geoQuery != null) {
                val storedCoordinates: String =
                    when (geoQuery.coordinates) {
                        is String -> geoQuery.coordinates
                        is List<*> -> geoQuery.coordinates.toString()
                        else -> geoQuery.coordinates.toString()
                    }
                val wktCoordinates = geoJsonToWkt(geoQuery.geometry, storedCoordinates)

                databaseClient.sql(
                    """
                    INSERT INTO geometry_query (georel, geometry, coordinates, pgis_geometry, 
                        geoproperty, subscription_id) 
                    VALUES (:georel, :geometry, :coordinates, ST_GeomFromText(:wkt_coordinates), 
                        :geoproperty, :subscription_id)
                """
                )
                    .bind("georel", geoQuery.georel)
                    .bind("geometry", geoQuery.geometry)
                    .bind("geoproperty", geoQuery.geoproperty)
                    .bind("coordinates", storedCoordinates)
                    .bind("wkt_coordinates", wktCoordinates)
                    .bind("subscription_id", subscriptionId)
                    .execute()
            } else
                Unit.right()
        }

    fun getById(id: URI): Mono<Subscription> {
        val selectStatement =
            """
            SELECT subscription.id as sub_id, subscription.type as sub_type, subscription_name, created_at,
                modified_at, description, watched_attributes, time_interval, q, notif_attributes, notif_format,
                endpoint_uri, endpoint_accept, endpoint_info, status, times_sent, is_active, last_notification,
                last_failure, last_success, entity_info.id as entity_id, id_pattern,
                entity_info.type as entity_type, georel, geometry, coordinates, pgis_geometry, geoproperty, expires_at
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
    suspend fun update(
        subscriptionId: URI,
        input: Map<String, Any>,
        contexts: List<String>
    ): Either<APIException, Unit> {
        return either {
            val subscriptionInputWithModifiedAt = input.plus("modifiedAt" to Instant.now().atZone(ZoneOffset.UTC))

            if (!subscriptionInputWithModifiedAt.containsKey(JSONLD_TYPE_TERM) ||
                subscriptionInputWithModifiedAt[JSONLD_TYPE_TERM]!! != NGSILD_SUBSCRIPTION_TERM
            )
                BadRequestDataException("type attribute must be present and equal to 'Subscription'")
                    .left().bind<Unit>()

            subscriptionInputWithModifiedAt.filterKeys {
                it !in JsonLdUtils.JSONLD_COMPACTED_ENTITY_MANDATORY_FIELDS
            }.forEach {
                when {
                    it.key == "geoQ" -> {
                        val geoQuery = ParsingUtils.parseGeoQuery(it.value as Map<String, Any>, contexts)
                        updateGeometryQuery(subscriptionId, geoQuery).bind()
                    }

                    it.key == "notification" -> {
                        val notification = it.value as Map<String, Any>
                        updateNotification(subscriptionId, notification, contexts).bind()
                    }

                    it.key == "entities" -> {
                        val entities = it.value as List<Map<String, Any>>
                        updateEntities(subscriptionId, entities, contexts).bind()
                    }

                    it.key == "expiresAt" -> {
                        val columnName = it.key.toSqlColumnName()
                        val expiresAt = checkExpiresAtInTheFuture(it.value as String).bind()
                        updateSubscriptionAttribute(subscriptionId, columnName, expiresAt).bind()
                    }

                    listOf(
                        "subscriptionName",
                        "description",
                        "watchedAttributes",
                        "timeInterval",
                        "q",
                        "isActive",
                        "modifiedAt"
                    ).contains(it.key) -> {
                        val columnName = it.key.toSqlColumnName()
                        val value = it.value.toSqlValue(it.key)
                        updateSubscriptionAttribute(subscriptionId, columnName, value).bind()
                    }

                    listOf("csf", "throttling", "temporalQ").contains(it.key) -> {
                        logger.warn("Subscription $subscriptionId has unsupported attribute: ${it.key}")
                        NotImplementedException(
                            "Subscription $subscriptionId has unsupported attribute: ${it.key}"
                        ).left().bind<Unit>()
                    }

                    else -> {
                        logger.warn("Subscription $subscriptionId has invalid attribute: ${it.key}")
                        BadRequestDataException(
                            "Subscription $subscriptionId has invalid attribute: ${it.key}"
                        ).left().bind<Unit>()
                    }
                }
            }
        }
    }

    private suspend fun updateSubscriptionAttribute(
        subscriptionId: URI,
        columnName: String,
        value: Any?
    ): Either<APIException, Unit> {
        val updateStatement = Update.update(columnName, value)
        return r2dbcEntityTemplate.update(Subscription::class.java)
            .matching(query(where("id").`is`(subscriptionId)))
            .apply(updateStatement)
            .map { Unit.right() }
            .awaitFirst()
    }

    suspend fun updateGeometryQuery(subscriptionId: URI, geoQuery: GeoQuery): Either<APIException, Unit> {
        val storedCoordinates: String =
            when (val coordinates = geoQuery.coordinates) {
                is String -> coordinates
                is List<*> -> coordinates.toString()
                else -> coordinates.toString()
            }
        val wktCoordinates = geoJsonToWkt(geoQuery.geometry, storedCoordinates)

        return databaseClient.sql(
            """
            UPDATE geometry_query
            SET georel = :georel, geometry = :geometry, coordinates = :coordinates,
                pgis_geometry = ST_GeomFromText(:wkt_coordinates), geoproperty= :geoproperty
            WHERE subscription_id = :subscription_id
            """
        )
            .bind("georel", geoQuery.georel)
            .bind("geometry", geoQuery.geometry)
            .bind("coordinates", storedCoordinates)
            .bind("wkt_coordinates", wktCoordinates)
            .bind("geoproperty", geoQuery.geoproperty)
            .bind("subscription_id", subscriptionId)
            .execute()
    }

    suspend fun updateNotification(
        subscriptionId: URI,
        notification: Map<String, Any>,
        contexts: List<String>?
    ): Either<APIException, Unit> {
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
                .map { Unit.right() }
                .awaitFirst()
        } catch (e: Exception) {
            return BadRequestDataException(e.message ?: "No values provided for the Notification attribute").left()
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
                    JsonLdUtils.expandJsonLdTerm(it, contexts!!)
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
                val endpointInfo = endpoint["info"] as? List<Map<String, String>>

                listOf(
                    Pair("endpoint_uri", endpoint["uri"]),
                    Pair("endpoint_accept", accept),
                    Pair("endpoint_info", Json.of(endpointInfoMapToString(endpointInfo)))
                )
            }
            else -> throw BadRequestDataException("Could not update attribute ${attribute.key}")
        }
    }

    suspend fun updateEntities(
        subscriptionId: URI,
        entities: List<Map<String, Any>>,
        contexts: List<String>?
    ): Either<APIException, Unit> {
        return either {
            deleteEntityInfo(subscriptionId).bind()
            entities.forEach {
                createEntityInfo(parseEntityInfo(it, contexts), subscriptionId).bind()
            }
        }
    }

    fun delete(subscriptionId: URI): Mono<Long> =
        r2dbcEntityTemplate.delete(
            query(where("id").`is`(subscriptionId)),
            Subscription::class.java
        )

    suspend fun deleteEntityInfo(subscriptionId: URI): Either<APIException, Unit> =
        databaseClient.sql(
            """
            DELETE FROM entity_info
            WHERE subscription_id = :subscription_id
            """.trimIndent()
        )
            .bind("subscription_id", subscriptionId)
            .execute()

    fun getSubscriptions(limit: Int, offset: Int, sub: Option<Sub>): Flux<Subscription> {
        val selectStatement =
            """
            SELECT subscription.id as sub_id, subscription.type as sub_type, subscription_name, created_at, 
                modified_At, description, watched_attributes, time_interval, q, notif_attributes, notif_format,
                endpoint_uri, endpoint_accept, endpoint_info, status, times_sent, is_active, last_notification,
                last_failure, last_success, entity_info.id as entity_id, id_pattern, entity_info.type as entity_type,
                georel, geometry, coordinates, pgis_geometry, geoproperty, expires_at
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

    fun getMatchingSubscriptions(id: URI, types: List<String>, updatedAttributes: String): Flux<Subscription> {
        val selectStatement =
            """
            SELECT subscription.id as sub_id, subscription.type as sub_type, subscription_name, description, q,
                   notif_attributes, notif_format, endpoint_uri, endpoint_accept, times_sent, endpoint_info
            FROM subscription 
            WHERE is_active
            AND ( expires_at is null OR expires_at >= :date )
            AND time_interval IS NULL
            AND ( string_to_array(watched_attributes, ',') && string_to_array(:updatedAttributes, ',')
                OR watched_attributes IS NULL)
            AND id IN (
                SELECT subscription_id
                FROM entity_info
                WHERE entity_info.type IN (:types)
                AND (entity_info.id IS NULL OR entity_info.id = :id)
                AND (entity_info.id_pattern IS NULL OR :id ~ entity_info.id_pattern)
            )
            """.trimIndent()
        return databaseClient.sql(selectStatement)
            .bind("id", id)
            .bind("types", types)
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

    fun isMatchingGeoQuery(subscriptionId: URI, geoProperty: NgsiLdGeoProperty?): Mono<Boolean> {
        return if (geoProperty == null)
            Mono.just(true)
        else {
            getMatchingGeoQuery(subscriptionId)
                .map {
                    createGeoQueryStatement(it, geoProperty)
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
    ): Mono<Long> {
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

    private var rowToSubscription: ((Row, RowMetadata) -> Subscription) = { row, rowMetadata ->
        Subscription(
            id = row.get("sub_id", String::class.java)!!.toUri(),
            type = row.get("sub_type", String::class.java)!!,
            subscriptionName = row.get("subscription_name", String::class.java),
            createdAt = row.get("created_at", ZonedDateTime::class.java)!!.toInstant().atZone(ZoneOffset.UTC),
            modifiedAt = row.get("modified_at", ZonedDateTime::class.java)?.toInstant()?.atZone(ZoneOffset.UTC),
            expiresAt = row.get("expires_at", ZonedDateTime::class.java)?.toInstant()?.atZone(ZoneOffset.UTC),
            description = row.get("description", String::class.java),
            watchedAttributes = row.get("watched_attributes", String::class.java)?.split(","),
            timeInterval = row.get("time_interval", Integer::class.java)?.toInt(),
            q = row.get("q", String::class.java),
            entities = setOf(
                EntityInfo(
                    id = row.get("entity_id", String::class.java)?.toUri(),
                    idPattern = row.get("id_pattern", String::class.java),
                    type = row.get("entity_type", String::class.java)!!
                )
            ),
            geoQ = rowToGeoQuery(row, rowMetadata),
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

    private var rowToRawSubscription: ((Row, RowMetadata) -> Subscription) = { row, _ ->
        Subscription(
            id = row.get("sub_id", String::class.java)!!.toUri(),
            type = row.get("sub_type", String::class.java)!!,
            subscriptionName = row.get("subscription_name", String::class.java),
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

    private var rowToGeoQuery: ((Row, RowMetadata) -> GeoQuery?) = { row, _ ->
        if (row.get("georel", String::class.java) != null)
            GeoQuery(
                georel = row.get("georel", String::class.java)!!,
                geometry = row.get("geometry", String::class.java)!!,
                coordinates = row.get("coordinates", String::class.java)!!,
                pgisGeometry = row.get("pgis_geometry", String::class.java)!!,
                geoproperty = row.get("geoproperty", ExpandedTerm::class.java) ?: NGSILD_LOCATION_PROPERTY
            )
        else
            null
    }

    private var matchesGeoQuery: ((Row, RowMetadata) -> Boolean) = { row, _ ->
        row.get("match", Object::class.java).toString() == "true"
    }

    private var rowToSubscriptionCount: ((Row, RowMetadata) -> Int) = { row, _ ->
        row.get("count", Integer::class.java)!!.toInt()
    }

    private var rowToSub: (Row, RowMetadata) -> String = { row, _ ->
        row.get("sub", String::class.java)!!
    }

    suspend fun getRecurringSubscriptionsToNotify(): List<Subscription> {
        val selectStatement =
            """
            SELECT subscription.id as sub_id, subscription.type as sub_type, subscription_name, created_at,
                modified_At, expires_at, description, watched_attributes, time_interval, q,  notif_attributes,
                notif_format, endpoint_uri, endpoint_accept, endpoint_info,  status, times_sent, last_notification,
                last_failure, last_success, is_active, entity_info.id as entity_id, id_pattern,
                entity_info.type as entity_type, georel, geometry, coordinates, pgis_geometry, geoproperty
            FROM subscription
            LEFT JOIN entity_info ON entity_info.subscription_id = subscription.id
            LEFT JOIN geometry_query ON geometry_query.subscription_id = subscription.id
            WHERE time_interval IS NOT NULL
            AND (last_notification IS NULL 
                OR ((EXTRACT(EPOCH FROM last_notification) + time_interval) < EXTRACT(EPOCH FROM :currentDate))
            )
            AND is_active 
            """.trimIndent()
        return databaseClient.sql(selectStatement)
            .bind("currentDate", Instant.now().atZone(ZoneOffset.UTC))
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
            .collectList()
            .awaitFirst()
    }
}
