package com.egm.stellio.subscription.service

import arrow.core.*
import arrow.core.raise.either
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_TYPE_TERM
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_LOCATION_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_SUBSCRIPTION_TERM
import com.egm.stellio.subscription.config.SubscriptionProperties
import com.egm.stellio.subscription.model.*
import com.egm.stellio.subscription.model.GeoQ
import com.egm.stellio.subscription.model.Subscription
import com.egm.stellio.subscription.utils.*
import com.egm.stellio.subscription.utils.ParsingUtils.endpointInfoMapToString
import com.egm.stellio.subscription.utils.ParsingUtils.endpointInfoToString
import com.egm.stellio.subscription.utils.ParsingUtils.parseEndpointInfo
import com.egm.stellio.subscription.utils.ParsingUtils.parseEntityInfo
import com.egm.stellio.subscription.utils.ParsingUtils.toSqlColumnName
import com.egm.stellio.subscription.utils.ParsingUtils.toSqlValue
import com.egm.stellio.subscription.utils.QueryUtils.createGeoQueryStatement
import com.egm.stellio.subscription.utils.QueryUtils.createQueryStatement
import com.egm.stellio.subscription.utils.QueryUtils.createScopeQueryStatement
import io.r2dbc.postgresql.codec.Json
import kotlinx.coroutines.reactive.awaitFirst
import org.locationtech.jts.geom.Geometry
import org.slf4j.LoggerFactory
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.data.relational.core.query.Criteria.where
import org.springframework.data.relational.core.query.Query.query
import org.springframework.data.relational.core.query.Update
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.r2dbc.core.bind
import org.springframework.stereotype.Component
import org.springframework.transaction.annotation.Transactional
import java.net.URI
import java.time.Instant
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.util.regex.Pattern

@Component
class SubscriptionService(
    private val subscriptionProperties: SubscriptionProperties,
    private val databaseClient: DatabaseClient,
    private val r2dbcEntityTemplate: R2dbcEntityTemplate
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
            BadRequestDataException(invalidUriMessage("${subscription.id}")).left()
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
        if (subscription.expiresAt != null && subscription.expiresAt.isBefore(ngsiLdDateTime()))
            BadRequestDataException("'expiresAt' must be in the future").left()
        else Unit.right()

    private fun checkExpiresAtInTheFuture(expiresAt: String): Either<BadRequestDataException, ZonedDateTime> =
        runCatching { ZonedDateTime.parse(expiresAt) }.fold({
            if (it.isBefore(ngsiLdDateTime()))
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

            val geoQuery =
                if (subscription.geoQ != null)
                    parseGeoQueryParameters(subscription.geoQ.toMap(), subscription.contexts).bind()
                else null

            val insertStatement =
                """
                INSERT INTO subscription(id, type, subscription_name, created_at, description, watched_attributes,
                    time_interval, q, scope_q, notif_attributes, notif_format, endpoint_uri, endpoint_accept, 
                    endpoint_info, times_sent, is_active, expires_at, sub, contexts)
                VALUES(:id, :type, :subscription_name, :created_at, :description, :watched_attributes, 
                    :time_interval, :q, :scope_q, :notif_attributes, :notif_format, :endpoint_uri, :endpoint_accept, 
                    :endpoint_info, :times_sent, :is_active, :expires_at, :sub, :contexts)
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
                .bind("scope_q", subscription.scopeQ)
                .bind("notif_attributes", subscription.notification.attributes?.joinToString(separator = ","))
                .bind("notif_format", subscription.notification.format.name)
                .bind("endpoint_uri", subscription.notification.endpoint.uri)
                .bind("endpoint_accept", subscription.notification.endpoint.accept.name)
                .bind("endpoint_info", Json.of(endpointInfoToString(subscription.notification.endpoint.info)))
                .bind("times_sent", subscription.notification.timesSent)
                .bind("is_active", subscription.isActive)
                .bind("expires_at", subscription.expiresAt)
                .bind("sub", sub.toStringValue())
                .bind("contexts", subscription.contexts.toTypedArray())
                .execute().bind()

            geoQuery?.let {
                createGeometryQuery(it, subscription.id).bind()
            }

            subscription.entities.forEach {
                createEntityInfo(it, subscription.id).bind()
            }
        }
    }

    suspend fun exists(subscriptionId: URI): Either<APIException, Boolean> =
        databaseClient.sql(
            """
            SELECT exists (
                SELECT 1
                FROM subscription
                WHERE id = :id
            ) as exists
            """.trimIndent()
        ).bind("id", subscriptionId)
            .oneToResult { toBoolean(it["exists"]) }

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
                .execute().bind()
        }

    private suspend fun createGeometryQuery(geoQuery: GeoQuery, subscriptionId: URI): Either<APIException, Unit> =
        either {
            databaseClient.sql(
                """
                INSERT INTO geometry_query (georel, geometry, coordinates, pgis_geometry, 
                    geoproperty, subscription_id) 
                VALUES (:georel, :geometry, :coordinates, public.ST_GeomFromText(:wkt_coordinates), 
                    :geoproperty, :subscription_id)
            """
            )
                .bind("georel", geoQuery.georel)
                .bind("geometry", geoQuery.geometry.type)
                .bind("coordinates", geoQuery.coordinates)
                .bind("wkt_coordinates", geoQuery.wktCoordinates.value)
                .bind("geoproperty", geoQuery.geoproperty)
                .bind("subscription_id", subscriptionId)
                .execute().bind()
        }

    suspend fun getById(id: URI): Subscription {
        val selectStatement =
            """
            SELECT subscription.id as sub_id, subscription.type as sub_type, subscription_name, created_at,
                modified_at, description, watched_attributes, time_interval, q, notif_attributes, notif_format,
                endpoint_uri, endpoint_accept, endpoint_info, status, times_sent, is_active, last_notification,
                last_failure, last_success, entity_info.id as entity_id, id_pattern,
                entity_info.type as entity_type, georel, geometry, coordinates, pgis_geometry, geoproperty, 
                scope_q, expires_at, contexts
            FROM subscription 
            LEFT JOIN entity_info ON entity_info.subscription_id = :id
            LEFT JOIN geometry_query ON geometry_query.subscription_id = :id 
            WHERE subscription.id = :id
            """.trimIndent()

        return databaseClient.sql(selectStatement)
            .bind("id", id)
            .allToMappedList { rowToSubscription(it) }
            .reduce { t: Subscription, u: Subscription ->
                t.copy(entities = t.entities.plus(u.entities))
            }
    }

    suspend fun getContextsForSubscription(id: URI): Either<APIException, List<String>> {
        val selectStatement =
            """
            SELECT contexts
            FROM subscription 
            WHERE id = :id
            """.trimIndent()

        return databaseClient.sql(selectStatement)
            .bind("id", id)
            .oneToResult {
                toList(it["contexts"])
            }
    }

    fun getContextsLink(subscription: Subscription): String =
        if (subscription.contexts.size > 1) {
            val linkToRetrieveContexts = subscriptionProperties.stellioUrl +
                "/ngsi-ld/v1/subscriptions/${subscription.id}/context"
            buildContextLinkHeader(linkToRetrieveContexts)
        } else
            buildContextLinkHeader(subscription.contexts[0])

    suspend fun isCreatorOf(subscriptionId: URI, sub: Option<Sub>): Either<APIException, Boolean> {
        val selectStatement =
            """
            SELECT sub
            FROM subscription
            WHERE id = :id
            """.trimIndent()

        return databaseClient.sql(selectStatement)
            .bind("id", subscriptionId)
            .oneToResult {
                it["sub"] == sub.toStringValue()
            }
    }

    @Transactional
    suspend fun update(
        subscriptionId: URI,
        input: Map<String, Any>,
        contexts: List<String>
    ): Either<APIException, Unit> = either {
        val subscriptionInputWithModifiedAt = input.plus("modifiedAt" to Instant.now().atZone(ZoneOffset.UTC))

        if (!subscriptionInputWithModifiedAt.containsKey(JSONLD_TYPE_TERM) ||
            subscriptionInputWithModifiedAt[JSONLD_TYPE_TERM]!! != NGSILD_SUBSCRIPTION_TERM
        )
            BadRequestDataException("type attribute must be present and equal to 'Subscription'").left().bind<Unit>()

        subscriptionInputWithModifiedAt.filterKeys {
            it !in JsonLdUtils.JSONLD_COMPACTED_ENTITY_CORE_MEMBERS
        }.forEach {
            when {
                it.key == "geoQ" ->
                    parseGeoQueryParameters(it.value as Map<String, String>, contexts).bind()
                        ?.let { updateGeometryQuery(subscriptionId, it).bind() }

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
                    "scopeQ",
                    "isActive",
                    "modifiedAt"
                ).contains(it.key) -> {
                    val columnName = it.key.toSqlColumnName()
                    val value = it.value.toSqlValue(it.key)
                    updateSubscriptionAttribute(subscriptionId, columnName, value).bind()
                }

                listOf("csf", "throttling", "temporalQ").contains(it.key) -> {
                    logger.warn("Subscription $subscriptionId has unsupported attribute: ${it.key}")
                    NotImplementedException("Subscription $subscriptionId has unsupported attribute: ${it.key}")
                        .left().bind<Unit>()
                }

                else -> {
                    logger.warn("Subscription $subscriptionId has invalid attribute: ${it.key}")
                    BadRequestDataException("Subscription $subscriptionId has invalid attribute: ${it.key}")
                        .left().bind<Unit>()
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

    suspend fun updateGeometryQuery(subscriptionId: URI, geoQ: GeoQuery): Either<APIException, Unit> =
        databaseClient.sql(
            """
            UPDATE geometry_query
            SET georel = :georel, geometry = :geometry, coordinates = :coordinates,
                pgis_geometry = public.ST_GeomFromText(:wkt_coordinates), geoproperty= :geoproperty
            WHERE subscription_id = :subscription_id
            """
        )
            .bind("georel", geoQ.georel)
            .bind("geometry", geoQ.geometry.type)
            .bind("coordinates", geoQ.coordinates)
            .bind("wkt_coordinates", geoQ.wktCoordinates.value)
            .bind("geoproperty", geoQ.geoproperty)
            .bind("subscription_id", subscriptionId)
            .execute()

    suspend fun updateNotification(
        subscriptionId: URI,
        notification: Map<String, Any>,
        contexts: List<String>
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
        contexts: List<String>
    ): List<Pair<String, Any?>> {
        return when (attribute.key) {
            "attributes" -> {
                var valueList = attribute.value as List<String>
                valueList = valueList.map {
                    JsonLdUtils.expandJsonLdTerm(it, contexts)
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
        contexts: List<String>
    ): Either<APIException, Unit> {
        return either {
            deleteEntityInfo(subscriptionId).bind()
            entities.forEach {
                createEntityInfo(parseEntityInfo(it, contexts), subscriptionId).bind()
            }
        }
    }

    suspend fun delete(subscriptionId: URI): Either<APIException, Unit> =
        r2dbcEntityTemplate.delete(Subscription::class.java)
            .matching(query(where("id").`is`(subscriptionId)))
            .execute()

    suspend fun deleteEntityInfo(subscriptionId: URI): Either<APIException, Unit> =
        databaseClient.sql(
            """
            DELETE FROM entity_info
            WHERE subscription_id = :subscription_id
            """.trimIndent()
        )
            .bind("subscription_id", subscriptionId)
            .execute()

    suspend fun getSubscriptions(limit: Int, offset: Int, sub: Option<Sub>): List<Subscription> {
        val selectStatement =
            """
            SELECT subscription.id as sub_id, subscription.type as sub_type, subscription_name, created_at, 
                modified_At, description, watched_attributes, time_interval, q, notif_attributes, notif_format,
                endpoint_uri, endpoint_accept, endpoint_info, status, times_sent, is_active, last_notification,
                last_failure, last_success, entity_info.id as entity_id, id_pattern, entity_info.type as entity_type,
                georel, geometry, coordinates, pgis_geometry, geoproperty, scope_q, expires_at, contexts
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
            .allToMappedList { rowToSubscription(it) }
            .groupBy { t: Subscription ->
                t.id
            }
            .mapValues { grouped ->
                grouped.value.reduce { t: Subscription, u: Subscription ->
                    t.copy(entities = t.entities.plus(u.entities))
                }
            }.values.toList()
    }

    suspend fun getSubscriptionsCount(sub: Option<Sub>): Either<APIException, Int> {
        val selectStatement =
            """
            SELECT count(*)
            FROM subscription
            WHERE subscription.sub = :sub
            """.trimIndent()
        return databaseClient.sql(selectStatement)
            .bind("sub", sub.toStringValue())
            .oneToResult { toInt(it["count"]) }
    }

    suspend fun getMatchingSubscriptions(
        id: URI,
        types: List<ExpandedTerm>,
        updatedAttributes: Set<ExpandedTerm>
    ): List<Subscription> {
        val selectStatement =
            """
            SELECT subscription.id as sub_id, subscription.type as sub_type, subscription_name, description, q,
                   scope_q, notif_attributes, notif_format, endpoint_uri, endpoint_accept, times_sent, endpoint_info, 
                   contexts
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
            .bind("updatedAttributes", updatedAttributes.joinToString(separator = ","))
            .bind("date", Instant.now().atZone(ZoneOffset.UTC))
            .allToMappedList { rowToRawSubscription(it) }
    }

    suspend fun isMatchingQQuery(
        query: String?,
        jsonLdEntity: JsonLdEntity,
        contexts: List<String>
    ): Either<APIException, Boolean> =
        if (query == null)
            true.right()
        else
            databaseClient
                .sql(createQueryStatement(query, jsonLdEntity, contexts))
                .oneToResult { toBoolean(it["match"]) }

    suspend fun isMatchingScopeQQuery(
        scopeQ: String?,
        jsonLdEntity: JsonLdEntity
    ): Either<APIException, Boolean> =
        if (scopeQ == null)
            true.right()
        else
            databaseClient
                .sql(createScopeQueryStatement(scopeQ, jsonLdEntity))
                .oneToResult { toBoolean(it["match"]) }

    suspend fun isMatchingGeoQuery(
        subscriptionId: URI,
        jsonLdEntity: JsonLdEntity
    ): Either<APIException, Boolean> =
        getMatchingGeoQuery(subscriptionId)?.let {
            val geoQueryStatement = createGeoQueryStatement(it, jsonLdEntity)
            runGeoQueryStatement(geoQueryStatement)
        } ?: true.right()

    suspend fun getMatchingGeoQuery(subscriptionId: URI): GeoQ? {
        val selectStatement =
            """
            SELECT *
            FROM geometry_query 
            WHERE subscription_id = :sub_id
            """.trimIndent()
        return databaseClient.sql(selectStatement)
            .bind("sub_id", subscriptionId)
            .oneToResult { rowToGeoQ(it) }
            .getOrElse { null }
    }

    suspend fun runGeoQueryStatement(geoQueryStatement: String): Either<APIException, Boolean> {
        return databaseClient.sql(geoQueryStatement.trimIndent())
            .oneToResult { toBoolean(it["match"]) }
    }

    suspend fun updateSubscriptionNotification(
        subscription: Subscription,
        notification: Notification,
        success: Boolean
    ): Long {
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
        ).awaitFirst()
    }

    private val rowToSubscription: ((Map<String, Any>) -> Subscription) = { row ->
        Subscription(
            id = toUri(row["sub_id"]),
            type = row["sub_type"] as String,
            subscriptionName = row["subscription_name"] as? String,
            createdAt = toZonedDateTime(row["created_at"]),
            modifiedAt = toNullableZonedDateTime(row["modified_at"]),
            expiresAt = toNullableZonedDateTime(row["expires_at"]),
            description = row["description"] as? String,
            watchedAttributes = (row["watched_attributes"] as? String)?.split(","),
            timeInterval = toNullableInt(row["time_interval"]),
            q = row["q"] as? String,
            entities = setOf(
                EntityInfo(
                    id = toNullableUri(row["entity_id"]),
                    idPattern = row["id_pattern"] as? String,
                    type = row["entity_type"] as String
                )
            ),
            geoQ = rowToGeoQ(row),
            scopeQ = row["scope_q"] as? String,
            notification = NotificationParams(
                attributes = (row["notif_attributes"] as? String)?.split(","),
                format = toEnum(row["notif_format"]!!),
                endpoint = Endpoint(
                    uri = toUri(row["endpoint_uri"]),
                    accept = toEnum(row["endpoint_accept"]!!),
                    info = parseEndpointInfo(toJsonString(row["endpoint_info"]))
                ),
                status = toOptionalEnum<NotificationParams.StatusType>(row["status"]),
                timesSent = row["times_sent"] as Int,
                lastNotification = toNullableZonedDateTime(row["last_notification"]),
                lastFailure = toNullableZonedDateTime(row["last_failure"]),
                lastSuccess = toNullableZonedDateTime(row["last_success"])
            ),
            isActive = toBoolean(row["is_active"]),
            contexts = toList(row["contexts"])
        )
    }

    private val rowToRawSubscription: ((Map<String, Any>) -> Subscription) = { row ->
        Subscription(
            id = toUri(row["sub_id"]),
            type = row["sub_type"] as String,
            subscriptionName = row["subscription_name"] as? String,
            description = row["description"] as? String,
            q = row["q"] as? String,
            scopeQ = row["scope_q"] as? String,
            entities = emptySet(),
            notification = NotificationParams(
                attributes = (row["notif_attributes"] as? String)?.split(","),
                format = toEnum(row["notif_format"]!!),
                endpoint = Endpoint(
                    uri = toUri(row["endpoint_uri"]),
                    accept = toEnum(row["endpoint_accept"]!!),
                    info = parseEndpointInfo(toJsonString(row["endpoint_info"]))
                ),
                status = null,
                timesSent = row["times_sent"] as Int,
                lastNotification = null,
                lastFailure = null,
                lastSuccess = null
            ),
            contexts = toList(row["contexts"])
        )
    }

    private val rowToGeoQ: ((Map<String, Any>) -> GeoQ?) = { row ->
        if (row["georel"] != null)
            GeoQ(
                georel = row["georel"] as String,
                geometry = row["geometry"] as String,
                coordinates = row["coordinates"] as String,
                pgisGeometry = (row["pgis_geometry"] as Geometry).toText(),
                geoproperty = row["geoproperty"] as? ExpandedTerm ?: NGSILD_LOCATION_PROPERTY
            )
        else
            null
    }

    suspend fun getRecurringSubscriptionsToNotify(): List<Subscription> {
        val selectStatement =
            """
            SELECT subscription.id as sub_id, subscription.type as sub_type, subscription_name, created_at,
                modified_At, expires_at, description, watched_attributes, time_interval, q, scope_q, notif_attributes,
                notif_format, endpoint_uri, endpoint_accept, endpoint_info,  status, times_sent, last_notification,
                last_failure, last_success, is_active, entity_info.id as entity_id, id_pattern,
                entity_info.type as entity_type, georel, geometry, coordinates, pgis_geometry, geoproperty, contexts
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
            .allToMappedList { rowToSubscription(it) }
            .groupBy { t: Subscription ->
                t.id
            }
            .mapValues { grouped ->
                grouped.value.reduce { t: Subscription, u: Subscription ->
                    t.copy(entities = t.entities.plus(u.entities))
                }
            }.values.toList()
    }
}
