package com.egm.stellio.subscription.service

import arrow.core.Either
import arrow.core.Option
import arrow.core.left
import arrow.core.raise.either
import arrow.core.right
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_TYPE_TERM
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_LOCATION_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_SUBSCRIPTION_TERM
import com.egm.stellio.shared.util.JsonLdUtils.checkJsonldContext
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdTerm
import com.egm.stellio.subscription.config.SubscriptionProperties
import com.egm.stellio.subscription.model.*
import com.egm.stellio.subscription.utils.*
import com.egm.stellio.subscription.utils.ParsingUtils.endpointInfoMapToString
import com.egm.stellio.subscription.utils.ParsingUtils.endpointInfoToString
import com.egm.stellio.subscription.utils.ParsingUtils.parseEndpointInfo
import com.egm.stellio.subscription.utils.ParsingUtils.parseEntitySelector
import com.egm.stellio.subscription.utils.ParsingUtils.toSqlColumnName
import com.egm.stellio.subscription.utils.ParsingUtils.toSqlValue
import com.egm.stellio.subscription.web.invalidSubscriptionAttributeMessage
import com.egm.stellio.subscription.web.unsupportedSubscriptionAttributeMessage
import io.r2dbc.postgresql.codec.Json
import kotlinx.coroutines.reactive.awaitFirst
import org.locationtech.jts.geom.Geometry
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

    suspend fun validateNewSubscription(subscription: Subscription): Either<APIException, Unit> = either {
        checkTypeIsSubscription(subscription).bind()
        checkIdIsValid(subscription).bind()
        checkEntitiesOrWatchedAttributes(subscription).bind()
        checkTimeIntervalGreaterThanZero(subscription).bind()
        checkThrottlingGreaterThanZero(subscription).bind()
        checkSubscriptionValidity(subscription).bind()
        checkExpiresAtInTheFuture(subscription).bind()
        checkIdPatternIsValid(subscription).bind()
        checkNotificationTriggersAreValid(subscription).bind()
        checkJsonLdContextIsValid(subscription).bind()
    }

    private fun checkTypeIsSubscription(subscription: Subscription): Either<APIException, Unit> =
        if (subscription.type != NGSILD_SUBSCRIPTION_TERM)
            BadRequestDataException("type attribute must be equal to 'Subscription'").left()
        else Unit.right()

    private fun checkIdIsValid(subscription: Subscription): Either<APIException, Unit> =
        if (!subscription.id.isAbsolute)
            BadRequestDataException(invalidUriMessage("${subscription.id}")).left()
        else Unit.right()

    private fun checkEntitiesOrWatchedAttributes(subscription: Subscription): Either<APIException, Unit> =
        if (subscription.watchedAttributes == null && subscription.entities == null)
            BadRequestDataException("At least one of entities or watchedAttributes shall be present").left()
        else Unit.right()

    private fun checkSubscriptionValidity(subscription: Subscription): Either<APIException, Unit> =
        when {
            subscription.watchedAttributes != null && subscription.timeInterval != null -> {
                BadRequestDataException(
                    "You can't use 'timeInterval' in conjunction with 'watchedAttributes'"
                ).left()
            }
            subscription.timeInterval != null && subscription.throttling != null -> {
                BadRequestDataException(
                    "You can't use 'timeInterval' in conjunction with 'throttling'"
                ).left()
            }
            else ->
                Unit.right()
        }

    private fun checkTimeIntervalGreaterThanZero(subscription: Subscription): Either<APIException, Unit> =
        if (subscription.timeInterval != null && subscription.timeInterval < 1)
            BadRequestDataException("The value of 'timeInterval' must be greater than zero (int)").left()
        else Unit.right()

    private fun checkThrottlingGreaterThanZero(subscription: Subscription): Either<APIException, Unit> =
        if (subscription.throttling != null && subscription.throttling < 1)
            BadRequestDataException("The value of 'throttling' must be greater than zero (int)").left()
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

    private fun checkIdPatternIsValid(subscription: Subscription): Either<BadRequestDataException, Unit> {
        val result = subscription.entities?.all { endpoint ->
            runCatching {
                endpoint.idPattern?.let { Pattern.compile(it) }
                true
            }.fold({ true }, { false })
        }

        return if (result == null || result)
            Unit.right()
        else BadRequestDataException("Invalid idPattern found in subscription").left()
    }

    private fun checkNotificationTriggersAreValid(subscription: Subscription): Either<BadRequestDataException, Unit> =
        subscription.notificationTrigger.all {
            NotificationTrigger.isValid(it)
        }.let {
            if (it) Unit.right()
            else BadRequestDataException("Unknown notification trigger in ${subscription.notificationTrigger}").left()
        }

    suspend fun checkJsonLdContextIsValid(subscription: Subscription): Either<APIException, Unit> = either {
        val jsonldContext = subscription.jsonldContext

        if (jsonldContext != null) {
            checkJsonldContext(jsonldContext).bind()
        }
    }

    @Transactional
    suspend fun create(subscription: Subscription, sub: Option<Sub>): Either<APIException, Unit> = either {
        validateNewSubscription(subscription).bind()

        val geoQuery =
            if (subscription.geoQ != null)
                parseGeoQueryParameters(subscription.geoQ.toMap(), subscription.contexts).bind()
            else null
        val endpoint = subscription.notification.endpoint

        val insertStatement =
            """
            INSERT INTO subscription(id, type, subscription_name, created_at, description, watched_attributes,
                notification_trigger, time_interval, q, scope_q, notif_attributes, notif_format, endpoint_uri, 
                endpoint_accept, endpoint_receiver_info, endpoint_notifier_info, times_sent, is_active, 
                expires_at, sub, contexts, throttling, sys_attrs, lang, datasetId, jsonld_context)
            VALUES(:id, :type, :subscription_name, :created_at, :description, :watched_attributes, 
                :notification_trigger, :time_interval, :q, :scope_q, :notif_attributes, :notif_format, :endpoint_uri, 
                :endpoint_accept, :endpoint_receiver_info, :endpoint_notifier_info, :times_sent, :is_active, 
                :expires_at, :sub, :contexts, :throttling, :sys_attrs, :lang, :datasetId, :jsonld_context)
            """.trimIndent()

        databaseClient.sql(insertStatement)
            .bind("id", subscription.id)
            .bind("type", subscription.type)
            .bind("subscription_name", subscription.subscriptionName)
            .bind("created_at", subscription.createdAt)
            .bind("description", subscription.description)
            .bind("watched_attributes", subscription.watchedAttributes?.joinToString(separator = ","))
            .bind("notification_trigger", subscription.notificationTrigger.toTypedArray())
            .bind("time_interval", subscription.timeInterval)
            .bind("q", subscription.q)
            .bind("scope_q", subscription.scopeQ)
            .bind("notif_attributes", subscription.notification.attributes?.joinToString(separator = ","))
            .bind("notif_format", subscription.notification.format.name)
            .bind("endpoint_uri", endpoint.uri)
            .bind("endpoint_accept", endpoint.accept.name)
            .bind("endpoint_receiver_info", Json.of(endpointInfoToString(endpoint.receiverInfo)))
            .bind("endpoint_notifier_info", Json.of(endpointInfoToString(endpoint.notifierInfo)))
            .bind("times_sent", subscription.notification.timesSent)
            .bind("is_active", subscription.isActive)
            .bind("expires_at", subscription.expiresAt)
            .bind("sub", sub.toStringValue())
            .bind("contexts", subscription.contexts.toTypedArray())
            .bind("throttling", subscription.throttling)
            .bind("sys_attrs", subscription.notification.sysAttrs)
            .bind("lang", subscription.lang)
            .bind("datasetId", subscription.datasetId?.toTypedArray())
            .bind("jsonld_context", subscription.jsonldContext)
            .execute().bind()

        geoQuery?.let {
            upsertGeometryQuery(it, subscription.id).bind()
        }

        subscription.entities?.forEach {
            createEntitySelector(it, subscription.id).bind()
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

    private suspend fun createEntitySelector(
        entitySelector: EntitySelector,
        subscriptionId: URI
    ): Either<APIException, Unit> = either {
        databaseClient.sql(
            """
            INSERT INTO entity_selector (id, id_pattern, type_selection, subscription_id) 
            VALUES (:id, :id_pattern, :type_selection, :subscription_id)
            """.trimIndent()
        )
            .bind("id", entitySelector.id)
            .bind("id_pattern", entitySelector.idPattern)
            .bind("type_selection", entitySelector.typeSelection)
            .bind("subscription_id", subscriptionId)
            .execute().bind()
    }

    private suspend fun upsertGeometryQuery(geoQuery: GeoQuery, subscriptionId: URI): Either<APIException, Unit> =
        either {
            databaseClient.sql(
                """
                INSERT INTO geometry_query (georel, geometry, coordinates, pgis_geometry, 
                    geoproperty, subscription_id) 
                VALUES (:georel, :geometry, :coordinates, public.ST_GeomFromText(:wkt_coordinates), 
                    :geoproperty, :subscription_id)
                ON CONFLICT (subscription_id)
                DO UPDATE SET georel = :georel, geometry = :geometry, coordinates = :coordinates,
                    pgis_geometry = public.ST_GeomFromText(:wkt_coordinates), geoproperty = :geoproperty
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
                modified_at, description, watched_attributes, notification_trigger, time_interval, q, notif_attributes,
                notif_format, endpoint_uri, endpoint_accept, endpoint_receiver_info, endpoint_notifier_info, status, 
                times_sent, is_active, last_notification, last_failure, last_success, entity_selector.id as entity_id, 
                id_pattern, entity_selector.type_selection as type_selection, georel, geometry, coordinates, 
                pgis_geometry, geoproperty, scope_q, expires_at, contexts, throttling, sys_attrs, lang, 
                datasetId, jsonld_context
            FROM subscription 
            LEFT JOIN entity_selector ON entity_selector.subscription_id = :id
            LEFT JOIN geometry_query ON geometry_query.subscription_id = :id 
            WHERE subscription.id = :id
            """.trimIndent()

        return databaseClient.sql(selectStatement)
            .bind("id", id)
            .allToMappedList { rowToSubscription(it) }
            .mergeEntitySelectorsOnSubscriptions()
            .first()
    }

    suspend fun getContextsForSubscription(id: URI): Either<APIException, List<String>> {
        val selectStatement =
            """
            SELECT contexts, jsonld_context
            FROM subscription 
            WHERE id = :id
            """.trimIndent()

        return databaseClient.sql(selectStatement)
            .bind("id", id)
            .oneToResult {
                it["jsonld_context"]?.let { listOf(it as String) } ?: toList(it["contexts"]!!)
            }
    }

    fun getContextsLink(subscription: Subscription): String {
        val contextLink = when {
            subscription.contexts.size > 1 && subscription.jsonldContext == null -> {
                val linkToRetrieveContexts = subscriptionProperties.stellioUrl +
                    "/ngsi-ld/v1/subscriptions/${subscription.id}/context"
                linkToRetrieveContexts
            }
            subscription.jsonldContext != null -> subscription.jsonldContext.toString()
            else -> subscription.contexts[0]
        }
        return buildContextLinkHeader(contextLink)
    }

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
        val subscriptionInputWithModifiedAt = input.plus("modifiedAt" to ngsiLdDateTime())

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
                        ?.let { upsertGeometryQuery(it, subscriptionId).bind() }

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

                it.key == "watchedAttributes" -> {
                    val value = (it.value as List<String>).map { watchedAttribute ->
                        expandJsonLdTerm(watchedAttribute, contexts)
                    }.toSqlValue(it.key)
                    updateSubscriptionAttribute(subscriptionId, it.key.toSqlColumnName(), value).bind()
                }

                listOf(
                    "subscriptionName",
                    "description",
                    "notificationTrigger",
                    "timeInterval",
                    "q",
                    "scopeQ",
                    "isActive",
                    "modifiedAt",
                    "throttling",
                    "lang",
                    "datasetId",
                    "jsonldContext"
                ).contains(it.key) -> {
                    val columnName = it.key.toSqlColumnName()
                    val value = it.value.toSqlValue(it.key)
                    updateSubscriptionAttribute(subscriptionId, columnName, value).bind()
                }

                listOf("csf", "temporalQ").contains(it.key) -> {
                    NotImplementedException(unsupportedSubscriptionAttributeMessage(subscriptionId, it.key))
                        .left().bind<Unit>()
                }

                else -> {
                    BadRequestDataException(invalidSubscriptionAttributeMessage(subscriptionId, it.key))
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
                val attributes = (attribute.value as List<String>).joinToString(separator = ",") {
                    expandJsonLdTerm(it, contexts)
                }
                listOf(Pair("notif_attributes", attributes))
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
                val endpointReceiverInfo = endpoint["receiverInfo"] as? List<Map<String, String>>
                val endpointNotifierInfo = endpoint["notifierInfo"] as? List<Map<String, String>>

                listOf(
                    Pair("endpoint_uri", endpoint["uri"]),
                    Pair("endpoint_accept", accept),
                    Pair("endpoint_receiver_info", Json.of(endpointInfoMapToString(endpointReceiverInfo))),
                    Pair("endpoint_notifier_info", Json.of(endpointInfoMapToString(endpointNotifierInfo)))
                )
            }

            else -> throw BadRequestDataException("Could not update attribute ${attribute.key}")
        }
    }

    suspend fun updateEntities(
        subscriptionId: URI,
        entities: List<Map<String, Any>>,
        contexts: List<String>
    ): Either<APIException, Unit> = either {
        deleteEntitySelector(subscriptionId).bind()
        entities.forEach {
            createEntitySelector(parseEntitySelector(it, contexts), subscriptionId).bind()
        }
    }

    suspend fun delete(subscriptionId: URI): Either<APIException, Unit> =
        r2dbcEntityTemplate.delete(Subscription::class.java)
            .matching(query(where("id").`is`(subscriptionId)))
            .execute()

    suspend fun deleteEntitySelector(subscriptionId: URI): Either<APIException, Unit> =
        databaseClient.sql(
            """
            DELETE FROM entity_selector
            WHERE subscription_id = :subscription_id
            """.trimIndent()
        )
            .bind("subscription_id", subscriptionId)
            .execute()

    suspend fun getSubscriptions(limit: Int, offset: Int, sub: Option<Sub>): List<Subscription> {
        val selectStatement =
            """
            SELECT subscription.id as sub_id, subscription.type as sub_type, subscription_name, created_at, 
                modified_At, description, watched_attributes, notification_trigger, time_interval, q, notif_attributes, 
                notif_format, endpoint_uri, endpoint_accept, endpoint_receiver_info, endpoint_notifier_info, status, 
                times_sent, is_active, last_notification, last_failure, last_success, entity_selector.id as entity_id,
                id_pattern, entity_selector.type_selection as type_selection, georel, geometry, coordinates, 
                pgis_geometry, geoproperty, scope_q, expires_at, contexts, throttling, sys_attrs, lang, 
                datasetId, jsonld_context
            FROM subscription 
            LEFT JOIN entity_selector ON entity_selector.subscription_id = subscription.id
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
            .mergeEntitySelectorsOnSubscriptions()
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

    internal suspend fun getMatchingSubscriptions(
        updatedAttributes: Set<ExpandedTerm>,
        notificationTrigger: NotificationTrigger
    ): List<Subscription> {
        val selectStatement =
            """
            SELECT subscription.id as sub_id, subscription.type as sub_type, subscription_name, description, q,
                   entity_selector.id as entity_id, entity_selector.id_pattern as id_pattern, 
                   entity_selector.type_selection as type_selection, georel, geometry, coordinates, pgis_geometry,
                   geoproperty, scope_q, notif_attributes, notif_format, endpoint_uri, endpoint_accept, times_sent, 
                   endpoint_receiver_info, endpoint_notifier_info, contexts, throttling, sys_attrs, lang, 
                   datasetId, jsonld_context
            FROM subscription 
            LEFT JOIN entity_selector on subscription.id = entity_selector.subscription_id
            LEFT JOIN geometry_query on subscription.id = geometry_query.subscription_id
            WHERE is_active
            AND ( expires_at is null OR expires_at >= :date )
            AND time_interval IS NULL
            AND ( throttling IS NULL 
                OR (last_notification + throttling * INTERVAL '1 second') < :date
                OR last_notification IS NULL)
            AND ( string_to_array(watched_attributes, ',') && string_to_array(:updatedAttributes, ',')
                OR watched_attributes IS NULL)
            AND CASE
                WHEN notification_trigger && '{ entityUpdated }'
                    THEN notification_trigger || '{ ${NotificationTrigger.expandEntityUpdated()} }' && '{ ${notificationTrigger.notificationTrigger} }'
                ELSE notification_trigger && '{ ${notificationTrigger.notificationTrigger} }'
            END
            """.trimIndent()
        return databaseClient.sql(selectStatement)
            .bind("updatedAttributes", updatedAttributes.joinToString(separator = ","))
            .bind("date", ngsiLdDateTime())
            .allToMappedList { rowToMinimalMatchSubscription(it) }
            .mergeEntitySelectorsOnSubscriptions()
    }

    suspend fun getMatchingSubscriptions(
        expandedEntity: ExpandedEntity,
        updatedAttributes: Set<ExpandedTerm>,
        notificationTrigger: NotificationTrigger
    ): Either<APIException, List<Subscription>> = either {
        getMatchingSubscriptions(updatedAttributes, notificationTrigger)
            .filter {
                val entitiesFilter = prepareEntitiesQuery(it.entities, expandedEntity)
                val qFilter = prepareQQuery(it.q?.decode(), expandedEntity, it.contexts)
                val scopeFilter = prepareScopeQQuery(it.scopeQ?.decode(), expandedEntity)
                val geoQueryFilter = prepareGeoQuery(it.geoQ, expandedEntity)

                val query = listOfNotNull(entitiesFilter, qFilter, scopeFilter, geoQueryFilter)
                if (query.isEmpty())
                    true
                else
                    databaseClient
                        .sql(query.joinToString(separator = " AND ", prefix = "SELECT ", postfix = " AS match"))
                        .oneToResult { row -> toBoolean(row["match"]) }
                        .bind()
            }
    }

    suspend fun prepareEntitiesQuery(
        entities: Set<EntitySelector>?,
        expandedEntity: ExpandedEntity
    ): String? =
        if (entities.isNullOrEmpty()) null
        else {
            val entityTypes = expandedEntity.types
            entities.joinToString(" OR ") {
                val typeSelectionQuery = buildTypeQuery(it.typeSelection, entityTypes)
                val idQuery =
                    if (it.id == null) null
                    else " '${expandedEntity.id}' = '${it.id}' "
                val idPatternQuery =
                    if (it.idPattern == null) null
                    else " '${expandedEntity.id}' ~ '${it.idPattern}' "
                listOfNotNull(typeSelectionQuery, idQuery, idPatternQuery)
                    .joinToString(separator = " AND ", prefix = "(", postfix = ")")
            }
        }

    suspend fun prepareQQuery(
        query: String?,
        expandedEntity: ExpandedEntity,
        contexts: List<String>
    ): String? =
        if (query == null) null
        else buildQQuery(query, contexts, expandedEntity)

    suspend fun prepareScopeQQuery(
        scopeQ: String?,
        expandedEntity: ExpandedEntity
    ): String? =
        if (scopeQ == null) null
        else buildScopeQQuery(scopeQ, expandedEntity)

    suspend fun prepareGeoQuery(
        geoQ: GeoQ?,
        expandedEntity: ExpandedEntity
    ): String? =
        if (geoQ == null) null
        else buildGeoQuery(
            GeoQuery(
                georel = geoQ.georel,
                geometry = GeoQuery.GeometryType.forType(geoQ.geometry)!!,
                coordinates = geoQ.coordinates,
                geoproperty = geoQ.geoproperty,
                wktCoordinates = WKTCoordinates(geoQ.pgisGeometry!!)
            ),
            expandedEntity
        )

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
            notificationTrigger = toList(row["notification_trigger"]!!),
            timeInterval = toNullableInt(row["time_interval"]),
            q = row["q"] as? String,
            entities = rowToEntityInfo(row)?.let { setOf(it) },
            geoQ = rowToGeoQ(row),
            scopeQ = row["scope_q"] as? String,
            notification = NotificationParams(
                attributes = (row["notif_attributes"] as? String)?.split(","),
                format = toEnum(row["notif_format"]!!),
                endpoint = Endpoint(
                    uri = toUri(row["endpoint_uri"]),
                    accept = toEnum(row["endpoint_accept"]!!),
                    receiverInfo = parseEndpointInfo(toJsonString(row["endpoint_receiver_info"])),
                    notifierInfo = parseEndpointInfo(toJsonString(row["endpoint_notifier_info"]))
                ),
                status = toOptionalEnum<NotificationParams.StatusType>(row["status"]),
                timesSent = row["times_sent"] as Int,
                lastNotification = toNullableZonedDateTime(row["last_notification"]),
                lastFailure = toNullableZonedDateTime(row["last_failure"]),
                lastSuccess = toNullableZonedDateTime(row["last_success"]),
                sysAttrs = row["sys_attrs"] as Boolean
            ),
            isActive = toBoolean(row["is_active"]),
            contexts = toList(row["contexts"]!!),
            throttling = toNullableInt(row["throttling"]),
            lang = row["lang"] as? String,
            datasetId = toNullableList(row["datasetId"]),
            jsonldContext = toNullableUri(row["jsonld_context"])
        )
    }

    private val rowToMinimalMatchSubscription: ((Map<String, Any>) -> Subscription) = { row ->
        Subscription(
            id = toUri(row["sub_id"]),
            type = row["sub_type"] as String,
            subscriptionName = row["subscription_name"] as? String,
            description = row["description"] as? String,
            q = row["q"] as? String,
            scopeQ = row["scope_q"] as? String,
            entities = rowToEntityInfo(row)?.let { setOf(it) },
            geoQ = rowToGeoQ(row),
            notification = NotificationParams(
                attributes = (row["notif_attributes"] as? String)?.split(","),
                format = toEnum(row["notif_format"]!!),
                endpoint = Endpoint(
                    uri = toUri(row["endpoint_uri"]),
                    accept = toEnum(row["endpoint_accept"]!!),
                    receiverInfo = parseEndpointInfo(toJsonString(row["endpoint_receiver_info"])),
                    notifierInfo = parseEndpointInfo(toJsonString(row["endpoint_notifier_info"]))
                ),
                status = null,
                timesSent = row["times_sent"] as Int,
                lastNotification = null,
                lastFailure = null,
                lastSuccess = null,
                sysAttrs = row["sys_attrs"] as Boolean
            ),
            contexts = toList(row["contexts"]!!),
            throttling = toNullableInt(row["throttling"]),
            lang = row["lang"] as? String,
            datasetId = toNullableList(row["datasetId"]),
            jsonldContext = toNullableUri(row["jsonld_context"])
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

    private val rowToEntityInfo: ((Map<String, Any>) -> EntitySelector?) = { row ->
        if (row["type_selection"] != null)
            EntitySelector(
                id = toNullableUri(row["entity_id"]),
                idPattern = row["id_pattern"] as? String,
                typeSelection = row["type_selection"] as String
            )
        else
            null
    }

    suspend fun getRecurringSubscriptionsToNotify(): List<Subscription> {
        val selectStatement =
            """
            SELECT subscription.id as sub_id, subscription.type as sub_type, subscription_name, created_at,
                modified_At, expires_at, description, watched_attributes, notification_trigger, time_interval, q, 
                scope_q, notif_attributes, notif_format, endpoint_uri, endpoint_accept, endpoint_receiver_info,
                endpoint_notifier_info, status, times_sent, last_notification, last_failure, last_success, is_active, 
                entity_selector.id as entity_id, id_pattern, entity_selector.type_selection as type_selection, georel,
                geometry, coordinates, pgis_geometry, geoproperty, contexts, throttling, sys_attrs, lang, 
                datasetId, jsonld_context
            FROM subscription
            LEFT JOIN entity_selector ON entity_selector.subscription_id = subscription.id
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
            .mergeEntitySelectorsOnSubscriptions()
    }
}
