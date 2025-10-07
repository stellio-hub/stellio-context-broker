package com.egm.stellio.subscription.service

import arrow.core.Either
import arrow.core.raise.either
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.EntitySelector
import com.egm.stellio.shared.model.ExpandedEntity
import com.egm.stellio.shared.model.ExpandedTerm
import com.egm.stellio.shared.model.NGSILD_LOCATION_IRI
import com.egm.stellio.shared.model.WKTCoordinates
import com.egm.stellio.shared.queryparameter.GeoQuery
import com.egm.stellio.shared.queryparameter.GeoQuery.Companion.parseGeoQueryParameters
import com.egm.stellio.shared.util.DataTypes.serialize
import com.egm.stellio.shared.util.Sub
import com.egm.stellio.shared.util.buildContextLinkHeader
import com.egm.stellio.shared.util.buildQQuery
import com.egm.stellio.shared.util.buildScopeQQuery
import com.egm.stellio.shared.util.buildTypeQuery
import com.egm.stellio.shared.util.decode
import com.egm.stellio.shared.util.ngsiLdDateTime
import com.egm.stellio.subscription.config.SubscriptionProperties
import com.egm.stellio.subscription.model.Endpoint
import com.egm.stellio.subscription.model.Endpoint.Companion.deserialize
import com.egm.stellio.subscription.model.GeoQ
import com.egm.stellio.subscription.model.Notification
import com.egm.stellio.subscription.model.NotificationParams
import com.egm.stellio.subscription.model.NotificationParams.JoinType
import com.egm.stellio.subscription.model.NotificationTrigger
import com.egm.stellio.subscription.model.Subscription
import com.egm.stellio.subscription.model.mergeEntitySelectorsOnSubscriptions
import com.egm.stellio.subscription.utils.allToMappedList
import com.egm.stellio.subscription.utils.execute
import com.egm.stellio.subscription.utils.oneToResult
import com.egm.stellio.subscription.utils.toBoolean
import com.egm.stellio.subscription.utils.toEnum
import com.egm.stellio.subscription.utils.toInt
import com.egm.stellio.subscription.utils.toJsonString
import com.egm.stellio.subscription.utils.toList
import com.egm.stellio.subscription.utils.toNullableInt
import com.egm.stellio.subscription.utils.toNullableList
import com.egm.stellio.subscription.utils.toNullableSet
import com.egm.stellio.subscription.utils.toNullableUri
import com.egm.stellio.subscription.utils.toNullableZonedDateTime
import com.egm.stellio.subscription.utils.toOptionalEnum
import com.egm.stellio.subscription.utils.toUri
import com.egm.stellio.subscription.utils.toZonedDateTime
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

@Component
class SubscriptionService(
    private val subscriptionProperties: SubscriptionProperties,
    private val databaseClient: DatabaseClient,
    private val r2dbcEntityTemplate: R2dbcEntityTemplate
) {

    @Transactional
    suspend fun upsert(subscription: Subscription, sub: Sub): Either<APIException, Unit> = either {
        val endpoint = subscription.notification.endpoint
        val insertStatement =
            """
            INSERT INTO subscription(id, type, subscription_name, created_at, modified_at, description,
                watched_attributes, notification_trigger, time_interval, q, scope_q, notif_attributes,
                notif_format, endpoint_uri, endpoint_accept, endpoint_receiver_info, endpoint_notifier_info,
                times_sent, is_active, expires_at, sub, contexts, throttling, sys_attrs, lang, datasetId,
                jsonld_context, join_type, join_level, show_changes, pick, omit)
            VALUES(:id, :type, :subscription_name, :created_at, :modified_at, :description,
                :watched_attributes, :notification_trigger, :time_interval, :q, :scope_q, :notif_attributes,
                :notif_format, :endpoint_uri, :endpoint_accept, :endpoint_receiver_info, :endpoint_notifier_info,
                :times_sent, :is_active, :expires_at, :sub, :contexts, :throttling, :sys_attrs, :lang, :datasetId,
                :jsonld_context, :join_type, :join_level, :show_changes, :pick, :omit)
            ON CONFLICT (id)
                DO UPDATE SET subscription_name = :subscription_name, modified_at = :modified_at, 
                    description = :description, watched_attributes = :watched_attributes, 
                    notification_trigger = :notification_trigger, time_interval = :time_interval, q = :q, 
                    scope_q = :scope_q, notif_attributes = :notif_attributes, notif_format = :notif_format, 
                    endpoint_uri = :endpoint_uri, endpoint_accept = :endpoint_accept, 
                    endpoint_receiver_info = :endpoint_receiver_info, times_sent = :times_sent, is_active = :is_active,
                    expires_at = :expires_at, sub = :sub, contexts = :contexts, throttling = :throttling,
                    sys_attrs = :sys_attrs, lang = :lang, datasetId = :datasetId, jsonld_context = :jsonld_context,
                    join_type = :join_type, join_level = :join_level, show_changes = :show_changes,
                    pick = :pick, omit = :omit
            """.trimIndent()

        databaseClient.sql(insertStatement)
            .bind("id", subscription.id)
            .bind("type", subscription.type)
            .bind("subscription_name", subscription.subscriptionName)
            .bind("created_at", subscription.createdAt)
            .bind("modified_at", subscription.modifiedAt)
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
            .bind("endpoint_receiver_info", Json.of(serialize(endpoint.receiverInfo)))
            .bind("endpoint_notifier_info", Json.of(serialize(endpoint.notifierInfo)))
            .bind("times_sent", subscription.notification.timesSent)
            .bind("is_active", subscription.isActive)
            .bind("expires_at", subscription.expiresAt)
            .bind("sub", sub.orEmpty())
            .bind("contexts", subscription.contexts.toTypedArray())
            .bind("throttling", subscription.throttling)
            .bind("sys_attrs", subscription.notification.sysAttrs)
            .bind("lang", subscription.lang)
            .bind("datasetId", subscription.datasetId?.toTypedArray())
            .bind("jsonld_context", subscription.jsonldContext)
            .bind("join_type", subscription.notification.join?.name)
            .bind("join_level", subscription.notification.joinLevel)
            .bind("show_changes", subscription.notification.showChanges)
            .bind("pick", subscription.notification.pick?.toTypedArray())
            .bind("omit", subscription.notification.omit?.toTypedArray())
            .execute().bind()

        subscription.geoQ?.let { geoQ ->
            val geoQuery = parseGeoQueryParameters(geoQ.toMap(), subscription.contexts).bind()
            geoQuery?.let { upsertGeometryQuery(it, subscription.id).bind() }
        }

        subscription.entities?.let {
            upsertEntitiesSelector(subscription.id, it).bind()
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

    private suspend fun upsertEntitiesSelector(
        subscriptionId: URI,
        entitiesSelector: Set<EntitySelector>
    ): Either<APIException, Unit> = either {
        deleteEntitySelector(subscriptionId).bind()
        entitiesSelector.forEach {
            createEntitySelector(it, subscriptionId).bind()
        }
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
                datasetId, jsonld_context, join_type, join_level, show_changes, pick, omit
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
            subscription.jsonldContext != null -> subscription.jsonldContext.toString()
            subscription.contexts.size > 1 -> {
                val linkToRetrieveContexts = subscriptionProperties.stellioUrl +
                    "/ngsi-ld/v1/subscriptions/${subscription.id}/context"
                linkToRetrieveContexts
            }
            else -> subscription.contexts[0]
        }
        return buildContextLinkHeader(contextLink)
    }

    suspend fun isCreatorOf(subscriptionId: URI, sub: Sub): Either<APIException, Boolean> {
        val selectStatement =
            """
            SELECT sub
            FROM subscription
            WHERE id = :id
            """.trimIndent()

        return databaseClient.sql(selectStatement)
            .bind("id", subscriptionId)
            .oneToResult {
                it["sub"] == sub.orEmpty()
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

    suspend fun getSubscriptions(limit: Int, offset: Int, sub: Sub): List<Subscription> {
        val selectStatement =
            """
            SELECT subscription.id as sub_id, subscription.type as sub_type, subscription_name, created_at, 
                modified_At, description, watched_attributes, notification_trigger, time_interval, q, notif_attributes, 
                notif_format, endpoint_uri, endpoint_accept, endpoint_receiver_info, endpoint_notifier_info, status, 
                times_sent, is_active, last_notification, last_failure, last_success, entity_selector.id as entity_id,
                id_pattern, entity_selector.type_selection as type_selection, georel, geometry, coordinates, 
                pgis_geometry, geoproperty, scope_q, expires_at, contexts, throttling, sys_attrs, lang, 
                datasetId, jsonld_context, join_type, join_level, show_changes, pick, omit
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
            .bind("sub", sub.orEmpty())
            .allToMappedList { rowToSubscription(it) }
            .mergeEntitySelectorsOnSubscriptions()
    }

    suspend fun getSubscriptionsCount(sub: Sub?): Either<APIException, Int> {
        val selectStatement =
            """
            SELECT count(*)
            FROM subscription
            WHERE subscription.sub = :sub
            """.trimIndent()
        return databaseClient.sql(selectStatement)
            .bind("sub", sub.orEmpty())
            .oneToResult { toInt(it["count"]) }
    }

    internal suspend fun getMatchingSubscriptions(
        updatedAttribute: Pair<ExpandedTerm, URI?>?,
        notificationTrigger: NotificationTrigger
    ): List<Subscription> {
        val selectStatement =
            """
            SELECT subscription.id as sub_id, subscription.type as sub_type, subscription_name, description, q,
                   entity_selector.id as entity_id, entity_selector.id_pattern as id_pattern, 
                   entity_selector.type_selection as type_selection, georel, geometry, coordinates, pgis_geometry,
                   geoproperty, scope_q, notif_attributes, notif_format, endpoint_uri, endpoint_accept, times_sent, 
                   endpoint_receiver_info, endpoint_notifier_info, contexts, throttling, sys_attrs, lang, 
                   datasetId, jsonld_context, join_type, join_level, show_changes, pick, omit
            FROM subscription 
            LEFT JOIN entity_selector on subscription.id = entity_selector.subscription_id
            LEFT JOIN geometry_query on subscription.id = geometry_query.subscription_id
            WHERE is_active
            AND ( expires_at is null OR expires_at >= :date )
            AND time_interval IS NULL
            AND ( throttling IS NULL 
                OR (last_notification + throttling * INTERVAL '1 second') < :date
                OR last_notification IS NULL)
            ${
                if (updatedAttribute != null)
                    "AND (string_to_array(watched_attributes, ',') && '{ ${updatedAttribute.first} }'" +
                        "  OR watched_attributes IS NULL)"
                else ""
            }
            AND CASE
                WHEN notification_trigger && '{ entityUpdated }'
                    THEN notification_trigger || '{ ${NotificationTrigger.expandEntityUpdated()} }' && '{ ${notificationTrigger.notificationTrigger} }'
                ELSE notification_trigger && '{ ${notificationTrigger.notificationTrigger} }'
            END
            """.trimIndent()
        return databaseClient.sql(selectStatement)
            .bind("date", ngsiLdDateTime())
            .allToMappedList { rowToMinimalMatchSubscription(it) }
            .mergeEntitySelectorsOnSubscriptions()
    }

    suspend fun getMatchingSubscriptions(
        expandedEntity: ExpandedEntity,
        updatedAttribute: Pair<ExpandedTerm, URI?>?,
        notificationTrigger: NotificationTrigger
    ): Either<APIException, List<Subscription>> = either {
        getMatchingSubscriptions(updatedAttribute, notificationTrigger)
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
                val typeSelectionQuery = buildTypeQuery(it.typeSelection, target = entityTypes)
                val idQuery = it.id?.let { " '${expandedEntity.id}' = '$it' " }
                val idPatternQuery = it.idPattern?.let { " '${expandedEntity.id}' ~ '$it' " }
                listOfNotNull(typeSelectionQuery, idQuery, idPatternQuery)
                    .joinToString(separator = " AND ", prefix = "(", postfix = ")")
            }
        }

    suspend fun prepareQQuery(
        query: String?,
        expandedEntity: ExpandedEntity,
        contexts: List<String>
    ): String? =
        query?.let { buildQQuery(query, contexts, expandedEntity) }

    suspend fun prepareScopeQQuery(
        scopeQ: String?,
        expandedEntity: ExpandedEntity
    ): String? =
        scopeQ?.let { buildScopeQQuery(scopeQ, expandedEntity) }

    suspend fun prepareGeoQuery(
        geoQ: GeoQ?,
        expandedEntity: ExpandedEntity
    ): String? =
        geoQ?.let {
            GeoQuery(
                georel = geoQ.georel,
                geometry = GeoQuery.GeometryType.forType(geoQ.geometry)!!,
                coordinates = geoQ.coordinates,
                geoproperty = geoQ.geoproperty,
                wktCoordinates = WKTCoordinates(geoQ.pgisGeometry!!)
            ).buildSqlFilter(expandedEntity)
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
            modifiedAt = toZonedDateTime(row["modified_at"]),
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
                pick = toNullableSet(row["pick"]),
                omit = toNullableSet(row["omit"]),
                format = toEnum(row["notif_format"]!!),
                endpoint = Endpoint(
                    uri = toUri(row["endpoint_uri"]),
                    accept = toEnum(row["endpoint_accept"]!!),
                    receiverInfo = deserialize(toJsonString(row["endpoint_receiver_info"])),
                    notifierInfo = deserialize(toJsonString(row["endpoint_notifier_info"]))
                ),
                status = toOptionalEnum<NotificationParams.StatusType>(row["status"]),
                timesSent = row["times_sent"] as Int,
                lastNotification = toNullableZonedDateTime(row["last_notification"]),
                lastFailure = toNullableZonedDateTime(row["last_failure"]),
                lastSuccess = toNullableZonedDateTime(row["last_success"]),
                sysAttrs = row["sys_attrs"] as Boolean,
                join = toOptionalEnum<JoinType>(row["join_type"]),
                joinLevel = row["join_level"] as? Int,
                showChanges = row["show_changes"] as Boolean
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
                pick = toNullableSet(row["pick"]),
                omit = toNullableSet(row["omit"]),
                format = toEnum(row["notif_format"]!!),
                endpoint = Endpoint(
                    uri = toUri(row["endpoint_uri"]),
                    accept = toEnum(row["endpoint_accept"]!!),
                    receiverInfo = deserialize(toJsonString(row["endpoint_receiver_info"])),
                    notifierInfo = deserialize(toJsonString(row["endpoint_notifier_info"]))
                ),
                status = null,
                timesSent = row["times_sent"] as Int,
                lastNotification = null,
                lastFailure = null,
                lastSuccess = null,
                sysAttrs = row["sys_attrs"] as Boolean,
                join = toOptionalEnum<JoinType>(row["join_type"]),
                joinLevel = row["join_level"] as? Int,
                showChanges = row["show_changes"] as Boolean
            ),
            contexts = toList(row["contexts"]!!),
            throttling = toNullableInt(row["throttling"]),
            lang = row["lang"] as? String,
            datasetId = toNullableList(row["datasetId"]),
            jsonldContext = toNullableUri(row["jsonld_context"])
        )
    }

    private val rowToGeoQ: ((Map<String, Any>) -> GeoQ?) = { row ->
        row["georel"]?.let {
            GeoQ(
                georel = row["georel"] as String,
                geometry = row["geometry"] as String,
                coordinates = row["coordinates"] as String,
                pgisGeometry = (row["pgis_geometry"] as Geometry).toText(),
                geoproperty = row["geoproperty"] as? ExpandedTerm ?: NGSILD_LOCATION_IRI
            )
        }
    }

    private val rowToEntityInfo: ((Map<String, Any>) -> EntitySelector?) = { row ->
        row["type_selection"]?.let {
            EntitySelector(
                id = toNullableUri(row["entity_id"]),
                idPattern = row["id_pattern"] as? String,
                typeSelection = row["type_selection"] as String
            )
        }
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
                datasetId, jsonld_context, join_type, join_level, show_changes, pick, omit
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
