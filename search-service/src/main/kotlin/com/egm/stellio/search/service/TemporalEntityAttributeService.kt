package com.egm.stellio.search.service

import arrow.core.*
import com.egm.stellio.search.model.AttributeInstance
import com.egm.stellio.search.model.AttributeMetadata
import com.egm.stellio.search.model.TemporalEntityAttribute
import com.egm.stellio.search.util.valueToDoubleOrNull
import com.egm.stellio.search.util.valueToStringOrNull
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.AuthContextModel.SpecificAccessPolicy
import com.egm.stellio.shared.util.JsonLdUtils.compactTerm
import io.r2dbc.spi.Row
import kotlinx.coroutines.reactive.awaitFirst
import org.slf4j.LoggerFactory
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.data.relational.core.query.Criteria.where
import org.springframework.data.relational.core.query.Query.query
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.r2dbc.core.bind
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.net.URI
import java.util.UUID

@Service
class TemporalEntityAttributeService(
    private val databaseClient: DatabaseClient,
    private val r2dbcEntityTemplate: R2dbcEntityTemplate,
    private val attributeInstanceService: AttributeInstanceService,
    private val entityPayloadService: EntityPayloadService
) {

    private val logger = LoggerFactory.getLogger(javaClass)

    @Transactional
    fun create(temporalEntityAttribute: TemporalEntityAttribute): Mono<Int> =
        databaseClient.sql(
            """
            INSERT INTO temporal_entity_attribute
                (id, entity_id, types, attribute_name, attribute_type, attribute_value_type, dataset_id)
            VALUES (:id, :entity_id, :types, :attribute_name, :attribute_type, :attribute_value_type, :dataset_id)
            """.trimIndent()
        )
            .bind("id", temporalEntityAttribute.id)
            .bind("entity_id", temporalEntityAttribute.entityId)
            .bind("types", temporalEntityAttribute.types.toTypedArray())
            .bind("attribute_name", temporalEntityAttribute.attributeName)
            .bind("attribute_type", temporalEntityAttribute.attributeType.toString())
            .bind("attribute_value_type", temporalEntityAttribute.attributeValueType.toString())
            .bind("dataset_id", temporalEntityAttribute.datasetId)
            .fetch()
            .rowsUpdated()

    fun createEntityTemporalReferences(payload: String, contexts: List<String>, sub: String? = null): Mono<Int> {
        val ngsiLdEntity = JsonLdUtils.expandJsonLdEntity(payload, contexts).toNgsiLdEntity()
        val parsedPayload = JsonUtils.deserializeObject(payload)

        logger.debug("Analyzing create event for entity ${ngsiLdEntity.id}")

        val temporalAttributes = prepareTemporalAttributes(ngsiLdEntity).ifEmpty {
            return Mono.just(0)
        }

        logger.debug("Found ${temporalAttributes.size} supported attributes in entity: ${ngsiLdEntity.id}")

        return Flux.fromIterable(temporalAttributes.asIterable())
            .map {
                val (expandedAttributeName, attributeMetadata) = it
                val temporalEntityAttribute = TemporalEntityAttribute(
                    entityId = ngsiLdEntity.id,
                    types = ngsiLdEntity.types,
                    attributeName = expandedAttributeName,
                    attributeType = attributeMetadata.type,
                    attributeValueType = attributeMetadata.valueType,
                    datasetId = attributeMetadata.datasetId
                )

                val attributeCreatedAtInstance = AttributeInstance(
                    temporalEntityAttribute = temporalEntityAttribute.id,
                    timeProperty = AttributeInstance.TemporalProperty.CREATED_AT,
                    time = attributeMetadata.createdAt,
                    measuredValue = attributeMetadata.measuredValue,
                    value = attributeMetadata.value,
                    payload = extractAttributeInstanceFromCompactedEntity(
                        parsedPayload,
                        compactTerm(expandedAttributeName, contexts),
                        attributeMetadata.datasetId
                    ),
                    sub = sub
                )

                val attributeObservedAtInstance =
                    if (attributeMetadata.observedAt != null)
                        attributeCreatedAtInstance.copy(
                            time = attributeMetadata.observedAt,
                            timeProperty = AttributeInstance.TemporalProperty.OBSERVED_AT
                        )
                    else null

                Pair(temporalEntityAttribute, listOfNotNull(attributeCreatedAtInstance, attributeObservedAtInstance))
            }
            .flatMap {
                val attributeObservedAtMono =
                    if (it.second.size == 2) attributeInstanceService.create(it.second[1])
                    else Mono.just(1)

                create(it.first)
                    .then(attributeInstanceService.create(it.second.first()))
                    .then(attributeObservedAtMono)
            }.then(entityPayloadService.createEntityPayload(ngsiLdEntity.id, payload))
    }

    fun updateSpecificAccessPolicy(entityId: URI, specificAccessPolicy: SpecificAccessPolicy): Mono<Int> =
        databaseClient.sql(
            """
            UPDATE temporal_entity_attribute
            SET specific_access_policy = :specific_access_policy
            WHERE entity_id = :entity_id
            """.trimIndent()
        )
            .bind("entity_id", entityId)
            .bind("specific_access_policy", specificAccessPolicy.toString())
            .fetch()
            .rowsUpdated()

    fun removeSpecificAccessPolicy(entityId: URI): Mono<Int> =
        databaseClient.sql(
            """
            UPDATE temporal_entity_attribute
            SET specific_access_policy = null
            WHERE entity_id = :entity_id
            """.trimIndent()
        )
            .bind("entity_id", entityId)
            .fetch()
            .rowsUpdated()

    fun hasSpecificAccessPolicies(entityId: URI, specificAccessPolicies: List<SpecificAccessPolicy>): Mono<Boolean> =
        databaseClient.sql(
            """
            SELECT count(id) as count
            FROM temporal_entity_attribute
            WHERE entity_id = :entity_id
            AND specific_access_policy IN (:specific_access_policies)
            """.trimIndent()
        )
            .bind("entity_id", entityId)
            .bind("specific_access_policies", specificAccessPolicies.map { it.toString() })
            .map { row ->
                row.get("count", Integer::class.java)!!.toInt()
            }
            .one()
            .map { it > 0 }

    fun deleteTemporalEntityReferences(entityId: URI): Mono<Int> =
        entityPayloadService.deleteEntityPayload(entityId)
            .then(deleteTemporalAttributesOfEntity(entityId))

    fun deleteTemporalAttributesOfEntity(entityId: URI): Mono<Int> =
        r2dbcEntityTemplate.delete(TemporalEntityAttribute::class.java)
            .matching(query(where("entity_id").`is`(entityId)))
            .all()

    fun deleteTemporalAttributeReferences(entityId: URI, attributeName: String, datasetId: URI?): Mono<Int> =
        databaseClient.sql(
            """
            DELETE FROM temporal_entity_attribute WHERE 
                entity_id = :entity_id
                ${if (datasetId != null) "AND dataset_id = :dataset_id" else "AND dataset_id IS NULL"}
                AND attribute_name = :attribute_name
            """.trimIndent()
        )
            .bind("entity_id", entityId)
            .bind("attribute_name", attributeName)
            .let {
                if (datasetId != null) it.bind("dataset_id", datasetId)
                else it
            }
            .fetch()
            .rowsUpdated()

    fun deleteTemporalAttributeAllInstancesReferences(entityId: URI, attributeName: String): Mono<Int> =
        databaseClient.sql(
            """
            DELETE FROM temporal_entity_attribute
            WHERE entity_id = :entity_id
            AND attribute_name = :attribute_name
            """.trimIndent()
        )
            .bind("entity_id", entityId)
            .bind("attribute_name", attributeName)
            .fetch()
            .rowsUpdated()

    private fun prepareTemporalAttributes(ngsiLdEntity: NgsiLdEntity): List<Pair<String, AttributeMetadata>> {
        val temporalAttributes = ngsiLdEntity.attributes
            .flatMapTo(
                arrayListOf()
            ) {
                it.getAttributeInstances().map { instance ->
                    Pair(it.name, toTemporalAttributeMetadata(instance))
                }
            }.filter {
                it.second.isValid
            }.map {
                Pair(it.first, it.second.toEither().orNull()!!)
            }

        return temporalAttributes
    }

    internal fun toTemporalAttributeMetadata(
        ngsiLdAttributeInstance: NgsiLdAttributeInstance
    ): Validated<String, AttributeMetadata> {
        val attributeType = when (ngsiLdAttributeInstance) {
            is NgsiLdPropertyInstance -> TemporalEntityAttribute.AttributeType.Property
            is NgsiLdRelationshipInstance -> TemporalEntityAttribute.AttributeType.Relationship
            else -> return "Unsupported attribute type ${ngsiLdAttributeInstance.javaClass}".invalid()
        }
        val attributeValue = when (ngsiLdAttributeInstance) {
            is NgsiLdPropertyInstance ->
                Pair(
                    valueToStringOrNull(ngsiLdAttributeInstance.value),
                    valueToDoubleOrNull(ngsiLdAttributeInstance.value)
                )
            is NgsiLdRelationshipInstance -> Pair(ngsiLdAttributeInstance.objectId.toString(), null)
            else -> Pair(null, null)
        }
        if (attributeValue == Pair(null, null)) {
            return "Unable to get a value from attribute: $ngsiLdAttributeInstance".invalid()
        }
        val attributeValueType =
            if (attributeValue.second != null) TemporalEntityAttribute.AttributeValueType.MEASURE
            else TemporalEntityAttribute.AttributeValueType.ANY

        return AttributeMetadata(
            measuredValue = attributeValue.second,
            value = attributeValue.first,
            valueType = attributeValueType,
            datasetId = ngsiLdAttributeInstance.datasetId,
            type = attributeType,
            createdAt = ngsiLdAttributeInstance.createdAt!!,
            modifiedAt = null, // only called at temporal entity creation time, modified date can only be null
            observedAt = ngsiLdAttributeInstance.observedAt
        ).valid()
    }

    fun getForEntities(
        queryParams: QueryParams,
        accessRightFilter: () -> String?
    ): Mono<List<TemporalEntityAttribute>> {
        val selectQuery =
            """
                SELECT id, entity_id, types, attribute_name, attribute_type, attribute_value_type, dataset_id
                FROM temporal_entity_attribute            
                WHERE
            """.trimIndent()

        val filterQuery = buildEntitiesQueryFilter(
            queryParams,
            accessRightFilter
        )
        val finalQuery = """
            $selectQuery
            $filterQuery
            ORDER BY entity_id
            limit :limit
            offset :offset
        """.trimIndent()
        return databaseClient
            .sql(finalQuery)
            .bind("limit", queryParams.limit)
            .bind("offset", queryParams.offset)
            .fetch()
            .all()
            .map { rowToTemporalEntityAttribute(it) }
            .collectList()
    }

    fun getCountForEntities(
        queryParams: QueryParams,
        accessRightFilter: () -> String?
    ): Mono<Int> {
        val selectStatement =
            """
            SELECT count(distinct(entity_id)) as count_entity from temporal_entity_attribute
            WHERE
            """.trimIndent()

        val filterQuery = buildEntitiesQueryFilter(
            queryParams,
            accessRightFilter
        )
        return databaseClient
            .sql("$selectStatement $filterQuery")
            .map { row ->
                row.get("count_entity", Integer::class.java)!!.toInt()
            }
            .one()
    }

    fun buildEntitiesQueryFilter(
        queryParams: QueryParams,
        accessRightFilter: () -> String?,
    ): String {
        val formattedIds =
            if (queryParams.ids.isNotEmpty())
                queryParams.ids.joinToString(separator = ",", prefix = "entity_id in(", postfix = ")") { "'$it'" }
            else null
        val formattedTypes =
            if (queryParams.types.isNotEmpty())
                queryParams.types.joinToString(separator = ",", prefix = "types && ARRAY[", postfix = "]") { "'$it'" }
            else null
        val formattedAttrs =
            if (queryParams.attrs.isNotEmpty())
                queryParams.attrs.joinToString(
                    separator = ",",
                    prefix = "attribute_name in (", postfix = ")"
                ) { "'$it'" }
            else null

        return listOfNotNull(formattedIds, formattedTypes, formattedAttrs, accessRightFilter())
            .joinToString(" AND ")
    }

    fun getForEntity(id: URI, attrs: Set<String>): Flux<TemporalEntityAttribute> {
        val selectQuery =
            """
                SELECT id, entity_id, types, attribute_name, attribute_type, attribute_value_type, dataset_id
                FROM temporal_entity_attribute            
                WHERE temporal_entity_attribute.entity_id = :entity_id
            """.trimIndent()

        val expandedAttrsList = attrs.joinToString(",") { "'$it'" }
        val finalQuery =
            if (attrs.isNotEmpty())
                "$selectQuery AND attribute_name in ($expandedAttrsList)"
            else
                selectQuery

        return databaseClient
            .sql(finalQuery)
            .bind("entity_id", id)
            .fetch()
            .all()
            .map { rowToTemporalEntityAttribute(it) }
    }

    fun getFirstForEntity(id: URI): Mono<UUID> {
        val selectQuery =
            """
            SELECT id
            FROM temporal_entity_attribute
            WHERE entity_id = :entity_id
            """.trimIndent()

        return databaseClient
            .sql(selectQuery)
            .bind("entity_id", id)
            .map(rowToId)
            .first()
    }

    fun getForEntityAndAttribute(id: URI, attributeName: String, datasetId: URI? = null): Mono<UUID> {
        val selectQuery =
            """
            SELECT id
            FROM temporal_entity_attribute
            WHERE entity_id = :entity_id
            ${if (datasetId != null) "AND dataset_id = :dataset_id" else ""}
            AND attribute_name = :attribute_name
            """.trimIndent()

        return databaseClient
            .sql(selectQuery)
            .bind("entity_id", id)
            .bind("attribute_name", attributeName)
            .let {
                if (datasetId != null) it.bind("dataset_id", datasetId)
                else it
            }
            .map(rowToId)
            .one()
    }

    private fun rowToTemporalEntityAttribute(row: Map<String, Any>) =
        TemporalEntityAttribute(
            id = row["id"] as UUID,
            entityId = (row["entity_id"] as String).toUri(),
            types = (row["types"] as Array<ExpandedTerm>).toList(),
            attributeName = row["attribute_name"] as ExpandedTerm,
            attributeType = TemporalEntityAttribute.AttributeType.valueOf(row["attribute_type"] as String),
            attributeValueType = TemporalEntityAttribute.AttributeValueType.valueOf(
                row["attribute_value_type"] as String
            ),
            datasetId = (row["dataset_id"] as? String)?.toUri()
        )

    private var rowToId: ((Row) -> UUID) = { row ->
        row.get("id", UUID::class.java)!!
    }

    suspend fun checkEntityAndAttributeExistence(
        entityId: URI,
        entityAttributeName: String
    ): Either<APIException, Unit> {
        val selectQuery =
            """
                select 
                    exists(
                        select 1 
                        from temporal_entity_attribute 
                        where entity_id = :entity_id
                    ) as entityExists,
                    exists(
                        select 1 
                        from temporal_entity_attribute 
                        where entity_id = :entity_id 
                        and attribute_name = :attribute_name
                    ) as attributeNameExists;
            """.trimIndent()

        val result = databaseClient
            .sql(selectQuery)
            .bind("entity_id", entityId)
            .bind("attribute_name", entityAttributeName)
            .fetch()
            .one()
            .awaitFirst()

        return if (result["entityExists"] as Boolean) {
            if (result["attributeNameExists"] as Boolean)
                Unit.right()
            else ResourceNotFoundException(attributeNotFoundMessage(entityAttributeName)).left()
        } else ResourceNotFoundException(entityNotFoundMessage(entityId.toString())).left()
    }
}
