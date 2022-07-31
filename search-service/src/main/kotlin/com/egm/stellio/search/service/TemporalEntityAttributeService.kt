package com.egm.stellio.search.service

import arrow.core.*
import arrow.core.continuations.either
import arrow.fx.coroutines.parTraverseEither
import com.egm.stellio.search.model.AttributeInstance
import com.egm.stellio.search.model.AttributeMetadata
import com.egm.stellio.search.model.TemporalEntityAttribute
import com.egm.stellio.search.util.*
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.AuthContextModel.SpecificAccessPolicy
import com.egm.stellio.shared.util.JsonLdUtils.compactTerm
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdEntity
import com.egm.stellio.shared.util.JsonUtils.deserializeObject
import com.egm.stellio.shared.util.attributeNotFoundMessage
import com.egm.stellio.shared.util.entityNotFoundMessage
import com.egm.stellio.shared.util.extractAttributeInstanceFromCompactedEntity
import com.egm.stellio.shared.util.toUri
import org.slf4j.LoggerFactory
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.data.relational.core.query.Criteria.where
import org.springframework.data.relational.core.query.Query.query
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.r2dbc.core.bind
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
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
    suspend fun create(temporalEntityAttribute: TemporalEntityAttribute): Either<APIException, Unit> =
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
            .execute()

    @Transactional
    suspend fun updateTemporalEntityTypes(entityId: URI, types: List<ExpandedTerm>): Either<APIException, Unit> =
        databaseClient.sql(
            """
            UPDATE temporal_entity_attribute
            SET types = :types
            WHERE entity_id = :entity_id
            """.trimIndent()
        )
            .bind("entity_id", entityId)
            .bind("types", types.toTypedArray())
            .execute()

    @Transactional
    suspend fun createEntityTemporalReferences(
        payload: String,
        contexts: List<String>,
        sub: String? = null
    ): Either<APIException, Unit> =
        createEntityTemporalReferences(
            expandJsonLdEntity(payload, contexts).toNgsiLdEntity(),
            deserializeObject(payload),
            contexts,
            sub
        )

    @Transactional
    suspend fun createEntityTemporalReferences(
        ngsiLdEntity: NgsiLdEntity,
        deserializedPayload: Map<String, Any>,
        contexts: List<String>,
        sub: String? = null
    ): Either<APIException, Unit> {
        logger.debug("Creating entity ${ngsiLdEntity.id}")

        val temporalAttributes = prepareTemporalAttributes(ngsiLdEntity).ifEmpty { return Unit.right() }
        logger.debug("Found ${temporalAttributes.size} supported attributes in entity: ${ngsiLdEntity.id}")

        return temporalAttributes.parTraverseEither {
            either<APIException, Unit> {
                val (expandedAttributeName, attributeMetadata) = it
                val temporalEntityAttribute = TemporalEntityAttribute(
                    entityId = ngsiLdEntity.id,
                    types = ngsiLdEntity.types,
                    attributeName = expandedAttributeName,
                    attributeType = attributeMetadata.type,
                    attributeValueType = attributeMetadata.valueType,
                    datasetId = attributeMetadata.datasetId
                )
                create(temporalEntityAttribute).bind()

                val attributeCreatedAtInstance = AttributeInstance(
                    temporalEntityAttribute = temporalEntityAttribute.id,
                    timeProperty = AttributeInstance.TemporalProperty.CREATED_AT,
                    time = attributeMetadata.createdAt,
                    measuredValue = attributeMetadata.measuredValue,
                    value = attributeMetadata.value,
                    payload = extractAttributeInstanceFromCompactedEntity(
                        deserializedPayload,
                        compactTerm(expandedAttributeName, contexts),
                        attributeMetadata.datasetId
                    ),
                    sub = sub
                )
                attributeInstanceService.create(attributeCreatedAtInstance).bind()

                if (attributeMetadata.observedAt != null) {
                    val attributeObservedAtInstance = attributeCreatedAtInstance.copy(
                        time = attributeMetadata.observedAt,
                        timeProperty = AttributeInstance.TemporalProperty.OBSERVED_AT
                    )
                    attributeInstanceService.create(attributeObservedAtInstance).bind()
                }
            }
        }.map {
            entityPayloadService.createEntityPayload(ngsiLdEntity.id, deserializedPayload)
        }
    }

    suspend fun updateSpecificAccessPolicy(
        entityId: URI,
        specificAccessPolicy: SpecificAccessPolicy
    ): Either<APIException, Unit> =
        databaseClient.sql(
            """
            UPDATE temporal_entity_attribute
            SET specific_access_policy = :specific_access_policy
            WHERE entity_id = :entity_id
            """.trimIndent()
        )
            .bind("entity_id", entityId)
            .bind("specific_access_policy", specificAccessPolicy.toString())
            .execute()

    suspend fun removeSpecificAccessPolicy(entityId: URI): Either<APIException, Unit> =
        databaseClient.sql(
            """
            UPDATE temporal_entity_attribute
            SET specific_access_policy = null
            WHERE entity_id = :entity_id
            """.trimIndent()
        )
            .bind("entity_id", entityId)
            .execute()

    suspend fun hasSpecificAccessPolicies(
        entityId: URI,
        specificAccessPolicies: List<SpecificAccessPolicy>
    ): Either<APIException, Boolean> =
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
            .oneToResult { it["count"] as Long > 0 }

    suspend fun deleteTemporalEntityReferences(entityId: URI): Either<APIException, Unit> =
        either {
            entityPayloadService.deleteEntityPayload(entityId).bind()
            deleteTemporalAttributesOfEntity(entityId).bind()
        }

    suspend fun deleteTemporalAttributesOfEntity(entityId: URI): Either<APIException, Unit> =
        r2dbcEntityTemplate.delete(TemporalEntityAttribute::class.java)
            .matching(query(where("entity_id").`is`(entityId)))
            .execute()

    suspend fun deleteTemporalAttributeReferences(
        entityId: URI,
        attributeName: String,
        datasetId: URI?
    ): Either<APIException, Unit> =
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
            .execute()

    suspend fun deleteTemporalAttributeAllInstancesReferences(
        entityId: URI,
        attributeName: String
    ): Either<APIException, Unit> =
        databaseClient.sql(
            """
            DELETE FROM temporal_entity_attribute
            WHERE entity_id = :entity_id
            AND attribute_name = :attribute_name
            """.trimIndent()
        )
            .bind("entity_id", entityId)
            .bind("attribute_name", attributeName)
            .execute()

    private fun prepareTemporalAttributes(ngsiLdEntity: NgsiLdEntity): List<Pair<String, AttributeMetadata>> =
        ngsiLdEntity.attributes
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

    suspend fun getForEntities(
        queryParams: QueryParams,
        accessRightFilter: () -> String?
    ): List<TemporalEntityAttribute> {
        val filterQuery = buildEntitiesQueryFilter(
            queryParams,
            accessRightFilter
        )

        val filterOnAttributesQuery = buildEntitiesQueryFilter(
            queryParams.copy(ids = emptySet(), idPattern = null, types = emptySet()),
            accessRightFilter,
            " AND "
        )

        val selectQuery =
            """
                WITH entities AS (
                    SELECT DISTINCT(entity_id)
                    FROM temporal_entity_attribute
                    WHERE $filterQuery
                    ORDER BY entity_id
                    LIMIT :limit
                    OFFSET :offset   
                )
                SELECT id, entity_id, types, attribute_name, attribute_type, attribute_value_type, dataset_id
                FROM temporal_entity_attribute            
                WHERE entity_id IN (SELECT entity_id FROM entities) $filterOnAttributesQuery
                ORDER BY entity_id
            """.trimIndent()

        return databaseClient
            .sql(selectQuery)
            .bind("limit", queryParams.limit)
            .bind("offset", queryParams.offset)
            .allToMappedList { rowToTemporalEntityAttribute(it) }
    }

    suspend fun getCountForEntities(
        queryParams: QueryParams,
        accessRightFilter: () -> String?
    ): Either<APIException, Int> {
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
            .oneToResult { it["count_entity"] as Long }
            .map { it.toInt() }
    }

    fun buildEntitiesQueryFilter(
        queryParams: QueryParams,
        accessRightFilter: () -> String?,
        prefix: String = ""
    ): String {
        val formattedIds =
            if (queryParams.ids.isNotEmpty())
                queryParams.ids.joinToString(separator = ",", prefix = "entity_id in(", postfix = ")") { "'$it'" }
            else null
        val formattedIdPattern =
            if (!queryParams.idPattern.isNullOrEmpty())
                "entity_id ~ '${queryParams.idPattern}'"
            else null
        val formattedTypes =
            if (queryParams.types.isNotEmpty())
                queryParams.types.joinToString(separator = ",", prefix = "types && ARRAY[", postfix = "]") { "'$it'" }
            else null
        val formattedAttrs =
            if (queryParams.attrs.isNotEmpty())
                queryParams.attrs.joinToString(
                    separator = ",",
                    prefix = "attribute_name in (",
                    postfix = ")"
                ) { "'$it'" }
            else null

        val queryFilter =
            listOfNotNull(formattedIds, formattedIdPattern, formattedTypes, formattedAttrs, accessRightFilter())

        return if (queryFilter.isNullOrEmpty())
            queryFilter.joinToString(separator = " AND ")
        else
            queryFilter.joinToString(separator = " AND ", prefix = prefix)
    }

    suspend fun getForEntity(id: URI, attrs: Set<String>): List<TemporalEntityAttribute> {
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
            .allToMappedList { rowToTemporalEntityAttribute(it) }
    }

    suspend fun getFirstForEntity(id: URI): Either<APIException, UUID> {
        val selectQuery =
            """
            SELECT id
            FROM temporal_entity_attribute
            WHERE entity_id = :entity_id
            """.trimIndent()

        return databaseClient
            .sql(selectQuery)
            .bind("entity_id", id)
            .oneToResult { it["id"] as UUID }
    }

    suspend fun getForEntityAndAttribute(
        id: URI,
        attributeName: String,
        datasetId: URI? = null
    ): Either<APIException, UUID> {
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
            .oneToResult { it["id"] as UUID }
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

        return databaseClient
            .sql(selectQuery)
            .bind("entity_id", entityId)
            .bind("attribute_name", entityAttributeName)
            .oneToResult { Pair(it["entityExists"] as Boolean, it["attributeNameExists"] as Boolean) }
            .flatMap {
                if (it.first) {
                    if (it.second)
                        Unit.right()
                    else ResourceNotFoundException(attributeNotFoundMessage(entityAttributeName)).left()
                } else ResourceNotFoundException(entityNotFoundMessage(entityId.toString())).left()
            }
    }
}
