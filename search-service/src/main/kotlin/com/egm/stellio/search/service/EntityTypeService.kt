package com.egm.stellio.search.service

import arrow.core.Either
import arrow.core.flatten
import arrow.core.left
import arrow.core.right
import com.egm.stellio.search.model.*
import com.egm.stellio.search.util.*
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.ExpandedTerm
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.util.AuthContextModel
import com.egm.stellio.shared.util.JsonLdUtils.compactTerm
import com.egm.stellio.shared.util.JsonLdUtils.compactTerms
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdTerm
import com.egm.stellio.shared.util.typeNotFoundMessage
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.stereotype.Component

@Component
class EntityTypeService(
    private val databaseClient: DatabaseClient
) {

    suspend fun getEntityTypeList(contexts: List<String>): EntityTypeList {
        val entityTypes = databaseClient.sql(
            """
            SELECT DISTINCT(types) FROM entity_payload
            ORDER BY types
            """.trimIndent()
        ).allToMappedList { rowToTypes(it) }.flatten()

        return EntityTypeList(typeList = compactTerms(entityTypes, contexts))
    }

    suspend fun getEntityTypes(contexts: List<String>): List<EntityType> {
        val result = databaseClient.sql(
            """
            SELECT types, attribute_name
            FROM entity_payload
            JOIN temporal_entity_attribute ON entity_payload.entity_id = temporal_entity_attribute.entity_id
            ORDER BY types
            """.trimIndent()
        ).allToMappedList { rowToEntityType(it) }.flatten().groupBy({ it.first }, { it.second }).toList()

        return result.map {
            EntityType(
                id = toUri(it.first),
                typeName = compactTerm(it.first, contexts),
                attributeNames = compactTerms(it.second, contexts).toSet().sorted()
            )
        }
    }

    suspend fun getEntityTypeInfoByType(
        type: String,
        contexts: List<String>
    ): Either<APIException, EntityTypeInfo> {
        val expandedType = expandJsonLdTerm(type, contexts)
        val result = databaseClient.sql(
            """
            WITH entities AS (
                SELECT entity_id 
                FROM entity_payload 
                WHERE :type = any (types)
                ORDER BY entity_id
            )    
            SELECT attribute_name, attribute_type, count(distinct(entity_id)) as count_entity
            FROM temporal_entity_attribute
            WHERE entity_id IN (SELECT entity_id FROM entities)
            GROUP BY attribute_name, attribute_type
            """.trimIndent()
        )
            .bind("type", expandedType)
            .allToMappedList { it }

        if (result.isEmpty())
            return ResourceNotFoundException(typeNotFoundMessage(type)).left()

        return EntityTypeInfo(
            id = toUri(expandedType),
            typeName = compactTerm(expandedType, contexts),
            entityCount = (result.first()["count_entity"] as Long).toInt(),
            attributeDetails = result.map {
                AttributeInfo(
                    id = toUri(it["attribute_name"]),
                    attributeName = compactTerm(it["attribute_name"] as ExpandedTerm, contexts),
                    attributeTypes = listOf(AttributeType.forKey(it["attribute_type"] as String))
                )
            }
        ).right()
    }

    private fun rowToTypes(row: Map<String, Any>): List<ExpandedTerm> =
        toList<ExpandedTerm>(row["types"]).filter { !AuthContextModel.IAM_TYPES.plus("Entity").contains(it) }

    private fun rowToEntityType(row: Map<String, Any>): List<Pair<String, String>> {
        val types =
            toList<ExpandedTerm>(row["types"]).filter { !AuthContextModel.IAM_TYPES.plus("Entity").contains(it) }
        val attributeName = row["attribute_name"] as ExpandedTerm
        return types.map { Pair(it, attributeName) }
    }
}
