package com.egm.stellio.search.service

import arrow.core.Either
import arrow.core.left
import arrow.core.right
import com.egm.stellio.search.model.*
import com.egm.stellio.search.util.allToMappedList
import com.egm.stellio.search.util.toInt
import com.egm.stellio.search.util.toUri
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.util.ExpandedTerm
import com.egm.stellio.shared.util.JsonLdUtils.compactTerm
import com.egm.stellio.shared.util.JsonLdUtils.compactTerms
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
            SELECT DISTINCT(unnest(types)) as type
            FROM entity_payload
            ORDER BY type
            """.trimIndent()
        ).allToMappedList { rowToType(it) }

        return EntityTypeList(typeList = compactTerms(entityTypes, contexts))
    }

    suspend fun getEntityTypes(contexts: List<String>): List<EntityType> {
        val result = databaseClient.sql(
            """
            SELECT unnest(types) as type, attribute_name
            FROM entity_payload
            JOIN temporal_entity_attribute ON entity_payload.entity_id = temporal_entity_attribute.entity_id
            ORDER BY type
            """.trimIndent()
        ).allToMappedList { rowToEntityType(it) }.groupBy({ it.first }, { it.second }).toList()

        return result.map {
            EntityType(
                id = toUri(it.first),
                typeName = compactTerm(it.first, contexts),
                attributeNames = compactTerms(it.second, contexts).toSet().sorted()
            )
        }
    }

    suspend fun getEntityTypeInfoByType(
        typeName: ExpandedTerm,
        contexts: List<String>
    ): Either<APIException, EntityTypeInfo> {
        val result = databaseClient.sql(
            """
            WITH entities AS (
                SELECT entity_id 
                FROM entity_payload 
                WHERE :type_name = any (types)
            )    
            SELECT attribute_name, attribute_type, count(distinct(entity_id)) as count_entity
            FROM temporal_entity_attribute
            WHERE entity_id IN (SELECT entity_id FROM entities)
            GROUP BY attribute_name, attribute_type
            """.trimIndent()
        )
            .bind("type_name", typeName)
            .allToMappedList { it }

        if (result.isEmpty())
            return ResourceNotFoundException(typeNotFoundMessage(typeName)).left()

        return EntityTypeInfo(
            id = toUri(typeName),
            typeName = compactTerm(typeName, contexts),
            entityCount = toInt(result.first()["count_entity"]),
            attributeDetails = result.map {
                AttributeInfo(
                    id = toUri(it["attribute_name"]),
                    attributeName = compactTerm(it["attribute_name"] as ExpandedTerm, contexts),
                    attributeTypes = listOf(AttributeType.forKey(it["attribute_type"] as String)).sorted()
                )
            }.sortedBy { it.attributeName }
        ).right()
    }

    private fun rowToType(row: Map<String, Any>): ExpandedTerm =
        row["type"] as ExpandedTerm

    private fun rowToEntityType(row: Map<String, Any>): Pair<ExpandedTerm, ExpandedTerm> {
        val type = row["type"] as ExpandedTerm
        val attributeName = row["attribute_name"] as ExpandedTerm
        return Pair(type, attributeName)
    }
}
