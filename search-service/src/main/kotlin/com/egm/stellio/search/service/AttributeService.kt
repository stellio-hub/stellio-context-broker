package com.egm.stellio.search.service

import arrow.core.Either
import arrow.core.flatten
import arrow.core.left
import arrow.core.right
import com.egm.stellio.search.model.AttributeDetails
import com.egm.stellio.search.model.AttributeList
import com.egm.stellio.search.model.AttributeType
import com.egm.stellio.search.model.AttributeTypeInfo
import com.egm.stellio.search.util.allToMappedList
import com.egm.stellio.search.util.toInt
import com.egm.stellio.search.util.toList
import com.egm.stellio.search.util.toUri
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.util.ExpandedTerm
import com.egm.stellio.shared.util.JsonLdUtils.compactTerm
import com.egm.stellio.shared.util.JsonLdUtils.compactTerms
import com.egm.stellio.shared.util.attributeNotFoundMessage
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.stereotype.Component

@Component
class AttributeService(
    private val databaseClient: DatabaseClient
) {
    suspend fun getAttributeList(contexts: List<String>): AttributeList {
        val attributeNames = databaseClient.sql(
            """
            SELECT DISTINCT(attribute_name)
            FROM temporal_entity_attribute
            ORDER BY attribute_name
            """.trimIndent()
        ).allToMappedList { rowToAttributeNames(it) }

        return AttributeList(attributeList = compactTerms(attributeNames, contexts))
    }

    suspend fun getAttributeDetails(contexts: List<String>): List<AttributeDetails> {
        val result = databaseClient.sql(
            """
            SELECT types, attribute_name
            FROM entity_payload
            JOIN temporal_entity_attribute ON entity_payload.entity_id = temporal_entity_attribute.entity_id
            ORDER BY attribute_name
            """.trimIndent()
        ).allToMappedList { rowToAttributeDetails(it) }.flatten().groupBy({ it.second }, { it.first }).toList()

        return result.map {
            AttributeDetails(
                id = toUri(it.first),
                attributeName = compactTerm(it.first, contexts),
                typeNames = compactTerms(it.second, contexts).sorted().toSet()
            )
        }
    }

    suspend fun getAttributeTypeInfoByAttribute(
        attrinuteName: ExpandedTerm,
        contexts: List<String>
    ): Either<APIException, AttributeTypeInfo> {
        val result = databaseClient.sql(
            """
            WITH entities AS (
                SELECT entity_id, attribute_name, attribute_type
                FROM temporal_entity_attribute 
                WHERE  attribute_name = :attribute_name
            )    
            SELECT attribute_name, attribute_type, types, count(distinct(attribute_name)) as attribute_count
            FROM entity_payload
            JOIN entities ON entity_payload.entity_id = entities.entity_id
            GROUP BY types, attribute_name, attribute_type
            """.trimIndent()
        )
            .bind("attribute_name", attrinuteName)
            .allToMappedList { it }

        if (result.isEmpty())
            return ResourceNotFoundException(attributeNotFoundMessage(attrinuteName)).left()

        return AttributeTypeInfo(
            id = toUri(attrinuteName),
            attributeName = compactTerm(attrinuteName, contexts),
            attributeTypes = result.map { AttributeType.forKey(it["attribute_type"] as String) }.sorted().toSet(),
            attributeCount = toInt(result.first()["attribute_count"]),
            typeNames = result.map { compactTerms(toList(it["types"]), contexts) }.flatten().sorted().toSet()
        ).right()
    }

    private fun rowToAttributeNames(row: Map<String, Any>): ExpandedTerm =
        row["attribute_name"] as ExpandedTerm

    private fun rowToAttributeDetails(row: Map<String, Any>): List<Pair<ExpandedTerm, ExpandedTerm>> {
        val types = toList<ExpandedTerm>(row["types"])
        val attributeName = row["attribute_name"] as ExpandedTerm
        return types.map { Pair(it, attributeName) }
    }
}
