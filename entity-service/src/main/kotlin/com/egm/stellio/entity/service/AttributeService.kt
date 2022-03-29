package com.egm.stellio.entity.service

import com.egm.stellio.entity.model.*
import com.egm.stellio.entity.repository.Neo4jRepository
import com.egm.stellio.shared.util.JsonLdUtils.compactTerm
import com.egm.stellio.shared.util.toUri
import org.springframework.stereotype.Component

@Component
class AttributeService(
    private val neo4jRepository: Neo4jRepository
) {

    fun getAttributeList(contexts: List<String>): AttributeList =
        AttributeList(
            attributeList = neo4jRepository.getAttributes().map { compactTerm(it, contexts) }
        )

    fun getAttributeDetails(contexts: List<String>): List<AttributeDetails> =
        neo4jRepository.getAttributesDetails().map {
            val attribute = it["attribute"] as String
            AttributeDetails(
                id = attribute.toUri(),
                attributeName = compactTerm(attribute, contexts),
                typeNames = (it["typeNames"] as Set<String>).compactElements(contexts)
            )
        }

    fun getAttributeTypeInfo(expandedType: String, context: String): AttributeTypeInfo? {
        val attributesInformation = neo4jRepository.getAttributeInformation(expandedType)
        if (attributesInformation.isEmpty()) return null

        return AttributeTypeInfo(
            id = expandedType.toUri(),
            attributeName = compactTerm(attributesInformation["attributeName"] as String, listOf(context)),
            attributeTypes = attributesInformation["attributeTypes"] as Set<String>,
            typeNames = (attributesInformation["typeNames"] as Set<String>).compactElements(listOf(context)),
            attributeCount = attributesInformation["attributeCount"] as Int
        )
    }

    private fun Set<String>.compactElements(contexts: List<String>): Set<String> =
        this.map { compactTerm(it, contexts) }.toSet()
}
