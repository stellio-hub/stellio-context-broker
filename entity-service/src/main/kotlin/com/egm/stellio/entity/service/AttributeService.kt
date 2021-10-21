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
            attributeList = neo4jRepository.getAttribute().map { compactTerm(it, contexts) }
        )

    fun getAttributeDetails(contexts: List<String>): List<AttributeDetails> =
        neo4jRepository.getAttributeDetails().map {
            val attribute = (it["attribute"] as String)
            AttributeDetails(
                id = attribute.toUri(),
                attributeName = compactTerm(attribute, contexts),
                typeNames = (it["typeNames"] as Set<String>).toList().map { compactTerm(it, contexts) }
            )
        }
}
