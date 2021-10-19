package com.egm.stellio.entity.service

import com.egm.stellio.entity.model.*
import com.egm.stellio.entity.repository.Neo4jRepository
import com.egm.stellio.shared.util.JsonLdUtils.compactTerm
import org.springframework.stereotype.Component

@Component
class AttributeService(
    private val neo4jRepository: Neo4jRepository
) {

    fun getAttributeList(contexts: List<String>): AttributeList =
        AttributeList(
            attributeList = neo4jRepository.getAtrribute().map { compactTerm(it, contexts) }
        )
}
