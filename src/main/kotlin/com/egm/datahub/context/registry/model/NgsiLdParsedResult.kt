package com.egm.datahub.context.registry.model

import com.egm.datahub.context.registry.service.EntityStatements
import com.egm.datahub.context.registry.service.RelationshipStatements

data class NgsiLdParsedResult(
    val entityType: String,
    val entityUrn: String,
    val entityStatements: EntityStatements,
    val relationshipStatements: RelationshipStatements,
    val ngsiLdPayload: String
)