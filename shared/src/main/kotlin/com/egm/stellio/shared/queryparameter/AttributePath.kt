package com.egm.stellio.shared.queryparameter

import com.egm.stellio.shared.model.ExpandedTerm
import com.egm.stellio.shared.model.JSONLD_ID_KW
import com.egm.stellio.shared.model.JSONLD_VALUE_KW
import com.egm.stellio.shared.model.NGSILD_CREATED_AT_IRI
import com.egm.stellio.shared.model.NGSILD_DELETED_AT_IRI
import com.egm.stellio.shared.model.NGSILD_MODIFIED_AT_IRI
import com.egm.stellio.shared.model.NGSILD_OBSERVED_AT_IRI
import com.egm.stellio.shared.model.NGSILD_PROPERTY_VALUE
import com.egm.stellio.shared.model.NGSILD_RELATIONSHIP_OBJECT
import com.egm.stellio.shared.util.JsonLdUtils

data class AttributePath(val term: String, val contexts: List<String>) {

    val mainPath: List<ExpandedTerm> = term.substringBefore("[").split(".")
        .map { JsonLdUtils.expandJsonLdTerm(it, contexts) }

    // some should not be expanded based on jsonKeys parameter
    val trailingPath: List<ExpandedTerm> = if (term.contains("["))
        term.substringAfter('[').substringBefore(']').split(".")
            .map { JsonLdUtils.expandJsonLdTerm(it, contexts) }
    else emptyList()

    fun buildJsonBPropertyPath(): String {
        val mainPathString = mainPath.joinToString(".") { "\"$it\"" }
        val trailingPathString = trailingPath.joinToString(".") { "\"$it\"" }
        return when {
            term == "id" -> """$."$JSONLD_ID_KW""""
            term == "type" -> """$."@type""""
            mainPath.last() == NGSILD_MODIFIED_AT_IRI ||
                mainPath.last() == NGSILD_CREATED_AT_IRI ||
                mainPath.last() == NGSILD_DELETED_AT_IRI ||
                mainPath.last() == NGSILD_OBSERVED_AT_IRI -> """$.$mainPathString."$JSONLD_VALUE_KW""""
            mainPath.size > 1 ->
                """$.$mainPathString.**{0 to 2}."$JSONLD_VALUE_KW""""
            trailingPath.isEmpty() ->
                """$."${mainPath[0]}"."$NGSILD_PROPERTY_VALUE"."$JSONLD_VALUE_KW""""
            else ->
                """$."${mainPath[0]}"."$NGSILD_PROPERTY_VALUE".$trailingPathString.**{0 to 1}."$JSONLD_VALUE_KW""""
        }
    }

    fun buildJsonBRelationshipPath(): String {
        val mainPathString = mainPath.joinToString(".") { "\"$it\"" }
        val trailingPathString = mainPath.joinToString(".") { "\"$it\"" }
        return when {
            mainPath.size > 1 ->
                """$.$mainPathString.**{0 to 2}."$JSONLD_ID_KW""""
            trailingPath.isEmpty() ->
                """$."${mainPath[0]}"."$NGSILD_RELATIONSHIP_OBJECT"."$JSONLD_ID_KW""""
            else ->
                """$."${mainPath[0]}"."$NGSILD_RELATIONSHIP_OBJECT".$trailingPathString.**{0 to 1}."$JSONLD_ID_KW""""
        }
    }

    fun buildSqlOrderClause() = """
        jsonb_path_query_array(
            entity_payload.payload,
            '${buildJsonBPropertyPath()}'
        )
    """.trimIndent()
}
