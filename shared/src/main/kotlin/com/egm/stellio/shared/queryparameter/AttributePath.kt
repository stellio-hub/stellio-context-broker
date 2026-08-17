package com.egm.stellio.shared.queryparameter

import com.egm.stellio.shared.model.ExpandedTerm
import com.egm.stellio.shared.model.JSONLD_ID_KW
import com.egm.stellio.shared.model.JSONLD_VALUE_KW
import com.egm.stellio.shared.model.NGSILD_CREATED_AT_IRI
import com.egm.stellio.shared.model.NGSILD_DATASET_ID_IRI
import com.egm.stellio.shared.model.NGSILD_DELETED_AT_IRI
import com.egm.stellio.shared.model.NGSILD_JSONPROPERTY_JSON
import com.egm.stellio.shared.model.NGSILD_LANGUAGEPROPERTY_LANGUAGEMAP
import com.egm.stellio.shared.model.NGSILD_MODIFIED_AT_IRI
import com.egm.stellio.shared.model.NGSILD_OBSERVED_AT_IRI
import com.egm.stellio.shared.model.NGSILD_PROPERTY_VALUE
import com.egm.stellio.shared.model.NGSILD_RELATIONSHIP_OBJECT
import com.egm.stellio.shared.model.NGSILD_VOCABPROPERTY_VOCAB
import com.egm.stellio.shared.util.JsonLdUtils
import java.util.Locale

data class AttributePath(
    val term: String,
    val contexts: List<String>,
    val jsonKeys: Set<String> = emptySet(),
    val expandValues: Set<String> = emptySet()
) {
    private val datasetIdSelectorStart = term.indexOf("#{")
    private val datasetIdSelectorEnd = term.indexOf('}', datasetIdSelectorStart + 2)
    val rawDatasetId: String? = if (datasetIdSelectorStart >= 0 && datasetIdSelectorEnd > datasetIdSelectorStart)
        term.substring(datasetIdSelectorStart + 2, datasetIdSelectorEnd)
    else
        null
    private val pathTerm = if (rawDatasetId == null)
        term
    else
        term.removeRange(datasetIdSelectorStart, datasetIdSelectorEnd + 1)

    val compactMainAttr: String = pathTerm.substringBefore("[").substringBefore(".").trim()
    val isJsonKeysAttribute: Boolean = compactMainAttr in jsonKeys
    val isExpandValuesAttribute: Boolean = compactMainAttr in expandValues
    val datasetId: ExpandedTerm? = rawDatasetId?.let { JsonLdUtils.expandJsonLdTerm(it, contexts) }

    val mainPath: List<ExpandedTerm> = pathTerm.substringBefore("[").split(".")
        .map { JsonLdUtils.expandJsonLdTerm(it, contexts) }

    val languageTag: String?
    val trailingPath: List<ExpandedTerm>

    init {
        if (pathTerm.contains("[")) {
            val bracketContent = pathTerm.substringAfter('[').substringBefore(']')
            val rawTerms = bracketContent.split(".")
            when {
                isJsonKeysAttribute -> {
                    languageTag = null
                    trailingPath = rawTerms
                }
                rawTerms.size == 1 && isValidLanguageTag(rawTerms[0]) -> {
                    languageTag = rawTerms[0]
                    trailingPath = emptyList()
                }
                else -> {
                    languageTag = null
                    trailingPath = rawTerms.map { JsonLdUtils.expandJsonLdTerm(it, contexts) }
                }
            }
        } else {
            languageTag = null
            trailingPath = emptyList()
        }
    }

    fun buildJsonBPropertyPath(): String {
        val mainPathString = buildJsonBMainPath()
        val trailingPathString = trailingPath.toQuotedJsonPath()
        return when {
            // id and type are only used for entity ordering (not applicable for q queries)
            pathTerm == "id" -> """$."$JSONLD_ID_KW""""
            pathTerm == "type" -> """$."@type""""
            mainPath.last() == NGSILD_MODIFIED_AT_IRI ||
                mainPath.last() == NGSILD_CREATED_AT_IRI ||
                mainPath.last() == NGSILD_DELETED_AT_IRI ||
                mainPath.last() == NGSILD_OBSERVED_AT_IRI -> """$mainPathString."$JSONLD_VALUE_KW""""
            mainPath.size > 1 && trailingPath.isEmpty() ->
                """$mainPathString.**{0 to 2}."$JSONLD_VALUE_KW""""
            mainPath.size > 1 ->
                """$mainPathString.**{0 to 2}.$trailingPathString.**{0 to 1}."$JSONLD_VALUE_KW""""
            trailingPath.isEmpty() ->
                """$mainPathString."$NGSILD_PROPERTY_VALUE"."$JSONLD_VALUE_KW""""
            else ->
                """$mainPathString."$NGSILD_PROPERTY_VALUE".$trailingPathString.**{0 to 1}."$JSONLD_VALUE_KW""""
        }
    }

    fun buildJsonBRelationshipPath(): String {
        val mainPathString = buildJsonBMainPath()
        // datasetId targets a URI value but has no hasObject path and cannot have a trailing path
        return if (mainPath.size > 1 && mainPath.last() != NGSILD_DATASET_ID_IRI)
            """$mainPathString.**{0 to 2}."$NGSILD_RELATIONSHIP_OBJECT"[*]."$JSONLD_ID_KW""""
        else if (mainPath.size > 1)
            """$mainPathString.**{0 to 2}."$JSONLD_ID_KW""""
        else
            """$mainPathString."$NGSILD_RELATIONSHIP_OBJECT"[*]."$JSONLD_ID_KW""""
    }

    fun buildJsonBVocabPath(): String {
        val mainPathString = buildJsonBMainPath()
        return if (mainPath.size > 1)
            """$mainPathString.**{0 to 2}."$NGSILD_VOCABPROPERTY_VOCAB"[*]."$JSONLD_ID_KW""""
        else
            """$mainPathString."$NGSILD_VOCABPROPERTY_VOCAB"[*]."$JSONLD_ID_KW""""
    }

    fun buildJsonBLanguageMapPath(): String {
        val mainPathString = buildJsonBMainPath()
        return if (mainPath.size > 1)
            """$mainPathString.**{0 to 2}."$NGSILD_LANGUAGEPROPERTY_LANGUAGEMAP"[*]."$JSONLD_VALUE_KW""""
        else
            """$mainPathString."$NGSILD_LANGUAGEPROPERTY_LANGUAGEMAP"[*]."$JSONLD_VALUE_KW""""
    }

    fun buildJsonBLanguageMapFilterPath(): String {
        val mainPathString = buildJsonBMainPath()
        return if (mainPath.size > 1)
            """$mainPathString.**{0 to 2}."$NGSILD_LANGUAGEPROPERTY_LANGUAGEMAP"[*]"""
        else
            """$mainPathString."$NGSILD_LANGUAGEPROPERTY_LANGUAGEMAP"[*]"""
    }

    fun buildJsonBJsonPropertyPath(): String {
        val mainPathString = buildJsonBMainPath()
        val keyPath = trailingPath.toQuotedJsonPath()
        return if (mainPath.size > 1)
            """$mainPathString."$NGSILD_JSONPROPERTY_JSON"."$JSONLD_VALUE_KW".$keyPath"""
        else
            """$mainPathString."$NGSILD_JSONPROPERTY_JSON"."$JSONLD_VALUE_KW".$keyPath"""
    }

    fun buildJsonBExistsPath(): String {
        return buildJsonBMainPath()
    }

    fun buildSqlOrderClause() = """
        jsonb_path_query_array(
            entity_payload.payload,
            '${buildJsonBPropertyPath()}'
        )
         ||
        jsonb_path_query_array(
            entity_payload.payload,
            '${buildJsonBRelationshipPath()}'
        )
    """.trimIndent()

    private fun isValidLanguageTag(tag: String): Boolean =
        runCatching {
            Locale.forLanguageTag(tag).isO3Language
        }.fold(
            { it.isNotEmpty() },
            { false }
        )

    private fun buildJsonBMainPath(): String {
        val firstAttributePath = """$."${mainPath.first()}""""
        val selectedAttributePath = if (datasetId == null)
            firstAttributePath
        else
            """$firstAttributePath[*] ? (@."$NGSILD_DATASET_ID_IRI"[*]."$JSONLD_ID_KW" == ${"$"}datasetId)"""

        return if (mainPath.size == 1)
            selectedAttributePath
        else
            "$selectedAttributePath.${mainPath.drop(1).toQuotedJsonPath()}"
    }

    private fun List<ExpandedTerm>.toQuotedJsonPath(): String =
        this.joinToString(".") { "\"$it\"" }
}
