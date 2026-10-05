package com.egm.stellio.shared.queryparameter

import com.egm.stellio.shared.model.ExpandedEntity
import com.egm.stellio.shared.model.JSONLD_LANGUAGE_KW
import com.egm.stellio.shared.model.JSONLD_VALUE_KW
import com.egm.stellio.shared.util.JsonLdUtils
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.util.escapeSingleQuotes
import com.egm.stellio.shared.util.isURI

fun QNode.toSqlJsonPath(
    jsonKeys: Set<String> = emptySet(),
    expandValues: Set<String> = emptySet(),
    contexts: List<String>,
    target: ExpandedEntity? = null
): String {
    val targetExpr = if (target == null)
        "entity_payload.payload"
    else
        "'" + serializeObject(target.members).escapeSingleQuotes() + "'"

    return toSql(targetExpr, jsonKeys, expandValues, contexts)
}

private fun QNode.toSql(
    targetExpr: String,
    jsonKeys: Set<String>,
    expandValues: Set<String>,
    contexts: List<String>
): String = when (this) {
    is AndNode -> "(${left.toSql(targetExpr, jsonKeys, expandValues, contexts)}) AND " +
        "(${right.toSql(targetExpr, jsonKeys, expandValues, contexts)})"
    is OrNode -> "(${left.toSql(targetExpr, jsonKeys, expandValues, contexts)}) OR " +
        "(${right.toSql(targetExpr, jsonKeys, expandValues, contexts)})"
    is NotExistsNode -> {
        val attrPath = AttributePath(rawPath, contexts, jsonKeys, expandValues)
        "NOT (${existsSql(targetExpr, attrPath)})"
    }
    is ExistsNode -> {
        val attrPath = AttributePath(rawPath, contexts, jsonKeys, expandValues)
        existsSql(targetExpr, attrPath)
    }
    is ComparisonNode -> {
        val attrPath = AttributePath(rawPath, contexts, jsonKeys, expandValues)
        comparisonSql(targetExpr, attrPath, operator, value, contexts)
    }
}

private fun existsSql(targetExpr: String, attrPath: AttributePath): String {
    val jsonPath = attrPath.buildJsonBExistsPath()
    return "jsonb_path_exists($targetExpr, '$jsonPath')"
}

private fun comparisonSql(
    targetExpr: String,
    attrPath: AttributePath,
    operator: ComparisonOperator,
    value: QValue,
    contexts: List<String>
): String {
    // 4.9 (unequal operator): the target element must exist and meet none of the conditions of the equal operator
    // (so an array target including the value is not unequal, and a value of another data type is unequal)
    if (operator == ComparisonOperator.NEQ) {
        val eqSql = comparisonSql(targetExpr, attrPath, ComparisonOperator.EQ, value, contexts)
        return "(${targetElementExistsSql(targetExpr, attrPath)}) AND NOT ($eqSql)"
    }

    if (attrPath.languageTag != null) {
        return languageTagComparisonSql(targetExpr, attrPath, operator, value)
    }

    if (attrPath.isJsonKeysAttribute && attrPath.trailingPath.isNotEmpty()) {
        return jsonPropertyComparisonSql(targetExpr, attrPath, operator, value)
    }

    return when (value) {
        is RangeValue -> rangeComparisonSql(targetExpr, attrPath, value)
        is ListValue -> listComparisonSql(targetExpr, attrPath, value, contexts)
        is SingleValue -> singleValueComparisonSql(targetExpr, attrPath, operator, value, contexts)
    }
}

private fun targetElementExistsSql(targetExpr: String, attrPath: AttributePath): String =
    when {
        attrPath.languageTag != null ->
            """
                jsonb_path_exists($targetExpr, '${attrPath.buildJsonBLanguageMapFilterPath()} ?
                    (@."$JSONLD_LANGUAGE_KW" == ${"$"}lang)', '{"lang": "${attrPath.languageTag}"}')
            """
        attrPath.isJsonKeysAttribute && attrPath.trailingPath.isNotEmpty() ->
            "jsonb_path_exists($targetExpr, '${attrPath.buildJsonBJsonPropertyPath()}')"
        attrPath.trailingPath.isNotEmpty() ->
            "jsonb_path_exists($targetExpr, '${attrPath.buildJsonBPropertyPath()}')"
        else -> existsSql(targetExpr, attrPath)
    }

private fun languageTagComparisonSql(
    targetExpr: String,
    attrPath: AttributePath,
    operator: ComparisonOperator,
    value: QValue
): String {
    val langFilterPath = attrPath.buildJsonBLanguageMapFilterPath()
    val lang = attrPath.languageTag ?: return "false"

    // LanguageProperty with range or list value is not specified by the NGSI-LD spec
    if (value !is SingleValue) return "false"

    val pattern = value.raw.escapeSingleQuotes()
    val langParams = """{"lang": "$lang"}"""
    return when (operator) {
        ComparisonOperator.LIKE_REGEX ->
            """
                jsonb_path_exists($targetExpr, '$langFilterPath ?
                    (@."$JSONLD_LANGUAGE_KW" == ${"$"}lang && @."$JSONLD_VALUE_KW" like_regex $pattern)', '$langParams')
            """
        ComparisonOperator.NOT_LIKE_REGEX ->
            """
                NOT (jsonb_path_exists($targetExpr, '$langFilterPath ? 
                    (@."$JSONLD_LANGUAGE_KW" == ${"$"}lang && @."$JSONLD_VALUE_KW" like_regex $pattern)', '$langParams'))
            """
        else -> {
            val sqlOp = operator.sqlOp
            val valueExpr = value.toJsonValue()
            val params = """{"lang": "$lang", "value": $valueExpr}"""
            """
                jsonb_path_exists($targetExpr, '$langFilterPath ? 
                    (@."$JSONLD_LANGUAGE_KW" == ${"$"}lang && @."$JSONLD_VALUE_KW" $sqlOp ${"$"}value)', '$params')
            """
        }
    }
}

private fun jsonPropertyComparisonSql(
    targetExpr: String,
    attrPath: AttributePath,
    operator: ComparisonOperator,
    value: QValue
): String {
    val jsonPath = attrPath.buildJsonBJsonPropertyPath()
    return when (value) {
        is SingleValue -> singlePathFilter(targetExpr, jsonPath, operator, value)
        is RangeValue -> rangeFilter(targetExpr, jsonPath, value)
        is ListValue -> listFilter(targetExpr, jsonPath, value)
    }
}

private fun singleValueComparisonSql(
    targetExpr: String,
    attrPath: AttributePath,
    operator: ComparisonOperator,
    value: SingleValue,
    contexts: List<String>
): String {
    val effectiveValue = if (attrPath.isExpandValuesAttribute && value.type == ValueType.STRING) {
        val expanded = JsonLdUtils.expandJsonLdTerm(value.raw.removeSurrounding("\""), contexts)
        SingleValue(expanded, ValueType.URI)
    } else value

    val scalarTypes = setOf(ValueType.NUMBER, ValueType.BOOLEAN, ValueType.DATE, ValueType.DATETIME, ValueType.TIME)
    return when {
        effectiveValue.type == ValueType.URI ||
            effectiveValue.type == ValueType.STRING && attrPath.isExpandValuesAttribute -> {
            uriValueSql(targetExpr, attrPath, operator, effectiveValue)
        }
        effectiveValue.type in scalarTypes -> {
            val propertyPath = attrPath.buildJsonBPropertyPath()
            singlePathFilter(targetExpr, propertyPath, operator, effectiveValue)
        }
        operator == ComparisonOperator.LIKE_REGEX || operator == ComparisonOperator.NOT_LIKE_REGEX -> {
            likeRegexMultiPathSql(targetExpr, attrPath, operator, effectiveValue)
        }
        else -> {
            stringValueSql(targetExpr, attrPath, operator, effectiveValue)
        }
    }
}

private fun uriValueSql(
    targetExpr: String,
    attrPath: AttributePath,
    operator: ComparisonOperator,
    value: SingleValue
): String {
    val preparedValue = if (value.raw.isURI()) value.raw else value.raw.removeSurrounding("\"")
    val uriValue = SingleValue(preparedValue, ValueType.URI)
    val relPath = attrPath.buildJsonBRelationshipPath()
    val propPath = attrPath.buildJsonBPropertyPath()
    val vocabPath = attrPath.buildJsonBVocabPath()
    val langMapPath = attrPath.buildJsonBLanguageMapPath()

    return """
        (${singlePathFilter(targetExpr, relPath, operator, uriValue)} OR
        ${singlePathFilter(targetExpr, propPath, operator, uriValue)} OR
        ${singlePathFilter(targetExpr, vocabPath, operator, uriValue)} OR
        ${singlePathFilter(targetExpr, langMapPath, operator, uriValue)})
    """
}

private fun stringValueSql(
    targetExpr: String,
    attrPath: AttributePath,
    operator: ComparisonOperator,
    value: SingleValue
): String {
    val propPath = attrPath.buildJsonBPropertyPath()
    val langMapPath = attrPath.buildJsonBLanguageMapPath()

    return """
        (${singlePathFilter(targetExpr, propPath, operator, value)} OR
            ${singlePathFilter(targetExpr, langMapPath, operator, value)})
        """
}

private fun likeRegexMultiPathSql(
    targetExpr: String,
    attrPath: AttributePath,
    operator: ComparisonOperator,
    value: SingleValue
): String {
    val propPath = attrPath.buildJsonBPropertyPath()
    val langMapPath = attrPath.buildJsonBLanguageMapPath()
    val pattern = value.toJsonValue().escapeSingleQuotes()

    return if (operator == ComparisonOperator.NOT_LIKE_REGEX) {
        """
            NOT ((jsonb_path_exists($targetExpr, '$propPath ? (@ like_regex $pattern)') OR 
                jsonb_path_exists($targetExpr, '$langMapPath ? (@ like_regex $pattern)')))
        """
    } else {
        """
            (jsonb_path_exists($targetExpr, '$propPath ? (@ like_regex $pattern)') OR
                jsonb_path_exists($targetExpr, '$langMapPath ? (@ like_regex $pattern)'))
        """
    }
}

private fun rangeComparisonSql(
    targetExpr: String,
    attrPath: AttributePath,
    value: RangeValue
): String {
    val propertyPath = attrPath.buildJsonBPropertyPath()
    return rangeFilter(targetExpr, propertyPath, value)
}

private fun listComparisonSql(
    targetExpr: String,
    attrPath: AttributePath,
    value: ListValue,
    contexts: List<String>
): String {
    val valueType = value.items.firstOrNull()?.type ?: ValueType.STRING
    return if (valueType == ValueType.URI || attrPath.isExpandValuesAttribute) {
        val paths = listOf(
            attrPath.buildJsonBRelationshipPath(),
            attrPath.buildJsonBPropertyPath(),
            attrPath.buildJsonBVocabPath()
        )
        val effectiveValue = if (attrPath.isExpandValuesAttribute) {
            ListValue(
                value.items.map { SingleValue(JsonLdUtils.expandJsonLdTerm(it.raw, contexts), ValueType.URI) }
            )
        } else {
            value
        }
        paths.joinToString(" OR ") { path -> listFilter(targetExpr, path, effectiveValue) }
    } else {
        listFilter(targetExpr, attrPath.buildJsonBPropertyPath(), value)
    }
}

private fun singlePathFilter(
    targetExpr: String,
    jsonPath: String,
    operator: ComparisonOperator,
    value: SingleValue
): String {
    return when {
        value.type == ValueType.BOOLEAN -> {
            val literal = value.raw
            val sqlOp = operator.sqlOp
            """jsonb_path_exists($targetExpr, '$jsonPath ? (@ $sqlOp $literal)')"""
        }
        operator == ComparisonOperator.LIKE_REGEX -> {
            val pattern = value.toJsonValue().escapeSingleQuotes()
            """jsonb_path_exists($targetExpr, '$jsonPath ? (@ like_regex $pattern)')"""
        }
        operator == ComparisonOperator.NOT_LIKE_REGEX -> {
            val pattern = value.toJsonValue().escapeSingleQuotes()
            """NOT (jsonb_path_exists($targetExpr, '$jsonPath ? (@ like_regex $pattern)'))"""
        }
        else -> {
            val sqlOp = operator.sqlOp
            val params = value.toJsonParameterizedValue().escapeSingleQuotes()
            """jsonb_path_exists($targetExpr, '$jsonPath ? (@ $sqlOp ${"$"}value)', '$params')"""
        }
    }
}

private fun rangeFilter(
    targetExpr: String,
    jsonPath: String,
    value: RangeValue
): String {
    val minJson = value.low.toJsonValue()
    val maxJson = value.high.toJsonValue()
    val params = """{"min": $minJson, "max": $maxJson}"""
    val filter = $$"""@ >= $min && @ <= $max"""
    return """jsonb_path_exists($targetExpr, '$jsonPath ? ($filter)', '${params.escapeSingleQuotes()}')"""
}

private fun listFilter(
    targetExpr: String,
    jsonPath: String,
    value: ListValue
): String {
    val filter = value.items.joinToString(" || ") { item ->
        "@ == ${item.toJsonValue()}"
    }
    return """jsonb_path_exists($targetExpr, '$jsonPath ? (${filter.escapeSingleQuotes()})')"""
}

private fun SingleValue.toJsonParameterizedValue(): String =
    """{"value": ${this.toJsonValue()}}"""

private fun SingleValue.toJsonValue(): String = when (type) {
    ValueType.NUMBER -> raw
    ValueType.BOOLEAN -> raw
    ValueType.STRING, ValueType.DATETIME, ValueType.DATE, ValueType.TIME -> {
        val unquoted = raw.removeSurrounding("\"")
        "\"$unquoted\""
    }
    ValueType.URI -> {
        val unquoted = raw.removeSurrounding("\"")
        "\"$unquoted\""
    }
}
