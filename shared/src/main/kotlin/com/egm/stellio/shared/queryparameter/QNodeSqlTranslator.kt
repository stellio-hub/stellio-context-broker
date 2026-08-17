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
    return jsonbPathExists(targetExpr, jsonPath, attrPath.datasetId)
}

private fun jsonbPathExists(
    targetExpr: String,
    jsonPath: String,
    datasetId: String?,
    vararg parameters: Pair<String, String>
): String {
    val allParameters = buildList {
        datasetId?.let { add("datasetId" to serializeObject(it)) }
        addAll(parameters)
    }
    val escapedPath = jsonPath.escapeSingleQuotes()
    if (allParameters.isEmpty()) return "jsonb_path_exists($targetExpr, '$escapedPath')"

    val jsonParameters = allParameters.joinToString(", ", prefix = "{", postfix = "}") {
        "\"${it.first}\": ${it.second}"
    }.escapeSingleQuotes()
    return "jsonb_path_exists($targetExpr, '$escapedPath', '$jsonParameters')"
}

private fun comparisonSql(
    targetExpr: String,
    attrPath: AttributePath,
    operator: ComparisonOperator,
    value: QValue,
    contexts: List<String>
): String {
    if (attrPath.languageTag != null) {
        return languageTagComparisonSql(targetExpr, attrPath, operator, value)
    }

    if (attrPath.isJsonKeysAttribute && attrPath.trailingPath.isNotEmpty()) {
        return jsonPropertyComparisonSql(targetExpr, attrPath, operator, value)
    }

    return when (value) {
        is RangeValue -> rangeComparisonSql(targetExpr, attrPath, operator, value)
        is ListValue -> listComparisonSql(targetExpr, attrPath, operator, value, contexts)
        is SingleValue -> singleValueComparisonSql(targetExpr, attrPath, operator, value, contexts)
    }
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

    val pattern = value.raw
    return when (operator) {
        ComparisonOperator.LIKE_REGEX ->
            jsonbPathExists(
                targetExpr,
                """$langFilterPath ? (@."$JSONLD_LANGUAGE_KW" == ${"$"}lang && """ +
                    """@."$JSONLD_VALUE_KW" like_regex $pattern)""",
                attrPath.datasetId,
                "lang" to serializeObject(lang)
            )
        ComparisonOperator.NOT_LIKE_REGEX ->
            "NOT (" + jsonbPathExists(
                targetExpr,
                """$langFilterPath ? (@."$JSONLD_LANGUAGE_KW" == ${"$"}lang && """ +
                    """@."$JSONLD_VALUE_KW" like_regex $pattern)""",
                attrPath.datasetId,
                "lang" to serializeObject(lang)
            ) + ")"
        else -> {
            val sqlOp = operator.sqlOp
            val valueExpr = value.toJsonValue()
            jsonbPathExists(
                targetExpr,
                """$langFilterPath ? (@."$JSONLD_LANGUAGE_KW" == ${"$"}lang && """ +
                    """@."$JSONLD_VALUE_KW" $sqlOp ${"$"}value)""",
                attrPath.datasetId,
                "lang" to serializeObject(lang),
                "value" to valueExpr
            )
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
        is SingleValue -> singlePathFilter(targetExpr, jsonPath, operator, value, attrPath.datasetId)
        is RangeValue -> rangeFilter(targetExpr, jsonPath, value, operator, attrPath.datasetId)
        is ListValue -> listFilter(targetExpr, jsonPath, operator, value, attrPath.datasetId)
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
            singlePathFilter(targetExpr, propertyPath, operator, effectiveValue, attrPath.datasetId)
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

    // For NEQ operator, it should be an AND between clauses, but if a path does not exist, PG returns an empty result.
    // So since an attribute name is unique within an entity, it works with an OR.
    return """
        (${singlePathFilter(targetExpr, relPath, operator, uriValue, attrPath.datasetId)} OR
        ${singlePathFilter(targetExpr, propPath, operator, uriValue, attrPath.datasetId)} OR
        ${singlePathFilter(targetExpr, vocabPath, operator, uriValue, attrPath.datasetId)} OR
        ${singlePathFilter(targetExpr, langMapPath, operator, uriValue, attrPath.datasetId)})
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

    // For NEQ operator, it should be an AND between clauses, but if a path does not exist, PG returns an empty result.
    // So since an attribute name is unique within an entity, it works with an OR.
    return """
        (${singlePathFilter(targetExpr, propPath, operator, value, attrPath.datasetId)} OR
            ${singlePathFilter(targetExpr, langMapPath, operator, value, attrPath.datasetId)})
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
    val pattern = value.toJsonValue()
    val propertyMatch = jsonbPathExists(
        targetExpr,
        "$propPath ? (@ like_regex $pattern)",
        attrPath.datasetId
    )
    val languageMatch = jsonbPathExists(
        targetExpr,
        "$langMapPath ? (@ like_regex $pattern)",
        attrPath.datasetId
    )

    return if (operator == ComparisonOperator.NOT_LIKE_REGEX) {
        """
            NOT (($propertyMatch OR
                $languageMatch))
        """
    } else {
        """
            ($propertyMatch OR
                $languageMatch)
        """
    }
}

private fun rangeComparisonSql(
    targetExpr: String,
    attrPath: AttributePath,
    operator: ComparisonOperator,
    value: RangeValue
): String {
    val propertyPath = attrPath.buildJsonBPropertyPath()
    return rangeFilter(targetExpr, propertyPath, value, operator, attrPath.datasetId)
}

private fun listComparisonSql(
    targetExpr: String,
    attrPath: AttributePath,
    operator: ComparisonOperator,
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
        val joinOp = if (operator == ComparisonOperator.NEQ) " AND " else " OR "
        paths.joinToString(joinOp) { path ->
            listFilter(targetExpr, path, operator, effectiveValue, attrPath.datasetId)
        }
    } else {
        listFilter(targetExpr, attrPath.buildJsonBPropertyPath(), operator, value, attrPath.datasetId)
    }
}

private fun singlePathFilter(
    targetExpr: String,
    jsonPath: String,
    operator: ComparisonOperator,
    value: SingleValue,
    datasetId: String?
): String {
    return when {
        value.type == ValueType.BOOLEAN -> {
            val literal = value.raw
            val sqlOp = operator.sqlOp
            jsonbPathExists(targetExpr, "$jsonPath ? (@ $sqlOp $literal)", datasetId)
        }
        operator == ComparisonOperator.LIKE_REGEX -> {
            val pattern = value.toJsonValue()
            jsonbPathExists(targetExpr, "$jsonPath ? (@ like_regex $pattern)", datasetId)
        }
        operator == ComparisonOperator.NOT_LIKE_REGEX -> {
            val pattern = value.toJsonValue()
            "NOT (" + jsonbPathExists(targetExpr, "$jsonPath ? (@ like_regex $pattern)", datasetId) + ")"
        }
        else -> {
            val sqlOp = operator.sqlOp
            jsonbPathExists(
                targetExpr,
                "$jsonPath ? (@ $sqlOp ${"$"}value)",
                datasetId,
                "value" to value.toJsonValue()
            )
        }
    }
}

private fun rangeFilter(
    targetExpr: String,
    jsonPath: String,
    value: RangeValue,
    operator: ComparisonOperator = ComparisonOperator.EQ,
    datasetId: String?
): String {
    val minJson = value.low.toJsonValue()
    val maxJson = value.high.toJsonValue()
    val filter = if (operator == ComparisonOperator.NEQ)
        $$"""@ < $min || @ > $max"""
    else
        $$"""@ >= $min && @ <= $max"""
    return jsonbPathExists(
        targetExpr,
        "$jsonPath ? ($filter)",
        datasetId,
        "min" to minJson,
        "max" to maxJson
    )
}

private fun listFilter(
    targetExpr: String,
    jsonPath: String,
    operator: ComparisonOperator,
    value: ListValue,
    datasetId: String?
): String {
    val (sqlOp, joinOp) =
        if (operator == ComparisonOperator.NEQ) "<>" to " && "
        else "==" to " || "
    val filter = value.items.joinToString(joinOp) { item ->
        "@ $sqlOp ${item.toJsonValue()}"
    }
    return jsonbPathExists(targetExpr, "$jsonPath ? ($filter)", datasetId)
}

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
