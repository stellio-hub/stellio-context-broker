package com.egm.stellio.shared.util

import com.egm.stellio.shared.model.QueryParams
import org.springframework.util.MultiValueMap

fun parseAndCheckQueryParams(
    pagination: Pair<Int, Int>,
    queryParams: MultiValueMap<String, String>,
    contextLink: String
): QueryParams {

    val ids = queryParams.getFirst(QUERY_PARAM_ID)?.split(",")?.toListOfUri()
    val type = queryParams.getFirst(QUERY_PARAM_TYPE)
    val idPattern = queryParams.getFirst(QUERY_PARAM_ID_PATTERN)
    val q = queryParams.getFirst(QUERY_PARAM_FILTER)
    val count = queryParams.getFirst(QUERY_PARAM_COUNT)?.toBoolean() ?: false
    val attrs = queryParams.getFirst(QUERY_PARAM_ATTRS)
    val expandedAttrs = parseAndExpandRequestParameter(attrs, contextLink)
    val includeSysAttrs = queryParams.getOrDefault(QUERY_PARAM_OPTIONS, emptyList())
        .contains(QUERY_PARAM_OPTIONS_SYSATTRS_VALUE)
    val useSimplifiedRepresentation = queryParams.getOrDefault(QUERY_PARAM_OPTIONS, emptyList())
        .contains(QUERY_PARAM_OPTIONS_KEYVALUES_VALUE)
    val (offset, limit) = extractAndValidatePaginationParameters(
        queryParams,
        pagination.first,
        pagination.second,
        count
    )

    return QueryParams(
        id = ids,
        expandedType = type,
        idPattern = idPattern,
        q = q,
        limit = limit,
        offset = offset,
        count = count,
        expandedAttrs = expandedAttrs,
        includeSysAttrs = includeSysAttrs,
        useSimplifiedRepresentation = useSimplifiedRepresentation
    )
}
