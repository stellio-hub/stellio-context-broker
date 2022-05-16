package com.egm.stellio.shared.util

import com.egm.stellio.shared.model.QueryParams
import org.springframework.util.MultiValueMap

fun parseAndCheckParams(
    pagination: Pair<Int, Int>,
    queryParams: MultiValueMap<String, String>,
    contextLink: String
): QueryParams {

    val ids = queryParams.getFirst(QUERY_PARAM_ID)?.split(",")?.toListOfUri()?.toSet()
    val type = parseAndExpandRequestParameter(queryParams.getFirst(QUERY_PARAM_TYPE), contextLink)
    val idPattern = queryParams.getFirst(QUERY_PARAM_ID_PATTERN)
    /**
     * Decoding query parameters is not supported by default so a call to a decode function was added query
     * with the right parameters values
     */
    val q = queryParams.getFirst(QUERY_PARAM_FILTER)?.decode()
    val count = queryParams.getFirst(QUERY_PARAM_COUNT)?.toBoolean() ?: false
    val attrs = parseAndExpandRequestParameter(queryParams.getFirst(QUERY_PARAM_ATTRS), contextLink)
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
        ids = ids,
        type = type,
        idPattern = idPattern,
        q = q,
        limit = limit,
        offset = offset,
        count = count,
        attrs = attrs,
        includeSysAttrs = includeSysAttrs,
        useSimplifiedRepresentation = useSimplifiedRepresentation
    )
}
