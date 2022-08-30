package com.egm.stellio.shared.util

import com.egm.stellio.shared.model.QueryParams
import org.springframework.util.MultiValueMap

fun parseAndCheckParams(
    pagination: Pair<Int, Int>,
    requestParams: MultiValueMap<String, String>,
    contextLink: String
): QueryParams {

    val ids = requestParams.getFirst(QUERY_PARAM_ID)?.split(",").orEmpty().toListOfUri().toSet()
    val types = parseAndExpandRequestParameter(requestParams.getFirst(QUERY_PARAM_TYPE), contextLink)
    val idPattern = requestParams.getFirst(QUERY_PARAM_ID_PATTERN)
    /**
     * Decoding query parameters is not supported by default so a call to a decode function was added query
     * with the right parameters values
     */
    val q = requestParams.getFirst(QUERY_PARAM_FILTER)?.decode()
    val count = requestParams.getFirst(QUERY_PARAM_COUNT)?.toBoolean() ?: false
    val attrs = parseAndExpandRequestParameter(requestParams.getFirst(QUERY_PARAM_ATTRS), contextLink)
    val includeSysAttrs = requestParams.getOrDefault(QUERY_PARAM_OPTIONS, emptyList())
        .contains(QUERY_PARAM_OPTIONS_SYSATTRS_VALUE)
    val useSimplifiedRepresentation = requestParams.getOrDefault(QUERY_PARAM_OPTIONS, emptyList())
        .contains(QUERY_PARAM_OPTIONS_KEYVALUES_VALUE)
    val (offset, limit) = extractAndValidatePaginationParameters(
        requestParams,
        pagination.first,
        pagination.second,
        count
    )

    val geoQuery = parseAndCheckGeoQuery(requestParams, contextLink)

    return QueryParams(
        ids = ids,
        types = types,
        idPattern = idPattern,
        q = q,
        limit = limit,
        offset = offset,
        count = count,
        attrs = attrs,
        includeSysAttrs = includeSysAttrs,
        useSimplifiedRepresentation = useSimplifiedRepresentation,
        geoQuery = geoQuery
    )
}
