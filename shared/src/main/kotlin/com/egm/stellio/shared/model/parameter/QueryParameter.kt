package com.egm.stellio.shared.model.parameter

import java.util.regex.Pattern

typealias QP = QueryParameter

enum class QueryParameter(
    val key: String,
) {
    ID("id"),
    TYPE("type"),
    ID_PATTERN("idPattern"),
    ATTRS("attrs"),
    Q("q"),
    SCOPEQ("scopeQ"),
    GEOMETRY_PROPERTY("geometryProperty"),
    LANG("lang"),
    DATASET_ID("datasetId"),
    CONTAINED_BY("containedBy"),
    JOIN("join"),
    JOIN_LEVEL("joinLevel"),
    OPTIONS("options"),

    // options
    SYS_ATTRS("sysAttrs"),
    KEY_VALUES("keyValues"),
    NO_OVERWRITE("noOverwrite"),
    OBSERVED_AT("observedAt"),

    // geoQuery
    GEOREL("georel"),
    GEOMETRY("geometry"),
    COORDINATES("coordinates"),
    GEOPROPERTY("geoproperty"),

    // temporal
    TIMEREL("timerel"),
    TIMEAT("timeAt"),
    ENDTIMEAT("endTimeAt"),
    AGGRPERIODDURATION("aggrPeriodDuration"),
    AGGRMETHODS("aggrMethods"),
    LASTN("lastN"),
    TIMEPROPERTY("timeproperty"),

    // pagination
    COUNT("count",),
    OFFSET("offset"),
    LIMIT("limit"),

    DELETE_ALL("deleteAll"),

    // not implemented yet
    FORMAT("format"),
    PICK("pick"),
    OMIT("omit"),
    EXPAND_VALUES("expandValues"),
    CSF("csf"),
    ENTITY_MAP("entityMap"),
    DETAILS("details"),

    // 6.3.18 limiting distributed operations
    LOCAL("local"), // 6.3.18
    VIA("Via"); // 6.3.18

    override fun toString(): String {
        return key
    }

    object Query {
        val qPattern: Pattern = Pattern.compile("([^();|]+)")
        val typeSelectionRegex: Regex = """([^(),;|]+)""".toRegex()
        val scopeSelectionRegex: Regex = """([^(),;|]+)""".toRegex()
    }
}
