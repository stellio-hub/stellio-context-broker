package com.egm.stellio.shared.queryparameter

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
    FORMAT("format"),
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

    // authz
    ACTION("action"),
    ASSIGNEE("assignee"),
    ASSIGNER("assigner"),
    TARGET_ID("targetId"),
    TARGET_TYPE("targetType"),
    TARGET_SCOPEQ("targetScopeQ"),
    DETAILS_PICK("detailsPick"),

    // pagination
    COUNT("count"),
    OFFSET("offset"),
    LIMIT("limit"),

    DELETE_ALL("deleteAll"),

    // not implemented yet
    PICK("pick"),
    OMIT("omit"),
    EXPAND_VALUES("expandValues"),
    CSF("csf"),
    ENTITY_MAP("entityMap"),
    DETAILS("details"),

    // 6.3.18 limiting distributed operations
    LOCAL("local"),
    VIA("Via");

    override fun toString(): String {
        return key
    }
}
