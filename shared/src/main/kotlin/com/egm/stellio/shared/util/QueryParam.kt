package com.egm.stellio.shared.util

import com.egm.stellio.shared.util.QueryParam.GeoQuery.GeorelValue
import java.util.regex.Pattern
import kotlin.reflect.KClass

sealed interface Parameter {
    val key: String
    val implemented: Boolean
}

enum class QueryParam(
    override val key: String,
    override val implemented: Boolean = true,
) : Parameter {
    COUNT("count",),
    OFFSET("offset"),
    LIMIT("limit"),
    ID("id"),
    TYPE("type"),
    ID_PATTERN("idPattern"),
    ATTRS("attrs"),
    Q("q"),
    SCOPEQ("scopeQ"),
    GEOMETRY_PROPERTY("geometryProperty"),
    LANG("lang"),
    DATASET_ID("datasetId"),
    OPTIONS("options"),
    PICK("pick", false),
    OMIT("omit", false); // todo other not implemented
    enum class Temporal(
        override val key: String,
        override val implemented: Boolean = true,
    ) : Parameter {
        TIMEREL("timerel"),
        TIMEAT("timeAt"),
        ENDTIMEAT("endTimeAt"),
        AGGRPERIODDURATION("aggrPeriodDuration"),
        AGGRMETHODS("aggrMethods"),
        LASTN("lastN"),
        TIMEPROPERTY("timeproperty");
        companion object {
            const val WHOLE_TIME_RANGE_DURATION = "PT0S"
        }
    }
    enum class GeoQuery(
        override val key: String,
        override val implemented: Boolean = true,
    ) : Parameter {
        GEOREL("georel"),
        GEOMETRY("geometry"),
        COORDINATES("coordinates"),
        GEOPROPERTY("geoproperty");

        enum class GeorelValue(val value: String) {
            NEAR("near"),
            WITHIN("within"),
            CONTAINS("contains"),
            INTERSECTS("intersects"),
            EQUALS("equals"),
            DISJOINT("disjoint"),
            OVERLAPS("overlaps");
            companion object {
                val ALL = GeorelValue.entries.map { it.value }
            }
        }
    }

    enum class OptionValue(val value: String) {
        SYS_ATTRS("sysAttrs"),
        KEY_VALUES("keyValues"),
        NO_OVERWRITE("noOverwrite"),
        OBSERVED_AT("observedAt");
        companion object {
            val ALL = OptionValue.entries.map { it.value }
        }
    }

    object Query {
        val pattern: Pattern = Pattern.compile("([^();|]+)")
        val typeSelectionRegex: Regex = """([^(),;|]+)""".toRegex()
        val scopeSelectionRegex: Regex = """([^(),;|]+)""".toRegex()
    }
}
