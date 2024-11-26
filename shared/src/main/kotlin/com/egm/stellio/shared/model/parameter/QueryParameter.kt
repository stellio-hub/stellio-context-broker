package com.egm.stellio.shared.model.parameter

import arrow.core.Either
import arrow.core.left
import arrow.core.right
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.PaginationQuery
import com.egm.stellio.shared.model.TooManyResultsException
import org.springframework.util.MultiValueMap
import java.util.regex.Pattern

sealed interface QueryParameter {
    val key: String
    val implemented: Boolean
}

enum class QueryParam(
    override val key: String,
    override val implemented: Boolean = true,
) : QueryParameter {
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
    OMIT("omit", false); // todo other not implemented parameters

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
        val qPattern: Pattern = Pattern.compile("([^();|]+)")
        val typeSelectionRegex: Regex = """([^(),;|]+)""".toRegex()
        val scopeSelectionRegex: Regex = """([^(),;|]+)""".toRegex()
    }

    companion object {



    }
}
