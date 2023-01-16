package com.egm.stellio.search.authorization

import com.egm.stellio.shared.util.AuthContextModel.COMPOUND_AUTHZ_CONTEXT
import com.egm.stellio.shared.util.AuthContextModel.NGSILD_AUTHORIZATION_CONTEXT
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CORE_CONTEXT

fun List<String>.addAuthzContextIfNeeded(): List<String> =
    if (this == listOf(NGSILD_CORE_CONTEXT))
        COMPOUND_AUTHZ_CONTEXT
    else this

fun String.replaceCoreContextByAuthzContext(): String =
    if (this == NGSILD_CORE_CONTEXT)
        NGSILD_AUTHORIZATION_CONTEXT
    else this
