package com.egm.stellio.search.authorization

import com.egm.stellio.shared.util.AuthContextModel.AUTHORIZATION_COMPOUND_CONTEXT
import com.egm.stellio.shared.util.AuthContextModel.AUTHORIZATION_CONTEXT
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CORE_CONTEXT

fun List<String>.addAuthzContextIfNeeded(): List<String> =
    if (this.size == 1 && this[0] == NGSILD_CORE_CONTEXT)
        this.plus(AUTHORIZATION_CONTEXT)
    else this

fun String.addAuthzContextIfNeeded(): String =
    if (this == NGSILD_CORE_CONTEXT)
        AUTHORIZATION_COMPOUND_CONTEXT
    else this
