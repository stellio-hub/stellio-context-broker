package com.egm.stellio.search.authorization.permission.model

import java.net.URI

data class TargetAsset(
    val id: URI,
) {
    fun expand(contexts: List<String>): TargetAsset =
        this

    fun compact(contexts: List<String>): TargetAsset =
        this
}
