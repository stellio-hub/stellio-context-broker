package com.egm.stellio.shared.util

fun entityNotFoundMessage(entityId: String) = "Entity $entityId was not found"
fun entityOrAttrsNotFoundMessage(
    entityId: String,
    attrs: Set<String>) =
    "Entity $entityId does not exist or it has none of the requested attributes : $attrs"
