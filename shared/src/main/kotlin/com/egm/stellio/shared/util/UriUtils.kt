package com.egm.stellio.shared.util

import com.egm.stellio.shared.model.BadRequestDataException
import java.net.URI
import java.net.URISyntaxException

fun String.toUri(): URI =
    try {
        val uri = URI(this)
        if (!uri.isAbsolute)
            throw BadRequestDataException(invalidUriMessage(this))
        uri
    } catch (e: URISyntaxException) {
        throw BadRequestDataException(invalidUriMessage("$this (cause was: $e)"))
    }

fun List<String>.toListOfUri(): List<URI> =
    this.map { it.toUri() }

fun Any.isURI(): Boolean =
    kotlin.runCatching {
        (this as? String)?.toUri()
    }.fold(
        { it != null },
        { false }
    )
