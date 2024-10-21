package com.egm.stellio.shared.util

import org.springframework.http.ResponseEntity.BodyBuilder
import java.util.*

open class NGSILDWarning(
    open val message: String
) {
    // new line are forbidden in headers
    fun getHeaderMessage(): String = Base64.getEncoder().encodeToString(message.toByteArray())

    companion object {
        const val HEADER_NAME = "NGSILD-Warning"
    }
}

data class ResponseIsStaleWarning(override val message: String) : NGSILDWarning(message)
data class RevalidationFailedWarning(override val message: String) : NGSILDWarning(message)
data class MiscellaneousWarning(override val message: String) : NGSILDWarning(message)
data class MiscellaneousPersistentWarning(override val message: String) : NGSILDWarning(message)

fun List<NGSILDWarning>.getHeaderMessages() = this.map { it.getHeaderMessage() }

fun List<NGSILDWarning>.addToResponse(response: BodyBuilder) {
    if (this.isNotEmpty()) response.header(
        NGSILDWarning.HEADER_NAME,
        *this.getHeaderMessages().toTypedArray()
    )
}
