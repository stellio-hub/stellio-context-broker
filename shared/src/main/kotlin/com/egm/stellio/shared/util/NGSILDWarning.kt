package com.egm.stellio.shared.util

import org.springframework.http.ResponseEntity.BodyBuilder

// todo i took the name from the spec could also be name APIWarning like ApiException
open class NGSILDWarning(
    override val message: String
) : Exception(message) {
    companion object {
        const val HEADER_NAME = "NGSILD-Warning"
    }
}

data class ResponseIsStaleWarning(override val message: String) : NGSILDWarning(message)
data class RevalidationFailedWarning(override val message: String) : NGSILDWarning(message)
data class MiscellaneousWarning(override val message: String) : NGSILDWarning(message)
data class MiscellaneousPersistentWarning(override val message: String) : NGSILDWarning(message)

fun List<NGSILDWarning>.addToResponse(response: BodyBuilder) {
    if (this.isNotEmpty()) response.header(NGSILDWarning.HEADER_NAME, *this.map { it.message }.toTypedArray())
}
