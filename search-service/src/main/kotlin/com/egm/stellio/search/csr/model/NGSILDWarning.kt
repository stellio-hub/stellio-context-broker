package com.egm.stellio.search.csr.model

import org.springframework.http.ResponseEntity
import java.util.Base64

/**
 * Implements NGSILD-Warning as defined in 6.3.17
 */
open class NGSILDWarning(
    private val code: Int,
    open val message: String,
    open val csr: ContextSourceRegistration
) {
    // follow rfc7234 https://www.rfc-editor.org/rfc/rfc7234.html#section-5.5
    fun getHeaderMessage(): String = "$code ${getWarnAgent()} \"${getWarnText()}\""

    // new line are forbidden in headers
    private fun getWarnText(): String = Base64.getEncoder().encodeToString(message.toByteArray())
    private fun getWarnAgent(): String = csr.registrationName ?: csr.id.toString()

    companion object {
        const val HEADER_NAME = "NGSILD-Warning"
        const val RESPONSE_IS_STALE_WARNING_CODE = 110
        const val REVALIDATION_FAILED_WARNING_CODE = 111
        const val MISCELLANEOUS_WARNING_CODE = 199
        const val MISCELLANEOUS_PERSISTENT_WARNING_CODE = 299
    }
}

data class ResponseIsStaleWarning(
    override val message: String,
    override val csr: ContextSourceRegistration
) : NGSILDWarning(RESPONSE_IS_STALE_WARNING_CODE, message, csr)

data class RevalidationFailedWarning(
    override val message: String,
    override val csr: ContextSourceRegistration
) : NGSILDWarning(REVALIDATION_FAILED_WARNING_CODE, message, csr)

data class MiscellaneousWarning(
    override val message: String,
    override val csr: ContextSourceRegistration
) : NGSILDWarning(MISCELLANEOUS_WARNING_CODE, message, csr)

data class MiscellaneousPersistentWarning(
    override val message: String,
    override val csr: ContextSourceRegistration
) : NGSILDWarning(MISCELLANEOUS_PERSISTENT_WARNING_CODE, message, csr)

fun ResponseEntity<*>.addWarnings(warnings: List<NGSILDWarning>?): ResponseEntity<*> {
    if (!warnings.isNullOrEmpty())
        this.headers.addAll(
            NGSILDWarning.HEADER_NAME,
            warnings.map { it.getHeaderMessage() }
        )
    return this
}
