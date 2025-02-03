package com.egm.stellio.shared.model

import com.apicatalog.jsonld.JsonLdError
import com.apicatalog.jsonld.JsonLdErrorCode
import com.egm.stellio.shared.util.JsonLdUtils.logger
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import com.egm.stellio.shared.util.toUri
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ProblemDetail
import org.springframework.http.ResponseEntity
import java.net.URI

const val DEFAULT_DETAIL = "If you have difficulty identifying the exact cause of the error, " +
    "please check the list of some usual causes on https://stellio.readthedocs.io/en/latest/TROUBLESHOOT.html . " +
    "If the error is still not clear or if you think it is a bug, feel free to open an issue on " +
    "https://github.com/stellio-hub/stellio-context-broker"
const val TYPE_PROPERTY = "type"
const val TITLE_PROPERTY = "title"
const val STATUS_PROPERTY = "status"
const val DETAIL_PROPERTY = "detail"

sealed class APIException(
    open val type: URI,
    open val status: HttpStatus,
    override val message: String,
    open val detail: String = DEFAULT_DETAIL
) : Exception(message) {
    private val logger = LoggerFactory.getLogger(javaClass)

    fun toProblemDetail(): ProblemDetail = ProblemDetail.forStatusAndDetail(status, this.detail).also {
        it.title = this.message
        it.type = this.type
    }
    fun toErrorResponse(): ResponseEntity<ProblemDetail> {
        return toProblemDetail().toErrorResponse()
    }
}

fun ProblemDetail.toErrorResponse(): ResponseEntity<ProblemDetail> {
    logger.info("Returning error ${this.type} (${this.title})")
    return ResponseEntity.status(this.status)
        .contentType(MediaType.APPLICATION_JSON)
        .body(this)
}

data class ContextSourceException(
    override val type: URI,
    override val status: HttpStatus,
    val title: String,
    override val detail: String
) : APIException(
    type = type,
    status = status,
    message = title,
    detail = detail,
) {
    companion object {
        fun fromResponse(response: String): ContextSourceException {
            return kotlin.runCatching {
                val responseMap = response.deserializeAsMap()
                // mandatory
                val type = responseMap[TYPE_PROPERTY].toString().toUri()
                val title = responseMap[TITLE_PROPERTY]!!.toString()

                // optional
                val status = responseMap[STATUS_PROPERTY]?.let { HttpStatus.valueOf(it as Int) }
                val detail = responseMap[DETAIL_PROPERTY]?.toString()

                ContextSourceException(
                    type = type,
                    status = status ?: HttpStatus.BAD_GATEWAY,
                    title = title,
                    detail = detail ?: "no detail provided"
                )
            }.fold({ it }, {
                ContextSourceException(
                    ErrorType.BAD_GATEWAY.type,
                    HttpStatus.BAD_GATEWAY,
                    "ContextSource sent a badly formed error",
                    response
                )
            })
        }
    }
}

data class ConflictException(override val message: String) : APIException(
    ErrorType.CONFLICT.type,
    HttpStatus.CONFLICT,
    message
)

data class GatewayTimeoutException(override val message: String) : APIException(
    ErrorType.GATEWAY_TIMEOUT.type,
    HttpStatus.GATEWAY_TIMEOUT,
    message
)

data class BadGatewayException(override val message: String) : APIException(
    ErrorType.BAD_GATEWAY.type,
    HttpStatus.BAD_GATEWAY,
    message
)

data class AlreadyExistsException(override val message: String) : APIException(
    ErrorType.ALREADY_EXISTS.type,
    HttpStatus.CONFLICT,
    message
)

data class InvalidRequestException(override val message: String) : APIException(
    ErrorType.INVALID_REQUEST.type,
    HttpStatus.BAD_REQUEST,
    message
)

data class BadRequestDataException(override val message: String) : APIException(
    ErrorType.BAD_REQUEST_DATA.type,
    HttpStatus.BAD_REQUEST,
    message
)
data class OperationNotSupportedException(override val message: String) : APIException(
    ErrorType.OPERATION_NOT_SUPPORTED.type,
    HttpStatus.BAD_REQUEST,
    message
)
data class ResourceNotFoundException(override val message: String) : APIException(
    ErrorType.RESOURCE_NOT_FOUND.type,
    HttpStatus.NOT_FOUND,
    message
)
data class InternalErrorException(override val message: String) : APIException(
    ErrorType.INTERNAL_ERROR.type,
    HttpStatus.INTERNAL_SERVER_ERROR,
    message
)

data class TooManyResultsException(override val message: String) : APIException(
    ErrorType.TOO_MANY_RESULTS.type,
    HttpStatus.FORBIDDEN,
    message
)
data class AccessDeniedException(override val message: String) : APIException(
    ErrorType.ACCESS_DENIED.type,
    HttpStatus.FORBIDDEN,
    message
)
data class NotImplementedException(override val message: String) : APIException(
    ErrorType.NOT_IMPLEMENTED.type,
    HttpStatus.NOT_IMPLEMENTED,
    message
)
data class LdContextNotAvailableException(override val message: String) : APIException(
    ErrorType.LD_CONTEXT_NOT_AVAILABLE.type,
    HttpStatus.SERVICE_UNAVAILABLE,
    message
)
data class NonexistentTenantException(override val message: String) : APIException(
    ErrorType.NONEXISTENT_TENANT.type,
    HttpStatus.NOT_FOUND,
    message
)
data class TooComplexQueryException(override val message: String) : APIException(
    ErrorType.TOO_COMPLEX_QUERY.type,
    HttpStatus.FORBIDDEN,
    message
)
data class NotAcceptableException(override val message: String) : APIException(
    ErrorType.NOT_ACCEPTABLE.type,
    HttpStatus.NOT_ACCEPTABLE,
    message
)

data class JsonParseApiException(override val message: String) : APIException(
    ErrorType.INVALID_REQUEST.type,
    HttpStatus.BAD_REQUEST,
    message
)

data class UnsupportedMediaTypeStatusApiException(override val message: String) : APIException(
    ErrorType.UNSUPPORTED_MEDIA_TYPE.type,
    HttpStatus.UNSUPPORTED_MEDIA_TYPE,
    message
)

data class JsonLdErrorApiResponse(override val message: String, override val detail: String) : APIException(
    ErrorType.BAD_REQUEST_DATA.type,
    HttpStatus.BAD_REQUEST,
    message,
    detail
)

fun Throwable.toAPIException(specificMessage: String? = null): APIException =
    when (this) {
        is APIException -> this
        is JsonLdError ->
            if (this.code == JsonLdErrorCode.LOADING_REMOTE_CONTEXT_FAILED ||
                this.code == JsonLdErrorCode.LOADING_DOCUMENT_FAILED
            )
                LdContextNotAvailableException(specificMessage ?: "Unable to load remote context (cause was: $this)")
            else BadRequestDataException("Unexpected error while parsing payload (cause was: $this)")
        else -> BadRequestDataException(specificMessage ?: this.localizedMessage)
    }

enum class ErrorType(val type: URI) {
    INVALID_REQUEST(URI("https://uri.etsi.org/ngsi-ld/errors/InvalidRequest")),
    BAD_REQUEST_DATA(URI("https://uri.etsi.org/ngsi-ld/errors/BadRequestData")),
    ALREADY_EXISTS(URI("https://uri.etsi.org/ngsi-ld/errors/AlreadyExists")),
    CONFLICT(URI("https://uri.etsi.org/ngsi-ld/errors/Conflict")), // defined only in 6.3.17
    BAD_GATEWAY(URI("https://uri.etsi.org/ngsi-ld/errors/BadGateway")), // defined only in 6.3.17
    GATEWAY_TIMEOUT(URI("https://uri.etsi.org/ngsi-ld/errors/GatewayTimeout")), // defined only in 6.3.17
    OPERATION_NOT_SUPPORTED(URI("https://uri.etsi.org/ngsi-ld/errors/OperationNotSupported")),
    RESOURCE_NOT_FOUND(URI("https://uri.etsi.org/ngsi-ld/errors/ResourceNotFound")),
    INTERNAL_ERROR(URI("https://uri.etsi.org/ngsi-ld/errors/InternalError")),
    TOO_COMPLEX_QUERY(URI("https://uri.etsi.org/ngsi-ld/errors/TooComplexQuery")),
    TOO_MANY_RESULTS(URI("https://uri.etsi.org/ngsi-ld/errors/TooManyResults")),
    LD_CONTEXT_NOT_AVAILABLE(URI("https://uri.etsi.org/ngsi-ld/errors/LdContextNotAvailable")),
    ACCESS_DENIED(URI("https://uri.etsi.org/ngsi-ld/errors/AccessDenied")),
    NOT_IMPLEMENTED(URI("https://uri.etsi.org/ngsi-ld/errors/NotImplemented")),
    UNSUPPORTED_MEDIA_TYPE(URI("https://uri.etsi.org/ngsi-ld/errors/UnsupportedMediaType")),
    NOT_ACCEPTABLE(URI("https://uri.etsi.org/ngsi-ld/errors/NotAcceptable")),
    NONEXISTENT_TENANT(URI("https://uri.etsi.org/ngsi-ld/errors/NonexistentTenant"))
}
