package com.egm.stellio.shared.model

import com.apicatalog.jsonld.JsonLdError
import com.apicatalog.jsonld.JsonLdErrorCode
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonProperty
import org.slf4j.LoggerFactory
import org.springframework.core.io.buffer.DefaultDataBufferFactory
import org.springframework.http.HttpHeaders.CONTENT_TYPE
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ProblemDetail
import org.springframework.http.ResponseEntity
import org.springframework.web.server.ServerWebExchange
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.net.URI

const val DEFAULT_DETAIL = "If you have difficulty finding the issue" +
    "you can check common issues here https://stellio.readthedocs.io/en/latest/TROUBLESHOOT.html" +
    "If you think this is a bug of the broker" +
    "you can create an issue on https://github.com/stellio-hub/stellio-context-broker"

sealed class APIException(
    val type: URI,
    @JsonIgnore
    val status: HttpStatus,
    open val title: String,
    @JsonProperty("detail")
    override val message: String = DEFAULT_DETAIL
) : Exception(message) {
    private val logger = LoggerFactory.getLogger(javaClass)

    fun toErrorResponse(): ResponseEntity<ProblemDetail> {
        logger.info("Returning error ${this.type} (${this.message})")
        return ResponseEntity.status(status)
            .contentType(MediaType.APPLICATION_JSON)
            .body(
                ProblemDetail.forStatusAndDetail(status, this.message).also {
                    it.title = this.title
                    it.type = this.type
                }
            )
    }

    fun toServerWebExchange(exchange: ServerWebExchange): Mono<Void> {
        logger.info("Returning server web exchange error ${this.type} (${this.message})")
        exchange.response.statusCode = this.status
        exchange.response.headers[CONTENT_TYPE] = MediaType.APPLICATION_JSON_VALUE
        val body = serializeObject(this)
        return exchange.response.writeWith(
            Flux.just(DefaultDataBufferFactory().wrap(body.toByteArray()))
        )
    }
}

data class AlreadyExistsException(override val message: String) : APIException(
    ErrorType.ALREADY_EXISTS.type,
    HttpStatus.CONFLICT,
    "The referred element already exists",
    message
)

data class InvalidRequestException(override val message: String) : APIException( // todo check
    ErrorType.INVALID_REQUEST.type,
    HttpStatus.BAD_REQUEST,
    "The request associated to the operation is syntactically invalid or includes wrong content",
    message
)

data class BadRequestDataException(override val message: String) : APIException(
    ErrorType.BAD_REQUEST_DATA.type,
    HttpStatus.BAD_REQUEST,
    "The request includes input data which does not meet the requirements of the operation",
    message
)
data class OperationNotSupportedException(override val message: String) : APIException(
    ErrorType.OPERATION_NOT_SUPPORTED.type,
    HttpStatus.BAD_REQUEST,
    "The operation is not supported",
    message
)
data class ResourceNotFoundException(override val message: String) : APIException(
    ErrorType.RESOURCE_NOT_FOUND.type,
    HttpStatus.NOT_FOUND,
    "The referred resource has not been found",
    message
)
data class InternalErrorException(override val message: String) : APIException(
    ErrorType.INTERNAL_ERROR.type,
    HttpStatus.INTERNAL_SERVER_ERROR,
    "There has been an error during the operation execution",
    message
)

data class TooManyResultsException(override val message: String) : APIException(
    ErrorType.TOO_MANY_RESULTS.type,
    HttpStatus.FORBIDDEN,
    "The query associated to the operation is producing so many results " +
        "that can exhaust client or server resources. " +
        "It should be made more restrictive",
    message
)
data class AccessDeniedException(override val message: String) : APIException(
    ErrorType.ACCESS_DENIED.type,
    HttpStatus.FORBIDDEN,
    "The request tried to access an unauthorized resource",
    message
)
data class NotImplementedException(override val message: String) : APIException(
    ErrorType.NOT_IMPLEMENTED.type,
    HttpStatus.NOT_IMPLEMENTED,
    "The requested functionality is not yet implemented",
    message
)
data class LdContextNotAvailableException(override val message: String) : APIException(
    ErrorType.LD_CONTEXT_NOT_AVAILABLE.type,
    HttpStatus.SERVICE_UNAVAILABLE,
    "A remote JSON-LD @context referenced in a request cannot be retrieved by the NGSI-LD Broker and " +
        "expansion or compaction cannot be performed",
    message
)
data class NonexistentTenantException(override val message: String) : APIException(
    ErrorType.NONEXISTENT_TENANT.type,
    HttpStatus.NOT_FOUND,
    "The addressed tenant does not exist",
    message
)
data class TooComplexQueryException(override val message: String) : APIException( // todo check
    ErrorType.TOO_COMPLEX_QUERY.type,
    HttpStatus.INTERNAL_SERVER_ERROR, // todo check
    "The query associated to the operation is too complex and cannot be resolved",
    message
)
data class NotAcceptableException(override val message: String) : APIException(
    ErrorType.NOT_ACCEPTABLE.type,
    HttpStatus.NOT_ACCEPTABLE,
    "The media type provided in Accept header is not supported",
    message
)

data class JsonParseApiException(override val message: String) : APIException(
    ErrorType.INVALID_REQUEST.type,
    HttpStatus.BAD_REQUEST,
    "The request includes invalid input data, an error occurred during JSON parsing",
    message
)

data class UnsupportedMediaTypeStatusApiException(override val message: String) : APIException(
    ErrorType.UNSUPPORTED_MEDIA_TYPE.type,
    HttpStatus.UNSUPPORTED_MEDIA_TYPE,
    "The content type of the request is not supported",
    message
)

data class JsonLdErrorApiResponse(override val title: String, val detail: String) : APIException(
    ErrorType.BAD_REQUEST_DATA.type, // todo check
    HttpStatus.BAD_REQUEST,
    title,
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
