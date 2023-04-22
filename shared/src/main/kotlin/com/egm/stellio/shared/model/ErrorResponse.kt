package com.egm.stellio.shared.model

import java.net.URI

sealed class ErrorResponse(
    val type: URI,
    open val title: String,
    open val detail: String
)

data class InvalidRequestResponse(override val detail: String) : ErrorResponse(
    ErrorType.INVALID_REQUEST.type,
    "The request associated to the operation is syntactically invalid or includes wrong content",
    detail
)

data class BadRequestDataResponse(override val detail: String) : ErrorResponse(
    ErrorType.BAD_REQUEST_DATA.type,
    "The request includes input data which does not meet the requirements of the operation",
    detail
)

data class AlreadyExistsResponse(override val detail: String) :
    ErrorResponse(ErrorType.ALREADY_EXISTS.type, "The referred element already exists", detail)

data class OperationNotSupportedResponse(override val detail: String) :
    ErrorResponse(ErrorType.OPERATION_NOT_SUPPORTED.type, "The operation is not supported", detail)

data class ResourceNotFoundResponse(override val detail: String) :
    ErrorResponse(ErrorType.RESOURCE_NOT_FOUND.type, "The referred resource has not been found", detail)

data class InternalErrorResponse(override val detail: String) :
    ErrorResponse(ErrorType.INTERNAL_ERROR.type, "There has been an error during the operation execution", detail)

data class TooComplexQueryResponse(override val detail: String) : ErrorResponse(
    ErrorType.TOO_COMPLEX_QUERY.type,
    "The query associated to the operation is too complex and cannot be resolved",
    detail
)

data class TooManyResultsResponse(override val detail: String) : ErrorResponse(
    ErrorType.TOO_MANY_RESULTS.type,
    """
    The query associated to the operation is producing so many results that can exhaust client or server resources. 
    It should be made more restrictive
    """,
    detail
)

data class LdContextNotAvailableResponse(override val detail: String) : ErrorResponse(
    ErrorType.LD_CONTEXT_NOT_AVAILABLE.type,
    "A remote JSON-LD @context referenced in a request cannot be retrieved by the NGSI-LD Broker and " +
        "expansion or compaction cannot be performed",
    detail
)

data class JsonLdErrorResponse(override val title: String, override val detail: String) :
    ErrorResponse(
        ErrorType.BAD_REQUEST_DATA.type,
        title,
        detail
    )

data class JsonParseErrorResponse(override val detail: String) : ErrorResponse(
    ErrorType.INVALID_REQUEST.type,
    "The request includes invalid input data, an error occurred during JSON parsing",
    detail
)

data class AccessDeniedResponse(override val detail: String) :
    ErrorResponse(
        ErrorType.ACCESS_DENIED.type,
        "The request tried to access an unauthorized resource",
        detail
    )

data class NotImplementedResponse(override val detail: String) :
    ErrorResponse(
        ErrorType.NOT_IMPLEMENTED.type,
        "The requested functionality is not yet implemented",
        detail
    )

data class UnsupportedMediaTypeResponse(override val detail: String) :
    ErrorResponse(
        ErrorType.UNSUPPORTED_MEDIA_TYPE.type,
        "The content type of the request is not supported",
        detail
    )

data class NotAcceptableResponse(override val detail: String) :
    ErrorResponse(
        ErrorType.NOT_ACCEPTABLE.type,
        "The media type provided in Accept header is not supported",
        detail
    )

data class NonexistentTenantResponse(override val detail: String) :
    ErrorResponse(
        ErrorType.NONEXISTENT_TENANT.type,
        "The addressed tenant does not exist",
        detail
    )

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
