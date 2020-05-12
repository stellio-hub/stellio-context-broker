package com.egm.stellio.shared.model

sealed class ErrorResponse(
    val type: String,
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
    "The query associated to the operation is producing so many results that can exhaust client or server resources. It should be made more restrictive",
    detail
)

data class LdContextNotAvailableResponse(override val detail: String) : ErrorResponse(
    ErrorType.LD_CONTEXT_NOT_AVAILABLE.type,
    "A remote JSON-LD @context referenced in a request cannot be retrieved by the NGSI-LD Broker and expansion or compaction cannot be performed",
    detail
)

data class JsonLdErrorResponse(override val title: String, override val detail: String) :
    ErrorResponse(
        ErrorType.BAD_REQUEST_DATA.type,
        title,
        detail
    )

data class JsonParseErrorResponse(override val detail: String) : ErrorResponse(
        ErrorType.BAD_REQUEST_DATA.type,
        "The request includes invalid input data, An error occurred during JSON parsing",
        detail
    )

enum class ErrorType(val type: String) {
    INVALID_REQUEST("https://uri.etsi.org/ngsi-ld/errors/InvalidRequest"),
    BAD_REQUEST_DATA("https://uri.etsi.org/ngsi-ld/errors/BadRequestData"),
    ALREADY_EXISTS("https://uri.etsi.org/ngsi-ld/errors/AlreadyExists"),
    OPERATION_NOT_SUPPORTED("https://uri.etsi.org/ngsi-ld/errors/OperationNotSupported"),
    RESOURCE_NOT_FOUND("https://uri.etsi.org/ngsi-ld/errors/ResourceNotFound"),
    INTERNAL_ERROR("https://uri.etsi.org/ngsi-ld/errors/InternalError"),
    TOO_COMPLEX_QUERY("https://uri.etsi.org/ngsi-ld/errors/TooComplexQuery"),
    TOO_MANY_RESULTS("https://uri.etsi.org/ngsi-ld/errors/TooManyResults"),
    LD_CONTEXT_NOT_AVAILABLE("https://uri.etsi.org/ngsi-ld/errors/LdContextNotAvailable")
}
