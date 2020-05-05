package com.egm.stellio.shared.model

data class ErrorResponse(
    val type: ErrorType,
    val title: String,
    val detail: String
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
    LD_CONTEXT_NOT_AVAILABLE("https://uri.etsi.org/ngsi-ld/errors/LdContextNotAvailable"),
}