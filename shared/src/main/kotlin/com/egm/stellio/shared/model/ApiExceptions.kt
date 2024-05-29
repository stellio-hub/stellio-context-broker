package com.egm.stellio.shared.model

import com.apicatalog.jsonld.JsonLdError
import com.apicatalog.jsonld.JsonLdErrorCode

sealed class APIException(
    override val message: String
) : Exception(message)

data class InvalidRequestException(override val message: String) : APIException(message)
data class BadRequestDataException(override val message: String) : APIException(message)
data class AlreadyExistsException(override val message: String) : APIException(message)
data class OperationNotSupportedException(override val message: String) : APIException(message)
data class ResourceNotFoundException(override val message: String) : APIException(message)
data class InternalErrorException(override val message: String) : APIException(message)
data class TooComplexQueryException(override val message: String) : APIException(message)
data class TooManyResultsException(override val message: String) : APIException(message)
data class AccessDeniedException(override val message: String) : APIException(message)
data class NotImplementedException(override val message: String) : APIException(message)
data class LdContextNotAvailableException(override val message: String) : APIException(message)
data class NonexistentTenantException(override val message: String) : APIException(message)
data class NotAcceptableException(override val message: String) : APIException(message)
data class BadSchemeException(override val message: String) : APIException(message)

fun Throwable.toAPIException(specificMessage: String? = null): APIException =
    when (this) {
        is APIException -> this
        is JsonLdError ->
            if (this.code == JsonLdErrorCode.LOADING_REMOTE_CONTEXT_FAILED)
                LdContextNotAvailableException(specificMessage ?: "Unable to load remote context (cause was: $this)")
            else BadRequestDataException("Unexpected error while parsing payload (cause was: $this)")

        else -> BadRequestDataException(specificMessage ?: this.localizedMessage)
    }
