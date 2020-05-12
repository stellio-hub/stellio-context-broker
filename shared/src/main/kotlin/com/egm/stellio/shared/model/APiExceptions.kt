package com.egm.stellio.shared.model

sealed class APiException(
    override val message: String
) : Exception(message)

data class InvalidRequestException(override val message: String) : APiException(message)
data class BadRequestDataException(override val message: String) : APiException(message)
data class AlreadyExistsException(override val message: String) : APiException(message)
data class OperationNotSupportedException(override val message: String) : APiException(message)
data class ResourceNotFoundException(override val message: String) : APiException(message)
data class InternalErrorException(override val message: String) : APiException(message)
data class TooComplexQueryException(override val message: String) : APiException(message)
data class TooManyResultsException(override val message: String) : APiException(message)