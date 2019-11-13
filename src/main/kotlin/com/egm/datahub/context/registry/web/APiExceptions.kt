package com.egm.datahub.context.registry.web

class InvalidRequestException(message: String) : Exception(message)
class BadRequestDataException(message: String) : Exception(message)
class AlreadyExistsException(message: String) : Exception(message)
class OperationNotSupportedException(message: String) : Exception(message)
class ResourceNotFoundException(message: String) : Exception(message)
class InternalErrorException(message: String) : Exception(message)
class TooComplexQueryException(message: String) : Exception(message)
class TooManyResultsException(message: String) : Exception(message)