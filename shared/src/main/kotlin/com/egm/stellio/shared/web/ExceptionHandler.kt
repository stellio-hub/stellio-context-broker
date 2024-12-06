package com.egm.stellio.shared.web

import com.apicatalog.jsonld.JsonLdError
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.InternalErrorException
import com.egm.stellio.shared.model.InvalidRequestException
import com.egm.stellio.shared.model.JsonLdErrorApiResponse
import com.egm.stellio.shared.model.JsonParseApiException
import com.egm.stellio.shared.model.NotAcceptableException
import com.egm.stellio.shared.model.NotImplementedException
import com.egm.stellio.shared.model.UnsupportedMediaTypeStatusApiException
import com.fasterxml.jackson.core.JsonParseException
import jakarta.validation.ConstraintViolationException
import org.springframework.core.codec.CodecException
import org.springframework.http.HttpStatus
import org.springframework.http.ProblemDetail
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.bind.annotation.RestControllerAdvice
import org.springframework.web.server.MethodNotAllowedException
import org.springframework.web.server.NotAcceptableStatusException
import org.springframework.web.server.UnsupportedMediaTypeStatusException

@RestControllerAdvice
class ExceptionHandler {

    @ExceptionHandler
    fun transformErrorResponse(throwable: Throwable): ResponseEntity<ProblemDetail> =
        when (val cause = throwable.cause ?: throwable) {
            is APIException -> cause.toErrorResponse()
            is JsonLdError ->
                JsonLdErrorApiResponse(cause.code.toString(), cause.message.orEmpty()).toErrorResponse()
            is JsonParseException, is CodecException ->
                JsonParseApiException(cause.message ?: "There has been a problem during JSON parsing").toErrorResponse()
            is UnsupportedMediaTypeStatusException ->
                UnsupportedMediaTypeStatusApiException(cause.message).toErrorResponse()
            is NotAcceptableStatusException ->
                NotAcceptableException(cause.message).toErrorResponse()
            is MethodNotAllowedException ->
                ResponseEntity.status(HttpStatus.METHOD_NOT_ALLOWED).body(cause.body)
            is ConstraintViolationException -> {
                val message = cause.constraintViolations.joinToString(". ") { it.message }
                if (cause.constraintViolations.flatMap { it.propertyPath }
                        .any { it.name == HttpStatus.NOT_IMPLEMENTED.name }
                )
                    NotImplementedException(message).toErrorResponse()
                else
                    InvalidRequestException(message).toErrorResponse()
            }

            else -> InternalErrorException("$cause").toErrorResponse()
        }
}
