package com.egm.stellio.shared.web

import com.egm.stellio.shared.model.AccessDeniedException
import com.egm.stellio.shared.model.AccessDeniedResponse
import com.egm.stellio.shared.model.AlreadyExistsException
import com.egm.stellio.shared.model.AlreadyExistsResponse
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.BadRequestDataResponse
import com.egm.stellio.shared.model.InternalErrorResponse
import com.egm.stellio.shared.model.JsonLdErrorResponse
import com.egm.stellio.shared.model.JsonParseErrorResponse
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.model.ResourceNotFoundResponse
import com.egm.stellio.shared.util.ApiUtils
import com.fasterxml.jackson.core.JsonParseException
import com.github.jsonldjava.core.JsonLdError
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.bind.annotation.RestControllerAdvice

@RestControllerAdvice
class ExceptionHandler {

    @ExceptionHandler
    fun transformErrorResponse(throwable: Throwable): ResponseEntity<String> =
        when (throwable) {
            is AlreadyExistsException -> generateErrorResponse(
                HttpStatus.CONFLICT,
                AlreadyExistsResponse(throwable.message)
            )
            is ResourceNotFoundException -> generateErrorResponse(
                HttpStatus.NOT_FOUND,
                ResourceNotFoundResponse(throwable.message)
            )
            is BadRequestDataException -> generateErrorResponse(
                HttpStatus.BAD_REQUEST,
                BadRequestDataResponse(throwable.message)
            )
            is JsonLdError -> generateErrorResponse(
                HttpStatus.BAD_REQUEST,
                JsonLdErrorResponse(throwable.type.toString(), throwable.message.orEmpty())
            )
            is JsonParseException -> generateErrorResponse(
                HttpStatus.BAD_REQUEST,
                JsonParseErrorResponse(throwable.message ?: "There has been a problem during JSON parsing")
            )
            is AccessDeniedException -> generateErrorResponse(
                HttpStatus.FORBIDDEN,
                AccessDeniedResponse(throwable.message)
            )
            else -> generateErrorResponse(
                HttpStatus.INTERNAL_SERVER_ERROR,
                InternalErrorResponse(throwable.message ?: "There has been an error during the operation execution")
            )
        }

    private fun generateErrorResponse(status: HttpStatus, exception: Any) =
        ResponseEntity.status(status)
            .contentType(MediaType.APPLICATION_JSON)
            .body(ApiUtils.serializeObject(exception))
}
