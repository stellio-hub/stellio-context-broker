package com.egm.stellio.shared.util

import com.egm.stellio.shared.model.*
import com.fasterxml.jackson.core.JsonParseException
import com.github.jsonldjava.core.JsonLdError
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.bind.annotation.RestControllerAdvice

@RestControllerAdvice
class RestControllerAdvice {

    @ExceptionHandler
    fun transformErrorResponse(throwable: Throwable): ResponseEntity<String> =
        when (throwable) {
            is AlreadyExistsException ->
                ResponseEntity.status(HttpStatus.CONFLICT)
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(ApiUtils.serializeObject(AlreadyExistsResponse(throwable.message)))
            is ResourceNotFoundException ->
                ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(ApiUtils.serializeObject(ResourceNotFoundResponse(throwable.message)))
            is BadRequestDataException ->
                ResponseEntity.badRequest()
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(ApiUtils.serializeObject(BadRequestDataResponse(throwable.message)))
            is JsonLdError ->
                ResponseEntity.badRequest()
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(ApiUtils.serializeObject(JsonLdErrorResponse(throwable.type.toString(), throwable.message.orEmpty())))
            is JsonParseException ->
                ResponseEntity.badRequest()
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(ApiUtils.serializeObject(JsonParseErrorResponse(throwable.message ?: "There has been a problem during JSON parsing")))
            else ->
                ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(ApiUtils.serializeObject(InternalErrorResponse(throwable.message ?: "There has been an error during the operation execution")))
        }
}