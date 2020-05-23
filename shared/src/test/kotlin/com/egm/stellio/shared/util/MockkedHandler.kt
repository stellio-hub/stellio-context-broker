package com.egm.stellio.shared.util

import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*

@RestController
@RequestMapping("/router/mockkedroute")
class MockkedHandler {

    @GetMapping
    fun get() = ResponseEntity.status(HttpStatus.OK).build<String>()

    @PostMapping
    fun post() = ResponseEntity.status(HttpStatus.CREATED).build<String>()

    @PatchMapping
    fun patch() = ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()

    @DeleteMapping
    fun delete() = ResponseEntity.status(HttpStatus.NO_CONTENT).build<String>()
}