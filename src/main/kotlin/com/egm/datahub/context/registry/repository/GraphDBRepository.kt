package com.egm.datahub.context.registry.repository

import arrow.core.Try
import org.eclipse.rdf4j.model.Statement
import org.eclipse.rdf4j.repository.http.HTTPRepository
import org.springframework.stereotype.Component

@Component
class GraphDBRepository(
        private val httpRepository: HTTPRepository
) {

    fun createEntity(statements: List<Statement>): Try<String> {
        val connection = httpRepository.connection
        return Try {
            connection.add(statements)
            return Try.just("OK")
        }
    }
}