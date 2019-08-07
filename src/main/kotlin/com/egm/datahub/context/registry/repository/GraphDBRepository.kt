package com.egm.datahub.context.registry.repository

import arrow.core.Try
import org.eclipse.rdf4j.model.Statement
import org.eclipse.rdf4j.query.QueryLanguage
import org.eclipse.rdf4j.repository.http.HTTPRepository
import org.eclipse.rdf4j.rio.Rio
import org.springframework.stereotype.Component
import org.eclipse.rdf4j.rio.helpers.JSONLDSettings
import org.eclipse.rdf4j.rio.helpers.JSONLDMode
import org.eclipse.rdf4j.rio.RDFFormat
import org.eclipse.rdf4j.rio.helpers.BasicWriterSettings
import org.slf4j.LoggerFactory
import java.io.StringWriter

@Component
class GraphDBRepository(
        private val httpRepository: HTTPRepository
) {

    private val logger = LoggerFactory.getLogger(GraphDBRepository::class.java)

    fun createEntity(statements: List<Statement>): Try<String> {
        val connection = httpRepository.connection
        return Try {
            connection.add(statements)
            return Try.just("OK")
        }
    }

    fun getByType(type: String): Try<String> {
        val queryString = """
            SELECT ?x ?y WHERE {
                ?x rdf:type $type.
                ?x ?p ?y.
            }
        """.trimIndent().replace("\n", "")

        logger.debug("Issuing query $queryString")

        return Try {
            httpRepository.connection.use { connection ->

                val stringWriter = StringWriter()
                val rdfWriter = Rio.createWriter(RDFFormat.JSONLD, stringWriter)

                rdfWriter.writerConfig.set(JSONLDSettings.JSONLD_MODE, JSONLDMode.COMPACT)
                rdfWriter.writerConfig.set(JSONLDSettings.OPTIMIZE, true)
                rdfWriter.writerConfig.set(BasicWriterSettings.PRETTY_PRINT, true)

                connection.prepareGraphQuery(QueryLanguage.SPARQL, queryString)
                        .evaluate(rdfWriter)

                return Try.just(stringWriter.toString())
            }
        }
    }
}