package com.egm.datahub.context.registry.repository

import org.eclipse.rdf4j.model.Model
import org.eclipse.rdf4j.query.QueryLanguage
import org.eclipse.rdf4j.repository.http.HTTPRepository
import org.eclipse.rdf4j.rio.Rio
import org.springframework.stereotype.Component
import org.eclipse.rdf4j.rio.helpers.JSONLDSettings
import org.eclipse.rdf4j.rio.helpers.JSONLDMode
import org.eclipse.rdf4j.rio.RDFFormat
import org.eclipse.rdf4j.rio.helpers.BasicWriterSettings
import org.slf4j.LoggerFactory
import org.springframework.kafka.core.KafkaTemplate
import reactor.core.publisher.toMono
import java.io.StringWriter

@Component
class GraphDBRepository(
        private val httpRepository: HTTPRepository,
        private val kafkaTemplate: KafkaTemplate<String, String>
) {

    private val logger = LoggerFactory.getLogger(GraphDBRepository::class.java)

    fun createEntity(originJsonLD: String, model: Model): String {
        val connection = httpRepository.connection
        connection.add(model.toList())
        kafkaTemplate.send("entities", originJsonLD).completable().toMono()
        return model.toString()
    }

    fun getById(id: String): String {
        val queryString = """
            SELECT ?x ?y WHERE {
                ?x rdf:type $id.
                ?x ?p ?y.
            }
        """.trimIndent().replace("\n", "")

        logger.debug("Issuing query $queryString")

        httpRepository.connection.use { connection ->

            val stringWriter = StringWriter()
            val rdfWriter = Rio.createWriter(RDFFormat.JSONLD, stringWriter)

            rdfWriter.writerConfig.set(JSONLDSettings.JSONLD_MODE, JSONLDMode.COMPACT)
            rdfWriter.writerConfig.set(JSONLDSettings.OPTIMIZE, true)
            rdfWriter.writerConfig.set(BasicWriterSettings.PRETTY_PRINT, true)

            connection.prepareGraphQuery(QueryLanguage.SPARQL, queryString)
                    .evaluate(rdfWriter)

            return stringWriter.toString()
        }
    }
}