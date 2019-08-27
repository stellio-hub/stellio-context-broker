package com.egm.datahub.context.registry.service

import org.eclipse.rdf4j.rio.RDFFormat
import org.eclipse.rdf4j.rio.Rio
import org.eclipse.rdf4j.rio.helpers.ParseErrorCollector
import org.eclipse.rdf4j.rio.helpers.StatementCollector
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

@Component
class JsonLDService {

    private val logger = LoggerFactory.getLogger(JsonLDService::class.java)

    fun parsePayload(payload: String): String {
        val rdfParser = Rio.createParser(RDFFormat.JSONLD)
        val errorCollector = ParseErrorCollector()
        val statementCollector = StatementCollector()
        rdfParser.setParseErrorListener(errorCollector)
        rdfParser.setRDFHandler(statementCollector)
        rdfParser.parse(payload.reader(), "")

        // FIXME not sure there are cases we go through them (parser raises runtime exceptions instead)
        //       keep an eye on them and handle them if necessary
        errorCollector.warnings.forEach {
            logger.warn("Got warning : $it")
        }
        errorCollector.errors.forEach {
            logger.warn("Got error : $it")
        }
        errorCollector.fatalErrors.forEach {
            logger.warn("Got fatal error : $it")
        }

        return statementCollector.statements.find { it.subject.stringValue().startsWith("urn:") }?.subject.toString()
    }
}