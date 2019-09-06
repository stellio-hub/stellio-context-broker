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
    private val mapOfNamespaces = mapOf("foaf" to "http://xmlns.com/foaf/0.1/", "diat" to "https://diatomic.eglobalmark.com/ontology#", "ngsild" to "https://uri.etsi.org/ngsi-ld/v1/ontology#")

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
    fun getValueOfProperty(payload: String, property: String, namespace: String): String {
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
        val prefix = mapOfNamespaces.get(namespace)
        return statementCollector.statements.find { it.predicate.stringValue().contains(prefix + property) }?.`object`.toString()
    }
    fun getContext(payload: String): String {
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
        return statementCollector.statements.find { it.predicate.stringValue().contains("@context") }?.`object`.toString()
    }
}