package com.egm.datahub.context.registry.web

import com.egm.datahub.context.registry.repository.GraphDBRepository
import org.eclipse.rdf4j.rio.RDFFormat
import org.eclipse.rdf4j.rio.Rio
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.BodyInserters
import org.springframework.web.reactive.function.server.ServerRequest
import org.springframework.web.reactive.function.server.ServerResponse
import org.springframework.web.reactive.function.server.ServerResponse.*
import org.springframework.web.reactive.function.server.bodyToMono
import reactor.core.publisher.Mono
import com.github.jsonldjava.utils.JsonUtils.toPrettyString
import com.github.jsonldjava.core.JsonLdProcessor
import com.github.jsonldjava.core.JsonLdOptions
import com.github.jsonldjava.utils.JsonUtils
import org.springframework.kafka.core.KafkaTemplate
import reactor.core.publisher.toMono
import java.net.URI


@Component
class EntityHandler(
        private val graphDBRepository: GraphDBRepository,
        private val kafkaTemplate: KafkaTemplate<String, String>
) {

    private val logger = LoggerFactory.getLogger(EntityHandler::class.java)

    fun create(req: ServerRequest): Mono<ServerResponse> {
        var originJsonLD = ""
        var entityUrn = ""
        return req.bodyToMono<String>()
            .log()
            .map {
                originJsonLD = it
                Rio.parse(it.reader(), "", RDFFormat.JSONLD)
            }
            .map {
                entityUrn = it.toList().find { it.subject.stringValue().startsWith("urn:") }?.subject.toString()
                graphDBRepository.createEntity(it)
            }.flatMap {
                kafkaTemplate.send("entities", originJsonLD).completable().toMono()
            }.flatMap {
                created(URI("/ngsi-ld/entities/$entityUrn")).build()
            }.onErrorResume {
                ServerResponse.badRequest().body(BodyInserters.fromObject(it.localizedMessage))
            }
    }

    fun getById(req: ServerRequest): Mono<ServerResponse> {
        // TODO throw a 400 if no entityId provided
        val entityId = req.pathVariable("entityId")

        return graphDBRepository.getById(entityId)
                    .fold(
                            { status(HttpStatus.INTERNAL_SERVER_ERROR).build() },
                            { ok().body(BodyInserters.fromObject(it)) }
                    )
    }

    fun parseAndPlay(req: ServerRequest): Mono<ServerResponse> {
        return req.bodyToMono<String>()
                .flatMap {
                    // Open a valid json(-ld) input file
                    val inputStream = it.byteInputStream() //FileInputStream("input.json")
                    // Read the file into an Object (The type of this object will be a List, Map, String, Boolean,
                    // Number or null depending on the root object in the file).
                    val jsonObject = JsonUtils.fromInputStream(inputStream)
                    logger.debug("JSON object is of type ${jsonObject::class.java}")
                    // Create a context JSON map containing prefixes and definitions
                    val context = HashMap<String, String>()
                    // Customise context...
                    // Create an instance of JsonLdOptions with the standard JSON-LD options
                    val options = JsonLdOptions()
                    options.explicit = true
                    options.omitGraph = false
                    options.produceGeneralizedRdf = true
                    // Customise options...
                    // Call whichever JSONLD function you want! (e.g. compact)
                    val compact = JsonLdProcessor.compact(jsonObject, context, options)
                    // Print out the result (or don't, it's your call!)
                    logger.debug(toPrettyString(compact))

                    ok().build()
                }
    }
}
