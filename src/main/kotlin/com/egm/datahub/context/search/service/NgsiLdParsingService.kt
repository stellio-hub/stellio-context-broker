package com.egm.datahub.context.search.service

import com.beust.klaxon.JsonReader
import com.egm.datahub.context.search.exception.InvalidNgsiLdPayloadException
import com.egm.datahub.context.search.model.*
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.io.StringReader
import java.time.OffsetDateTime

@Component
class NgsiLdParsingService {

    private val logger = LoggerFactory.getLogger(NgsiLdParsingService::class.java)

    private var mapper = jacksonObjectMapper()

    init {
        // TODO check if this registration is still required
        mapper.findAndRegisterModules()
    }

    data class JsonLdContext(
        val name: String,
        val entites: List<String>,
        val properties: Map<String, String>,
        val relationships: List<String>
    )

    val namespacesMapping: List<JsonLdContext> = listOf(
        JsonLdContext("ngsild",
            emptyList(),
            mapOf("id" to "String", "type" to "String", "observedAt" to "DateTime", "createdAt" to "DateTime",
                "datasetId" to "String", "instanceId" to "String"),
            listOf("connectsTo")),
        JsonLdContext("sosa",
            listOf("Sensor", "Observation"),
            emptyMap(),
            emptyList()),
        JsonLdContext("diat",
            listOf("Beekeeper", "Beehive", "Door", "DoorNumber", "SmartDoor"),
            mapOf("name" to "String"),
            listOf("observedBy", "managedBy", "hasMeasure")),
        JsonLdContext("example",
            listOf("OffStreetParking", "Vehicle", "Camera", "Person"),
            mapOf("availableSpotNumber" to "Int"),
            listOf("isParked", "providedBy", "hasSensor"))
    )

    private fun isAProperty(name: String): Boolean {
        namespacesMapping.forEach { namespacesMapping ->
            if (namespacesMapping.properties.containsKey(name))
                return true
        }

        return false
    }

    private fun isARelationship(name: String): Boolean {
        namespacesMapping.forEach { namespacesMapping ->
            if (namespacesMapping.relationships.contains(name))
                return true
        }

        return false
    }

    fun parseTemporalPropertyUpdate(content: String): Observation {
        val rawParsedData = mapper.readTree(content)
        val propertyName = rawParsedData.fieldNames().next()
        val propertyValues = rawParsedData[propertyName]

        logger.debug("Received property $rawParsedData (values: $propertyValues)")
        val isContentValid = listOf("type", "value", "unitCode", "observedAt", "observedBy")
            .map { propertyValues.has(it) }
            .all { it }
        if (!isContentValid)
            throw InvalidNgsiLdPayloadException("Received content misses one or more required attributes")

        val location =
            if (propertyValues["location"] != null) {
                val coordinates = propertyValues["location"]["value"]["coordinates"].asIterable()
                Pair(coordinates.first().asDouble(), coordinates.last().asDouble())
            } else {
                null
            }

        return Observation(
            attributeName = propertyName,
            observedBy = propertyValues["observedBy"]["object"].asText(),
            observedAt = OffsetDateTime.parse(propertyValues["observedAt"].asText()),
            value = propertyValues["value"].asDouble(),
            unitCode = propertyValues["unitCode"].asText(),
            latitude = location?.first,
            longitude = location?.second
        )
    }

    // TODO : it is a basic yet enough parsing logic for current needs
    //        switch to a more typesafe way to do it
    fun parse(ngsiLdPayload: String): Entity {
        logger.debug("Received entity: $ngsiLdPayload")
        val entity = Entity()
        JsonReader(StringReader(ngsiLdPayload)).use { reader ->
            reader.beginObject {
                while (reader.hasNext()) {
                    val nextName = reader.nextName()
                    when {
                        isAProperty(nextName) -> {
                            logger.debug("$nextName is a property")
                            entity.addProperty(nextName, reader.nextString())
                        }
                        isARelationship(nextName) -> {
                            logger.debug("$nextName is a relationship")
                            entity.addRelationship(nextName, reader.nextObject())
                        }
                        "@context" == nextName -> {
                            logger.debug("adding @context")
                            entity.addContexts(reader.nextArray() as List<String>)
                        }
                        else -> logger.warn("Unknown entry : $nextName")
                    }
                }
            }
        }

        return entity
    }
}
