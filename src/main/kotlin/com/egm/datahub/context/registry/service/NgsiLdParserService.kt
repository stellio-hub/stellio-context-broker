package com.egm.datahub.context.registry.service

import com.egm.datahub.context.registry.model.NgsiLdParsedResult
import com.google.gson.GsonBuilder
import com.google.gson.reflect.TypeToken
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.*
import java.util.regex.Pattern

typealias EntityStatement = String
typealias RelationshipStatement = String
typealias EntityStatements = List<EntityStatement>
typealias RelationshipStatements = List<RelationshipStatement>

@Component
class NgsiLdParserService {

    class Entity(var attrs: MutableMap<String, Any>, var ns: String?) {
        var label: String = ""

        fun getUri(): String {
            return this.attrs.get("uri").toString()
        }

        fun getLabelWithPrefix(): String {
            this.attrs.get("uri")?.let {
                this.label = it.toString().split(":")[2]
            }
            return this.ns + "__" + this.label
        }
    }

    private val logger = LoggerFactory.getLogger(NgsiLdParserService::class.java)

    companion object {
        private val gson = GsonBuilder().setPrettyPrinting().create()
        val namespacesMapping: Map<String, List<String>> = mapOf(
            "ngsild" to listOf("hasValue", "observedAt", "createdAt", "modifiedAt", "datasetId", "instanceId", "GeoProperty", "Point", "Property", "Relationship"),
            "sosa" to listOf("Sensor", "Observation"),
            "diat" to listOf("Beekeeper", "BeeHive", "Door", "DoorNumber", "SmartDoor", "ObservedBy", "ManagedBy", "hasMeasure", "connectsTo"),
            "fiware" to listOf("Building", "BuildingOperation"),
            "example" to listOf("availableSpotNumber", "OffStreetParking", "Vehicle", "isParked", "providedBy", "hasSensor", "Camera", "Person") // this is property of property in order to allow nested property we need to add it to model
        )

        /**
         * Temporary map to have an error-prone mapping between linked JSON-LD contexts and internal label prefixing
         *
         * Later on, we'll have to dynamically resolve linked JSON-LD contexts so we'll have to find another way.
         * Maybe store the context along entities, properties and relationships ?
         */
        val contextLinksToNamespace = mapOf(
            "http://easyglobalmarket.com/contexts/diat.jsonld" to "diat",
            "https://fiware.github.io/data-models/context.jsonld" to "fiware",
            "http://easyglobalmarket.com/contexts/sosa.jsonld" to "sosa",
            "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld" to "ngsild"
        )

        fun expandObjToMap(obj: Any?): Map<String, Any> {
            try {
                if (obj is String) {
                    return gson.fromJson(obj.toString(), object : TypeToken<Map<String, Any>>() {}.type)
                } else {
                    return obj as Map<String, Any>
                }
            } catch (e: Exception) {
                return emptyMap()
            }
        }
        fun formatAttributes(attributes: Map<String, Any>): String {
            var attrs = gson.toJson(attributes)
            val p = Pattern.compile("\\\"(\\w+)\\\"\\:")
            attrs = p.matcher(attrs).replaceAll("$1:")
            attrs = attrs.replace("\n", "")
            return attrs
        }
    }

    fun checkResourceNSmatch(contextLink: String, type: String): Boolean {
        return if (contextLinksToNamespace[contextLink] == null) {
            false
        } else {
            namespacesMapping[contextLinksToNamespace[contextLink]]!!.contains(type)
        }
    }

    fun addPrefixToType(contextLink: String, type: String): String {
        return contextLinksToNamespace[contextLink] + "__" + type
    }

    fun prependNsToQuery(q: List<String>, contextLink: String): List<String> {
        val ns = contextLinksToNamespace[contextLink]
        return q.map {
            if (it.split("==")[1].startsWith("urn:")) {
                // is a relationship
                val newel = ns + "__" + it.split("==")[0] + "==" + it.split("==")[1]
                newel
            } else {
                // is a property
                it
            }
        }.toList()
    }

    fun parseEntity(ngsiLdPayload: String): NgsiLdParsedResult {
        val entityUrn = extractEntityUrn(ngsiLdPayload)
        val entityMap: Map<String, Any> = gson.fromJson(ngsiLdPayload, object : TypeToken<Map<String, Any>>() {}.type)
        val entityType = entityMap["type"] as String

        val statements = transformNgsiLdToCypher(
            entityMap,
            null,
            null,
            mutableListOf(),
            mutableListOf()
        )

        return NgsiLdParsedResult(entityType, entityUrn, statements.first, statements.second, ngsiLdPayload)
    }

    fun extractEntityUrn(ngsiLdPayload: String): String {
        val entityMap: Map<String, Any> = gson.fromJson(ngsiLdPayload, object : TypeToken<Map<String, Any>>() {}.type)
        return entityMap["id"] as String
    }

    fun extractEntityType(ngsiLdPayload: String): String {
        val entityMap: Map<String, Any> = gson.fromJson(ngsiLdPayload, object : TypeToken<Map<String, Any>>() {}.type)
        return entityMap["type"] as String
    }

    fun transformNgsiLdToCypher(
        node: Map<String, Any>,
        uuid: String?,
        parentAttribute: String?,
        accEntityStatements: MutableList<String>,
        accRelationshipStatements: MutableList<String>
    ): Pair<EntityStatements, RelationshipStatements> {

        logger.info("Traversing node $node")

        val parentIsRelationship: Boolean = node["type"]?.toString().equals("Relationship")

        val nodeType = node["type"].toString()
        val nodeUuid = node.getOrDefault("id", uuid).toString()
        val nodeNs = getLabelNamespace(nodeType)
        val attrs = getAttributes(node).plus("uri" to nodeUuid)
        val nodeEntity = Entity(
            attrs.toMutableMap(),
            nodeNs
        )

        // if is Property override the generic Property type with the attribute
        if (node["type"].toString() == "Property") {
            parentAttribute?.let {
                nodeEntity.ns = getLabelNamespace(it)
                nodeEntity.label = parentAttribute
            }
        }

        for (item in node) {
            // foreach attribute get the Map and check type is Property
            val content = expandObjToMap(item.value)
            if (isAttribute(content)) {
                logger.debug(item.key + " is attribute")
                // add to attr map
            }
            if (isRelationship(content)) {
                // a relationship without an object? not possible skip!
                if (content.get("object") == null)
                    continue

                logger.debug(item.key + " is relationship")
                // THIS IS THE NODE --> REL --> NODE (object)
                val rel = item.key
                val nsPredicate = getLabelNamespace(rel)
                // add materialized relationship NODE
                val urnRel = "urn:ngsi-ld:$rel:${UUID.randomUUID()}"
                val relObject = content["object"].toString()

                val typeObj = relObject.split(":")[2]
                val nsObj = getLabelNamespace(typeObj)
                // ADD RELATIONSHIP

                if (parentIsRelationship) {
                    parentAttribute?.let {
                        nodeEntity.ns = getLabelNamespace(parentAttribute)
                        val newStatements = buildInsert(
                            nodeEntity,
                            Entity(hashMapOf("uri" to urnRel), nsPredicate),
                            Entity(hashMapOf("uri" to relObject), nsObj)
                        )
                        logger.debug("Built statement : ${newStatements.second}")
                        accRelationshipStatements.add(newStatements.second)
                    }
                } else {
                    val newStatements = buildInsert(
                        nodeEntity,
                        Entity(hashMapOf("uri" to urnRel), nsPredicate),
                        Entity(hashMapOf("uri" to relObject), nsObj)
                    )
                    logger.debug("Built statement : ${newStatements.second}")
                    accRelationshipStatements.add(newStatements.second)
                }

                // ADD the "object"
                val newStatements = buildInsert(Entity(hashMapOf("uri" to relObject), nsObj), null, null)
                logger.debug("Built statement : ${newStatements.first}")
                accEntityStatements.add(newStatements.first)

                if (hasAttributes(content)) {
                    // go deeper using the materialized rel Node
                    transformNgsiLdToCypher(
                        content,
                        urnRel,
                        item.key,
                        accEntityStatements,
                        accRelationshipStatements
                    )
                }
            }

            if (isProperty(content)) {
                logger.debug(item.key + " is property")

                // is not a map or the only attributes are type and value
                if (!hasAttributes(content)) {
                    // has attributes or just value and type? if so store as attribute  (es. name and available spot number in vehicle)
                    logger.debug("this property has just type and value")

                    val value = content.get("value")
                    val obj = item.key
                    value?.let { nodeEntity.attrs.put(obj, value) }
                } else {
                    // this property has one ore more nested objects ==> use the attr. key (es. availableSpotNumber) as object to create a Relationship between entity and Property
                    // MATERIALIZED PROPERTY
                    val labelObj = item.key
                    val nsObj = getLabelNamespace(labelObj)

                    // create uri for object
                    val urn = "urn:ngsi-ld:$labelObj:${UUID.randomUUID()}"
                    // add to statement list SUBJECT -- RELATION [:hasObject] -- OBJECT

                    // object attributes will be set in the next travestPropertiesIteration with a match on URI
                    // ADD THE RELATIONSHIP
                    val uuidRel = UUID.randomUUID().toString()
                    val urnPredicate = "urn:ngsild:hasValue:$uuidRel"
                    val newStatements = buildInsert(
                        nodeEntity,
                        Entity(hashMapOf("uri" to urnPredicate), "ngsild"),
                        Entity(hashMapOf("uri" to urn), nsObj)
                    )
                    logger.debug("Built statement : ${newStatements.second}")
                    accRelationshipStatements.add(newStatements.second)
                    // go deeper
                    transformNgsiLdToCypher(
                        content,
                        urn,
                        item.key,
                        accEntityStatements,
                        accRelationshipStatements
                    )
                }
            }
        }

        val newStatements = buildInsert(nodeEntity, null, null)
        logger.debug("Built statement : ${newStatements.first}")
        accEntityStatements.add(newStatements.first)

        return Pair(accEntityStatements, accRelationshipStatements)
    }

    fun ngsiLdToUpdateEntityQuery(payload: String, uri: String): String {
        val expandedPayload = expandObjToMap(payload)
        val timestamp = getFormattedTimestamp()
        val attrsUriMatch = formatAttributes(mapOf("uri" to uri))

        var out = hashMapOf("uri" to uri, "modifiedAt" to timestamp)
        expandedPayload.filter {
            it.key != "type" && it.key != "object"
        }.map {
            out.put(it.key, it.value.toString())
        }

        expandedPayload.filter {
            it.key != "type" && it.key != "object"
        }.map {
            // activate the parser to update nested entities TODO
        }

        val attrsUriSubj = formatAttributes(out)
        return "MERGE (a $attrsUriMatch) ON  MATCH  SET a += $attrsUriSubj return a"
    }

    fun ngsiLdToUpdateEntityAttributeQuery(payload: String, uri: String, attr: String): String {
        val expandedPayload = expandObjToMap(payload)
        val attrsUriMatch = formatAttributes(mapOf("uri" to uri))
        val timestamp = getFormattedTimestamp()
        var out = hashMapOf("uri" to uri, "modifiedAt" to timestamp)
        expandedPayload.filter {
            it.key != "type" && it.key != "object" && it.key == attr
        }.map {
            out.put(it.key, it.value.toString())
        }
        val attrsUriSubj = formatAttributes(out)
        return "MERGE (a $attrsUriMatch) ON  MATCH  SET a += $attrsUriSubj return a"
    }

    private fun isProperty(prop: Map<String, Any>): Boolean {
        return prop["type"]?.toString().equals("Property")
    }

    private fun isGeoProperty(prop: Map<String, Any>): Boolean {
        return prop["type"]?.toString().equals("GeoProperty")
    }

    private fun isRelationship(prop: Map<String, Any>): Boolean {
        return prop["type"]?.toString().equals("Relationship")
    }

    private fun isAttribute(prop: Map<String, Any>): Boolean {
        if (prop.isEmpty()) return true
        val type = prop["type"]?.toString()
        if (type.equals("Property") || type.equals("Relationship") || type.equals("GeoProperty")) return false
        return hasAttributes(prop)
    }

    private fun getAttributes(node: Map<String, Any>): Map<String, Any> {
        return node.filterKeys { key ->
            !listOf("type", "@context", "id", "object").contains(key)
        }.filter {
            !hasAttributes(expandObjToMap(it.value))
        }.mapValues {
            if (it.value is String || it.value is Double) {
                it.value
            } else if (isGeoProperty(expandObjToMap(it.value))) {
                val value = expandObjToMap(it.value)["value"] as Map<String, Any>
                val coordinates = value["coordinates"] as List<Double>
                val lon = coordinates[0]
                val lat = coordinates[1]
                "point({ x: $lon , y: $lat, crs: 'WGS-84' })"
            } else if (expandObjToMap(it.value).containsKey("value")) {
                val innerValue = expandObjToMap(it.value)["value"]
                innerValue!!
            } else {
                ""
            }
        }
    }

    private fun buildInsert(subject: Entity, predicate: Entity?, obj: Entity?): Pair<EntityStatement, RelationshipStatement> {
        return if (predicate == null || obj == null) {
            val labelSubject = subject.getLabelWithPrefix()
            val uri = subject.getUri()
            val attrsUriSubj = formatAttributes(hashMapOf("uri" to uri))

            val timeStamp = getFormattedTimestamp()
            if (!subject.attrs.containsKey("createdAt")) {
                subject.attrs.put("createdAt", timeStamp)
            }
            subject.attrs.put("modifiedAt", timeStamp)
            val attrsSubj = formatAttributes(subject.attrs.filter { it.value != "" })
            Pair("MERGE (a : $labelSubject $attrsUriSubj) ON CREATE SET a = $attrsSubj ON MATCH  SET a += $attrsSubj return a", "")
        } else {
            val labelObj = obj.getLabelWithPrefix()
            val labelPredicate = predicate.getLabelWithPrefix()
            val labelSubject = subject.getLabelWithPrefix()
            val uriSubj = subject.getUri()
            val uriPredicate = predicate.getUri()
            val uriObj = obj.getUri()
            val attrsSubj = formatAttributes(hashMapOf("uri" to uriSubj))
            val attrsObj = formatAttributes(hashMapOf("uri" to uriObj))
            val attrsPredicate = formatAttributes(hashMapOf("uri" to uriPredicate))
            Pair("", "MATCH (a : $labelSubject $attrsSubj), (b : $labelObj $attrsObj ) MERGE (a)-[r: $labelPredicate $attrsPredicate]->(b) return a,b")
        }
    }

    private fun hasAttributes(node: Map<String, Any>): Boolean {
        // if a Property has just type and value we save it as attribute value in the parent entity
        node.forEach {
            if (!it.key.equals("type") && !it.key.equals("value") && !it.key.equals("object") && !it.key.equals("id")) {
                return true
            }
        }
        return false
    }

    private fun getLabelNamespace(label: String): String {
        namespacesMapping.forEach {
            if (it.value.contains(label)) {
                return it.key
            }
        }

        // fallback to default core NGSI-LD namespace
        // TODO : we should instead raise a 400-like exception
        return "ngsild"
    }

    private fun getFormattedTimestamp(): String {
        return ZonedDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT)
    }
}
