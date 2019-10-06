package com.egm.datahub.context.registry.service

import com.egm.datahub.context.registry.repository.Neo4jRepository
import com.google.gson.GsonBuilder
import com.google.gson.reflect.TypeToken
import org.neo4j.ogm.response.model.NodeModel
import org.neo4j.ogm.response.model.RelationshipModel
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.text.SimpleDateFormat
import java.time.Instant
import java.time.format.DateTimeFormatter
import java.util.*
import java.util.regex.Pattern

typealias EntityStatements = List<String>
typealias RelationshipStatements = List<String>

@Component
class NgsiLdParserService(
    private val neo4jRepository: Neo4jRepository
) {

    class Entity {
        var label: String = ""
        var attrs: MutableMap<String, Any>
        var ns: String?
        constructor(attrs: MutableMap<String, Any>, ns: String?) {
            this.attrs = attrs
            this.ns = ns
        }
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
            "diat" to listOf("Beekeeper", "BeeHive", "Door", "DoorNumber", "SmartDoor", "Sensor", "Observation", "ObservedBy", "ManagedBy", "hasMeasure"),
            "ngsild" to listOf("connectsTo", "hasObject", "observedAt", "createdAt", "modifiedAt", "datasetId", "instanceId", "GeoProperty", "Point", "Property", "Relationship", "name"),
            "example" to listOf("availableSpotNumber", "OffStreetParking", "Vehicle", "isParked", "providedBy", "Camera", "Person") // this is property of property in order to allow nested property we need to add it to model
        )

        val contextsMap = mapOf(
            "@context" to listOf(
                "https://diatomic.eglobalmark.com/diatomic-context.jsonld",
                "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld",
                "https://fiware.github.io/dataModels/fiware-datamodels-context.jsonld"
            )
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
            var p = Pattern.compile("\\\"(\\w+)\\\"\\:")
            attrs = p.matcher(attrs).replaceAll("$1:")
            attrs = attrs.replace("\n", "")
            return attrs
        }
    }
    private fun iterateOverRelationships(node: Map<String, Any>): Map<String, Any> {
        val nodeModel = node["n"] as NodeModel
        val uriProperty = nodeModel.propertyList.find { it.key == "uri" }!!
        var inneroutput = mutableMapOf<String, Any>()
        // get relationships
        val relationships = neo4jRepository.getRelationshipByURI(uriProperty.value.toString())
        // add relationships
        relationships.map {
            val target = it.get("t") as NodeModel
            val relationship = it.get("r") as RelationshipModel
            var relMapProperties = mutableMapOf<String, Any>("type" to "Relationship")
            target.propertyList.filter {
                it.key == "uri"
            }.map {
                relMapProperties.put("object", it.value)
            }
            // add relationships properties stored in materialized node
            // 1. get relationship URI
            val relmodel = relationships.first().get("r") as RelationshipModel
            val matRelUri = relmodel.propertyList.filter { it.key == "uri" }.map { it.value }.get(0).toString()
            // 2. find materialized rel node
            val matRelNode = neo4jRepository.getNodeByURI(matRelUri)
            if (!matRelNode.isEmpty()) {
                val nestedObj1 = iterateOverRelationships(matRelNode)
                val nestedObj2 = iterateOverProperties(matRelNode)
                // do things with it!
                val mrn = matRelNode.get("n") as NodeModel
                // 3. add to existing map
                mrn.propertyList.filter {
                    it.key != "uri" && it.value != "" && !relMapProperties.containsKey(it.key)
                }.map {
                    relMapProperties.put(it.key, it.value)
                }
                relMapProperties.putAll(nestedObj1)
                relMapProperties.putAll(nestedObj2)
            }
            inneroutput.put(it.get("rel").toString().split("__")[1], relMapProperties)
        }
        return inneroutput
    }
    private fun iterateOverProperties(node: Map<String, Any>): Map<String, Any> {
        val nodeModel = node["n"] as NodeModel
        val uriProperty = nodeModel.propertyList.find { it.key == "uri" }!!
        var inneroutput = mutableMapOf<String, Any>()
        // get properties
        val nestedProperties = neo4jRepository.getNestedPropertiesByURI(uriProperty.value.toString())
        // add properties
        nestedProperties.map {
            val target = it.get("t") as NodeModel
            var nestedPropertyMap = mutableMapOf<String, Any>("type" to "Property")
            // get target URI to get the attribute name
            val nestedPropUri = target.propertyList.filter { it.key == "uri" }.map { it.value }.get(0).toString()
            val attrname = nestedPropUri.split(":")[2]
            val nestedPropNode = neo4jRepository.getNodeByURI(nestedPropUri)
            if (!nestedPropNode.isEmpty()) {
                // in case of FLAG compact instead of explode all the properties just show type and id
                target.propertyList.filter { it.key != "uri" }.forEach {
                    nestedPropertyMap.put(it.key, it.value)
                }
                val nestedObj1 = iterateOverRelationships(nestedPropNode)
                val nestedObj2 = iterateOverProperties(nestedPropNode)
                nestedPropertyMap.putAll(nestedObj1)
                nestedPropertyMap.putAll(nestedObj2)
            }

            inneroutput.put(attrname, nestedPropertyMap)
        }
        return inneroutput
    }

    fun parseEntity(ngsiLdPayload: String): Triple<String, EntityStatements, RelationshipStatements> {
        val entityMap: Map<String, Any> = gson.fromJson(ngsiLdPayload, object : TypeToken<Map<String, Any>>() {}.type)
        val entityUrn = entityMap["id"] as String

        val statements = transformNgsiLdToCypher(
            entityMap,
            null,
            null,
            mutableListOf(),
            mutableListOf()
        )

        return Triple(entityUrn, statements.first, statements.second)
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
        val nsSubj = getLabelNamespace(nodeType)
        val attrs = getAttributes(node).toMutableMap()
        attrs.put("uri", nodeUuid)
        val nodeEntity = Entity(
            attrs,
            nsSubj
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
                logger.debug(item.key + " is relationship")
                // THIS IS THE NODE --> REL --> NODE (object)
                val rel = item.key
                val nsPredicate = getLabelNamespace(rel)
                val predicate = nsPredicate + "__" + rel
                // create random uri for rel
                val uuid = UUID.randomUUID().toString()
                // add materialized relationship NODE
                val urnRel = "urn:$nsPredicate:$rel:$uuid"
                // a Relationship witemhout a object? not possible skip!
                if (content.get("object") == null)
                    continue

                content.get("object").let {
                    val urn: String = it.toString()
                    val typeObj = urn.split(":")[2]
                    val nsObj = getLabelNamespace(typeObj)
                    // ADD RELATIONSHIP

                    if (parentIsRelationship) {
                        parentAttribute?.let {
                            nodeEntity.ns = getLabelNamespace(parentAttribute)
                            val newStatements = buildInsert(
                                nodeEntity,
                                Entity(hashMapOf("uri" to urnRel), nsPredicate),
                                Entity(hashMapOf("uri" to urn), nsObj)
                            )
                            if (newStatements.first.isNotEmpty()) accEntityStatements.add(newStatements.first[0])
                            if (newStatements.second.isNotEmpty()) accRelationshipStatements.add(newStatements.second[0])
                        }
                    } else {
                        val ns = getLabelNamespace(typeObj)
                        val newStatements = buildInsert(
                            nodeEntity,
                            Entity(hashMapOf("uri" to urnRel), nsPredicate),
                            Entity(hashMapOf("uri" to urn), ns)
                        )
                        if (newStatements.first.isNotEmpty()) accEntityStatements.add(newStatements.first[0])
                        if (newStatements.second.isNotEmpty()) accRelationshipStatements.add(newStatements.second[0])
                    }

                    // ADD the "object"
                    val newStatements = buildInsert(Entity(hashMapOf("uri" to urn), nsObj), null, null)
                    if (newStatements.first.isNotEmpty()) accEntityStatements.add(newStatements.first[0])
                    if (newStatements.second.isNotEmpty()) accRelationshipStatements.add(newStatements.second[0])

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
                    val uuid = UUID.randomUUID().toString()
                    val urn = "urn:$nsObj:$labelObj:$uuid"
                    // add to statement list SUBJECT -- RELATION [:hasObject] -- OBJECT
                    val predicate = "ngsild__hasObject"

                    // object attributes will be set in the next travestPropertiesIteration with a match on URI
                    // ADD THE RELATIONSHIP
                    val uuidRel = UUID.randomUUID().toString()
                    val urnPredicate = "urn:ngsild:hasObject:$uuidRel"
                    val newStatements = buildInsert(
                        nodeEntity,
                        Entity(hashMapOf("uri" to urnPredicate), "ngsild"),
                        Entity(hashMapOf("uri" to urn), nsObj)
                    )
                    if (newStatements.first.isNotEmpty()) accEntityStatements.add(newStatements.first[0])
                    if (newStatements.second.isNotEmpty()) accRelationshipStatements.add(newStatements.second[0])
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
        if (newStatements.first.isNotEmpty()) accEntityStatements.add(newStatements.first[0])
        if (newStatements.second.isNotEmpty()) accRelationshipStatements.add(newStatements.second[0])

        return Pair(accEntityStatements, accRelationshipStatements)
    }

    fun ngsiLdToUpdateQuery(payload: String, uri: String, attr: String): String {
        val expandedPayload = expandObjToMap(payload)
        val value = expandedPayload[attr].toString()
        val timestamp = DateTimeFormatter.ISO_INSTANT.format(Instant.now()).toString()
        val attrsUriMatch = formatAttributes(mapOf("uri" to uri))
        val attrsUriSubj = formatAttributes(mapOf("uri" to uri, "modifiedAt" to timestamp, attr to value))

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
        val type = prop["type"]?.toString()
        if (type.equals("Property") || type.equals("Relationship") || type.equals("GeoProperty")) return false
        if (prop.isEmpty()) return true
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

    private fun buildInsert(subject: Entity, predicate: Entity?, obj: Entity?): Pair<EntityStatements, RelationshipStatements> {
        return if (predicate == null || obj == null) {
            val labelSubject = subject.getLabelWithPrefix()
            val uri = subject.getUri()
            val attrsUriSubj = formatAttributes(hashMapOf("uri" to uri))
            val timeStamp = SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(Date())
            if (!subject.attrs.containsKey("createdAt")) {
                subject.attrs.put("createdAt", timeStamp)
            }
            subject.attrs.put("modifiedAt", timeStamp)
            val attrsSubj = formatAttributes(subject.attrs.filter { it.value != "" })
            Pair(listOf("MERGE (a : $labelSubject $attrsUriSubj) ON CREATE SET a = $attrsSubj ON MATCH  SET a += $attrsSubj return a"), emptyList())
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
            Pair(emptyList(), listOf("MATCH (a : $labelSubject $attrsSubj), (b : $labelObj $attrsObj ) MERGE (a)-[r: $labelPredicate $attrsPredicate]->(b) return a,b"))
        }
    }

    private fun hasAttributes(node: Map<String, Any>): Boolean {
        // if a Property has just type and value we save it as attribute value in the parent entity
        var resp = false
        node.forEach {
            if (!it.key.equals("type") && !it.key.equals("value") && !it.key.equals("object") && !it.key.equals("id")) {
                resp = true
                return resp
            }
        }
        return resp
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

    /**
     * Sample (simple) query result :
     *
     *  {"n": {
     *      "id":57,
     *      "version":null,
     *      "labels":["diat__Door"],
     *      "primaryIndex":null,
     *      "previousDynamicLabels":[],
     *      "propertyList":[
     *          {"key":"createdAt","value":"2019.09.26.14.31.44"},
     *          {"key":"doorNumber","value":"15"},
     *          {"key":"modifiedAt","value":"2019.09.26.14.31.44"},
     *          {"key":"uri","value":"urn:diat:Door:0015"}
     *      ]
     *   }}
     */
    fun queryResultToNgsiLd(queryResult: Map<String, Any>): Map<String, Any> {
        val nodeModel = queryResult["n"] as NodeModel
        val uriProperty = nodeModel.propertyList.find { it.key == "uri" }!!
        logger.debug("Transforming node ${nodeModel.id} ($uriProperty)")
        val properties = nodeModel.propertyList
            .filter { it.key != "uri" && it.key != "location" && it.value != "" }
            .map {
                it.key to it.value
            }

        var output = mutableMapOf(
            "id" to uriProperty.value,
            "type" to nodeModel.labels[0].split("__")[1]
        )
        val location = nodeModel.propertyList.find { it.key == "location" }
        if (location != null) {
            val lon = location.value.toString().split("x:")[1].split(",")[0].toDouble()
            val lat = location.value.toString().split("y:")[1].split(",")[0].toDouble()
            output.put("location", mapOf("type" to "GeoProperty", "value" to mapOf("type" to "Point", "coordinates" to arrayOf(lon, lat))))
        }

        // go deeper

        var rr = iterateOverRelationships(queryResult)
        var pp = iterateOverProperties(queryResult)

        return output.plus(rr).plus(pp).plus(properties).plus(contextsMap)
    }
}
