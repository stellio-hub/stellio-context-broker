package com.egm.datahub.context.registry.service

import com.google.gson.GsonBuilder
import com.google.gson.reflect.TypeToken
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.util.*
import java.util.regex.Pattern
import kotlin.collections.ArrayList

typealias EntityStatements = List<String>
typealias RelationshipStatements = List<String>

@Component
class NgsiLdParserService {

    class Entity(var label: String, var attrs: Map<String, Any>, var ns: String?) {

        fun getLabelWithPrefix(): String {
            return this.ns + "__" + this.label
        }
    }

    private val gson = GsonBuilder().setPrettyPrinting().create()
    private val logger = LoggerFactory.getLogger(NgsiLdParserService::class.java)

    private var namespacesMapping: Map<String, List<String>> = mapOf(
        "diat" to listOf("Beekeeper", "BeeHive", "Door", "DoorNumber", "SmartDoor", "Sensor", "Observation", "ObservedBy", "ManagedBy", "hasMeasure"),
        "ngsild" to listOf("connectsTo", "hasObject", "observedAt", "createdAt", "modifiedAt", "datasetId", "instanceId", "GeoProperty", "Point", "Property", "Relationship", "name"),
        "example" to listOf("availableSpotNumber", "OffStreetParking", "Vehicle", "isParked", "providedBy", "Camera") // this is property of property in order to allow nested property we need to add it to model
    )

    fun parseEntity(ngsiLdPayload: String): Pair<String, Pair<EntityStatements, RelationshipStatements>> {
        val entityMap: Map<String, Any> = gson.fromJson(ngsiLdPayload, object : TypeToken<Map<String, Any>>() {}.type)
        val entityUrn = entityMap["id"] as String

        val statements = transformNgsiLdToCypher(
            entityMap,
            null,
            null,
            mutableListOf(),
            mutableListOf()
        )

        return Pair(entityUrn, statements)
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
        val nodeEntity = Entity(
            nodeType,
            getAttributes(node).plus("uri" to nodeUuid),
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

                // a Relationship witemhout a object? not possible skip!
                if (content.get("object") == null)
                    continue

                content.get("object").let {
                    val urn: String = it.toString()
                    val typeObj = urn.split(":")[2]
                    val nsObj = getLabelNamespace(typeObj)

                    if (parentIsRelationship) {
                        parentAttribute?.let {
                            nodeEntity.ns = getLabelNamespace(parentAttribute)
                            val newStatements = buildInsert(
                                nodeEntity,
                                predicate,
                                Entity(typeObj, hashMapOf("uri" to urn), nsObj)
                            )
                            if (newStatements.first.isNotEmpty()) accEntityStatements.add(newStatements.first[0])
                            if (newStatements.second.isNotEmpty()) accRelationshipStatements.add(newStatements.second[0])
                        }
                    } else {
                        val ns = getLabelNamespace(typeObj)
                        val newStatements = buildInsert(
                            nodeEntity,
                            predicate,
                            Entity(typeObj, hashMapOf("uri" to urn), ns)
                        )
                        if (newStatements.first.isNotEmpty()) accEntityStatements.add(newStatements.first[0])
                        if (newStatements.second.isNotEmpty()) accRelationshipStatements.add(newStatements.second[0])
                    }

                    // DowntownParking can exist or not
                    val newStatements = buildInsert(
                        Entity(
                            rel,
                            hashMapOf("uri" to urn),
                            nsPredicate
                        ), null, null
                    )
                    if (newStatements.first.isNotEmpty()) accEntityStatements.add(newStatements.first[0])
                    if (newStatements.second.isNotEmpty()) accRelationshipStatements.add(newStatements.second[0])

                    // create random uri for mat rel
                    val uuid = UUID.randomUUID()
                    val str = uuid.toString()
                    // add materialized relationship NODE
                    val urnMatRel = "urn:$nsPredicate:$rel:$str"
                    if (hasAttributes(content)) {
                        // go deeper using the materialized rel Node
                        transformNgsiLdToCypher(
                            content,
                            urnMatRel,
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
                if (hasAttributes(content)) {
                    // has attributes or just value and type? if so store as attribute  (es. name and available spot number in vehicle)
                    logger.debug("this property has just type and value, it is already in node entity")
                } else {
                    // this property has one ore more nested objects ==> use the attr. key (es. availableSpotNumber) as object to create a Relationship between entity and Property
                    // MATERIALIZED PROPERTY
                    val labelObj = item.key
                    val nsObj = getLabelNamespace(labelObj)

                    // create uri for object
                    val uuid = UUID.randomUUID()
                    val str = uuid.toString()

                    val urn = "urn:$nsObj:$labelObj:$str"
                    // add to statement list SUBJECT -- RELATION [:hasObject] -- OBJECT
                    val predicate = "ngsild__hasObject"

                    // object attributes will be set in the next travestPropertiesIteration with a match on URI
                    // ADD THE RELATIONSHIP
                    val newStatements = buildInsert(
                        nodeEntity,
                        predicate,
                        Entity(labelObj, hashMapOf("uri" to urn), nsObj)
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
//            if (isGeoProperty(content)) {
//                val obj = item.key
//                val value = expandObjToMap(content.get("value"))
//                val coordinates = value.get("coordinates") as ArrayList<Double>
//                val lon = coordinates.get(0)
//                val lat = coordinates.get(1)
//                val location: String = "point({ x: $lon , y: $lat, crs: 'WGS-84' })"
//                nodeEntity.attrs.put(obj, location)
//            }
        }
        val newStatements = buildInsert(nodeEntity, null, null)
        if (newStatements.first.isNotEmpty()) accEntityStatements.add(newStatements.first[0])
        if (newStatements.second.isNotEmpty()) accRelationshipStatements.add(newStatements.second[0])

        return Pair(accEntityStatements, accRelationshipStatements)
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
            !listOf("type", "@context", "id").contains(key)
        }.filter {
            /*isGeoProperty(expandObjToMap(it.value)) ||*/ hasAttributes(expandObjToMap(it.value))
        }.mapValues {
            if (it.value is String) {
                it.value
            } else if (isGeoProperty(expandObjToMap(it.value))) {
                val value = expandObjToMap(it.value)
                val coordinates = value["coordinates"] as ArrayList<Double>
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

    private fun formatAttributes(attributes: Map<String, Any>): String {
        var attrs = gson.toJson(attributes)
        val p = Pattern.compile("\\\"(\\w+)\\\"\\:")
        attrs = p.matcher(attrs).replaceAll("$1:")
        attrs = attrs.replace("\n", "")
        return attrs
    }

    private fun buildInsert(subject: Entity, predicate: String?, obj: Entity?): Pair<EntityStatements, RelationshipStatements> {
        return if (predicate == null || obj == null) {
            val labelSubject = subject.getLabelWithPrefix()
            val attrsSubj = formatAttributes(subject.attrs)
            Pair(listOf("CREATE (a : $labelSubject $attrsSubj) return a"), emptyList())
        } else {
            val labelObj = obj.getLabelWithPrefix()
            val attrsObj = formatAttributes(obj.attrs)
            val labelSubject = subject.getLabelWithPrefix()
            val subjectUri = subject.attrs["uri"]!!
            val attrsSubj =
                formatAttributes(mapOf("uri" to subjectUri))
            Pair(emptyList(), listOf("MATCH (a : $labelSubject $attrsSubj), (b : $labelObj $attrsObj) CREATE (a)-[r:$predicate]->(b) return a,b"))
        }
    }

    private fun hasAttributes(node: Map<String, Any>): Boolean {
        // if a Property has just type and value we save it as attribute value in the parent entity
        return node.size == 1 || (node.size == 2 && node.containsKey("type") && node.containsKey("value"))
    }

    private fun expandObjToMap(obj: Any?): Map<String, Any> {
        return when (obj) {
            is Map<*,*> -> obj as Map<String, Any>
            is String -> mapOf(obj to obj)
            else -> emptyMap()
        }
    }

    private fun getLabelNamespace(label: String): String {
        namespacesMapping.forEach {
            if (it.value.contains(label)) {
                return it.key
            }
        }

        // fallback to default core NGSI-LD namespace
        // TODO : we should instead raise a 400-like exception
        return "ngsi-ld"
    }
}