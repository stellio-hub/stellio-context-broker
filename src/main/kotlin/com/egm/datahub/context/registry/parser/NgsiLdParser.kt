package com.egm.datahub.context.registry.parser

import com.google.gson.GsonBuilder
import com.google.gson.reflect.TypeToken
import org.slf4j.LoggerFactory
import java.util.*
import java.util.regex.Pattern
import kotlin.collections.ArrayList
import kotlin.collections.HashMap

class Entity(var label: String, var attrs: HashMap<String, Any>, var ns: String?) {

    fun getLabelWithPrefix(): String {
        return this.ns + "__" + this.label
    }
}

typealias EntityStatements = List<String>
typealias RelationshipStatements = List<String>

object NgsiLdParser {

    private val gson = GsonBuilder().setPrettyPrinting().create()
    private val logger = LoggerFactory.getLogger(NgsiLdParser::class.java)

    fun parseEntity(entity: Map<String, Any>, namespacesMapping: Map<String, List<String>>): Pair<EntityStatements, RelationshipStatements> {
        return travestNgsiLd(entity, namespacesMapping, null, null, mutableListOf(), mutableListOf())
    }

    fun travestNgsiLd(node: Map<String, Any>, namespacesMapping: Map<String, List<String>>, uuid: String?, parentAttribute: String?,
                      accEntityStatements: MutableList<String>, accRelationshipStatements: MutableList<String>):
            Pair<EntityStatements, RelationshipStatements> {

        logger.info("traversing node with label {}", node["type"])

        val parentIsRelationship: Boolean = node["type"]?.toString().equals("Relationship")

        val nsSubj: String? = getLabelNamespace(node["type"].toString(), namespacesMapping)
        val nodeEntity = Entity(node["type"].toString(), getAttributes(node), nsSubj)
        if (node.containsKey("id")) {
            nodeEntity.attrs["uri"] = node["id"].toString()
        } else {
            uuid?.let { nodeEntity.attrs.put("uri", it) }
        }

        // if is Property override the generic Property type with the attribute
        if (node["type"].toString() == "Property") {
            parentAttribute?.let {
                nodeEntity.ns = getLabelNamespace(it, namespacesMapping)
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
                val nsPredicate = getLabelNamespace(rel, namespacesMapping)
                val predicate = nsPredicate + "__" + rel

                // a Relationship witemhout a object? not possible skip!
                if (content.get("object") == null)
                    continue

                content.get("object").let {
                    val urn: String = it.toString()
                    val typeObj = urn.split(":")[2]
                    val nsObj = getLabelNamespace(typeObj, namespacesMapping)

                    if (parentIsRelationship) {
                        parentAttribute?.let {
                            nodeEntity.ns = getLabelNamespace(parentAttribute, namespacesMapping)
                            val newStatements = buildInsert(nodeEntity, predicate, Entity(typeObj, hashMapOf("uri" to urn), nsObj))
                            if (newStatements.first.isNotEmpty()) accEntityStatements.add(newStatements.first[0])
                            if (newStatements.second.isNotEmpty()) accRelationshipStatements.add(newStatements.second[0])
                        }
                    } else {
                        val ns = getLabelNamespace(typeObj, namespacesMapping)
                        val newStatements = buildInsert(nodeEntity, predicate, Entity(typeObj, hashMapOf("uri" to urn), ns))
                        if (newStatements.first.isNotEmpty()) accEntityStatements.add(newStatements.first[0])
                        if (newStatements.second.isNotEmpty()) accRelationshipStatements.add(newStatements.second[0])
                    }

                    // DowntownParking can exist or not
                    val newStatements = buildInsert(Entity(rel, hashMapOf("uri" to urn), nsPredicate), null, null)
                    if (newStatements.first.isNotEmpty()) accEntityStatements.add(newStatements.first[0])
                    if (newStatements.second.isNotEmpty()) accRelationshipStatements.add(newStatements.second[0])

                    // create random uri for mat rel
                    val uuid = UUID.randomUUID()
                    val str = uuid.toString()
                    // add materialized relationship NODE
                    val urnMatRel = "urn:$nsPredicate:$rel:$str"
                    if (hasAttributes(content)) {
                        // go deeper using the materialized rel Node
                        travestNgsiLd(content, namespacesMapping, urnMatRel, item.key, accEntityStatements, accRelationshipStatements)
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
                    val nsObj = getLabelNamespace(labelObj, namespacesMapping)

                    // create uri for object
                    val uuid = UUID.randomUUID()
                    val str = uuid.toString()

                    val urn = "urn:$nsObj:$labelObj:$str"
                    // add to statement list SUBJECT -- RELATION [:hasObject] -- OBJECT
                    val predicate = "ngsild__hasObject"

                    // object attributes will be set in the next travestPropertiesIteration with a match on URI
                    // ADD THE RELATIONSHIP
                    val newStatements = buildInsert(nodeEntity, predicate, Entity(labelObj, hashMapOf("uri" to urn), nsObj))
                    if (newStatements.first.isNotEmpty()) accEntityStatements.add(newStatements.first[0])
                    if (newStatements.second.isNotEmpty()) accRelationshipStatements.add(newStatements.second[0])

                    // go deeper
                    travestNgsiLd(content, namespacesMapping, urn, item.key, accEntityStatements, accRelationshipStatements)
                }
            }
            if (isGeoProperty(content)) {
                val obj = item.key
                val value = expandObjToMap(content.get("value"))
                val coordinates = value.get("coordinates") as ArrayList<Double>
                val lon = coordinates.get(0)
                val lat = coordinates.get(1)
                val location: String = "point({ x: $lon , y: $lat, crs: 'WGS-84' })"
                nodeEntity.attrs.put(obj, location)
            }
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

    private fun getAttributes(node: Map<String, Any>): HashMap<String, Any> {
        var out = HashMap<String, Any>()
        for (item in node) {
            if (!item.key.equals("type") && !item.key.equals("value") && !item.key.equals("@context") && !item.key.equals("id")) {
                if (item.value is String) {
                    out.put(item.key, item.value)
                } else {
                    if (isGeoProperty(expandObjToMap(item.value))) continue
                    // check if Nested Attributes es:
                    /*"brandName": {
                        "type": "Property",
                        "value": "Mercedes"
                    },*/
                    if (!hasAttributes(expandObjToMap(item.value))) {
                        var attrName = item.key
                        val innerValue = expandObjToMap(item.value).get("value")
                        innerValue?.let {
                            out.put(attrName, it)
                        }
                    }
                }
            }
        }
        return out
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
            val attrsSubj = formatAttributes(mapOf("uri" to subjectUri))
            Pair(emptyList(), listOf("MATCH (a : $labelSubject $attrsSubj), (b : $labelObj $attrsObj) CREATE (a)-[r:$predicate]->(b) return a,b"))
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

    private fun expandObjToMap(obj: Any?): Map<String, Any> {
        try {
            if (obj is String) {
                // FIXME : it always raises an exception
                return gson.fromJson(obj.toString(), object : TypeToken<Map<String, Any>>() {}.type)
            } else {
                return obj as Map<String, Object>
            }
        } catch (e: Exception) {
            return emptyMap()
        }
    }

    private fun getLabelNamespace(label: String, namespacesMapping: Map<String, List<String>>): String? {
        namespacesMapping.forEach {
            if (it.value.contains(label)) {
                return it.key
            }
        }
        return null
    }
}