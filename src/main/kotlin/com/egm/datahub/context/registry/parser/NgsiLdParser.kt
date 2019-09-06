package com.egm.datahub.context.registry.parser

import com.google.gson.GsonBuilder
import com.google.gson.reflect.TypeToken
import org.slf4j.LoggerFactory
import java.util.*
import java.util.regex.Pattern
import kotlin.collections.ArrayList
import kotlin.collections.HashMap

class Statement(subject: String, predicate: String?, obj: String?) {
    val subject = subject
    val predicate = predicate
    val obj = obj
}

class NgsiLdParser(
    val entity: Map<String, Any>?,
    val relationshipLabels: List<String>?,
    val nodeLabels: List<String>?,
    var entityStatements: ArrayList<Statement>?,
    var relStatements: ArrayList<Statement>?

) {

    data class Builder(
        var entity: Map<String, Any>? = HashMap(),
        var relationshipLabels: MutableList<String>? = ArrayList(),
        var nodeLabels: MutableList<String>? = ArrayList(),
        var properties: MutableList<Map<String, Any>>? = ArrayList(),
        var entityStatements: ArrayList<Statement> = ArrayList<Statement>(),
        var relStatements: ArrayList<Statement> = ArrayList<Statement>()

    ) {
        private val gson = GsonBuilder().setPrettyPrinting().create()
        private val logger = LoggerFactory.getLogger(NgsiLdParser::class.java)

        private var context: Map<String, Any> = emptyMap()
        // TO EXTERNALIZE?  var namespacesMapping : Map<String,List<String>>
        private var namespacesMapping: Map<String, List<String>> = mapOf(
            "diat" to listOf("Beekeeper", "BeeHive", "Door", "DoorNumber", "SmartDoor", "Sensor", "Observation"),
            "ngsild" to listOf("connectsTo", "hasObject", "observedAt", "createdAt", "modifiedAt", "datasetId", "instanceId", "GeoProperty", "Point", "Property", "Relationship"),
            "example" to listOf("availableSpotNumber", "OffStreetParking", "Vehicle") // this is property of property in order to allow nested property we need to add it to model
        )

        fun entity(entity: Map<String, Any>) = apply {
            this.entity = entity
            // scan entity attributes
            travestContext(expandObjToMap(entity.get("@context")))
            travestNgsiLd(entity)
        }

        /*fun withNamespaces(namespaces: Map<String,List<String>>) = apply {
            this.namespacesMapping = namespacesMapping
        }*/
        fun getNamespaceByLabel(label: String): String? {
            this.namespacesMapping.forEach {
                if (it.value.contains(label)) {
                    return it.key
                }
            }
            return null
        }

        fun travestContext(context: Any) {
            // TODO
            try {
                val obj = context as Map<String, Object>
            } catch (e: Exception) {
            }
            try {
                val obj = context as List<String>
            } catch (e: Exception) {
            }
        }

        fun travestNgsiLd(entity: Map<String, Any>) {
            travestProperties(entity, null, null)
        }

        fun travestProperties(node: Map<String, Any>, uuid: String?, parentAttribute: String?) {
            var attributes: HashMap<String, Any> = getAttributes(node)
            var uriSubj: String? = null
            if (node.containsKey("id")) {
                uriSubj = node.get("id").toString()
                attributes.put("uri", uriSubj)
            } else {
                uuid?.let { attributes.put("uri", it) }
            }
            var attrs = gson.toJson(attributes)
            val nsSubj: String? = getNamespaceByLabel(node.get("type").toString())

            // if is Property override the Property type
            var typeSubj = nsSubj + "__" + node.get("type").toString()
            if (node.get("type").toString().equals("Property")) {
                parentAttribute?.let {
                    val ns = getNamespaceByLabel(parentAttribute)
                    typeSubj = ns + "__" + parentAttribute
                }
            }

            node.forEach {
                // foreach attribute get the Map and check type is Property
                logger.info("attribute : " + it.key)
                val content = expandObjToMap(it.value)
                if (isProperty(content)) {
                    logger.info(it.key + " is property")
                    // add to statement list SUBJECT -- RELATION [:hasObject] -- OBJECT
                    val predicate = "ngsild__hasObject"
                    // is not a map or the only attributes are type and value
                    if (!hasAttributes(content)) {
                        // has attributes or just value and type? if so store as attribute  (es. name and available spot number in vehicle)
                        logger.info("this property has just type and value")

                        val value = content.get("value")
                        val obj = it.key
                        value?.let { attributes.put(obj, value) }
                    } else {
                        // this property has one ore more nested objects ==> use the attr. key (es. availableSpotNumber) as object to create a Relationship between entity and Property
                        // MATERIALIZED PROPERTY
                        val nsObj = getNamespaceByLabel(it.key)
                        val typeObj = nsObj + "__" + it.key
                        // subject attributes
                        val attrs = gson.toJson(attributes)
                        var subject = "$typeSubj {uri : '$uriSubj'}"

                        // create uri for object
                        val uuid = UUID.randomUUID()
                        val str = uuid.toString()

                        val urn = "urn:$nsObj:$typeObj:$str"
                        val objAttrs = "{uri : '$urn'}"
                        // object attributes will be set in the next travestPropertiesIteration with a match on URI
                        // ADD THE RELATIONSHIP
                        relStatements.add(Statement(subject, predicate, "$typeObj $objAttrs"))
                        // go deeper
                        travestProperties(content, urn, it.key)
                    }
                }
                if(isGeoProperty(content)){
                    val obj = it.key
                    val value = expandObjToMap(content.get("value"))
                    val coordinates = value.get("coordinates")
                    val coords = coordinates as Array<Double>
                    val lon = coords[0]
                    val lat = coords[1]
                    val location : String ="point({ x: $lon , y: $lat, crs: 'WGS-84' })"
                    attributes.put("location",location)
                }
            }
            val p = Pattern.compile("\\\"(\\w+)\\\"\\:")
            attrs = p.matcher(attrs).replaceAll("$1:")
            attrs = attrs.replace("\n", "")
            entityStatements.add(Statement("$typeSubj $attrs", null, null))
        }

        fun isProperty(prop: Map<String, Any>): Boolean {
            return if (prop.get("type")?.toString().equals("Property")) true else false
        }
        fun isGeoProperty(prop: Map<String, Any>): Boolean{
            return if (prop.get("type")?.toString().equals("GeoProperty")) true else false
        }

        fun isRelationship(prop: Map<String, Any>): Boolean {
            return if (prop.get("type")?.toString().equals("Relationship")) true else false
        }

        fun getAttributes(node: Map<String, Any>?): HashMap<String, Any> {
            var out = HashMap<String, Any>()
            node?.forEach {
                if (!it.key.equals("type") && !it.key.equals("value") && !it.key.equals("@context") && !it.key.equals("id")) {
                    // check if entry.value has Attributes

                    if (!hasAttributes(expandObjToMap(it.value))) {
                        var attrName = it.key
                        val innerValue = expandObjToMap(it.value).get("value")
                        innerValue?.let {
                            out.put(attrName, it)
                        }
                    }
                }
            }
            return out
        }

        fun hasAttributes(node: Map<String, Any>): Boolean {
            // if a Property has just type and value we save it as attribute value in the parent entity
            var resp = false
            node.forEach {
                if (!it.key.equals("type") && !it.key.equals("value") && !it.key.equals("id")) {
                    resp = true
                }
            }
            return resp
        }

        fun expandObjToMap(obj: Any?): Map<String, Any> {
            try {
                if (obj is String) {
                    return gson.fromJson(obj.toString(), object : TypeToken<Map<String, Any>>() {}.type)
                } else {
                    return obj as Map<String, Object>
                }
            } catch (e: Exception) {
                return emptyMap()
            }
        }

        fun build() = NgsiLdParser(entity, nodeLabels, relationshipLabels, entityStatements, relStatements)
    }
}