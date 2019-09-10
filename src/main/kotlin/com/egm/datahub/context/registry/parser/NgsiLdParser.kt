package com.egm.datahub.context.registry.parser

import com.google.gson.GsonBuilder
import com.google.gson.reflect.TypeToken
import org.slf4j.LoggerFactory
import java.util.*
import kotlin.collections.ArrayList
import kotlin.collections.HashMap



class Statement(subject: Entity, predicate: String?, obj: Entity?) {
    val subject = subject
    val predicate = predicate
    val obj = obj
}
class Entity(label: String, attrs: Map<String,Any>) {
    val attrs = attrs
    val label = label
}

class NgsiLdParser(
    //val entity: Map<String, Any>?,
    //var namespacesMapping:  Map<String, List<String>>?,
    var entityStatements: ArrayList<Statement>?,
    var relStatements: ArrayList<Statement>?

) {

    data class Builder(
        var entity: Map<String, Any>? = HashMap(),
        var namespacesMapping:  Map<String, List<String>>? = HashMap(),
        var entityStatements: ArrayList<Statement> = ArrayList<Statement>(),
        var relStatements: ArrayList<Statement> = ArrayList<Statement>(),
        var matRelStatements: ArrayList<Statement> = ArrayList<Statement>()

    ) {
        private val gson = GsonBuilder().setPrettyPrinting().create()
        private val logger = LoggerFactory.getLogger(NgsiLdParser::class.java)

        private var context: Map<String, Any> = emptyMap()
        // TO EXTERNALIZE?  var namespacesMapping : Map<String,List<String>>

        fun withContext(namespacesMapping: Map<String, List<String>>) = apply {
            this.namespacesMapping = namespacesMapping
        }
        fun entity(entity: Map<String, Any>) = apply {
            this.entity = entity
            // scan entity attributes
            travestContext(expandObjToMap(entity.get("@context")))
            travestNgsiLd(entity, null, null)
        }

        /*fun withNamespaces(namespaces: Map<String,List<String>>) = apply {
            this.namespacesMapping = namespacesMapping
        }*/
        fun getNamespaceByLabel(label: String): String? {
            this.namespacesMapping?.forEach {
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


        fun travestNgsiLd(node: Map<String, Any>, uuid: String?, parentAttribute: String?) {

            logger.info("travesting node : "+node.get("type"))

            val parentIsRelationship : Boolean =  if (node.get("type")?.toString().equals("Relationship"))   true else  false

            var attributes: HashMap<String, Any> = getAttributes(node)
            var uriSubj: String? = null
            if (node.containsKey("id")) {
                uriSubj = node.get("id").toString()
                attributes.put("uri", uriSubj)
            } else {
                uuid?.let { attributes.put("uri", it) }
            }

            val nsSubj: String? = getNamespaceByLabel(node.get("type").toString())

            // if is Property override the Property type
            var typeSubj = nsSubj + "__" + node.get("type").toString()
            if (node.get("type").toString().equals("Property")) {
                parentAttribute?.let {
                    val ns = getNamespaceByLabel(parentAttribute)
                    typeSubj = ns + "__" + parentAttribute
                }
            }
            var subject = "$typeSubj {uri : '$uriSubj'}"

            node.forEach {
                // foreach attribute get the Map and check type is Property
                val content = expandObjToMap(it.value)
                if(isAttribute(content)){
                    logger.debug(it.key + " is attribute")
                    // add to attr map
                }
                if (isRelationship(content)) {
                    logger.debug(it.key + " is relationship")
                    //THIS IS THE NODE --> REL --> NODE (object)
                    val rel = it.key
                    val ns = getNamespaceByLabel(rel)
                    val predicate = ns + "__" +rel

                    val urn : String= content.get("object") as String
                    val typeObj = urn.split(":")[2]
                    if(parentIsRelationship){
                        parentAttribute?.let {
                            val ns = getNamespaceByLabel(parentAttribute)
                            val parentSubj = ns + "__" + parentAttribute
                            relStatements.add(Statement(Entity(parentSubj, emptyMap()),predicate, Entity(typeObj, mapOf("uri" to urn))))
                        }

                    } else{
                        val ns = getNamespaceByLabel(typeObj)
                        val typeObj = ns + "__" + typeObj
                        relStatements.add(Statement(Entity(typeSubj, emptyMap()),predicate, Entity(typeObj, mapOf("uri" to urn))))
                    }

                    // DowntownParking can exist or not
                    entityStatements.add(Statement(Entity(predicate,mapOf("uri" to urn)), null, null))

                    //create random uri for mat rel
                    val uuid = UUID.randomUUID()
                    val str = uuid.toString()
                    //add materialized relationship NODE
                    val urnMatRel = "urn:$ns:$rel:$str"
                    //matRelStatements.add(Statement("$predicate {uri : $urnMatRel} ", null, null))

                    if (hasAttributes(content)) {
                        // go deeper using the materialized rel Node
                        travestNgsiLd(content, urnMatRel, it.key)
                    }
                }
                if (isProperty(content)) {
                    logger.debug(it.key + " is property")
                    // add to statement list SUBJECT -- RELATION [:hasObject] -- OBJECT
                    val predicate = "ngsild__hasObject"
                    // is not a map or the only attributes are type and value
                    if (!hasAttributes(content)) {
                        // has attributes or just value and type? if so store as attribute  (es. name and available spot number in vehicle)
                        logger.debug("this property has just type and value")

                        val value = content.get("value")
                        val obj = it.key
                        value?.let { attributes.put(obj, value) }
                    } else {
                        // this property has one ore more nested objects ==> use the attr. key (es. availableSpotNumber) as object to create a Relationship between entity and Property
                        // MATERIALIZED PROPERTY
                        val nsObj = getNamespaceByLabel(it.key)
                        val type = it.key
                        val typeObj = nsObj + "__" + type

                        // create uri for object
                        val uuid = UUID.randomUUID()
                        val str = uuid.toString()

                        val urn = "urn:$nsObj:$typeObj:$str"
                        // object attributes will be set in the next travestPropertiesIteration with a match on URI
                        // ADD THE RELATIONSHIP
                        logger.info("$subject hasObject")
                        relStatements.add(Statement(Entity(typeSubj, attributes),predicate, Entity(typeObj, mapOf("uri" to urn))))
                        // go deeper
                        travestNgsiLd(content, urn, it.key)
                    }
                }
                if(isGeoProperty(content)){
                    val obj = it.key
                    val value = expandObjToMap(content.get("value"))
                    val coordinates  = value.get("coordinates") as ArrayList<Double>
                    val lon = coordinates.get(0)
                    val lat = coordinates.get(1)
                    val location : String ="point({ x: $lon , y: $lat, crs: 'WGS-84' })"
                    attributes.put(obj,location)
                }
            }

            /*var attrs = gson.toJson(attributes)
            var p = Pattern.compile("\\\"(\\w+)\\\"\\:")
            attrs = p.matcher(attrs).replaceAll("$1:")*/

            //attrs = attrs.replace("(\"point([^\"]|\"\")*\")", "")

            entityStatements.add(Statement(Entity(typeSubj,attributes), null, null))
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
        fun isAttribute(prop: Map<String, Any>): Boolean {
            val type = prop.get("type")?.toString()
            if(type.equals("Property") || type.equals("Relationship") || type.equals("GeoProperty")) return false
            if (prop.isEmpty()) return true
            return if (hasAttributes(prop)) true else false
        }

        fun getAttributes(node: Map<String, Any>?): HashMap<String, Any> {
            var out = HashMap<String, Any>()
            node?.forEach {
                if (!it.key.equals("type") && !it.key.equals("value") && !it.key.equals("@context") && !it.key.equals("id")) {
                    if(it.value is String){
                        out.put(it.key, it.value)
                    }
                    else{
                        // check if Nested Attributes es:
                        /*"brandName": {
                            "type": "Property",
                            "value": "Mercedes"
                        },*/
                        if (!hasAttributes(expandObjToMap(it.value))) {
                            var attrName = it.key
                            val innerValue = expandObjToMap(it.value).get("value")
                            innerValue?.let {
                                out.put(attrName, it)
                            }
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
                if (!it.key.equals("type") && !it.key.equals("value") && !it.key.equals("object") && !it.key.equals("id")) {
                    resp = true
                    return resp
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

        fun build() = NgsiLdParser(entityStatements, relStatements)
    }
}