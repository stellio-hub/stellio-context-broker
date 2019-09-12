package com.egm.datahub.context.registry.parser

import com.google.gson.GsonBuilder
import com.google.gson.reflect.TypeToken
import org.slf4j.LoggerFactory
import java.util.*
import java.util.regex.Pattern
import kotlin.collections.ArrayList
import kotlin.collections.HashMap



class Entity(label: String, attrs: HashMap<String,Any>, ns : String?) {
    var attrs = attrs
    var label = label
    var ns = ns
    fun getLabelWithPrefix() : String{
        return this.ns +"__"+this.label
    }
}

class NgsiLdParser(
    //val entity: Map<String, Any>?,
    //var namespacesMapping:  Map<String, List<String>>?,
    var entityStatements: ArrayList<String>?,
    var relStatements: ArrayList<String>?

) {

    data class Builder(
        var entity: Map<String, Any>? = HashMap(),
        var namespacesMapping:  Map<String, List<String>>? = HashMap(),
        var entityStatements: ArrayList<String> = ArrayList<String>(),
        var relStatements: ArrayList<String> = ArrayList<String>()

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

            logger.info("traversing node with label {} **** ", node.get("type"))

            val parentIsRelationship : Boolean =  if (node.get("type")?.toString().equals("Relationship"))   true else  false

            val nsSubj: String? = getNamespaceByLabel(node.get("type").toString())
            var nodeEntity = Entity(node.get("type").toString(),getAttributes(node), nsSubj)
            if (node.containsKey("id")) {
                nodeEntity.attrs.put("uri", node.get("id").toString())
            } else {
                uuid?.let { nodeEntity.attrs.put("uri", it) }
            }

            // if is Property override the generic Property type with the attribute
            if (node.get("type").toString().equals("Property")) {
                parentAttribute?.let {
                    nodeEntity.ns = getNamespaceByLabel(it)
                    nodeEntity.label = parentAttribute
                }
            }
            //var typeSubj = nodeEntity.getLabelWithPrefix()
           // var subject = "$typeSubj {uri : '$uriSubj'}"

            for (item in node) {
                // foreach attribute get the Map and check type is Property
                val content = expandObjToMap(item.value)
                if(isAttribute(content)){
                    logger.debug(item.key + " is attribute")
                    // add to attr map
                }
                if (isRelationship(content)) {
                    logger.debug(item.key + " is relationship")
                    //THIS IS THE NODE --> REL --> NODE (object)
                    val rel = item.key
                    val nsPredicate = getNamespaceByLabel(rel)
                    val predicate = nsPredicate + "__" +rel

                    //a Relationship witemhout a object? not possible skip!
                    if (content.get("object") == null)
                        continue

                    content.get("object").let {
                        val urn : String = it.toString()
                        val typeObj = urn.split(":")[2]
                        val nsObj = getNamespaceByLabel(typeObj)

                        if(parentIsRelationship){
                            parentAttribute?.let {
                                nodeEntity.ns = getNamespaceByLabel(parentAttribute)
                                buildInsert(nodeEntity,predicate, Entity(typeObj, hashMapOf("uri" to urn), nsObj))
                            }

                        } else{
                            val ns = getNamespaceByLabel(typeObj)
                            buildInsert(nodeEntity,predicate, Entity(typeObj, hashMapOf("uri" to urn),ns))
                        }

                        // DowntownParking can exist or not
                        buildInsert(Entity(rel,hashMapOf("uri" to urn), nsPredicate), null, null)

                        //create random uri for mat rel
                        val uuid = UUID.randomUUID()
                        val str = uuid.toString()
                        //add materialized relationship NODE
                        val urnMatRel = "urn:$nsPredicate:$rel:$str"
                        if (hasAttributes(content)) {
                            // go deeper using the materialized rel Node
                            travestNgsiLd(content, urnMatRel, item.key)
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
                        val nsObj = getNamespaceByLabel(labelObj)

                        // create uri for object
                        val uuid = UUID.randomUUID()
                        val str = uuid.toString()

                        val urn = "urn:$nsObj:$labelObj:$str"
                        // add to statement list SUBJECT -- RELATION [:hasObject] -- OBJECT
                        val predicate = "ngsild__hasObject"

                        // object attributes will be set in the next travestPropertiesIteration with a match on URI
                        // ADD THE RELATIONSHIP
                        buildInsert(nodeEntity,predicate, Entity(labelObj, hashMapOf("uri" to urn), nsObj))
                        // go deeper
                        travestNgsiLd(content, urn, item.key)
                    }
                }
                if(isGeoProperty(content)){
                    val obj = item.key
                    val value = expandObjToMap(content.get("value"))
                    val coordinates  = value.get("coordinates") as ArrayList<Double>
                    val lon = coordinates.get(0)
                    val lat = coordinates.get(1)
                    val location : String ="point({ x: $lon , y: $lat, crs: 'WGS-84' })"
                    nodeEntity.attrs.put(obj,location)
                }
            }
            buildInsert(nodeEntity, null, null)
        }

        fun isProperty(prop: Map<String, Any>): Boolean {
            return if (prop.get("type")?.toString().equals("Property")) true else false
        }
        fun isGeoProperty(prop: Map<String, Any>): Boolean{
            return if (prop.get("type")?.toString().equals("GeoProperty")) true else false
        }
        fun isPoint(prop: Map<String, Any>): Boolean{
            return if (prop.get("type")?.toString().equals("Point")) true else false
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

        fun getAttributes(node: Map<String, Any>): HashMap<String, Any> {
            var out = HashMap<String, Any>()
            for(item in node) {
                if (!item.key.equals("type") && !item.key.equals("value") && !item.key.equals("@context") && !item.key.equals("id") ) {
                    if(item.value is String){
                        out.put(item.key, item.value)
                    }
                    else{
                        if( isGeoProperty(expandObjToMap(item.value))) continue
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
        fun formatAttributes(attributes  : Map<String,Any>) : String{
            var attrs = gson.toJson(attributes)
            var p = Pattern.compile("\\\"(\\w+)\\\"\\:")
            attrs = p.matcher(attrs).replaceAll("$1:")
            attrs = attrs.replace("\n", "")
            return attrs
        }
        fun buildInsert(subject: Entity, predicate: String?, obj: Entity?) {
            if (predicate == null || obj == null) {
                val labelSubject = subject.getLabelWithPrefix()
                val attrsSubj = formatAttributes(subject.attrs)
                entityStatements.add("CREATE (a : $labelSubject $attrsSubj) return a")
            }
            else{
                val labelObj = obj.getLabelWithPrefix()
                val attrsObj = formatAttributes(obj.attrs)
                val labelSubject = subject.getLabelWithPrefix()
                subject.attrs.get("uri")?.let {
                    val attrsSubj = formatAttributes(mapOf("uri" to it))
                    relStatements.add("MATCH (a : $labelSubject $attrsSubj), (b : $labelObj $attrsObj) CREATE (a)-[r:$predicate]->(b) return a,b")
                }
            }
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