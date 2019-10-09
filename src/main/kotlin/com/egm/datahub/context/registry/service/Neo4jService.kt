package com.egm.datahub.context.registry.service

import com.egm.datahub.context.registry.repository.Neo4jRepository
import org.neo4j.ogm.response.model.NodeModel
import org.neo4j.ogm.response.model.RelationshipModel
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

@Component
class Neo4jService(
    private val neo4jRepository: Neo4jRepository
) {
    private val logger = LoggerFactory.getLogger(Neo4jService::class.java)
    private fun iterateOverRelationships(node: Map<String, Any>): Map<String, Any> {
        val nodeModel = node["n"] as NodeModel
        val uriProperty = nodeModel.propertyList.find { it.key == "uri" }!!
        var inneroutput = mutableMapOf<String, Any>()
        // get relationships
        val relationships = neo4jRepository.getRelationshipByURI(uriProperty.value.toString())
        // add relationships
        relationships.map {
            val target = it.get("t") as NodeModel
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
    fun queryResultToNgsiLd(queryResult: Map<String, Any>): Map<String, Any> {
        val nodeModel = queryResult["n"] as NodeModel
        val uriProperty = nodeModel.propertyList.find { it.key == "uri" }!!
        logger.debug("Transforming node ${nodeModel.id} ($uriProperty)")

        val properties = nodeModel.propertyList
            .filter { it.key != "uri" && it.key != "location" && it.value != "" }
            .map {
                it.key to it.value
            }

        val idAndType = mutableMapOf(
            "id" to uriProperty.value,
            "type" to nodeModel.labels[0].split("__")[1]
        )

        val location = nodeModel.propertyList.find { it.key == "location" }
        if (location != null) {
            val lon = location.value.toString().split("x:")[1].split(",")[0].toDouble()
            val lat = location.value.toString().split("y:")[1].split(",")[0].toDouble()
            idAndType.put("location", mapOf("type" to "GeoProperty", "value" to mapOf("type" to "Point", "coordinates" to arrayOf(lon, lat))))
        }

        // go deeper

        val rr = iterateOverRelationships(queryResult)
        val pp = iterateOverProperties(queryResult)

        return idAndType.plus(rr).plus(pp).plus(properties).plus(NgsiLdParserService.contextsMap)
    }
}