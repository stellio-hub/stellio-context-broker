package com.egm.datahub.context.registry.service

import com.egm.datahub.context.registry.model.Observation
import com.egm.datahub.context.registry.repository.Neo4jRepository
import com.google.gson.GsonBuilder
import org.neo4j.ogm.response.model.NodeModel
import org.neo4j.ogm.response.model.RelationshipModel
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

@Component
class Neo4jService(
    private val neo4jRepository: Neo4jRepository
) {
    private val gson = GsonBuilder().setPrettyPrinting().create()
    private val logger = LoggerFactory.getLogger(Neo4jService::class.java)
    private fun iterateOverRelationships(nodeModel: NodeModel): Map<String, Any> {
        val uriProperty = nodeModel.propertyList.find { it.key == "uri" }!!
        var inneroutput = mutableMapOf<String, Any>()
        // get relationships
        val relationships = neo4jRepository.getRelationshipByURI(uriProperty.value.toString())
        // logger.info("relationships to be mocked for " + uriProperty.value.toString())
        // logger.info(gson.toJson(relationships))
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
            // logger.info("matRelNode to be mocked for " + matRelUri)
            // logger.info(gson.toJson(matRelNode))
            if (!matRelNode.isEmpty()) {
                val nestedObj1 = iterateOverRelationships(matRelNode["n"] as NodeModel)
                val nestedObj2 = iterateOverProperties(matRelNode["n"] as NodeModel)
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

    private fun iterateOverProperties(nodeModel: NodeModel): Map<String, Any> {
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
                val nestedObj1 = iterateOverRelationships(nestedPropNode["n"] as NodeModel)
                val nestedObj2 = iterateOverProperties(nestedPropNode["n"] as NodeModel)
                nestedPropertyMap.putAll(nestedObj1)
                nestedPropertyMap.putAll(nestedObj2)
            }

            inneroutput.put(attrname, nestedPropertyMap)
        }
        return inneroutput
    }

    fun queryResultToNgsiLd(nodeModel: NodeModel): Map<String, Any> {

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

        val rr = iterateOverRelationships(nodeModel)
        val pp = iterateOverProperties(nodeModel)

        return idAndType.plus(rr).plus(pp).plus(properties).plus(NgsiLdParserService.contextsMap)
    }

    fun updateEntityLastMeasure(observation: Observation) {
        val observingEntity = neo4jRepository.getNodeByURI(observation.observedBy.target)
        if (observingEntity.isEmpty()) {
            logger.warn("Unable to find observing entity ${observation.observedBy.target} for observation ${observation.id}")
            return
        }
        val observingNode = observingEntity["n"] as NodeModel

        // Find the previous observation of the same unit for the given sensor, then delete it
        val previousMeasureQuery = listOf("observedBy==${observation.observedBy.target}", "unitCode==${observation.unitCode}")
        val previousMeasures =
            neo4jRepository.getEntitiesByLabelAndQuery(previousMeasureQuery, "Measure")
        if (previousMeasures.isEmpty()) {
            logger.debug("No previous observation of kind ${observation.unitCode} for ${observation.observedBy.target}")
            val newMeasureQuery = """
                CREATE (m:Measure { 
                            uri: "${observation.id}", 
                            unitCode: "${observation.unitCode}",
                            value: ${observation.value},
                            observedAt: "${observation.observedAt}",
                            location: point({ x: ${observation.location.value.coordinates[0]}, 
                                              y: ${observation.location.value.coordinates[1]}, 
                                              crs: "WGS-84"
                                            })
                       })           
            """.trimIndent()

            val observedByRelationship = """
                MATCH (m : Measure { uri: "${observation.id}" }),
                      (e : ${observingNode.labels[0]} { uri: "${observingNode.property("uri")}" }) 
                MERGE (e)-[r1:hasMeasure]->(m)
                MERGE (m)-[r2:observedBy]->(e)
            """.trimIndent()

            neo4jRepository.createEntity(observation.id, listOf(newMeasureQuery), listOf(observedByRelationship))
        } else {
            // we should only have one result
            // TODO delete all the found measures in case something gone wild in the repository ?
            val previousMeasure = previousMeasures[0]["n"] as NodeModel
            val updateMeasureQuery = """
                MATCH (m:Measure { uri: '${previousMeasure.property("uri")}' })
                SET m.uri = "${observation.id}",
                    m.value = ${observation.value},
                    m.observedAt = "${observation.observedAt}",
                    m.location = point({ x: ${observation.location.value.coordinates[0]}, 
                                         y: ${observation.location.value.coordinates[1]}, 
                                         crs: "WGS-84"
                                       })
            """.trimIndent()

            neo4jRepository.updateEntity(updateMeasureQuery, previousMeasure.property("uri") as String)
        }
    }
}
