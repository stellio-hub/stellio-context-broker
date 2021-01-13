package com.egm.stellio.entity.service

import com.egm.stellio.entity.model.AttributeInfo
import com.egm.stellio.entity.repository.Neo4jRepository
import com.egm.stellio.shared.model.JsonLdEntity
import com.egm.stellio.shared.util.JsonLdUtils
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdEntity
import com.egm.stellio.shared.util.toUri
import com.ninjasquad.springmockk.MockkBean
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = [EntityTypeService::class])
@ActiveProfiles("test")
class EntityTypeServiceTests {

    @Autowired
    private lateinit var entityTypeService: EntityTypeService

    @MockkBean(relaxed = true)
    private lateinit var entityService: EntityService

    @MockkBean(relaxed = true)
    private lateinit var neo4jRepository: Neo4jRepository

    val apicContext = listOf(
        "https://raw.githubusercontent.com/easy-global-market/ngsild-api-data-models/master/shared-jsonld-contexts/" +
            "egm.jsonld",
        "https://raw.githubusercontent.com/easy-global-market/ngsild-api-data-models/master/apic/jsonld-contexts/" +
            "apic.jsonld",
        "http://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"
    )
    val firstBeehive =
        """
        {
           "id":"urn:ngsi-ld:BeeHive:TESTC",
           "type":"BeeHive",
           "temperature":{
              "type":"Property",
              "value":22.2
           },
           "@context": [
  "https://raw.githubusercontent.com/easy-global-market/ngsild-api-data-models/master/apic/jsonld-contexts/apic.jsonld",
  "http://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld" 
           ]
        }
        """.trimIndent()
    val secondBeehive =
        """
        {
           "id":"urn:ngsi-ld:BeeHive:TESTB",
           "type":"BeeHive",
           "temperature":{
              "type":"Property",
              "value":30.2
           },
           "@context": [
  "https://raw.githubusercontent.com/easy-global-market/ngsild-api-data-models/master/apic/jsonld-contexts/apic.jsonld",
  "http://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld" 
           ]
        }
        """.trimIndent()

    val thirdBeehive =
        """
     {
        "id":"urn:ngsi-ld:BeeHive:TESTD",
        "type":"BeeHive",
        "location":{
           "type":"GeoProperty",
           "value":{
              "type":"Point",
              "coordinates":[
                 24.30623,
                 60.07966
              ]
           }
        },
        "managedBy":{
           "type":"Relationship",
           "object":"urn:ngsi-ld:Beekeeper:1230"
        },
        "@context":[
  "https://raw.githubusercontent.com/easy-global-market/ngsild-api-data-models/master/apic/jsonld-contexts/apic.jsonld",
  "http://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"
        ]
     }
        """.trimIndent()

    @Test
    fun `it should create an EntityTypeInformation for one beehive entity with no attributeDetails`() {
        val entities = listOf(
            JsonLdEntity(
                mapOf(
                    "@id" to "urn:ngsi-ld:Beehive:TESTC",
                    "@type" to listOf("Beehive")
                ),
                listOf(JsonLdUtils.NGSILD_CORE_CONTEXT)
            )
        )

        val entityTypeInformation = entityTypeService
            .getEntityTypeInformation("https://ontology.eglobalmark.com/apic#Beehive", entities)

        assert(entityTypeInformation.id == "https://ontology.eglobalmark.com/apic#Beehive".toUri())
        assert(entityTypeInformation.type == "EntityTypeInformation")
        assert(entityTypeInformation.typeName == "Beehive")
        assert(entityTypeInformation.entityCount == 1)
        assert(entityTypeInformation.attributeDetails.isEmpty())
    }

    @Test
    fun `it should create an EntityTypeInformation with an attributeDetails with one attributeInfo`() {
        val entities = listOf(
            expandJsonLdEntity(firstBeehive, apicContext),
            expandJsonLdEntity(secondBeehive, apicContext)
        )

        val entityTypeInformation = entityTypeService
            .getEntityTypeInformation("https://ontology.eglobalmark.com/apic#Beehive", entities)

        assert(entityTypeInformation.id == "https://ontology.eglobalmark.com/apic#Beehive".toUri())
        assert(entityTypeInformation.type == "EntityTypeInformation")
        assert(entityTypeInformation.typeName == "Beehive")
        assert(entityTypeInformation.entityCount == 2)
        assert(entityTypeInformation.attributeDetails.size == 1)
        assert(
            entityTypeInformation.attributeDetails.contains(
                AttributeInfo(
                    "https://ontology.eglobalmark.com/apic#temperature".toUri(),
                    "Attribute",
                    "temperature",
                    listOf("Property")
                )
            )
        )
    }

    @Test
    fun `it should create an EntityTypeInformation with an attributeDetails with more than one attributeInfo`() {
        val entities = listOf(
            expandJsonLdEntity(firstBeehive, apicContext),
            expandJsonLdEntity(secondBeehive, apicContext),
            expandJsonLdEntity(thirdBeehive, apicContext)
        )

        val entityTypeInformation = entityTypeService
            .getEntityTypeInformation("https://ontology.eglobalmark.com/apic#Beehive", entities)

        assert(entityTypeInformation.id == "https://ontology.eglobalmark.com/apic#Beehive".toUri())
        assert(entityTypeInformation.type == "EntityTypeInformation")
        assert(entityTypeInformation.typeName == "Beehive")
        assert(entityTypeInformation.entityCount == 3)
        assert(entityTypeInformation.attributeDetails.size == 3)
        assert(
            entityTypeInformation.attributeDetails.containsAll(
                listOf(
                    AttributeInfo(
                        "https://ontology.eglobalmark.com/apic#temperature".toUri(),
                        "Attribute",
                        "temperature",
                        listOf("Property")
                    ),
                    AttributeInfo(
                        "https://ontology.eglobalmark.com/egm#managedBy".toUri(),
                        "Attribute",
                        "managedBy",
                        listOf("Relationship")
                    ),
                    AttributeInfo(
                        "https://uri.etsi.org/ngsi-ld/location".toUri(),
                        "Attribute",
                        "location",
                        listOf("GeoProperty")
                    )
                )
            )
        )
    }
}
