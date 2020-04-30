package com.egm.stellio.entity.service

import com.egm.stellio.entity.model.*
import com.egm.stellio.entity.repository.*
import com.egm.stellio.shared.model.EventType
import com.egm.stellio.shared.util.NgsiLdParsingUtils
import com.egm.stellio.shared.util.NgsiLdParsingUtils.EGM_VENDOR_ID
import com.egm.stellio.shared.util.NgsiLdParsingUtils.NGSILD_OBSERVED_AT_PROPERTY
import com.egm.stellio.shared.util.NgsiLdParsingUtils.NGSILD_PROPERTY_TYPE
import com.egm.stellio.shared.util.NgsiLdParsingUtils.NGSILD_PROPERTY_VALUE
import com.egm.stellio.shared.util.NgsiLdParsingUtils.NGSILD_UNIT_CODE_PROPERTY
import com.egm.stellio.shared.util.NgsiLdParsingUtils.EGM_RAISED_NOTIFICATION
import com.egm.stellio.shared.model.Observation
import com.egm.stellio.shared.util.NgsiLdParsingUtils.NGSILD_DATE_TIME_TYPE
import com.egm.stellio.shared.util.loadAndParseSampleData
import com.egm.stellio.shared.util.toRelationshipTypeName
import com.ninjasquad.springmockk.MockkBean
import io.mockk.*
import org.junit.jupiter.api.Test
import org.neo4j.ogm.types.spatial.GeographicPoint2d
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.core.io.ClassPathResource
import org.springframework.test.context.ActiveProfiles
import java.time.OffsetDateTime
import java.util.*

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = [ Neo4jService::class ])
@ActiveProfiles("test")
class Neo4jServiceTests {

    @Value("\${application.jsonld.aquac_context}")
    val aquacContext: String? = null

    @Autowired
    private lateinit var neo4jService: Neo4jService

    @MockkBean
    private lateinit var neo4jRepository: Neo4jRepository

    @MockkBean
    private lateinit var entityRepository: EntityRepository

    @MockkBean
    private lateinit var propertyRepository: PropertyRepository

    @MockkBean
    private lateinit var relationshipRepository: RelationshipRepository

    @MockkBean
    private lateinit var attributeRepository: AttributeRepository

    /**
     * As Spring's ApplicationEventPublisher is not easily mockable (https://github.com/spring-projects/spring-framework/issues/18907),
     * we are directly mocking the event listener to check it receives what is expected
     */
    @MockkBean
    private lateinit var repositoryEventsListener: RepositoryEventsListener

    @Test
    fun `it should notify of a new entity`() {

        val expectedPayloadInEvent = """
        {"id":"urn:ngsi-ld:MortalityRemovalService:014YFA9Z","type":"MortalityRemovalService","@context":["https://raw.githubusercontent.com/easy-global-market/ngsild-api-data-models/master/shared-jsonld-contexts/egm.jsonld","https://raw.githubusercontent.com/easy-global-market/ngsild-api-data-models/master/aquac/jsonld-contexts/aquac.jsonld","http://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"]}
        """.trimIndent()
        val sampleDataWithContext = loadAndParseSampleData("aquac/MortalityRemovalService_standalone.json")
        val mockedBreedingService = mockkClass(Entity::class)

        every { entityRepository.save<Entity>(any()) } returns mockedBreedingService
        every { repositoryEventsListener.handleRepositoryEvent(any()) } just Runs
        every { mockedBreedingService.properties } returns mutableListOf()
        every { mockedBreedingService.id } returns "urn:ngsi-ld:MortalityRemovalService:014YFA9Z"
        every { entityRepository.getEntityCoreById(any()) } returns listOf(mapOf("entity" to mockedBreedingService))
        every { mockedBreedingService.serializeCoreProperties() } returns mutableMapOf("@id" to "urn:ngsi-ld:MortalityRemovalService:014YFA9Z", "@type" to listOf("MortalityRemovalService"))
        every { entityRepository.getEntitySpecificProperties(any()) } returns listOf()
        every { entityRepository.getEntityRelationships(any()) } returns listOf()
        every { mockedBreedingService.contexts } returns sampleDataWithContext.second

        neo4jService.createEntity(sampleDataWithContext.first, sampleDataWithContext.second)

        verify(timeout = 1000, exactly = 1) { repositoryEventsListener.handleRepositoryEvent(match { entityEvent ->
            entityEvent.entityType == "MortalityRemovalService" &&
                    entityEvent.entityId == "urn:ngsi-ld:MortalityRemovalService:014YFA9Z" &&
                    entityEvent.operationType == EventType.CREATE &&
                    entityEvent.payload == expectedPayloadInEvent &&
                    entityEvent.updatedEntity == null
        }) }
        // I don't know where does this call come from (probably a Spring internal thing) but it is required for verification
        verify { repositoryEventsListener.equals(any()) }
        confirmVerified(repositoryEventsListener)
    }

    @Test
    fun `it should notify of a two attributes update`() {

        val payload = String(ClassPathResource("/ngsild/aquac/fragments/BreedingService_twoNewProperties.json").inputStream.readAllBytes())
        val expectedFishNumberPayloadInEvent = """
            {"fishNumber":{"type":"Property","value":500}}
        """.trimIndent()
        val expectedFishSizePayloadInEvent = """
            {"fishSize":{"type":"Property","value":12}}
        """.trimIndent()
        val mockedBreedingService = mockkClass(Entity::class)
        val mockedFishNumberProperty = mockkClass(Property::class)
        val mockedFishSizeProperty = mockkClass(Property::class)

        every { entityRepository.findById(any()) } returns Optional.of(mockedBreedingService)
        every { neo4jRepository.hasPropertyOfName(any(), any()) } returns true
        every { mockedBreedingService.id } returns "urn:ngsi-ld:BreedingService:0214"
        every { neo4jRepository.getPropertyOfSubject(any(), eq("https://ontology.eglobalmark.com/aquac#fishNumber")) } returns mockedFishNumberProperty
        every { mockedFishNumberProperty.updateValues(any(), any(), any()) } just Runs
        every { neo4jRepository.getPropertyOfSubject(any(), eq("https://ontology.eglobalmark.com/aquac#fishSize")) } returns mockedFishSizeProperty
        every { mockedFishSizeProperty.updateValues(any(), any(), any()) } just Runs
        every { propertyRepository.save<Property>(any()) } returns mockedFishNumberProperty

        every { mockedBreedingService.type } returns listOf("https://ontology.eglobalmark.com/aquac#BreedingService")
        every { repositoryEventsListener.handleRepositoryEvent(any()) } just Runs

        neo4jService.updateEntityAttributes("urn:ngsi-ld:BreedingService:0214", payload, aquacContext!!)

        verify(timeout = 1000) { repositoryEventsListener.handleRepositoryEvent(match { entityEvent ->
            entityEvent.entityType == "BreedingService" &&
                    entityEvent.entityId == "urn:ngsi-ld:BreedingService:0214" &&
                    entityEvent.operationType == EventType.UPDATE &&
                    entityEvent.payload == expectedFishNumberPayloadInEvent &&
                    entityEvent.updatedEntity == null
        }) }
        verify(timeout = 1000) { repositoryEventsListener.handleRepositoryEvent(match { entityEvent ->
            entityEvent.entityType == "BreedingService" &&
                    entityEvent.entityId == "urn:ngsi-ld:BreedingService:0214" &&
                    entityEvent.operationType == EventType.UPDATE &&
                    entityEvent.payload == expectedFishSizePayloadInEvent &&
                    entityEvent.updatedEntity == null
        }) }
        // I don't know where does this call come from (probably a Spring internal thing) but it is required for verification
        verify { repositoryEventsListener.equals(any()) }
        confirmVerified(repositoryEventsListener)
    }

    @Test
    fun `it should create an entity with a property having a property`() {

        val sampleDataWithContext = loadAndParseSampleData("aquac/BreedingService_propWithProp.json")

        val mockedBreedingService = mockkClass(Entity::class)
        val mockedProperty = mockkClass(Property::class)

        every { entityRepository.save<Entity>(any()) } returns mockedBreedingService
        every { propertyRepository.save<Property>(any()) } returns mockedProperty
        every { mockedBreedingService.properties } returns mutableListOf()
        every { mockedProperty.properties } returns mutableListOf()
        every { attributeRepository.save<Attribute>(any()) } returns mockedProperty
        every { mockedBreedingService.id } returns "urn:ngsi-ld:BreedingService:PropWithProp"
        every { repositoryEventsListener.handleRepositoryEvent(any()) } just Runs
        every { entityRepository.getEntityCoreById(any()) } returns listOf(mapOf("entity" to mockedBreedingService))
        every { mockedBreedingService.serializeCoreProperties() } returns mutableMapOf("@id" to "urn:ngsi-ld:MortalityRemovalService:014YFA9Z", "@type" to listOf("MortalityRemovalService"))
        every { entityRepository.getEntitySpecificProperties(any()) } returns listOf()
        every { entityRepository.getEntityRelationships(any()) } returns listOf()
        every { mockedBreedingService.contexts } returns sampleDataWithContext.second

        neo4jService.createEntity(sampleDataWithContext.first, sampleDataWithContext.second)

        verifyAll {
            propertyRepository.save<Property>(match {
                it.name == "https://ontology.eglobalmark.com/aquac#fishName"
            })
            propertyRepository.save<Property>(match {
                it.name == "https://ontology.eglobalmark.com/aquac#foodName" &&
                    it.unitCode == "slice"
            })
            propertyRepository.save<Property>(match {
                it.name == "https://ontology.eglobalmark.com/aquac#fishSize"
            })
        }

        verify(exactly = 2) { attributeRepository.save<Attribute>(any()) }
    }

    @Test
    fun `it should ignore measures from an unknown sensor`() {
        val observation = gimmeAnObservation()

        every { neo4jRepository.getObservingSensorEntity(any(), any(), any()) } returns null

        neo4jService.updateEntityLastMeasure(observation)

        verify { neo4jRepository.getObservingSensorEntity("urn:ngsi-ld:Sensor:01XYZ", EGM_VENDOR_ID, "incoming") }

        confirmVerified(entityRepository)
    }

    @Test
    fun `it should do nothing if temporal property is not found`() {
        val observation = gimmeAnObservation()
        val sensorId = "urn:ngsi-ld:Sensor:01XYZ"

        val mockkedSensor = mockkClass(Entity::class)
        val mockkedObservation = mockkClass(Property::class)
        val mockkedObservationId = "urn:ngsi-ld:Property:${UUID.randomUUID()}"

        every { mockkedSensor.id } returns sensorId
        every { mockkedObservation.id } returns mockkedObservationId
        every { neo4jRepository.getObservingSensorEntity(any(), any(), any()) } returns mockkedSensor
        every { neo4jRepository.getObservedProperty(any(), any()) } returns null

        neo4jService.updateEntityLastMeasure(observation)

        verify { neo4jRepository.getObservingSensorEntity(sensorId, EGM_VENDOR_ID, "incoming") }
        verify { neo4jRepository.getObservedProperty(sensorId, "OBSERVED_BY") }
        verify { propertyRepository wasNot Called }

        confirmVerified()
    }

    @Test
    fun `it should update an existing measure`() {
        val observation = gimmeAnObservation()
        val sensorId = "urn:ngsi-ld:Sensor:01XYZ"
        val expectedPropertyUpdate = Property(
            name = "fishNumber",
            value = 400.0
        )
        val expectedEventPayload = """
            {
              "fishNumber":{
                "type":"Property",
                "createdAt":"${expectedPropertyUpdate.createdAt}",
                "value":400.0
              },
              "@context":"$aquacContext"
            }
        """.trimIndent().replace("\n", "").replace(" ", "")

        val mockkedEntity = mockkClass(Entity::class)
        val mockkedSensor = mockkClass(Entity::class)
        val mockkedObservation = mockkClass(Property::class)

        every { mockkedSensor.id } returns sensorId
        every { mockkedObservation.name } returns observation.attributeName
        every { mockkedObservation.updateValues(any(), any(), any()) } just Runs
        every { mockkedEntity setProperty "location" value any<GeographicPoint2d>() } answers { value }
        every { mockkedEntity.id } returns "urn:ngsi-ld:BreedingService:01234"
        every { mockkedEntity.type } returns listOf("https://ontology.eglobalmark.com/aquac#BreedingService")
        every { mockkedEntity.contexts } returns listOf(aquacContext!!)
        every { mockkedObservation.id } returns "property-9999"

        every { neo4jRepository.getObservingSensorEntity(any(), any(), any()) } returns mockkedSensor
        every { neo4jRepository.getObservedProperty(any(), any()) } returns mockkedObservation
        every { neo4jRepository.getEntityByProperty(any()) } returns mockkedEntity
        every { propertyRepository.save(any<Property>()) } returns mockkedObservation
        every { entityRepository.save(any<Entity>()) } returns mockkedEntity
        every { entityRepository.getEntitySpecificProperty(any(), any()) } returns listOf(
            mapOf("property" to expectedPropertyUpdate)
        )
        every { repositoryEventsListener.handleRepositoryEvent(any()) } just Runs

        neo4jService.updateEntityLastMeasure(observation)

        verify { neo4jRepository.getObservingSensorEntity(sensorId, EGM_VENDOR_ID, "incoming") }
        verify { neo4jRepository.getObservedProperty(sensorId, "OBSERVED_BY") }
        verify { neo4jRepository.getEntityByProperty(mockkedObservation) }
        verify { propertyRepository.save(any<Property>()) }
        verify { entityRepository.save(any<Entity>()) }
        verify(timeout = 2000) { repositoryEventsListener.handleRepositoryEvent(match { entityEvent ->
            entityEvent.entityType == "BreedingService" &&
                    entityEvent.entityId == "urn:ngsi-ld:BreedingService:01234" &&
                    entityEvent.operationType == EventType.UPDATE &&
                    entityEvent.payload == expectedEventPayload &&
                    entityEvent.updatedEntity == null
        }) }

        confirmVerified()
    }

    @Test
    fun `it should replace an existing relationship`() {

        val sensorId = "urn:ngsi-ld:Sensor:013YFZ"
        val relationshipId = "urn:ngsi-ld:Relationship:92033f60-bb8b-4640-9464-bca23199ac"
        val relationshipTargetId = "urn:ngsi-ld:FishContainment:8792"

        val payload = """
            {
              "filledIn": {
                "type": "Relationship",
                "object": "urn:ngsi-ld:FishContainment:1234"
              }
            }
        """.trimIndent()

        val mockkedSensor = mockkClass(Entity::class)
        val mockkedRelationshipTarget = mockkClass(Entity::class)
        val mockkedRelationship = mockkClass(Relationship::class)

        every { mockkedSensor.id } returns sensorId
        every { mockkedSensor.type } returns listOf("Sensor")
        every { mockkedRelationship.type } returns listOf("Relationship")
        every { mockkedRelationship.id } returns relationshipId
        every { mockkedRelationshipTarget.id } returns relationshipTargetId
        every { mockkedRelationship setProperty "observedAt" value any<OffsetDateTime>() } answers { value }

        every { neo4jRepository.hasRelationshipOfType(any(), any()) } returns true
        every { neo4jRepository.getRelationshipOfSubject(any(), any()) } returns mockkedRelationship
        every { neo4jRepository.getRelationshipTargetOfSubject(any(), any()) } returns mockkedRelationshipTarget
        every { neo4jRepository.updateRelationshipTargetOfAttribute(any(), any(), any(), any()) } returns Pair(1, 1)
        every { entityRepository.findById(any()) } returns Optional.of(mockkedSensor)
        every { relationshipRepository.save(any<Relationship>()) } returns mockkedRelationship
        every { repositoryEventsListener.handleRepositoryEvent(any()) } just Runs

        neo4jService.updateEntityAttributes(sensorId, payload, aquacContext!!)

        verify { neo4jRepository.hasRelationshipOfType(any(), any()) }
        verify { neo4jRepository.getRelationshipOfSubject(any(), any()) }
        verify { neo4jRepository.getRelationshipTargetOfSubject(any(), any()) }
        verify { neo4jRepository.updateRelationshipTargetOfAttribute(any(), any(), any(), any()) }
        verify { entityRepository.findById(eq(sensorId)) }
        verify { relationshipRepository.save(mockkedRelationship) }

        confirmVerified()
    }

    @Test
    fun `it should replace an existing property`() {

        val sensorId = "urn:ngsi-ld:Sensor:013YFZ"
            val payload = """
            {
              "fishAge": {
                "type": "Property",
                "value": 5,
                "unitCode": "months"
              }
            }
        """.trimIndent()

        val mockkedSensor = mockkClass(Entity::class)
        val mockkedPropertyEntity = mockkClass(Property::class)

        every { mockkedSensor.id } returns sensorId
        every { mockkedSensor.type } returns listOf("Sensor")
        every { mockkedPropertyEntity setProperty "value" value any<Double>() } answers { value }
        every { mockkedPropertyEntity setProperty "unitCode" value any<String>() } answers { value }
        every { mockkedPropertyEntity setProperty "observedAt" value any<OffsetDateTime>() } answers { value }

        every { mockkedPropertyEntity.updateValues(any(), any(), any()) } just Runs
        every { neo4jRepository.hasPropertyOfName(any(), any()) } returns true
        every { neo4jRepository.getPropertyOfSubject(any(), any()) } returns mockkedPropertyEntity
        every { entityRepository.findById(any()) } returns Optional.of(mockkedSensor)
        every { propertyRepository.save(any<Property>()) } returns mockkedPropertyEntity
        every { repositoryEventsListener.handleRepositoryEvent(any()) } just Runs

        neo4jService.updateEntityAttributes(sensorId, payload, aquacContext!!)

        verify { mockkedPropertyEntity.updateValues(any(), any(), any()) }
        verify { neo4jRepository.hasPropertyOfName(any(), any()) }
        verify { neo4jRepository.getPropertyOfSubject(any(), any()) }
        verify { entityRepository.findById(eq(sensorId)) }
        verify { propertyRepository.save(mockkedPropertyEntity) }

        confirmVerified()
    }

    @Test
    fun `it should replace an existing geoProperty`() {

        val sensorId = "urn:ngsi-ld:Sensor:013YFZ"
        val payload = """
            {
              "location": {
                  "type": "GeoProperty",
                  "value": {
                    "type": "Point",
                    "coordinates": [
                      9.30623,
                      8.07966
                    ]
                  }
              }
            }
        """.trimIndent()

        val mockkedSensor = mockkClass(Entity::class)

        every { mockkedSensor.id } returns sensorId
        every { mockkedSensor.type } returns listOf("Sensor")

        every { neo4jRepository.hasGeoPropertyOfName(any(), any()) } returns true
        every { entityRepository.findById(any()) } returns Optional.of(mockkedSensor)
        every { neo4jRepository.updateLocationPropertyOfEntity(any(), any()) } returns 1
        every { repositoryEventsListener.handleRepositoryEvent(any()) } just Runs

        neo4jService.updateEntityAttributes(sensorId, payload, aquacContext!!)

        verify { neo4jRepository.hasGeoPropertyOfName(any(), any()) }
        verify { entityRepository.findById(eq(sensorId)) }
        verify { neo4jRepository.updateLocationPropertyOfEntity(sensorId, Pair(9.30623, 8.07966)) }

        confirmVerified()
    }

    @Test
    fun `it should correctly parse location property for an entity`() {

        val entityId = "urn:ngsi-ld:Beehive:123456"
        val geoPropertyMap = mapOf(
            NGSILD_PROPERTY_VALUE to listOf(
                mapOf(
                    "@type" to listOf("https://uri.etsi.org/ngsi-ld/default-context/Point"),
                    NgsiLdParsingUtils.NGSILD_COORDINATES_PROPERTY to listOf(
                        mapOf("@value" to 23.45),
                        mapOf("@value" to 67.87)
                    )

                )
            )
        )
        val mockkedEntity = mockkClass(Entity::class)

        every { mockkedEntity.id } returns entityId
        every { neo4jRepository.addLocationPropertyToEntity(any(), any()) } returns 1

        neo4jService.createLocationProperty(mockkedEntity, "location", geoPropertyMap)

        verify { neo4jRepository.addLocationPropertyToEntity(entityId, Pair(23.45, 67.87)) }

        confirmVerified()
    }

    @Test
    fun `it should create a temporal property with all provided attributes`() {

        val entityId = "urn:ngsi-ld:Beehive:123456"
        val temperatureMap = mapOf(
            NGSILD_PROPERTY_VALUE to listOf(
                mapOf(
                    "@type" to listOf(NGSILD_PROPERTY_TYPE),
                    "@value" to 250
                )
            ),
            NGSILD_UNIT_CODE_PROPERTY to listOf(
                mapOf("@value" to "kg")
            ),
            NGSILD_OBSERVED_AT_PROPERTY to listOf(
                mapOf("@type" to NGSILD_DATE_TIME_TYPE,
                    "@value" to "2019-12-18T10:45:44.248755+01:00")
            )
        )

        val mockkedEntity = mockkClass(Entity::class)
        val mockkedProperty = mockkClass(Property::class)

        every { mockkedEntity.id } returns entityId
        every { mockkedEntity.properties } returns mutableListOf()
        every { propertyRepository.save<Property>(any()) } returns mockkedProperty
        every { entityRepository.save<Entity>(any()) } returns mockkedEntity

        neo4jService.createEntityProperty(mockkedEntity, "temperature", temperatureMap)

        verify { propertyRepository.save(match<Property> {
            it.name == "temperature" &&
                it.value == 250 &&
                it.unitCode == "kg" &&
                it.observedAt.toString() == "2019-12-18T10:45:44.248755+01:00"
        }) }
        verify { entityRepository.save(any<Entity>()) }

        confirmVerified()
    }

    @Test
    fun `it should create a new relationship`() {

        val entityId = "urn:ngsi-ld:Beehive:123456"
        val targetEntityId = "urn:ngsi-ld:Beekeeper:654321"
        val relationshipId = UUID.randomUUID().toString()
        val newRelationship = """
            {
                "connectsTo": {
                    "type":"Relationship",
                    "object":"$targetEntityId"
                }
            }
        """.trimIndent()
        val expandedNewRelationship =
            NgsiLdParsingUtils.expandJsonLdFragment(
                newRelationship,
                aquacContext!!
            )

        val mockkedEntity = mockkClass(Entity::class)
        val mockkedRelationship = mockkClass(Relationship::class)

        every { mockkedEntity.relationships } returns mutableListOf()
        every { mockkedEntity.id } returns entityId
        every { mockkedRelationship.id } returns relationshipId

        every { neo4jRepository.hasRelationshipOfType(any(), any()) } returns false
        every { entityRepository.findById(any()) } returns Optional.of(mockkedEntity)
        every { relationshipRepository.save(match<Relationship> {
            it.type == listOf("https://ontology.eglobalmark.com/egm#connectsTo")
        }) } returns mockkedRelationship
        every { entityRepository.save(match<Entity> { it.id == entityId }) } returns mockkedEntity
        every { neo4jRepository.createRelationshipToEntity(any(), any(), any()) } returns 1

        neo4jService.appendEntityAttributes(entityId, expandedNewRelationship, false)

        verify { neo4jRepository.hasRelationshipOfType(eq(entityId), "CONNECTS_TO") }
        verify { entityRepository.findById(eq(entityId)) }
        verify { neo4jRepository.createRelationshipToEntity(eq(relationshipId), "CONNECTS_TO", eq(targetEntityId)) }

        confirmVerified()
    }

    @Test
    fun `it should not replace a relationship if overwrite is disallowed`() {

        val entityId = "urn:ngsi-ld:Beehive:123456"
        val newRelationship = """
            {
                "connectsTo": {
                    "type":"Relationship",
                    "object":"urn:ngsi-ld:Beekeeper:654321"
                }
            }
        """.trimIndent()
        val expandedNewRelationship =
            NgsiLdParsingUtils.expandJsonLdFragment(
                newRelationship,
                aquacContext!!
            )

        every { neo4jRepository.hasRelationshipOfType(any(), any()) } returns true

        neo4jService.appendEntityAttributes(entityId, expandedNewRelationship, true)

        verify { neo4jRepository.hasRelationshipOfType(eq(entityId), "CONNECTS_TO") }

        confirmVerified()
    }

    @Test
    fun `it should replace a relationship if overwrite is allowed`() {

        val entityId = "urn:ngsi-ld:Beehive:123456"
        val targetEntityId = "urn:ngsi-ld:Beekeeper:654321"
        val relationshipId = UUID.randomUUID().toString()
        val newRelationship = """
            {
                "connectsTo": {
                    "type":"Relationship",
                    "object":"$targetEntityId"
                }
            }
        """.trimIndent()
        val expandedNewRelationship =
            NgsiLdParsingUtils.expandJsonLdFragment(
                newRelationship,
                aquacContext!!
            )

        val mockkedEntity = mockkClass(Entity::class)
        val mockkedRelationship = mockkClass(Relationship::class)

        every { mockkedEntity.relationships } returns mutableListOf()
        every { mockkedEntity.id } returns entityId
        every { mockkedRelationship.id } returns relationshipId

        every { neo4jRepository.hasRelationshipOfType(any(), any()) } returns true
        every { neo4jRepository.deleteRelationshipFromEntity(any(), any()) } returns 1
        every { entityRepository.findById(any()) } returns Optional.of(mockkedEntity)
        every { relationshipRepository.save(match<Relationship> {
            it.type == listOf("https://ontology.eglobalmark.com/egm#connectsTo")
        }) } returns mockkedRelationship
        every { entityRepository.save(match<Entity> { it.id == entityId }) } returns mockkedEntity
        every { neo4jRepository.createRelationshipToEntity(any(), any(), any()) } returns 1

        neo4jService.appendEntityAttributes(entityId, expandedNewRelationship, false)

        verify { neo4jRepository.hasRelationshipOfType(eq(entityId), "CONNECTS_TO") }
        verify { neo4jRepository.deleteRelationshipFromEntity(eq(entityId), "CONNECTS_TO") }
        verify { entityRepository.findById(eq(entityId)) }
        verify { neo4jRepository.createRelationshipToEntity(eq(relationshipId), "CONNECTS_TO", eq(targetEntityId)) }

        confirmVerified()
    }

    @Test
    fun `it should create a new property`() {

        val entityId = "urn:ngsi-ld:Beehive:123456"
        val newProperty = """
            {
              "fishNumber": {
                "type": "Property",
                "value": 500
              }
            }
        """.trimIndent()
        val expandedNewProperty =
            NgsiLdParsingUtils.expandJsonLdFragment(
                newProperty,
                aquacContext!!
            )

        val mockkedEntity = mockkClass(Entity::class)
        val mockkedProperty = mockkClass(Property::class)

        every { mockkedEntity.id } returns entityId
        every { mockkedEntity.properties } returns mutableListOf()

        every { neo4jRepository.hasPropertyOfName(any(), any()) } returns false
        every { entityRepository.findById(any()) } returns Optional.of(mockkedEntity)
        every { propertyRepository.save<Property>(any()) } returns mockkedProperty
        every { entityRepository.save<Entity>(any()) } returns mockkedEntity

        neo4jService.appendEntityAttributes(entityId, expandedNewProperty, false)

        verify { neo4jRepository.hasPropertyOfName(eq(entityId), "https://ontology.eglobalmark.com/aquac#fishNumber") }
        verify { entityRepository.findById(eq(entityId)) }

        confirmVerified()
    }

    @Test
    fun `it should create a new geoproperty`() {

        val entityId = "urn:ngsi-ld:Beehive:123456"
        val newGeoProperty = """
            {
              "location": {
                  "type": "GeoProperty",
                  "value": {
                    "type": "Point",
                    "coordinates": [
                      29.30623,
                      83.07966
                    ]
                  }
              }
            }
        """.trimIndent()

        val expandedNewGeoProperty =
            NgsiLdParsingUtils.expandJsonLdFragment(
                newGeoProperty,
                aquacContext!!
            )

        val mockkedEntity = mockkClass(Entity::class)

        every { mockkedEntity.id } returns entityId

        every { neo4jRepository.hasGeoPropertyOfName(any(), any()) } returns false
        every { entityRepository.findById(any()) } returns Optional.of(mockkedEntity)
        every { entityRepository.save<Entity>(any()) } returns mockkedEntity
        every { neo4jRepository.addLocationPropertyToEntity(any(), any()) } returns 1

        neo4jService.appendEntityAttributes(entityId, expandedNewGeoProperty, false)

        verify { neo4jRepository.hasGeoPropertyOfName(any(), any()) }
        verify { entityRepository.findById(eq(entityId)) }
        verify { neo4jRepository.addLocationPropertyToEntity(entityId, Pair(29.30623, 83.07966)) }

        confirmVerified()
    }

    @Test
    fun `it should replace a geoproperty if overwrite is allowed`() {

        val entityId = "urn:ngsi-ld:Beehive:123456"
        val newGeoProperty = """
            {
              "location": {
                  "type": "GeoProperty",
                  "value": {
                    "type": "Point",
                    "coordinates": [
                      29.30623,
                      83.07966
                    ]
                  }
              }
            }
        """.trimIndent()

        val expandedNewGeoProperty =
            NgsiLdParsingUtils.expandJsonLdFragment(
                newGeoProperty,
                aquacContext!!
            )

        val mockkedEntity = mockkClass(Entity::class)

        every { mockkedEntity.id } returns entityId

        every { neo4jRepository.hasGeoPropertyOfName(any(), any()) } returns true
        every { entityRepository.findById(any()) } returns Optional.of(mockkedEntity)
        every { entityRepository.save<Entity>(any()) } returns mockkedEntity
        every { neo4jRepository.updateLocationPropertyOfEntity(any(), any()) } returns 1

        neo4jService.appendEntityAttributes(entityId, expandedNewGeoProperty, false)

        verify { neo4jRepository.hasGeoPropertyOfName(any(), any()) }
        verify { entityRepository.findById(eq(entityId)) }
        verify { neo4jRepository.updateLocationPropertyOfEntity(entityId, Pair(29.30623, 83.07966)) }

        confirmVerified()
    }

    @Test
    fun `it should create a new subscription`() {

        val subscriptionId = "urn:ngsi-ld:Subscription:04"
        val subscriptionType = "Subscription"
        val properties = mapOf(
            "q" to "foodQuantity<150;foodName=='dietary fibres'"
        )
        val mockkedSubscription = mockkClass(Entity::class)
        val mockkedProperty = mockkClass(Property::class)

        every { neo4jService.exists(any()) } returns false
        every { propertyRepository.save<Property>(any()) } returns mockkedProperty
        every { entityRepository.save<Entity>(any()) } returns mockkedSubscription

        neo4jService.createSubscriptionEntity(subscriptionId, subscriptionType, properties)

        verify { neo4jService.exists(eq(subscriptionId)) }
        verify { propertyRepository.save(any<Property>()) }
        verify { entityRepository.save(any<Entity>()) }

        confirmVerified()
    }

    @Test
    fun `it should not create subscription if exists`() {

        val subscriptionId = "urn:ngsi-ld:Subscription:04"
        val subscriptionType = "Subscription"
        val properties = mapOf(
            "q" to "foodQuantity<150;foodName=='dietary fibres'"
        )

        every { neo4jService.exists(any()) } returns true

        neo4jService.createSubscriptionEntity(subscriptionId, subscriptionType, properties)

        verify { neo4jService.exists(eq(subscriptionId)) }
        verify { propertyRepository wasNot Called }

        confirmVerified()
    }

    @Test
    fun `it should create a new notification and add a relationship to the subscription`() {

        val subscriptionId = "urn:ngsi-ld:Subscription:1234"
        val notificationId = "urn:ngsi-ld:Notification:1234"
        val relationshipId = "urn:ngsi-ld:Relationship:7d0ea653-c932-43cc-aa41-29ac1c77c610"
        val notificationType = "Notification"
        val properties = mapOf(
            "notifiedAt" to "2020-03-10T00:00:00Z"
        )
        val mockkedSubscription = mockkClass(Entity::class)
        val mockkedNotification = mockkClass(Entity::class)
        val mockkedProperty = mockkClass(Property::class)
        val mockkedRelationship = mockkClass(Relationship::class)

        every { entityRepository.findById(any()) } returns Optional.of(mockkedSubscription)
        every { propertyRepository.save<Property>(any()) } returns mockkedProperty
        every { entityRepository.save<Entity>(any()) } returns mockkedNotification
        every { neo4jRepository.getRelationshipTargetOfSubject(any(), any()) } returns null
        every { relationshipRepository.save(any<Relationship>()) } returns mockkedRelationship
        every { mockkedSubscription.relationships } returns mutableListOf()
        every { mockkedRelationship.id } returns relationshipId
        every { neo4jRepository.createRelationshipToEntity(any(), any(), any()) } returns 1

        neo4jService.createNotificationEntity(notificationId, notificationType, subscriptionId, properties)

        verify { entityRepository.findById(eq(subscriptionId)) }
        verify { propertyRepository.save(any<Property>()) }
        verify { entityRepository.save(any<Entity>()) }
        verify { neo4jRepository.getRelationshipTargetOfSubject(subscriptionId, EGM_RAISED_NOTIFICATION.toRelationshipTypeName()) }
        verify { relationshipRepository.save(any<Relationship>()) }
        verify { neo4jRepository.createRelationshipToEntity(relationshipId, EGM_RAISED_NOTIFICATION.toRelationshipTypeName(), notificationId) }

        confirmVerified()
    }

    @Test
    fun `it should remove the last notification create a new one and update the relationship to the subscription`() {

        val subscriptionId = "urn:ngsi-ld:Subscription:1234"
        val notificationId = "urn:ngsi-ld:Notification:1234"
        val lastNotificationId = "urn:ngsi-ld:Notification:1233"
        val relationshipId = "urn:ngsi-ld:Relationship:7d0ea653-c932-43cc-aa41-29ac1c77c610"
        val notificationType = "Notification"
        val properties = mapOf(
            "notifiedAt" to "2020-03-10T00:00:00Z"
        )
        val mockkedSubscription = mockkClass(Entity::class)
        val mockkedNotification = mockkClass(Entity::class)
        val mockkedLastNotification = mockkClass(Entity::class)
        val mockkedProperty = mockkClass(Property::class)
        val mockkedRelationship = mockkClass(Relationship::class)

        every { entityRepository.findById(any()) } returns Optional.of(mockkedSubscription)
        every { propertyRepository.save<Property>(any()) } returns mockkedProperty
        every { entityRepository.save<Entity>(any()) } returns mockkedNotification
        every { neo4jRepository.getRelationshipTargetOfSubject(any(), any()) } returns mockkedLastNotification
        every { neo4jRepository.getRelationshipOfSubject(any(), any()) } returns mockkedRelationship
        every { mockkedRelationship.id } returns relationshipId
        every { mockkedNotification.id } returns notificationId
        every { mockkedLastNotification.id } returns lastNotificationId
        every { neo4jRepository.updateRelationshipTargetOfAttribute(any(), any(), any(), any()) } returns Pair(1, 1)
        every { relationshipRepository.save<Relationship>(any()) } returns mockkedRelationship
        every { neo4jRepository.deleteEntity(any()) } returns Pair(1, 1)

        neo4jService.createNotificationEntity(notificationId, notificationType, subscriptionId, properties)

        verify { entityRepository.findById(eq(subscriptionId)) }
        verify { propertyRepository.save(any<Property>()) }
        verify { entityRepository.save(any<Entity>()) }
        verify { neo4jRepository.getRelationshipTargetOfSubject(subscriptionId, EGM_RAISED_NOTIFICATION.toRelationshipTypeName()) }
        verify { neo4jRepository.getRelationshipOfSubject(subscriptionId, EGM_RAISED_NOTIFICATION.toRelationshipTypeName()) }
        verify { neo4jRepository.updateRelationshipTargetOfAttribute(relationshipId, EGM_RAISED_NOTIFICATION.toRelationshipTypeName(), lastNotificationId, notificationId) }
        verify { relationshipRepository.save(any<Relationship>()) }
        every { neo4jRepository.deleteEntity(lastNotificationId) }

        confirmVerified()
    }

    @Test
    fun `it should not create notification if the related subscription does not exist`() {

        val notificationId = "urn:ngsi-ld:Notification:1234"
        val notificationType = "Notification"
        val subscriptionId = "urn:ngsi-ld:Subscription:1234"
        val properties = mapOf(
            "notifiedAt" to "2020-03-10T00:00:00Z"
        )

        every { entityRepository.findById(any()) } returns Optional.empty()

        neo4jService.createNotificationEntity(notificationId, notificationType, subscriptionId, properties)

        verify { entityRepository.findById(eq(subscriptionId)) }
        verify { propertyRepository wasNot Called }

        confirmVerified()
    }

        private fun gimmeAnObservation(): Observation {
        return Observation(
            attributeName = "incoming",
            latitude = 43.12,
            longitude = 65.43,
            observedBy = "urn:ngsi-ld:Sensor:01XYZ",
            unitCode = "CEL",
            value = 12.4,
            observedAt = OffsetDateTime.now()
        )
    }
}
