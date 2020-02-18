package com.egm.datahub.context.registry.service

import com.egm.datahub.context.registry.model.*
import com.egm.datahub.context.registry.repository.*
import com.egm.datahub.context.registry.util.NgsiLdParsingUtils
import com.egm.datahub.context.registry.util.NgsiLdParsingUtils.NGSILD_OBSERVED_AT_PROPERTY
import com.egm.datahub.context.registry.util.NgsiLdParsingUtils.NGSILD_PROPERTY_TYPE
import com.egm.datahub.context.registry.util.NgsiLdParsingUtils.NGSILD_PROPERTY_VALUE
import com.egm.datahub.context.registry.util.NgsiLdParsingUtils.NGSILD_UNIT_CODE_PROPERTY
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

    @MockkBean
    private lateinit var repositoryEventsListener: RepositoryEventsListener

    // TODO https://redmine.eglobalmark.com/issues/851
//    @Test
    fun `it should notify of a new entity`() {

        every { repositoryEventsListener.handleRepositoryEvent(any()) } just Runs

        neo4jService.createEntity(emptyMap(), emptyList())

        verify(timeout = 1000, exactly = 1) { repositoryEventsListener.handleRepositoryEvent(match { entityEvent ->
            entityEvent.entityType == "BreedingService" &&
                    entityEvent.entityUrn == "breedingServiceId" &&
                    entityEvent.operation == EventType.POST
        }) }
        confirmVerified(repositoryEventsListener)
    }

    @Test
    fun `it should create an entity with a property having a property`() {

        val sampleDataWithContext = loadAndParseSampleData("BreedingService_propWithProp.json")

        val mockedBreedingService = mockkClass(Entity::class)
        val mockedProperty = mockkClass(Property::class)

        every { entityRepository.save<Entity>(any()) } returns mockedBreedingService
        every { propertyRepository.save<Property>(any()) } returns mockedProperty
        every { mockedBreedingService.properties } returns mutableListOf()
        every { mockedProperty.properties } returns mutableListOf()
        every { attributeRepository.save<Attribute>(any()) } returns mockedProperty
        every { mockedBreedingService.id } returns "urn:ngsi-ld:BreedingService:PropWithProp"
        every { repositoryEventsListener.handleRepositoryEvent(any()) } just Runs

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

        every { entityRepository.findById(any()) } returns Optional.empty()

        neo4jService.updateEntityLastMeasure(observation)

        verify { entityRepository.findById("urn:ngsi-ld:Sensor:01XYZ") }

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
        every { entityRepository.findById(any()) } returns Optional.of(mockkedSensor)
        every { neo4jRepository.getObservedProperty(any(), any()) } returns null

        neo4jService.updateEntityLastMeasure(observation)

        verify { entityRepository.findById(sensorId) }
        verify { neo4jRepository.getObservedProperty(sensorId, "OBSERVED_BY") }
        verify { propertyRepository wasNot Called }

        confirmVerified()
    }

    @Test
    fun `it should update an existing measure`() {
        val observation = gimmeAnObservation()
        val sensorId = "urn:ngsi-ld:Sensor:01XYZ"

        val mockkedEntity = mockkClass(Entity::class)
        val mockkedSensor = mockkClass(Entity::class)
        val mockkedObservation = mockkClass(Property::class)

        every { mockkedSensor.id } returns sensorId
        every { mockkedObservation.name } returns observation.attributeName
        every { mockkedObservation.updateValues(any(), any(), any()) } just Runs
        every { mockkedEntity setProperty "location" value any<GeographicPoint2d>() } answers { value }

        every { entityRepository.findById(any()) } returns Optional.of(mockkedSensor)
        every { neo4jRepository.getObservedProperty(any(), any()) } returns mockkedObservation
        every { neo4jRepository.getEntityByProperty(any()) } returns mockkedEntity
        every { propertyRepository.save(any<Property>()) } returns mockkedObservation
        every { entityRepository.save(any<Entity>()) } returns mockkedEntity

        neo4jService.updateEntityLastMeasure(observation)

        verify { entityRepository.findById(sensorId) }
        verify { neo4jRepository.getObservedProperty(sensorId, "OBSERVED_BY") }
        verify { neo4jRepository.getEntityByProperty(mockkedObservation) }
        verify { propertyRepository.save(any<Property>()) }
        verify { entityRepository.save(any<Entity>()) }

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
        every { mockkedPropertyEntity setProperty "value" value any<Double>() } answers { value }
        every { mockkedPropertyEntity setProperty "unitCode" value any<String>() } answers { value }
        every { mockkedPropertyEntity setProperty "observedAt" value any<OffsetDateTime>() } answers { value }

        every { mockkedPropertyEntity.updateValues(any(), any(), any()) } just Runs
        every { neo4jRepository.hasPropertyOfName(any(), any()) } returns true
        every { neo4jRepository.getPropertyOfSubject(any(), any()) } returns mockkedPropertyEntity
        every { entityRepository.findById(any()) } returns Optional.of(mockkedSensor)
        every { propertyRepository.save(any<Property>()) } returns mockkedPropertyEntity

        neo4jService.updateEntityAttributes(sensorId, payload, aquacContext!!)

        verify { mockkedPropertyEntity.updateValues(any(), any(), any()) }
        verify { neo4jRepository.hasPropertyOfName(any(), any()) }
        verify { neo4jRepository.getPropertyOfSubject(any(), any()) }
        verify { entityRepository.findById(eq(sensorId)) }
        verify { propertyRepository.save(mockkedPropertyEntity) }

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
                mapOf("@value" to "2019-12-18T10:45:44.248755+01:00")
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
            it.type == listOf("https://uri.etsi.org/ngsi-ld/default-context/connectsTo")
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
            it.type == listOf("https://uri.etsi.org/ngsi-ld/default-context/connectsTo")
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

    private fun loadAndParseSampleData(filename: String): Pair<Map<String, Any>, List<String>> {
        val sampleData = ClassPathResource("/ngsild/aquac/$filename")
        return NgsiLdParsingUtils.parseEntity(String(sampleData.inputStream.readAllBytes()))
    }
}
