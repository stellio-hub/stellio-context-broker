package com.egm.datahub.context.registry.service

import com.egm.datahub.context.registry.model.*
import com.egm.datahub.context.registry.repository.EntityRepository
import com.egm.datahub.context.registry.repository.Neo4jRepository
import com.egm.datahub.context.registry.repository.PropertyRepository
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.ninjasquad.springmockk.MockkBean
import io.mockk.*
import org.junit.jupiter.api.Test
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
    fun `it should ignore measures from an unknown sensor`() {
        val observation = gimmeAnObservation()

        every { entityRepository.findById(any()) } returns Optional.empty()

        neo4jService.updateEntityLastMeasure(observation)

        verify { entityRepository.findById("urn:ngsi-ld:Sensor:013YFZ") }

        confirmVerified(entityRepository)
    }

    @Test
    fun `it should do nothing if temporal property is not found`() {
        val observation = gimmeAnObservation()
        val sensorId = "urn:ngsi-ld:Sensor:013YFZ"

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
        val sensorId = "urn:ngsi-ld:Sensor:013YFZ"

        val mockkedSensor = mockkClass(Entity::class)
        val mockkedObservation = mockkClass(Property::class)

        every { mockkedSensor.id } returns sensorId
        every { mockkedObservation setProperty "value" value any<Double>() } answers { value }
        every { mockkedObservation setProperty "observedAt" value any<OffsetDateTime>() } answers { value }
        every { entityRepository.findById(any()) } returns Optional.of(mockkedSensor)
        every { neo4jRepository.getObservedProperty(any(), any()) } returns mockkedObservation
        every { propertyRepository.save(any<Property>()) } returns mockkedObservation

        neo4jService.updateEntityLastMeasure(observation)

        verify { entityRepository.findById(sensorId) }
        verify { neo4jRepository.getObservedProperty(sensorId, "OBSERVED_BY") }
        verify { propertyRepository.save(any<Property>()) }

        confirmVerified()
    }

    @Test
    fun `it should replace an existing relationship`() {

        val sensorId = "urn:ngsi-ld:Sensor:013YFZ"
        val relationshipId = UUID.randomUUID().toString()
        val payload = """
            {
              "filledIn": {
                "type": "Relationship",
                "object": "urn:ngsi-ld:FishContainment:1234"
              }
            }
        """.trimIndent()

        val mockkedSensor = mockkClass(Entity::class)
        val mockkedRelationshipEntity = mockkClass(Entity::class)

        every { mockkedSensor.relationships } returns mutableListOf()
        every { mockkedRelationshipEntity.id } returns relationshipId

        every { neo4jRepository.deleteRelationshipFromEntity(any(), any()) } returns 1
        every { entityRepository.findById(any()) } returns Optional.of(mockkedSensor)
        every { entityRepository.save(any<Entity>()) } returns mockkedRelationshipEntity
        every { neo4jRepository.createRelationshipFromEntity(any(), any(), any()) } returns 1

        neo4jService.updateEntityAttributes(sensorId, payload, aquacContext!!)

        verify { neo4jRepository.deleteRelationshipFromEntity(eq(sensorId), "FILLED_IN") }
        verify { entityRepository.findById(eq(sensorId)) }
        verify(exactly = 2) { entityRepository.save(any<Entity>()) }
        verify { neo4jRepository.createRelationshipFromEntity(eq(relationshipId), "FILLED_IN", "urn:ngsi-ld:FishContainment:1234") }

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

        every { mockkedSensor.properties } returns mutableListOf()

        every { neo4jRepository.deletePropertyFromEntity(any(), any()) } returns 1
        every { entityRepository.findById(any()) } returns Optional.of(mockkedSensor)
        every { propertyRepository.save(any<Property>()) } returns mockkedPropertyEntity
        every { entityRepository.save(any<Entity>()) } returns mockkedSensor

        neo4jService.updateEntityAttributes(sensorId, payload, aquacContext!!)

        verify { neo4jRepository.deletePropertyFromEntity(eq(sensorId), "https://ontology.eglobalmark.com/aquac#fishAge") }
        verify { entityRepository.findById(eq(sensorId)) }
        verify { propertyRepository.save(match<Property> {
            it.name == "https://ontology.eglobalmark.com/aquac#fishAge" &&
                it.value == 5 &&
                it.unitCode == "months"
        }) }
        verify { entityRepository.save(any<Entity>()) }

        confirmVerified()
    }

    private fun gimmeAnObservation(): Observation {
        val observation = ClassPathResource("/ngsild/aquac/Observation.json")
        val mapper = jacksonObjectMapper()
        mapper.findAndRegisterModules()
        return mapper.readValue(observation.inputStream, Observation::class.java)
    }
}
