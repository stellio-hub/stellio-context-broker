package com.egm.stellio.entity.service

import com.egm.stellio.entity.model.Entity
import com.egm.stellio.entity.model.Property
import com.egm.stellio.entity.model.Relationship
import com.egm.stellio.entity.repository.*
import com.egm.stellio.entity.util.EntitiesGraphBuilder
import com.egm.stellio.entity.web.BatchEntityError
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.EventType
import com.egm.stellio.shared.model.NgsiLdEntity
import com.egm.stellio.shared.model.NgsiLdPropertyInstance
import com.egm.stellio.shared.model.Observation
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.model.parseToNgsiLdAttributes
import com.egm.stellio.shared.util.JsonLdUtils.EGM_VENDOR_ID
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_DATE_TIME_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_OBSERVED_AT_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_PROPERTY_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_PROPERTY_VALUE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_UNIT_CODE_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdFragment
import com.egm.stellio.shared.util.parseLocationFragmentToPointGeoProperty
import com.egm.stellio.shared.util.parseSampleDataToNgsiLd
import com.ninjasquad.springmockk.MockkBean
import io.mockk.*
import junit.framework.TestCase.assertTrue
import org.jgrapht.graph.DefaultEdge
import org.jgrapht.graph.DirectedPseudograph
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.neo4j.ogm.types.spatial.GeographicPoint2d
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import java.net.URI
import java.time.Instant
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.util.Optional
import java.util.UUID

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = [EntityService::class])
@ActiveProfiles("test")
class EntityServiceTests {

    @Value("\${application.jsonld.aquac_context}")
    val aquacContext: String? = null

    @Autowired
    private lateinit var entityService: EntityService

    @MockkBean(relaxed = true)
    private lateinit var entitiesGraphBuilder: EntitiesGraphBuilder

    @MockkBean(relaxed = true)
    private lateinit var neo4jRepository: Neo4jRepository

    @MockkBean(relaxed = true)
    private lateinit var entityRepository: EntityRepository

    @MockkBean
    private lateinit var propertyRepository: PropertyRepository

    /**
     * As Spring's ApplicationEventPublisher is not easily mockable (https://github.com/spring-projects/spring-framework/issues/18907),
     * we are directly mocking the event listener to check it receives what is expected
     */
    @MockkBean
    private lateinit var repositoryEventsListener: RepositoryEventsListener

    @Test
    fun `it should notify of a new entity`() {
        val expectedPayloadInEvent =
            """
        {"id":"urn:ngsi-ld:MortalityRemovalService:014YFA9Z","type":"MortalityRemovalService","@context":["https://raw.githubusercontent.com/easy-global-market/ngsild-api-data-models/master/shared-jsonld-contexts/egm.jsonld","https://raw.githubusercontent.com/easy-global-market/ngsild-api-data-models/master/aquac/jsonld-contexts/aquac.jsonld","http://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"]}
            """.trimIndent()
        val sampleDataWithContext =
            parseSampleDataToNgsiLd("aquac/MortalityRemovalService_standalone.json")
        val mockedBreedingService = mockkClass(Entity::class)

        every { entityRepository.exists(eq("urn:ngsi-ld:MortalityRemovalService:014YFA9Z")) } returns false
        every { entitiesGraphBuilder.build(any()) } returns
            Pair(DirectedPseudograph<NgsiLdEntity, DefaultEdge>(DefaultEdge::class.java), emptyList())
        every { entityRepository.save<Entity>(any()) } returns mockedBreedingService
        every { repositoryEventsListener.handleRepositoryEvent(any()) } just Runs
        every { mockedBreedingService.properties } returns mutableListOf()
        every { mockedBreedingService.id } returns URI.create("urn:ngsi-ld:MortalityRemovalService:014YFA9Z")
        every { entityRepository.getEntityCoreById(any()) } returns mockedBreedingService
        every { mockedBreedingService.serializeCoreProperties(true) } returns mutableMapOf(
            "@id" to "urn:ngsi-ld:MortalityRemovalService:014YFA9Z",
            "@type" to listOf("MortalityRemovalService")
        )
        every { entityRepository.getEntitySpecificProperties(any()) } returns listOf()
        every { entityRepository.getEntityRelationships(any()) } returns listOf()
        every { mockedBreedingService.contexts } returns sampleDataWithContext.contexts

        entityService.createEntity(sampleDataWithContext)

        verify(timeout = 1000, exactly = 1) {
            repositoryEventsListener.handleRepositoryEvent(
                match { entityEvent ->
                    entityEvent.entityType == "MortalityRemovalService" &&
                        entityEvent.entityId == URI.create("urn:ngsi-ld:MortalityRemovalService:014YFA9Z") &&
                        entityEvent.operationType == EventType.CREATE &&
                        entityEvent.payload == expectedPayloadInEvent &&
                        entityEvent.updatedEntity == null
                }
            )
        }
        // I don't know where does this call come from (probably a Spring internal thing)
        // but it is required for verification
        verify { repositoryEventsListener.equals(any()) }
        confirmVerified(repositoryEventsListener)
    }

    @Test
    fun `it should create an entity with a property having a property`() {
        val sampleDataWithContext =
            parseSampleDataToNgsiLd("aquac/BreedingService_propWithProp.json")

        val mockedBreedingService = mockkClass(Entity::class)
        every { mockedBreedingService.id } returns URI.create("urn:ngsi-ld:BreedingService:PropWithProp")

        every { entityRepository.exists(eq("urn:ngsi-ld:BreedingService:PropWithProp")) } returns false
        every { entitiesGraphBuilder.build(any()) } returns
            Pair(DirectedPseudograph<NgsiLdEntity, DefaultEdge>(DefaultEdge::class.java), emptyList())
        every { entityRepository.save<Entity>(any()) } returns mockedBreedingService
        every { neo4jRepository.createPropertyOfSubject(any(), any()) } returns URI.create(UUID.randomUUID().toString())
        every { repositoryEventsListener.handleRepositoryEvent(any()) } just Runs
        every { entityRepository.getEntityCoreById(any()) } returns mockedBreedingService
        every { mockedBreedingService.serializeCoreProperties(true) } returns mutableMapOf(
            "@id" to "urn:ngsi-ld:MortalityRemovalService:014YFA9Z",
            "@type" to listOf("MortalityRemovalService")
        )
        every { entityRepository.getEntitySpecificProperties(any()) } returns listOf()
        every { entityRepository.getEntityRelationships(any()) } returns listOf()
        every { mockedBreedingService.contexts } returns sampleDataWithContext.contexts

        entityService.createEntity(sampleDataWithContext)

        confirmVerified()
    }

    @Test
    fun `it should not create an entity referencing an unknown entity`() {
        val sampleDataWithContext =
            parseSampleDataToNgsiLd("aquac/FeedingService.json")

        every { entityRepository.exists(eq("urn:ngsi-ld:FeedingService:018z59")) } returns false
        every { entitiesGraphBuilder.build(any()) } returns
            Pair(
                DirectedPseudograph<NgsiLdEntity, DefaultEdge>(DefaultEdge::class.java),
                listOf(
                    BatchEntityError(
                        URI.create("urn:ngsi-ld:FeedingService:018z59"),
                        arrayListOf(
                            "Target entity urn:ngsi-ld:Feeder:018z5 does not exist",
                            "Target entity urn:ngsi-ld:FishContainment:0012 does not exist"
                        )
                    )
                )
            )

        val exception = assertThrows<BadRequestDataException>("Creation should have failed") {
            entityService.createEntity(sampleDataWithContext)
        }
        assertEquals(
            "Entity urn:ngsi-ld:FeedingService:018z59 targets unknown entities: " +
                "Target entity urn:ngsi-ld:Feeder:018z5 does not exist," +
                "Target entity urn:ngsi-ld:FishContainment:0012 does not exist",
            exception.message
        )
    }

    @Test
    fun `it should ignore measures from an unknown sensor`() {
        val observation = gimmeAnObservation()

        every { neo4jRepository.getObservingSensorEntity(any(), any(), any()) } returns null

        entityService.updateEntityLastMeasure(observation)

        verify {
            neo4jRepository.getObservingSensorEntity(
                URI.create("urn:ngsi-ld:Sensor:01XYZ"),
                EGM_VENDOR_ID,
                "incoming"
            )
        }

        confirmVerified(neo4jRepository)
    }

    @Test
    fun `it should do nothing if temporal property is not found`() {
        val observation = gimmeAnObservation()
        val sensorId = URI.create("urn:ngsi-ld:Sensor:01XYZ")

        val mockkedSensor = mockkClass(Entity::class)
        val mockkedObservation = mockkClass(Property::class)
        val mockkedObservationId = URI.create("urn:ngsi-ld:Property:${UUID.randomUUID()}")

        every { mockkedSensor.id } returns sensorId
        every { mockkedObservation.id } returns mockkedObservationId
        every { neo4jRepository.getObservingSensorEntity(any(), any(), any()) } returns mockkedSensor
        every { neo4jRepository.getObservedProperty(any(), any()) } returns null

        entityService.updateEntityLastMeasure(observation)

        verify { neo4jRepository.getObservingSensorEntity(sensorId, EGM_VENDOR_ID, "incoming") }
        verify { neo4jRepository.getObservedProperty(sensorId, "observedBy") }
        verify { propertyRepository wasNot Called }

        confirmVerified()
    }

    @Test
    fun `it should update an existing measure`() {
        val observation = gimmeAnObservation()
        val sensorId = URI.create("urn:ngsi-ld:Sensor:01XYZ")
        val expectedPropertyUpdate = Property(
            name = "fishNumber",
            value = 400.0
        )

        val mockkedEntity = mockkClass(Entity::class)
        val mockkedSensor = mockkClass(Entity::class)
        val mockkedObservation = mockkClass(Property::class)

        every { mockkedSensor.id } returns sensorId
        every { mockkedObservation.name } returns observation.attributeName
        every { mockkedObservation.updateValues(any(), any(), any()) } just Runs
        every { mockkedEntity setProperty "location" value any<GeographicPoint2d>() } answers { value }
        every { mockkedEntity.id } returns URI.create("urn:ngsi-ld:BreedingService:01234")
        every { mockkedEntity.type } returns listOf("https://ontology.eglobalmark.com/aquac#BreedingService")
        every { mockkedEntity.contexts } returns listOf(aquacContext!!)
        every { mockkedObservation.id } returns URI.create("property-9999")

        every { neo4jRepository.getObservingSensorEntity(any(), any(), any()) } returns mockkedSensor
        every { neo4jRepository.getObservedProperty(any(), any()) } returns mockkedObservation
        every { neo4jRepository.getEntityByProperty(any()) } returns mockkedEntity
        every { propertyRepository.save(any<Property>()) } returns mockkedObservation
        every { entityRepository.save(any<Entity>()) } returns mockkedEntity
        every { entityRepository.getEntitySpecificProperty(any(), any()) } returns listOf(
            mapOf("property" to expectedPropertyUpdate)
        )
        every { repositoryEventsListener.handleRepositoryEvent(any()) } just Runs

        entityService.updateEntityLastMeasure(observation)

        verify { neo4jRepository.getObservingSensorEntity(sensorId, EGM_VENDOR_ID, "incoming") }
        verify { neo4jRepository.getObservedProperty(sensorId, "observedBy") }
        verify { neo4jRepository.getEntityByProperty(mockkedObservation) }
        verify { propertyRepository.save(any<Property>()) }
        verify { entityRepository.save(any<Entity>()) }
        verify(timeout = 2000) {
            repositoryEventsListener.handleRepositoryEvent(
                match { entityEvent ->
                    entityEvent.entityType == "BreedingService" &&
                        entityEvent.entityId == URI.create("urn:ngsi-ld:BreedingService:01234") &&
                        entityEvent.operationType == EventType.UPDATE &&
                        entityEvent.payload != null &&
                        entityEvent.payload!!.contains("fishNumber") &&
                        entityEvent.updatedEntity == null
                }
            )
        }

        confirmVerified()
    }

    @Test
    fun `it should replace an existing relationship`() {
        val sensorId = URI.create("urn:ngsi-ld:Sensor:013YFZ")
        val relationshipId = URI.create("urn:ngsi-ld:Relationship:92033f60-bb8b-4640-9464-bca23199ac")
        val relationshipTargetId = URI.create("urn:ngsi-ld:FishContainment:8792")

        val payload =
            """
            {
              "filledIn": {
                "type": "Relationship",
                "object": "urn:ngsi-ld:FishContainment:1234"
              }
            }
            """.trimIndent()
        val ngsiLdPayload = parseToNgsiLdAttributes(expandJsonLdFragment(payload, aquacContext!!))

        val mockkedSensor = mockkClass(Entity::class)
        val mockkedRelationshipTarget = mockkClass(Entity::class)
        val mockkedRelationship = mockkClass(Relationship::class)

        every { mockkedSensor.id } returns sensorId
        every { mockkedSensor.type } returns listOf("Sensor")
        every { mockkedRelationship.type } returns listOf("Relationship")
        every { mockkedRelationship.id } returns relationshipId
        every { mockkedRelationshipTarget.id } returns relationshipTargetId
        every { mockkedRelationship setProperty "observedAt" value any<ZonedDateTime>() } answers { value }

        every { neo4jRepository.hasRelationshipOfType(any(), any()) } returns true
        every { neo4jRepository.deleteEntityRelationship(any(), any()) } returns 1
        every { neo4jRepository.createRelationshipOfSubject(any(), any(), any()) } returns URI.create("relId")

        entityService.updateEntityAttributes(sensorId, ngsiLdPayload)

        verify { neo4jRepository.hasRelationshipOfType(any(), eq("filledIn")) }
        verify {
            neo4jRepository.deleteEntityRelationship(
                match {
                    it.id == sensorId
                },
                eq("filledIn"), null, true
            )
        }
        verify {
            neo4jRepository.createRelationshipOfSubject(
                any(),
                any(),
                eq(URI.create("urn:ngsi-ld:FishContainment:1234"))
            )
        }

        confirmVerified()
    }

    @Test
    fun `it should replace an existing default property`() {
        val sensorId = URI.create("urn:ngsi-ld:Sensor:013YFZ")
        val payload =
            """
            {
              "fishAge": {
                "type": "Property",
                "value": 5,
                "unitCode": "months"
              }
            }
            """.trimIndent()
        val ngsiLdPayload = parseToNgsiLdAttributes(expandJsonLdFragment(payload, aquacContext!!))

        val mockkedSensor = mockkClass(Entity::class)

        every { mockkedSensor.id } returns sensorId
        every { mockkedSensor.type } returns listOf("Sensor")

        every { neo4jRepository.hasPropertyInstance(any(), any(), any()) } returns true
        every { neo4jRepository.updateEntityPropertyInstance(any(), any(), any()) } returns 1

        entityService.updateEntityAttributes(sensorId, ngsiLdPayload)

        verify { neo4jRepository.hasPropertyInstance(any(), any(), any()) }
        verify { neo4jRepository.updateEntityPropertyInstance(any(), any(), any()) }

        confirmVerified()
    }

    @Test
    fun `it should replace an existing property having the given datasetId`() {
        val sensorId = URI.create("urn:ngsi-ld:Sensor:013YFZ")
        val payload =
            """
            {
              "fishAge": {
                "type": "Property",
                "value": 5,
                "unitCode": "months",
                "datasetId": "urn:ngsi-ld:Dataset:fishAge:1"
              }
            }
            """.trimIndent()
        val ngsiLdPayload = parseToNgsiLdAttributes(expandJsonLdFragment(payload, aquacContext!!))

        val mockkedSensor = mockkClass(Entity::class)

        every { mockkedSensor.id } returns sensorId
        every { mockkedSensor.type } returns listOf("Sensor")

        every { neo4jRepository.hasPropertyInstance(any(), any(), any()) } returns true
        every { neo4jRepository.updateEntityPropertyInstance(any(), any(), any()) } returns 1

        entityService.updateEntityAttributes(sensorId, ngsiLdPayload)

        verify { neo4jRepository.hasPropertyInstance(any(), any(), URI.create("urn:ngsi-ld:Dataset:fishAge:1")) }
        verify { neo4jRepository.updateEntityPropertyInstance(any(), any(), any()) }

        confirmVerified()
    }

    @Test
    fun `it should replace an existing geoProperty`() {
        val sensorId = URI.create("urn:ngsi-ld:Sensor:013YFZ")
        val payload =
            """
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
        val ngsiLdPayload = parseToNgsiLdAttributes(expandJsonLdFragment(payload, aquacContext!!))

        val mockkedSensor = mockkClass(Entity::class)

        every { mockkedSensor.id } returns sensorId
        every { mockkedSensor.type } returns listOf("Sensor")

        every { neo4jRepository.hasGeoPropertyOfName(any(), any()) } returns true
        every { neo4jRepository.updateLocationPropertyOfEntity(any(), any()) } returns 1

        entityService.updateEntityAttributes(sensorId, ngsiLdPayload)

        verify { neo4jRepository.hasGeoPropertyOfName(any(), any()) }
        verify { neo4jRepository.updateLocationPropertyOfEntity(sensorId, Pair(9.30623, 8.07966)) }

        confirmVerified()
    }

    @Test
    fun `it should correctly parse location property for an entity`() {
        val entityId = URI.create("urn:ngsi-ld:Beehive:123456")
        val ngsiLdGeoProperty = parseLocationFragmentToPointGeoProperty(23.45, 67.87)

        every { neo4jRepository.addLocationPropertyToEntity(any(), any()) } returns 1

        entityService.createLocationProperty(entityId, "location", ngsiLdGeoProperty.instances[0])

        verify { neo4jRepository.addLocationPropertyToEntity(entityId, Pair(23.45, 67.87)) }

        confirmVerified()
    }

    @Test
    fun `it should create a temporal property with all provided attributes`() {
        val entityId = URI.create("urn:ngsi-ld:Beehive:123456")
        val ngsiLdPropertyInstance = NgsiLdPropertyInstance(
            "temperature",
            mapOf(
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
                    mapOf(
                        "@type" to NGSILD_DATE_TIME_TYPE,
                        "@value" to "2019-12-18T10:45:44.248755Z"
                    )
                )
            )
        )

        val mockkedEntity = mockkClass(Entity::class)

        every { mockkedEntity.id } returns entityId
        every { mockkedEntity.properties } returns mutableListOf()
        every { neo4jRepository.createPropertyOfSubject(any(), any()) } returns URI.create(UUID.randomUUID().toString())

        entityService.createEntityProperty(entityId, "temperature", ngsiLdPropertyInstance)

        verify {
            neo4jRepository.createPropertyOfSubject(
                match {
                    it.id == entityId &&
                        it.label == "Entity"
                },
                match {
                    it.name == "temperature" &&
                        it.value == 250 &&
                        it.unitCode == "kg" &&
                        it.observedAt.toString() == "2019-12-18T10:45:44.248755Z"
                }
            )
        }

        confirmVerified()
    }

    @Test
    fun `it should create a new relationship`() {
        val entityId = URI.create("urn:ngsi-ld:Beehive:123456")
        val targetEntityId = URI.create("urn:ngsi-ld:Beekeeper:654321")
        val relationshipId = URI.create(UUID.randomUUID().toString())
        val newRelationship =
            """
            {
                "connectsTo": {
                    "type":"Relationship",
                    "object":"$targetEntityId"
                }
            }
            """.trimIndent()
        val expandedNewRelationship = parseToNgsiLdAttributes(
            expandJsonLdFragment(newRelationship, aquacContext!!)
        )

        val mockkedEntity = mockkClass(Entity::class)
        val mockkedRelationship = mockkClass(Relationship::class)

        every { mockkedEntity.relationships } returns mutableListOf()
        every { mockkedEntity.id } returns entityId
        every { mockkedRelationship.id } returns relationshipId

        every { neo4jRepository.hasRelationshipOfType(any(), any()) } returns false
        every { neo4jRepository.createRelationshipOfSubject(any(), any(), any()) } returns relationshipId

        entityService.appendEntityAttributes(entityId, expandedNewRelationship, false)

        verify {
            neo4jRepository.hasRelationshipOfType(
                match {
                    it.id == entityId &&
                        it.label == "Entity"
                },
                "connectsTo"
            )
        }
        verify {
            neo4jRepository.createRelationshipOfSubject(
                match {
                    it.id == entityId &&
                        it.label == "Entity"
                },
                any(), eq(targetEntityId)
            )
        }

        confirmVerified()
    }

    @Test
    fun `it should not replace a relationship if overwrite is disallowed`() {
        val entityId = URI.create("urn:ngsi-ld:Beehive:123456")
        val newRelationship =
            """
            {
                "connectsTo": {
                    "type":"Relationship",
                    "object":"urn:ngsi-ld:Beekeeper:654321"
                }
            }
            """.trimIndent()
        val expandedNewRelationship = parseToNgsiLdAttributes(
            expandJsonLdFragment(newRelationship, aquacContext!!)
        )

        every { neo4jRepository.hasRelationshipOfType(any(), any()) } returns true

        entityService.appendEntityAttributes(entityId, expandedNewRelationship, true)

        verify {
            neo4jRepository.hasRelationshipOfType(
                match {
                    it.id == entityId &&
                        it.label == "Entity"
                },
                "connectsTo"
            )
        }

        confirmVerified()
    }

    @Test
    fun `it should replace a relationship if overwrite is allowed`() {
        val entityId = URI.create("urn:ngsi-ld:Beehive:123456")
        val targetEntityId = URI.create("urn:ngsi-ld:Beekeeper:654321")
        val relationshipId = URI.create(UUID.randomUUID().toString())
        val newRelationship =
            """
            {
                "connectsTo": {
                    "type":"Relationship",
                    "object":"$targetEntityId"
                }
            }
            """.trimIndent()
        val expandedNewRelationship = parseToNgsiLdAttributes(
            expandJsonLdFragment(newRelationship, aquacContext!!)
        )

        val mockkedEntity = mockkClass(Entity::class)
        val mockkedRelationship = mockkClass(Relationship::class)

        every { mockkedEntity.relationships } returns mutableListOf()
        every { mockkedEntity.id } returns entityId
        every { mockkedRelationship.id } returns relationshipId

        every { neo4jRepository.hasRelationshipOfType(any(), any()) } returns true
        every { neo4jRepository.deleteEntityRelationship(any(), any()) } returns 1
        every { neo4jRepository.createRelationshipOfSubject(any(), any(), any()) } returns relationshipId

        entityService.appendEntityAttributes(entityId, expandedNewRelationship, false)

        verify {
            neo4jRepository.hasRelationshipOfType(
                match {
                    it.id == entityId &&
                        it.label == "Entity"
                },
                "connectsTo"
            )
        }
        verify {
            neo4jRepository.deleteEntityRelationship(
                match {
                    it.id == entityId &&
                        it.label == "Entity"
                },
                "connectsTo", null, false
            )
        }

        confirmVerified()
    }

    @Test
    fun `it should create a new property`() {
        val entityId = URI.create("urn:ngsi-ld:Beehive:123456")
        val newProperty =
            """
            {
              "fishNumber": {
                "type": "Property",
                "value": 500
              }
            }
            """.trimIndent()
        val expandedNewProperty = parseToNgsiLdAttributes(
            expandJsonLdFragment(newProperty, aquacContext!!)
        )

        val mockkedEntity = mockkClass(Entity::class)

        every { mockkedEntity.id } returns entityId
        every { mockkedEntity.properties } returns mutableListOf()

        every { neo4jRepository.hasPropertyInstance(any(), any()) } returns false
        every { mockkedEntity.id } returns entityId
        every { neo4jRepository.createPropertyOfSubject(any(), any()) } returns URI.create(UUID.randomUUID().toString())

        entityService.appendEntityAttributes(entityId, expandedNewProperty, false)

        verify {
            neo4jRepository.hasPropertyInstance(
                match {
                    it.id == entityId &&
                        it.label == "Entity"
                },
                "https://ontology.eglobalmark.com/aquac#fishNumber"
            )
        }
        verify {
            neo4jRepository.createPropertyOfSubject(
                match {
                    it.id == entityId &&
                        it.label == "Entity"
                },
                match {
                    it.value == 500 &&
                        it.name == "https://ontology.eglobalmark.com/aquac#fishNumber"
                }
            )
        }
        verify { neo4jRepository.updateEntityModifiedDate(eq(entityId)) }

        confirmVerified()
    }

    @Test
    fun `it should create a new multi attribute property`() {
        val entityId = URI.create("urn:ngsi-ld:Beehive:123456")
        val newProperty =
            """
            {
              "fishNumber": [{
                "type": "Property",
                "value": 500,
                "datasetId": "urn:ngsi-ld:Dataset:fishNumber:1"
              },
              {
                "type": "Property",
                "value": 600
              }]
            }
            """.trimIndent()
        val expandedNewProperty = parseToNgsiLdAttributes(
            expandJsonLdFragment(newProperty, aquacContext!!)
        )

        val mockkedEntity = mockkClass(Entity::class)

        every { mockkedEntity.id } returns entityId
        every { mockkedEntity.properties } returns mutableListOf()

        val datasetSetIds = mutableListOf<URI>()
        val createdProperties = mutableListOf<Property>()

        every { neo4jRepository.hasPropertyInstance(any(), any(), capture(datasetSetIds)) } returns false
        every {
            neo4jRepository.createPropertyOfSubject(any(), capture(createdProperties))
        } returns URI.create(UUID.randomUUID().toString())

        entityService.appendEntityAttributes(entityId, expandedNewProperty, false)

        assertTrue(datasetSetIds.contains(URI.create("urn:ngsi-ld:Dataset:fishNumber:1")))

        assertTrue(
            createdProperties.any {
                it.value == 500 &&
                    it.name == "https://ontology.eglobalmark.com/aquac#fishNumber" &&
                    it.datasetId == URI.create("urn:ngsi-ld:Dataset:fishNumber:1")
            }.and(
                createdProperties.any {
                    it.value == 600 &&
                        it.name == "https://ontology.eglobalmark.com/aquac#fishNumber" &&
                        it.datasetId == null
                }
            )
        )

        verify(exactly = 2) {
            neo4jRepository.hasPropertyInstance(
                match {
                    it.id == entityId &&
                        it.label == "Entity"
                },
                "https://ontology.eglobalmark.com/aquac#fishNumber",
                datasetId = any()
            )
        }

        verify(exactly = 2) {
            neo4jRepository.createPropertyOfSubject(
                match {
                    it.id == entityId &&
                        it.label == "Entity"
                },
                any()
            )
        }

        verify { neo4jRepository.updateEntityModifiedDate(eq(entityId)) }

        confirmVerified()
    }

    @Test
    fun `it should not override the default instance if overwrite is disallowed`() {
        val entityId = URI.create("urn:ngsi-ld:Beehive:123456")
        val newProperty =
            """
            {
              "fishNumber": [{
                "type": "Property",
                "value": 600
              }]
            }
            """.trimIndent()
        val expandedNewProperty = parseToNgsiLdAttributes(
            expandJsonLdFragment(newProperty, aquacContext!!)
        )

        val mockkedEntity = mockkClass(Entity::class)

        every { mockkedEntity.id } returns entityId
        every { entityRepository.findById(any()) } returns Optional.of(mockkedEntity)

        every { neo4jRepository.hasPropertyInstance(any(), any()) } returns true

        entityService.appendEntityAttributes(entityId, expandedNewProperty, true)

        verify {
            neo4jRepository.hasPropertyInstance(
                match {
                    it.id == entityId &&
                        it.label == "Entity"
                },
                "https://ontology.eglobalmark.com/aquac#fishNumber",
                datasetId = null
            )
        }

        verify(inverse = true) {
            neo4jRepository.createPropertyOfSubject(
                match {
                    it.id == entityId &&
                        it.label == "Entity"
                },
                any()
            )
        }
        confirmVerified()
    }

    @Test
    fun `it should create a new geoproperty`() {
        val entityId = URI.create("urn:ngsi-ld:Beehive:123456")
        val newGeoProperty =
            """
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

        val expandedNewGeoProperty = parseToNgsiLdAttributes(
            expandJsonLdFragment(newGeoProperty, aquacContext!!)
        )

        val mockkedEntity = mockkClass(Entity::class)

        every { mockkedEntity.id } returns entityId

        every { neo4jRepository.hasGeoPropertyOfName(any(), any()) } returns false
        every { entityRepository.save<Entity>(any()) } returns mockkedEntity
        every { neo4jRepository.addLocationPropertyToEntity(any(), any()) } returns 1

        entityService.appendEntityAttributes(entityId, expandedNewGeoProperty, false)

        verify { neo4jRepository.hasGeoPropertyOfName(any(), any()) }
        verify { neo4jRepository.addLocationPropertyToEntity(entityId, Pair(29.30623, 83.07966)) }

        confirmVerified()
    }

    @Test
    fun `it should replace a geoproperty if overwrite is allowed`() {
        val entityId = URI.create("urn:ngsi-ld:Beehive:123456")
        val newGeoProperty =
            """
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

        val expandedNewGeoProperty = parseToNgsiLdAttributes(
            expandJsonLdFragment(newGeoProperty, aquacContext!!)
        )

        every { neo4jRepository.hasGeoPropertyOfName(any(), any()) } returns true
        every { neo4jRepository.updateLocationPropertyOfEntity(any(), any()) } returns 1

        entityService.appendEntityAttributes(entityId, expandedNewGeoProperty, false)

        verify { neo4jRepository.hasGeoPropertyOfName(any(), any()) }
        verify { neo4jRepository.updateLocationPropertyOfEntity(entityId, Pair(29.30623, 83.07966)) }

        confirmVerified()
    }

    @Test
    fun `it should delete all entity property instances`() {
        val entityId = URI.create("urn:ngsi-ld:Beehive:123456")

        every { neo4jRepository.hasPropertyOfName(any(), any()) } returns true
        every { neo4jRepository.deleteEntityProperty(any(), any(), any()) } returns 1

        entityService.deleteEntityAttribute(entityId, "https://ontology.eglobalmark.com/aquac#fishNumber")

        verify {
            neo4jRepository.hasPropertyOfName(
                match {
                    it.id == URI.create("urn:ngsi-ld:Beehive:123456") &&
                        it.label == "Entity"
                },
                "https://ontology.eglobalmark.com/aquac#fishNumber"
            )
        }
        verify {
            neo4jRepository.deleteEntityProperty(
                match {
                    it.id == URI.create("urn:ngsi-ld:Beehive:123456") &&
                        it.label == "Entity"
                },
                "https://ontology.eglobalmark.com/aquac#fishNumber", null, true
            )
        }

        confirmVerified()
    }

    @Test
    fun `it should delete an entity relationship instance with the provided datasetId`() {
        val entityId = URI.create("urn:ngsi-ld:Beehive:123456")

        every { neo4jRepository.hasPropertyOfName(any(), any()) } returns false
        every { neo4jRepository.hasRelationshipInstance(any(), any(), any()) } returns true
        every { neo4jRepository.deleteEntityRelationship(any(), any(), any(), any()) } returns 1

        entityService.deleteEntityAttributeInstance(
            entityId,
            "https://ontology.eglobalmark.com/aquac#connectsTo",
            URI.create("urn:ngsi-ld:Dataset:connectsTo:01")
        )

        verify {
            neo4jRepository.hasRelationshipInstance(
                match {
                    it.id == URI.create("urn:ngsi-ld:Beehive:123456") &&
                        it.label == "Entity"
                },
                any(), URI.create("urn:ngsi-ld:Dataset:connectsTo:01")
            )
        }

        verify {
            neo4jRepository.deleteEntityRelationship(
                match {
                    it.id == URI.create("urn:ngsi-ld:Beehive:123456") &&
                        it.label == "Entity"
                },
                any(), URI.create("urn:ngsi-ld:Dataset:connectsTo:01"), false
            )
        }

        confirmVerified()
    }

    @Test
    fun `it should not delete all entity attribute instances if the attribute is not found`() {
        val entityId = URI.create("urn:ngsi-ld:Beehive:123456")

        every { neo4jRepository.hasPropertyOfName(any(), any()) } returns false
        every { neo4jRepository.hasRelationshipOfType(any(), any()) } returns false

        val exception = assertThrows<ResourceNotFoundException>(
            "Attribute fishNumber not found in entity urn:ngsi-ld:Beehive:123456"
        ) {
            entityService.deleteEntityAttribute(
                entityId, "https://ontology.eglobalmark.com/aquac#fishNumber"
            )
        }
        assertEquals(
            "Attribute https://ontology.eglobalmark.com/aquac#fishNumber not found " +
                "in entity urn:ngsi-ld:Beehive:123456",
            exception.message
        )
        confirmVerified()
    }

    @Test
    fun `it should not delete the default property instance if not found`() {
        val entityId = URI.create("urn:ngsi-ld:Beehive:123456")

        every { neo4jRepository.hasPropertyInstance(any(), any(), any()) } returns false
        every { neo4jRepository.hasRelationshipInstance(any(), any(), any()) } returns false

        val exception = assertThrows<ResourceNotFoundException>(
            "Default instance of fishNumber not found in entity urn:ngsi-ld:Beehive:123456"
        ) {
            entityService.deleteEntityAttributeInstance(
                entityId, "https://ontology.eglobalmark.com/aquac#fishNumber", null
            )
        }
        assertEquals(
            "Default instance of https://ontology.eglobalmark.com/aquac#fishNumber not found " +
                "in entity urn:ngsi-ld:Beehive:123456",
            exception.message
        )
        confirmVerified()
    }

    private fun gimmeAnObservation(): Observation {
        return Observation(
            attributeName = "incoming",
            latitude = 43.12,
            longitude = 65.43,
            observedBy = URI.create("urn:ngsi-ld:Sensor:01XYZ"),
            unitCode = "CEL",
            value = 12.4,
            observedAt = Instant.now().atZone(ZoneOffset.UTC)
        )
    }
}
