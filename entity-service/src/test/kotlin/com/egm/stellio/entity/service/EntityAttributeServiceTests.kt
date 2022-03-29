package com.egm.stellio.entity.service

import com.egm.stellio.entity.model.Property
import com.egm.stellio.entity.model.Relationship
import com.egm.stellio.entity.model.UpdateOperationResult
import com.egm.stellio.entity.model.UpdatedDetails
import com.egm.stellio.entity.repository.Neo4jRepository
import com.egm.stellio.entity.repository.PropertyRepository
import com.egm.stellio.entity.repository.RelationshipRepository
import com.egm.stellio.shared.model.InternalErrorException
import com.egm.stellio.shared.util.AQUAC_COMPOUND_CONTEXT
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdFragment
import com.egm.stellio.shared.util.toUri
import com.ninjasquad.springmockk.MockkBean
import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import java.net.URI
import java.util.UUID

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = [EntityAttributeService::class])
@ActiveProfiles("test")
class EntityAttributeServiceTests {

    @Autowired
    private lateinit var entityAttributeService: EntityAttributeService

    @MockkBean(relaxed = true)
    private lateinit var entityService: EntityService

    @MockkBean(relaxed = true)
    private lateinit var neo4jRepository: Neo4jRepository

    @MockkBean
    private lateinit var propertyRepository: PropertyRepository

    @MockkBean
    private lateinit var relationshipRepository: RelationshipRepository

    private val fishContainmentUri = "urn:ngsi-ld:FishContainment:1234".toUri()
    private val fishUri = "urn:ngsi-ld:Fish:013YFZ".toUri()

    @Test
    fun `it should partially update the default instance of property`() {
        val propertyName = "fishAge"

        val payload =
            """
            {
                "value": 5,
                "unitCode": "months"
            }
            """.trimIndent()

        val expandedPayload = expandJsonLdFragment(propertyName, payload, listOf(AQUAC_COMPOUND_CONTEXT))
        val property = Property(name = propertyName, unitCode = "years", value = 0)

        every { neo4jRepository.hasRelationshipInstance(any(), any(), any()) } returns false
        every { neo4jRepository.hasPropertyInstance(any(), any(), any()) } returns true
        every { propertyRepository.getPropertyOfSubject(any(), any()) } returns property
        every { propertyRepository.save(any()) } returns property

        entityAttributeService.partialUpdateEntityAttribute(fishUri, expandedPayload, listOf(AQUAC_COMPOUND_CONTEXT))

        confirmVerified()
    }

    @Test
    fun `it should partially update the property instances with given datasetId`() {
        val propertyName = "fishAge"

        val payload =
            """
            {
                "value": 10,
                "observedAt": "2019-12-18T10:45:44.248755Z",
                "datasetId": "urn:ngsi-ld:Dataset:fishAge:1"
            }
            """.trimIndent()

        val expandedPayload = expandJsonLdFragment(propertyName, payload, listOf(AQUAC_COMPOUND_CONTEXT))
        val property = Property(name = propertyName, unitCode = "years", value = 0)

        every { neo4jRepository.hasRelationshipInstance(any(), any(), any()) } returns false
        every { neo4jRepository.hasPropertyInstance(any(), any(), any()) } returns true
        every { propertyRepository.getPropertyOfSubject(any(), any(), any()) } returns property
        every { propertyRepository.save(any()) } returns property

        entityAttributeService.partialUpdateEntityAttribute(fishUri, expandedPayload, listOf(AQUAC_COMPOUND_CONTEXT))

        confirmVerified()
    }

    @Test
    fun `it should partially update a property and create its new properties`() {
        val propertyName = "fishAge"
        val payload =
            """
            {
                "value": 5,
                "unitCode": "months",
                "depth": {
                    "type": "Property",
                    "value": 10
                },
                "since": {
                    "type": "Property",
                    "value": 2019
                }
            }
            """.trimIndent()

        val expandedPayload = expandJsonLdFragment(propertyName, payload, listOf(AQUAC_COMPOUND_CONTEXT))
        val property = Property(name = propertyName, unitCode = "months", value = 0)
        val depthProperty = Property(name = "depth", value = 0)

        every { neo4jRepository.hasRelationshipInstance(any(), any(), any()) } returns false
        every { neo4jRepository.hasPropertyInstance(any(), any(), any()) } returns true
        every { propertyRepository.getPropertyOfSubject(any(), any()) } returns property
        every { propertyRepository.save(any()) } returns property
        every {
            neo4jRepository.hasPropertyInstance(
                match { it.label == "Attribute" },
                "https://uri.etsi.org/ngsi-ld/default-context/since",
                any()
            )
        } returns false
        every { entityService.createAttributeProperties(any(), any()) } returns true
        every {
            propertyRepository.getPropertyOfSubject(
                property.id,
                "https://ontology.eglobalmark.com/egm#depth",
                any()
            )
        } returns depthProperty

        entityAttributeService.partialUpdateEntityAttribute(fishUri, expandedPayload, listOf(AQUAC_COMPOUND_CONTEXT))

        verify {
            entityService.createAttributeProperties(
                match { it == property.id },
                match {
                    it.size == 1 &&
                        it[0].name == "https://uri.etsi.org/ngsi-ld/default-context/since" &&
                        it[0].instances[0].value == 2019
                }
            )
        }

        confirmVerified()
    }

    @Test
    fun `it should partially update a property and its relationships`() {
        val propertyName = "fishAge"

        val payload =
            """
            {
                "unitCode": "years",
                "measuredBy": {
                    "type": "Relationship",
                    "object": "urn:ngsi-ld:Sensor:789D21",
                    "datasetId": "urn:ngsi-ld:Dataset:measuredBy:1",
                    "observedAt": "2020-06-18T10:45:44.248755Z"
                }
            }
            """.trimIndent()

        val expandedPayload = expandJsonLdFragment(propertyName, payload, listOf(AQUAC_COMPOUND_CONTEXT))
        val property = Property(name = propertyName, unitCode = "years", value = 0)
        val relationship = Relationship(objectId = generateRandomObjectId(), type = listOf("measuredBy"))

        every { neo4jRepository.hasRelationshipInstance(match { it.label == "Entity" }, any(), any()) } returns false
        every { neo4jRepository.hasPropertyInstance(any(), any(), any()) } returns true
        every { propertyRepository.getPropertyOfSubject(fishUri, any()) } returns property
        every { propertyRepository.save(any()) } returns property
        every { neo4jRepository.hasRelationshipInstance(match { it.label == "Attribute" }, any(), any()) } returns true
        every { relationshipRepository.getRelationshipOfSubject(property.id, any()) } returns relationship
        every { neo4jRepository.updateRelationshipTargetOfSubject(any(), any(), any()) } returns true
        every { relationshipRepository.save(any()) } returns relationship

        entityAttributeService.partialUpdateEntityAttribute(fishUri, expandedPayload, listOf(AQUAC_COMPOUND_CONTEXT))

        verify {
            neo4jRepository.updateRelationshipTargetOfSubject(
                property.id,
                "measuredBy",
                "urn:ngsi-ld:Sensor:789D21".toUri()
            )
        }
        confirmVerified()
    }

    @Test
    fun `it should throw an exception if an update could not be performed`() {
        val propertyName = "fishAge"
        val payload =
            """
            {
                "value": 5,
                "unitCode": "months",
                "depth": {
                    "type": "Property",
                    "value": 10
                }
            }
            """.trimIndent()

        val expandedPayload = expandJsonLdFragment(propertyName, payload, listOf(AQUAC_COMPOUND_CONTEXT))
        val property = Property(name = propertyName, unitCode = "months", value = 0)

        every { neo4jRepository.hasRelationshipInstance(any(), any(), any()) } returns false
        every { neo4jRepository.hasPropertyInstance(any(), any(), any()) } returns true
        every { propertyRepository.getPropertyOfSubject(fishUri, any()) } returns property
        every { propertyRepository.save(any()) } returns property
        every {
            neo4jRepository.hasPropertyInstance(
                match { it.id == property.id },
                any()
            )
        } returns false
        every { entityService.createAttributeProperties(any(), any()) } returns false

        val exception = assertThrows<InternalErrorException> {
            entityAttributeService.partialUpdateEntityAttribute(
                fishUri, expandedPayload, listOf(AQUAC_COMPOUND_CONTEXT)
            )
        }
        assertEquals("Partial update operation failed to perform the whole update", exception.message)
    }

    @Test
    fun `it should partially update the default instance of relationship`() {
        val relationshipType = "isContainedIn"

        val payload =
            """
            {
                "object": "$fishContainmentUri"
            }
            """.trimIndent()

        val expandedPayload = expandJsonLdFragment(relationshipType, payload, listOf(AQUAC_COMPOUND_CONTEXT))
        val relationship = Relationship(objectId = generateRandomObjectId(), type = listOf(relationshipType))

        every { neo4jRepository.hasRelationshipInstance(any(), any(), any()) } returns true
        every { relationshipRepository.getRelationshipOfSubject(any(), any()) } returns relationship
        every { neo4jRepository.updateRelationshipTargetOfSubject(any(), any(), any()) } returns true
        every { relationshipRepository.save(any()) } returns relationship

        entityAttributeService.partialUpdateEntityAttribute(fishUri, expandedPayload, listOf(AQUAC_COMPOUND_CONTEXT))

        verify {
            neo4jRepository.updateRelationshipTargetOfSubject(
                fishUri,
                relationshipType,
                fishContainmentUri
            )
        }

        confirmVerified()
    }

    @Test
    fun `it should partially update the relationship instance with given datasetId`() {
        val relationshipType = "isContainedIn"

        val payload =
            """
            {
                "object": "$fishContainmentUri",
                "observedAt": "2019-12-18T10:45:44.248755Z",
                "datasetId": "urn:ngsi-ld:Dataset:isContainedIn:1"
            }
            """.trimIndent()

        val expandedPayload = expandJsonLdFragment(relationshipType, payload, listOf(AQUAC_COMPOUND_CONTEXT))
        val relationship = Relationship(objectId = generateRandomObjectId(), type = listOf(relationshipType))

        every { neo4jRepository.hasRelationshipInstance(any(), any(), any()) } returns true
        every { relationshipRepository.getRelationshipOfSubject(any(), any(), any()) } returns relationship
        every { neo4jRepository.updateRelationshipTargetOfSubject(any(), any(), any()) } returns true
        every { relationshipRepository.save(any()) } returns relationship

        entityAttributeService.partialUpdateEntityAttribute(fishUri, expandedPayload, listOf(AQUAC_COMPOUND_CONTEXT))

        verify {
            neo4jRepository.updateRelationshipTargetOfSubject(
                fishUri,
                relationshipType,
                fishContainmentUri,
                "urn:ngsi-ld:Dataset:isContainedIn:1".toUri()
            )
        }

        confirmVerified()
    }

    @Test
    fun `it should partially update a relationship and its properties`() {
        val relationshipType = "isContainedIn"

        val payload =
            """
            {
                "object": "$fishContainmentUri",
                "observedAt": "2013-12-18T10:45:44.248755Z",
                "datasetId": "urn:ngsi-ld:Dataset:isContainedIn:5",
                "depth": {
                    "type": "Property",
                    "value": 17
                }
            }
            """.trimIndent()

        val expandedPayload = expandJsonLdFragment(relationshipType, payload, listOf(AQUAC_COMPOUND_CONTEXT))
        val relationship = Relationship(objectId = generateRandomObjectId(), type = listOf(relationshipType))
        val property = Property(name = "depth", value = 0)

        every { neo4jRepository.hasRelationshipInstance(match { it.label == "Entity" }, any(), any()) } returns true
        every { relationshipRepository.getRelationshipOfSubject(any(), any(), any()) } returns relationship
        every { neo4jRepository.updateRelationshipTargetOfSubject(any(), any(), any()) } returns true
        every { relationshipRepository.save(any()) } returns relationship
        every { neo4jRepository.hasRelationshipInstance(match { it.label == "Attribute" }, any(), any()) } returns false
        every { neo4jRepository.hasPropertyInstance(any(), any(), any()) } returns true
        every { propertyRepository.getPropertyOfSubject(any(), any(), any()) } returns property
        every { propertyRepository.getPropertyOfSubject(any(), any()) } returns property
        every { propertyRepository.save(any()) } returns property

        entityAttributeService.partialUpdateEntityAttribute(fishUri, expandedPayload, listOf(AQUAC_COMPOUND_CONTEXT))

        verify {
            neo4jRepository.updateRelationshipTargetOfSubject(
                fishUri,
                relationshipType,
                fishContainmentUri,
                "urn:ngsi-ld:Dataset:isContainedIn:5".toUri()
            )
        }

        confirmVerified()
    }

    @Test
    fun `it should partially update a relationship and create its new relationships`() {
        val relationshipType = "isContainedIn"
        val payload =
            """
            {
                "observedAt": "2018-12-18T10:45:44.248755Z",
                "measuredBy": {
                    "type": "Relationship",
                    "object": "urn:ngsi-ld:Sensor:789D21"
                },
                "observedBy": {
                    "type": "Relationship",
                    "object": "urn:ngsi-ld:Sensor:124701"
                }
            }
            """.trimIndent()

        val expandedPayload = expandJsonLdFragment(relationshipType, payload, listOf(AQUAC_COMPOUND_CONTEXT))
        val relationship = Relationship(objectId = generateRandomObjectId(), type = listOf(relationshipType))
        val measuredByRelationship = Relationship(objectId = generateRandomObjectId(), type = listOf(relationshipType))

        every { neo4jRepository.hasRelationshipInstance(any(), any(), any()) } returns true
        every { relationshipRepository.getRelationshipOfSubject(fishUri, any()) } returns relationship
        every { relationshipRepository.save(any()) } returns relationship
        every { neo4jRepository.hasRelationshipInstance(any(), "observedBy", any()) } returns false
        every { relationshipRepository.getRelationshipOfSubject(relationship.id, any()) } returns measuredByRelationship
        every { neo4jRepository.updateRelationshipTargetOfSubject(any(), any(), any()) } returns true
        every { entityService.createAttributeRelationships(any(), any()) } returns true

        entityAttributeService.partialUpdateEntityAttribute(fishUri, expandedPayload, listOf(AQUAC_COMPOUND_CONTEXT))

        verify {
            neo4jRepository.updateRelationshipTargetOfSubject(
                relationship.id,
                "measuredBy",
                "urn:ngsi-ld:Sensor:789D21".toUri()
            )
        }
        verify {
            entityService.createAttributeRelationships(
                relationship.id,
                match {
                    it.size == 1 &&
                        it[0].compactName == "observedBy" &&
                        it[0].instances[0].objectId == "urn:ngsi-ld:Sensor:124701".toUri()
                }
            )
        }
        confirmVerified()
    }

    @Test
    fun `it should return ignored UpdateOperationResult for unknown attribute`() {
        val relationshipType = "isContainedIn"

        val payload =
            """
            {
                "object": "$fishContainmentUri"
            }
            """.trimIndent()

        val expandedPayload = expandJsonLdFragment(relationshipType, payload, listOf(AQUAC_COMPOUND_CONTEXT))
        every { neo4jRepository.hasRelationshipOfType(any(), any()) } returns false
        every { neo4jRepository.hasPropertyInstance(any(), any(), any()) } returns false

        val result = entityAttributeService.partialUpdateEntityAttribute(
            fishUri,
            expandedPayload,
            listOf(AQUAC_COMPOUND_CONTEXT)
        )
        assertTrue(result.updated.isEmpty())
        assertTrue(result.notUpdated.isNotEmpty())
        confirmVerified()
    }

    @Test
    fun `it should partially update a multi instances property`() {
        val propertyName = "fishAge"
        val payload =
            """
            [
                {
                    "value": 10,
                    "observedAt": "2019-12-18T10:45:44.248755Z",
                    "datasetId": "urn:ngsi-ld:Dataset:fishAge:1"
                },
                {
                    "value": 12,
                    "observedAt": "2019-12-18T10:45:44.248755Z"
                }
            ]
            """.trimIndent()

        val expandedPayload = expandJsonLdFragment(propertyName, payload, listOf(AQUAC_COMPOUND_CONTEXT))
        val property = Property(name = propertyName, unitCode = "years", value = 0)

        every { neo4jRepository.hasRelationshipInstance(any(), any(), any()) } returns false
        every { neo4jRepository.hasPropertyInstance(any(), any(), any()) } returns true
        every { propertyRepository.getPropertyOfSubject(any(), any()) } returns property
        every { propertyRepository.getPropertyOfSubject(any(), any(), any()) } returns property
        every { propertyRepository.save(any()) } returns property

        val result = entityAttributeService.partialUpdateEntityAttribute(
            fishUri,
            expandedPayload,
            listOf(AQUAC_COMPOUND_CONTEXT)
        )

        assertTrue(
            result.updated.containsAll(
                listOf(
                    UpdatedDetails(
                        "https://ontology.eglobalmark.com/aquac#fishAge",
                        "urn:ngsi-ld:Dataset:fishAge:1".toUri(),
                        UpdateOperationResult.UPDATED
                    ),
                    UpdatedDetails(
                        "https://ontology.eglobalmark.com/aquac#fishAge",
                        null,
                        UpdateOperationResult.UPDATED
                    )
                )
            )
        )
        assertTrue(result.notUpdated.isEmpty())
        confirmVerified()
    }

    @Test
    fun `it should partially update a multi instances relationship`() {
        val relationshipType = "isContainedIn"
        val payload =
            """
            [
                {
                    "object": "$fishContainmentUri",
                    "datasetId": "urn:ngsi-ld:Dataset:isContainedIn:1",
                    "observedAt": "2020-06-18T11:45:44.248755Z"
                },
                {
                    "object": "$fishContainmentUri",
                    "datasetId": "urn:ngsi-ld:Dataset:isContainedIn:2",
                    "observedAt": "2020-06-18T11:45:44.248755Z"
                }
            ]
            """.trimIndent()

        val expandedPayload = expandJsonLdFragment(relationshipType, payload, listOf(AQUAC_COMPOUND_CONTEXT))
        val relationship = Relationship(objectId = generateRandomObjectId(), type = listOf(relationshipType))
        every { neo4jRepository.hasRelationshipInstance(any(), any(), any()) } returns true
        every { relationshipRepository.getRelationshipOfSubject(any(), any(), any()) } returns relationship
        every { neo4jRepository.updateRelationshipTargetOfSubject(any(), any(), any()) } returns true
        every { relationshipRepository.save(any()) } returns relationship

        val result = entityAttributeService.partialUpdateEntityAttribute(
            fishUri,
            expandedPayload,
            listOf(AQUAC_COMPOUND_CONTEXT)
        )
        verify {
            neo4jRepository.updateRelationshipTargetOfSubject(
                fishUri,
                "isContainedIn",
                fishContainmentUri,
                match {
                    it in listOf(
                        "urn:ngsi-ld:Dataset:isContainedIn:1".toUri(), "urn:ngsi-ld:Dataset:isContainedIn:2".toUri()
                    )
                }
            )
        }
        assertTrue(
            result.updated.containsAll(
                listOf(
                    UpdatedDetails(
                        "https://uri.etsi.org/ngsi-ld/default-context/isContainedIn",
                        "urn:ngsi-ld:Dataset:isContainedIn:1".toUri(),
                        UpdateOperationResult.UPDATED
                    ),
                    UpdatedDetails(
                        "https://uri.etsi.org/ngsi-ld/default-context/isContainedIn",
                        "urn:ngsi-ld:Dataset:isContainedIn:2".toUri(),
                        UpdateOperationResult.UPDATED
                    )
                )

            )
        )
        assertTrue(result.notUpdated.isEmpty())

        confirmVerified()
    }

    private fun generateRandomObjectId(): URI =
        "urn:ngsi-ld:Entity:${UUID.randomUUID()}".toUri()
}
