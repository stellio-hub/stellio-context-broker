package com.egm.stellio.search.discovery.service

import arrow.core.Either
import com.egm.stellio.search.common.util.execute
import com.egm.stellio.search.common.util.toUri
import com.egm.stellio.search.discovery.model.AttributeDetails
import com.egm.stellio.search.discovery.model.AttributeType
import com.egm.stellio.search.discovery.model.AttributeTypeInfo
import com.egm.stellio.search.entity.model.Attribute
import com.egm.stellio.search.entity.model.Entity
import com.egm.stellio.search.support.EMPTY_JSON_PAYLOAD
import com.egm.stellio.search.support.WithKafkaContainer
import com.egm.stellio.search.support.WithTimescaleContainer
import com.egm.stellio.search.support.gimmeEntityPayload
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.NGSILD_LOCATION_IRI
import com.egm.stellio.shared.model.NGSILD_LOCATION_TERM
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.util.APIARY_IRI
import com.egm.stellio.shared.util.APIARY_TERM
import com.egm.stellio.shared.util.APIC_COMPOUND_CONTEXTS
import com.egm.stellio.shared.util.BEEHIVE_IRI
import com.egm.stellio.shared.util.BEEHIVE_TERM
import com.egm.stellio.shared.util.INCOMING_IRI
import com.egm.stellio.shared.util.INCOMING_TERM
import com.egm.stellio.shared.util.MANAGED_BY_IRI
import com.egm.stellio.shared.util.MANAGED_BY_TERM
import com.egm.stellio.shared.util.OUTGOING_IRI
import com.egm.stellio.shared.util.OUTGOING_TERM
import com.egm.stellio.shared.util.SENSOR_IRI
import com.egm.stellio.shared.util.SENSOR_TERM
import com.egm.stellio.shared.util.TEMPERATURE_IRI
import com.egm.stellio.shared.util.attributeNotFoundMessage
import com.egm.stellio.shared.util.shouldFail
import com.egm.stellio.shared.util.shouldSucceedWith
import com.egm.stellio.shared.util.toUri
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.data.r2dbc.core.delete
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.test.context.ActiveProfiles
import java.time.Instant
import java.time.ZoneOffset

@SpringBootTest
@ActiveProfiles("test")
class AttributeServiceTests : WithTimescaleContainer, WithKafkaContainer() {

    @Autowired
    private lateinit var attributeService: AttributeService

    @Autowired
    private lateinit var databaseClient: DatabaseClient

    @Autowired
    private lateinit var r2dbcEntityTemplate: R2dbcEntityTemplate

    private val now = Instant.now().atZone(ZoneOffset.UTC)

    private val entityPayload1 = gimmeEntityPayload("urn:ngsi-ld:BeeHive:TESTA", listOf(BEEHIVE_IRI, SENSOR_IRI))
    private val entityPayload2 = gimmeEntityPayload("urn:ngsi-ld:Sensor:TESTB", listOf(SENSOR_IRI))
    private val entityPayload3 = gimmeEntityPayload("urn:ngsi-ld:Apiary:TESTC", listOf(APIARY_IRI))
    private val attribute1 = newAttribute(
        "urn:ngsi-ld:BeeHive:TESTA",
        INCOMING_IRI,
        Attribute.AttributeType.Property,
        Attribute.AttributeValueType.NUMBER
    )
    private val attribute2 = newAttribute(
        "urn:ngsi-ld:BeeHive:TESTA",
        MANAGED_BY_IRI,
        Attribute.AttributeType.Relationship,
        Attribute.AttributeValueType.STRING
    )
    private val attribute3 = newAttribute(
        "urn:ngsi-ld:Apiary:TESTC",
        NGSILD_LOCATION_IRI,
        Attribute.AttributeType.GeoProperty,
        Attribute.AttributeValueType.GEOMETRY
    )
    private val attribute4 = newAttribute(
        "urn:ngsi-ld:Sensor:TESTB",
        OUTGOING_IRI,
        Attribute.AttributeType.Property,
        Attribute.AttributeValueType.GEOMETRY
    )

    @AfterEach
    fun clearPreviousAttributesAndObservations() {
        r2dbcEntityTemplate.delete<Entity>().from("entity_payload").all().block()
        r2dbcEntityTemplate.delete<Attribute>().from("temporal_entity_attribute").all().block()
    }

    @BeforeEach
    fun createEntity() {
        createEntityPayload(entityPayload1)
        createEntityPayload(entityPayload2)
        createEntityPayload(entityPayload3)
        createAttribute(attribute1)
        createAttribute(attribute2)
        createAttribute(attribute3)
        createAttribute(attribute4)
    }

    @Test
    fun `it should return an AttributeList`() = runTest {
        val attributeNames = attributeService.getAttributeList(APIC_COMPOUND_CONTEXTS)
        assertTrue(
            attributeNames.attributeList == listOf(
                INCOMING_TERM,
                OUTGOING_TERM,
                MANAGED_BY_TERM,
                NGSILD_LOCATION_TERM
            )
        )
    }

    @Test
    fun `it should return an empty list of attributes if no attributes was found`() = runTest {
        clearPreviousAttributesAndObservations()

        val attributeNames = attributeService.getAttributeList(APIC_COMPOUND_CONTEXTS)
        assert(attributeNames.attributeList.isEmpty())
    }

    @Test
    fun `it should return a list of AttributeDetails`() = runTest {
        val attributeDetails = attributeService.getAttributeDetails(APIC_COMPOUND_CONTEXTS)
        assertEquals(4, attributeDetails.size)
        assertTrue(
            attributeDetails.containsAll(
                listOf(
                    AttributeDetails(
                        id = INCOMING_IRI.toUri(),
                        attributeName = INCOMING_TERM,
                        typeNames = setOf(BEEHIVE_TERM, SENSOR_TERM)
                    ),
                    AttributeDetails(
                        id = OUTGOING_IRI.toUri(),
                        attributeName = OUTGOING_TERM,
                        typeNames = setOf(SENSOR_TERM)
                    ),
                    AttributeDetails(
                        id = MANAGED_BY_IRI.toUri(),
                        attributeName = MANAGED_BY_TERM,
                        typeNames = setOf(BEEHIVE_TERM, SENSOR_TERM)
                    ),
                    AttributeDetails(
                        id = NGSILD_LOCATION_IRI.toUri(),
                        attributeName = NGSILD_LOCATION_TERM,
                        typeNames = setOf(APIARY_TERM)
                    )
                )
            )
        )
    }

    @Test
    fun `it should return an empty list of AttributeDetails if no attribute was found`() = runTest {
        clearPreviousAttributesAndObservations()

        val attributeDetails = attributeService.getAttributeDetails(APIC_COMPOUND_CONTEXTS)
        assertTrue(attributeDetails.isEmpty())
    }

    @Test
    fun `it should return an attribute Information by specific attribute`() = runTest {
        val attributeTypeInfo =
            attributeService.getAttributeTypeInfoByAttribute(INCOMING_IRI, APIC_COMPOUND_CONTEXTS)

        attributeTypeInfo.shouldSucceedWith {
            AttributeTypeInfo(
                id = toUri(INCOMING_IRI),
                attributeName = INCOMING_TERM,
                attributeCount = 1,
                attributeTypes = setOf(AttributeType.Property),
                typeNames = setOf(BEEHIVE_TERM)
            )
        }
    }

    @Test
    fun `it should error when type doesn't exist`() = runTest {
        val attributeTypeInfo =
            attributeService.getAttributeTypeInfoByAttribute(TEMPERATURE_IRI, APIC_COMPOUND_CONTEXTS)

        attributeTypeInfo.shouldFail {
            assertEquals(ResourceNotFoundException(attributeNotFoundMessage(TEMPERATURE_IRI)), it)
        }
    }

    private fun createAttribute(
        attribute: Attribute
    ): Either<APIException, Unit> =
        runBlocking {
            databaseClient.sql(
                """
                INSERT INTO temporal_entity_attribute 
                    (id, entity_id, attribute_name, attribute_type, attribute_value_type, created_at, modified_at)
                VALUES 
                    (:id, :entity_id, :attribute_name, :attribute_type, :attribute_value_type, :created_at, :modified_at)
                """.trimIndent()
            )
                .bind("id", attribute.id)
                .bind("entity_id", attribute.entityId)
                .bind("attribute_name", attribute.attributeName)
                .bind("attribute_type", attribute.attributeType.toString())
                .bind("attribute_value_type", attribute.attributeValueType.toString())
                .bind("created_at", attribute.createdAt)
                .bind("modified_at", attribute.createdAt)
                .execute()
        }

    private fun newAttribute(
        id: String,
        attributeName: String,
        attributeType: Attribute.AttributeType,
        attributeValueType: Attribute.AttributeValueType
    ): Attribute =
        Attribute(
            entityId = toUri(id),
            attributeName = attributeName,
            attributeType = attributeType,
            attributeValueType = attributeValueType,
            createdAt = now,
            payload = EMPTY_JSON_PAYLOAD
        )

    private fun createEntityPayload(
        entity: Entity
    ): Either<APIException, Unit> =
        runBlocking {
            databaseClient.sql(
                """
                INSERT INTO entity_payload (entity_id, types, modified_at)
                VALUES (:entity_id, :types, :modified_at)
                """.trimIndent()
            )
                .bind("entity_id", entity.entityId)
                .bind("types", entity.types.toTypedArray())
                .bind("modified_at", entity.modifiedAt)
                .execute()
        }
}
