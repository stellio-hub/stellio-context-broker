package com.egm.stellio.search.service

import arrow.core.Either
import com.egm.stellio.search.model.*
import com.egm.stellio.search.model.AttributeType
import com.egm.stellio.search.support.EMPTY_JSON_PAYLOAD
import com.egm.stellio.search.support.WithKafkaContainer
import com.egm.stellio.search.support.WithTimescaleContainer
import com.egm.stellio.search.util.execute
import com.egm.stellio.search.util.toUri
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_LOCATION_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_LOCATION_TERM
import kotlinx.coroutines.ExperimentalCoroutinesApi
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
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.test.context.ActiveProfiles
import java.time.Instant
import java.time.ZoneOffset

@OptIn(ExperimentalCoroutinesApi::class)
@SpringBootTest
@ActiveProfiles("test")
class EntityTypeServiceTests : WithTimescaleContainer, WithKafkaContainer {

    @Autowired
    private lateinit var entityTypeService: EntityTypeService

    @Autowired
    private lateinit var databaseClient: DatabaseClient

    @Autowired
    private lateinit var r2dbcEntityTemplate: R2dbcEntityTemplate

    private val now = Instant.now().atZone(ZoneOffset.UTC)

    private val entityPayload1 = newEntityPayload(
        "urn:ngsi-ld:BeeHive:TESTA",
        listOf(BEEHIVE_TYPE, SENSOR_TYPE),
        DEVICE_COMPACT_TYPE
    )
    private val entityPayload2 = newEntityPayload("urn:ngsi-ld:Sensor:TESTB", listOf(SENSOR_TYPE), DEVICE_COMPACT_TYPE)
    private val entityPayload3 = newEntityPayload("urn:ngsi-ld:Apiary:TESTC", listOf(APIARY_TYPE), DEVICE_COMPACT_TYPE)
    private val temporalEntityAttribute1 = newTemporalEntityAttribute(
        "urn:ngsi-ld:BeeHive:TESTA",
        INCOMING_PROPERTY,
        TemporalEntityAttribute.AttributeType.Property,
        TemporalEntityAttribute.AttributeValueType.NUMBER
    )
    private val temporalEntityAttribute2 = newTemporalEntityAttribute(
        "urn:ngsi-ld:BeeHive:TESTA",
        MANAGED_BY_RELATIONSHIP,
        TemporalEntityAttribute.AttributeType.Relationship,
        TemporalEntityAttribute.AttributeValueType.STRING
    )
    private val temporalEntityAttribute3 = newTemporalEntityAttribute(
        "urn:ngsi-ld:Apiary:TESTC",
        NGSILD_LOCATION_PROPERTY,
        TemporalEntityAttribute.AttributeType.GeoProperty,
        TemporalEntityAttribute.AttributeValueType.GEOMETRY
    )
    private val temporalEntityAttribute4 = newTemporalEntityAttribute(
        "urn:ngsi-ld:Sensor:TESTB",
        OUTGOING_PROPERTY,
        TemporalEntityAttribute.AttributeType.Property,
        TemporalEntityAttribute.AttributeValueType.GEOMETRY
    )

    @AfterEach
    fun clearPreviousTemporalEntityAttributesAndObservations() {
        r2dbcEntityTemplate.delete(EntityPayload::class.java)
            .all()
            .block()
        r2dbcEntityTemplate.delete(TemporalEntityAttribute::class.java)
            .all()
            .block()
    }

    @BeforeEach
    fun createEntity() {
        createEntityPayload(entityPayload1)
        createEntityPayload(entityPayload2)
        createEntityPayload(entityPayload3)
        createTemporalEntityAttribute(temporalEntityAttribute1)
        createTemporalEntityAttribute(temporalEntityAttribute2)
        createTemporalEntityAttribute(temporalEntityAttribute3)
        createTemporalEntityAttribute(temporalEntityAttribute4)
    }

    @Test
    fun `it should return an EntityTypeList`() = runTest {
        val entityTypes = entityTypeService.getEntityTypeList(listOf(APIC_COMPOUND_CONTEXT))

        assertTrue(
            entityTypes.typeList == listOf(APIARY_COMPACT_TYPE, BEEHIVE_COMPACT_TYPE, SENSOR_COMPACT_TYPE)
        )
    }

    @Test
    fun `it should return an empty list of types if no entity was found`() = runTest {
        clearPreviousTemporalEntityAttributesAndObservations()

        val entityTypes = entityTypeService.getEntityTypeList(listOf(AQUAC_COMPOUND_CONTEXT))
        assert(entityTypes.typeList.isEmpty())
    }

    @Test
    fun `it should return a list of EntityType`() = runTest {
        val entityTypes = entityTypeService.getEntityTypes(listOf(APIC_COMPOUND_CONTEXT))

        assertTrue(entityTypes.size == 3)
        assertTrue(
            entityTypes.containsAll(
                listOf(
                    EntityType(
                        id = toUri(APIARY_TYPE),
                        typeName = APIARY_COMPACT_TYPE,
                        attributeNames = listOf(NGSILD_LOCATION_TERM)
                    ),
                    EntityType(
                        id = toUri(BEEHIVE_TYPE),
                        typeName = BEEHIVE_COMPACT_TYPE,
                        attributeNames = listOf(INCOMING_COMPACT_PROPERTY, MANAGED_BY_COMPACT_RELATIONSHIP)
                    ),
                    EntityType(
                        id = toUri(SENSOR_TYPE),
                        typeName = SENSOR_COMPACT_TYPE,
                        attributeNames = listOf(
                            INCOMING_COMPACT_PROPERTY,
                            MANAGED_BY_COMPACT_RELATIONSHIP,
                            OUTGOING_COMPACT_PROPERTY
                        )
                    )
                )
            )
        )
    }

    @Test
    fun `it should return an empty list of EntityTypes if no entity was found`() = runTest {
        clearPreviousTemporalEntityAttributesAndObservations()

        val entityTypes = entityTypeService.getEntityTypes(listOf(AQUAC_COMPOUND_CONTEXT))
        assert(entityTypes.isEmpty())
    }

    @Test
    fun `it should return an EntityTypeInfo for a specific type`() = runTest {
        val entityTypeInfo = entityTypeService.getEntityTypeInfoByType(
            BEEHIVE_TYPE,
            listOf(APIC_COMPOUND_CONTEXT)
        )

        entityTypeInfo.shouldSucceedWith {
            EntityTypeInfo(
                id = toUri(BEEHIVE_TYPE),
                typeName = BEEHIVE_COMPACT_TYPE,
                entityCount = 1,
                attributeDetails = listOf(
                    AttributeInfo(
                        id = toUri(INCOMING_PROPERTY),
                        attributeName = INCOMING_COMPACT_PROPERTY,
                        attributeTypes = listOf(AttributeType.Property)
                    ),
                    AttributeInfo(
                        id = toUri(MANAGED_BY_RELATIONSHIP),
                        attributeName = MANAGED_BY_COMPACT_RELATIONSHIP,
                        attributeTypes = listOf(AttributeType.Relationship)
                    )
                )
            )
        }
    }

    @Test
    fun `it should error when type doesn't exist`() = runTest {
        val entityTypeInfo =
            entityTypeService.getEntityTypeInfoByType(TEMPERATURE_PROPERTY, listOf(APIC_COMPOUND_CONTEXT))

        entityTypeInfo.shouldFail {
            assertEquals(ResourceNotFoundException(typeNotFoundMessage(TEMPERATURE_PROPERTY)), it)
        }
    }

    private fun createTemporalEntityAttribute(
        temporalEntityAttribute: TemporalEntityAttribute
    ): Either<APIException, Unit> =
        runBlocking {
            databaseClient.sql(
                """
                INSERT INTO temporal_entity_attribute 
                    (id, entity_id, attribute_name, attribute_type, attribute_value_type, created_at)
                VALUES 
                    (:id, :entity_id, :attribute_name, :attribute_type, :attribute_value_type, :created_at)
                """.trimIndent()
            )
                .bind("id", temporalEntityAttribute.id)
                .bind("entity_id", temporalEntityAttribute.entityId)
                .bind("attribute_name", temporalEntityAttribute.attributeName)
                .bind("attribute_type", temporalEntityAttribute.attributeType.toString())
                .bind("attribute_value_type", temporalEntityAttribute.attributeValueType.toString())
                .bind("created_at", temporalEntityAttribute.createdAt)
                .execute()
        }

    private fun newTemporalEntityAttribute(
        id: String,
        attributeName: String,
        attributeType: TemporalEntityAttribute.AttributeType,
        attributeValueType: TemporalEntityAttribute.AttributeValueType
    ): TemporalEntityAttribute =
        TemporalEntityAttribute(
            entityId = toUri(id),
            attributeName = attributeName,
            attributeType = attributeType,
            attributeValueType = attributeValueType,
            createdAt = now,
            payload = EMPTY_JSON_PAYLOAD
        )

    private fun createEntityPayload(
        entityPayload: EntityPayload
    ): Either<APIException, Unit> =
        runBlocking {
            databaseClient.sql(
                """
                INSERT INTO entity_payload (entity_id, types)
                VALUES (:entity_id, :types)
                """.trimIndent()
            )
                .bind("entity_id", entityPayload.entityId)
                .bind("types", entityPayload.types.toTypedArray())
                .execute()
        }

    private fun newEntityPayload(id: String, types: List<String>, contexts: String): EntityPayload =
        EntityPayload(
            entityId = toUri(id),
            types = types,
            createdAt = now,
            payload = EMPTY_JSON_PAYLOAD,
            contexts = listOf(contexts)
        )
}
