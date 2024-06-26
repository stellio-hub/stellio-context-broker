package com.egm.stellio.search.service

import arrow.core.Either
import com.egm.stellio.search.model.*
import com.egm.stellio.search.model.AttributeType
import com.egm.stellio.search.support.EMPTY_JSON_PAYLOAD
import com.egm.stellio.search.support.WithKafkaContainer
import com.egm.stellio.search.support.WithTimescaleContainer
import com.egm.stellio.search.support.gimmeEntityPayload
import com.egm.stellio.search.util.execute
import com.egm.stellio.search.util.toUri
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_LOCATION_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_LOCATION_TERM
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.test.context.ActiveProfiles

@SpringBootTest
@ActiveProfiles("test")
class EntityTypeServiceTests : WithTimescaleContainer, WithKafkaContainer {

    @Autowired
    private lateinit var entityTypeService: EntityTypeService

    @Autowired
    private lateinit var databaseClient: DatabaseClient

    @Autowired
    private lateinit var r2dbcEntityTemplate: R2dbcEntityTemplate

    private val now = ngsiLdDateTime()

    private val entityPayload1 = gimmeEntityPayload("urn:ngsi-ld:BeeHive:TESTA", listOf(BEEHIVE_TYPE, SENSOR_TYPE))
    private val entityPayload2 = gimmeEntityPayload("urn:ngsi-ld:Sensor:TESTB", listOf(SENSOR_TYPE))
    private val entityPayload3 = gimmeEntityPayload("urn:ngsi-ld:Apiary:TESTC", listOf(APIARY_TYPE))
    private val incomingProperty = newTemporalEntityAttribute(
        "urn:ngsi-ld:BeeHive:TESTA",
        INCOMING_PROPERTY,
        TemporalEntityAttribute.AttributeType.Property,
        TemporalEntityAttribute.AttributeValueType.NUMBER
    )
    private val managedByRelationship = newTemporalEntityAttribute(
        "urn:ngsi-ld:BeeHive:TESTA",
        MANAGED_BY_RELATIONSHIP,
        TemporalEntityAttribute.AttributeType.Relationship,
        TemporalEntityAttribute.AttributeValueType.STRING
    )
    private val locationGeoProperty = newTemporalEntityAttribute(
        "urn:ngsi-ld:Apiary:TESTC",
        NGSILD_LOCATION_PROPERTY,
        TemporalEntityAttribute.AttributeType.GeoProperty,
        TemporalEntityAttribute.AttributeValueType.GEOMETRY
    )
    private val outgoingProperty = newTemporalEntityAttribute(
        "urn:ngsi-ld:Sensor:TESTB",
        OUTGOING_PROPERTY,
        TemporalEntityAttribute.AttributeType.Property,
        TemporalEntityAttribute.AttributeValueType.GEOMETRY
    )
    private val luminosityJsonProperty = newTemporalEntityAttribute(
        "urn:ngsi-ld:Sensor:TESTB",
        LUMINOSITY_JSONPROPERTY,
        TemporalEntityAttribute.AttributeType.JsonProperty,
        TemporalEntityAttribute.AttributeValueType.JSON
    )
    private val friendlyNameLanguageProperty = newTemporalEntityAttribute(
        "urn:ngsi-ld:BeeHive:TESTA",
        FRIENDLYNAME_LANGUAGEPROPERTY,
        TemporalEntityAttribute.AttributeType.LanguageProperty,
        TemporalEntityAttribute.AttributeValueType.OBJECT
    )
    private val categoryVocabProperty = newTemporalEntityAttribute(
        "urn:ngsi-ld:Apiary:TESTC",
        CATEGORY_VOCAPPROPERTY,
        TemporalEntityAttribute.AttributeType.VocabProperty,
        TemporalEntityAttribute.AttributeValueType.ARRAY
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
        createTemporalEntityAttribute(incomingProperty)
        createTemporalEntityAttribute(managedByRelationship)
        createTemporalEntityAttribute(locationGeoProperty)
        createTemporalEntityAttribute(outgoingProperty)
        createTemporalEntityAttribute(luminosityJsonProperty)
        createTemporalEntityAttribute(friendlyNameLanguageProperty)
        createTemporalEntityAttribute(categoryVocabProperty)
    }

    @Test
    fun `it should return a list of all known entity types`() = runTest {
        val entityTypes = entityTypeService.getEntityTypeList(APIC_COMPOUND_CONTEXTS)

        assertEquals(listOf(APIARY_COMPACT_TYPE, BEEHIVE_COMPACT_TYPE, SENSOR_COMPACT_TYPE), entityTypes.typeList)
    }

    @Test
    fun `it should return an empty list of types if no entity exists`() = runTest {
        clearPreviousTemporalEntityAttributesAndObservations()

        val entityTypes = entityTypeService.getEntityTypeList(listOf(AQUAC_COMPOUND_CONTEXT))
        assertThat(entityTypes.typeList).isEmpty()
    }

    @Test
    fun `it should return all known entity types with details`() = runTest {
        val entityTypes = entityTypeService.getEntityTypes(APIC_COMPOUND_CONTEXTS)

        assertThat(entityTypes)
            .hasSize(3)
            .containsAll(
                listOf(
                    EntityType(
                        id = toUri(APIARY_TYPE),
                        typeName = APIARY_COMPACT_TYPE,
                        attributeNames = listOf(
                            CATEGORY_COMPACT_VOCABPROPERTY,
                            NGSILD_LOCATION_TERM
                        )
                    ),
                    EntityType(
                        id = toUri(BEEHIVE_TYPE),
                        typeName = BEEHIVE_COMPACT_TYPE,
                        attributeNames = listOf(
                            FRIENDLYNAME_COMPACT_LANGUAGEPROPERTY,
                            INCOMING_COMPACT_PROPERTY,
                            MANAGED_BY_COMPACT_RELATIONSHIP
                        )
                    ),
                    EntityType(
                        id = toUri(SENSOR_TYPE),
                        typeName = SENSOR_COMPACT_TYPE,
                        attributeNames = listOf(
                            FRIENDLYNAME_COMPACT_LANGUAGEPROPERTY,
                            INCOMING_COMPACT_PROPERTY,
                            LUMINOSITY_COMPACT_JSONPROPERTY,
                            MANAGED_BY_COMPACT_RELATIONSHIP,
                            OUTGOING_COMPACT_PROPERTY
                        )
                    )
                )
            )
    }

    @Test
    fun `it should return an empty list of detailed entity types if no entity exists`() = runTest {
        clearPreviousTemporalEntityAttributesAndObservations()

        val entityTypes = entityTypeService.getEntityTypes(listOf(AQUAC_COMPOUND_CONTEXT))
        assertThat(entityTypes).isEmpty()
    }

    @Test
    fun `it should return entity type info for a specific type`() = runTest {
        val entityTypeInfo = entityTypeService.getEntityTypeInfoByType(SENSOR_TYPE, APIC_COMPOUND_CONTEXTS)

        entityTypeInfo.shouldSucceedWith {
            assertEquals(
                EntityTypeInfo(
                    id = SENSOR_TYPE.toUri(),
                    typeName = SENSOR_COMPACT_TYPE,
                    entityCount = 2,
                    attributeDetails = listOf(
                        AttributeInfo(
                            id = toUri(FRIENDLYNAME_LANGUAGEPROPERTY),
                            attributeName = FRIENDLYNAME_COMPACT_LANGUAGEPROPERTY,
                            attributeTypes = listOf(AttributeType.LanguageProperty)
                        ),
                        AttributeInfo(
                            id = toUri(INCOMING_PROPERTY),
                            attributeName = INCOMING_COMPACT_PROPERTY,
                            attributeTypes = listOf(AttributeType.Property)
                        ),
                        AttributeInfo(
                            id = toUri(LUMINOSITY_JSONPROPERTY),
                            attributeName = LUMINOSITY_COMPACT_JSONPROPERTY,
                            attributeTypes = listOf(AttributeType.JsonProperty)
                        ),
                        AttributeInfo(
                            id = toUri(MANAGED_BY_RELATIONSHIP),
                            attributeName = MANAGED_BY_COMPACT_RELATIONSHIP,
                            attributeTypes = listOf(AttributeType.Relationship)
                        ),
                        AttributeInfo(
                            id = toUri(OUTGOING_PROPERTY),
                            attributeName = OUTGOING_COMPACT_PROPERTY,
                            attributeTypes = listOf(AttributeType.Property)
                        )
                    )
                ),
                it
            )
        }
    }

    @Test
    fun `it should return an error when entity type doesn't exist`() = runTest {
        entityTypeService.getEntityTypeInfoByType(TEMPERATURE_PROPERTY, APIC_COMPOUND_CONTEXTS)
            .shouldFail {
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
            entityId = id.toUri(),
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
}
