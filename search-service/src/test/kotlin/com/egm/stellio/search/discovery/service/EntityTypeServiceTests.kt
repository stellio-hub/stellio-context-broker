package com.egm.stellio.search.discovery.service

import arrow.core.Either
import com.egm.stellio.search.common.util.execute
import com.egm.stellio.search.common.util.toUri
import com.egm.stellio.search.discovery.model.AttributeInfo
import com.egm.stellio.search.discovery.model.AttributeType
import com.egm.stellio.search.discovery.model.EntityType
import com.egm.stellio.search.discovery.model.EntityTypeInfo
import com.egm.stellio.search.entity.model.Attribute
import com.egm.stellio.search.entity.model.Entity
import com.egm.stellio.search.support.EMPTY_JSON_PAYLOAD
import com.egm.stellio.search.support.WithKafkaContainer
import com.egm.stellio.search.support.WithTimescaleContainer
import com.egm.stellio.search.support.gimmeEntityPayload
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.util.APIARY_COMPACT_TYPE
import com.egm.stellio.shared.util.APIARY_TYPE
import com.egm.stellio.shared.util.APIC_COMPOUND_CONTEXTS
import com.egm.stellio.shared.util.AQUAC_COMPOUND_CONTEXT
import com.egm.stellio.shared.util.BEEHIVE_COMPACT_TYPE
import com.egm.stellio.shared.util.BEEHIVE_TYPE
import com.egm.stellio.shared.util.CATEGORY_COMPACT_VOCABPROPERTY
import com.egm.stellio.shared.util.CATEGORY_VOCAPPROPERTY
import com.egm.stellio.shared.util.FRIENDLYNAME_COMPACT_LANGUAGEPROPERTY
import com.egm.stellio.shared.util.FRIENDLYNAME_LANGUAGEPROPERTY
import com.egm.stellio.shared.util.INCOMING_COMPACT_PROPERTY
import com.egm.stellio.shared.util.INCOMING_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_LOCATION_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_LOCATION_TERM
import com.egm.stellio.shared.util.LUMINOSITY_COMPACT_JSONPROPERTY
import com.egm.stellio.shared.util.LUMINOSITY_JSONPROPERTY
import com.egm.stellio.shared.util.MANAGED_BY_COMPACT_RELATIONSHIP
import com.egm.stellio.shared.util.MANAGED_BY_RELATIONSHIP
import com.egm.stellio.shared.util.OUTGOING_COMPACT_PROPERTY
import com.egm.stellio.shared.util.OUTGOING_PROPERTY
import com.egm.stellio.shared.util.SENSOR_COMPACT_TYPE
import com.egm.stellio.shared.util.SENSOR_TYPE
import com.egm.stellio.shared.util.TEMPERATURE_PROPERTY
import com.egm.stellio.shared.util.ngsiLdDateTime
import com.egm.stellio.shared.util.shouldFail
import com.egm.stellio.shared.util.shouldSucceedWith
import com.egm.stellio.shared.util.toUri
import com.egm.stellio.shared.util.typeNotFoundMessage
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
import org.springframework.data.r2dbc.core.delete
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.test.context.ActiveProfiles

@SpringBootTest
@ActiveProfiles("test")
class EntityTypeServiceTests : WithTimescaleContainer, WithKafkaContainer() {

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
    private val incomingProperty = newAttribute(
        "urn:ngsi-ld:BeeHive:TESTA",
        INCOMING_PROPERTY,
        Attribute.AttributeType.Property,
        Attribute.AttributeValueType.NUMBER
    )
    private val managedByRelationship = newAttribute(
        "urn:ngsi-ld:BeeHive:TESTA",
        MANAGED_BY_RELATIONSHIP,
        Attribute.AttributeType.Relationship,
        Attribute.AttributeValueType.STRING
    )
    private val locationGeoProperty = newAttribute(
        "urn:ngsi-ld:Apiary:TESTC",
        NGSILD_LOCATION_PROPERTY,
        Attribute.AttributeType.GeoProperty,
        Attribute.AttributeValueType.GEOMETRY
    )
    private val outgoingProperty = newAttribute(
        "urn:ngsi-ld:Sensor:TESTB",
        OUTGOING_PROPERTY,
        Attribute.AttributeType.Property,
        Attribute.AttributeValueType.GEOMETRY
    )
    private val luminosityJsonProperty = newAttribute(
        "urn:ngsi-ld:Sensor:TESTB",
        LUMINOSITY_JSONPROPERTY,
        Attribute.AttributeType.JsonProperty,
        Attribute.AttributeValueType.JSON
    )
    private val friendlyNameLanguageProperty = newAttribute(
        "urn:ngsi-ld:BeeHive:TESTA",
        FRIENDLYNAME_LANGUAGEPROPERTY,
        Attribute.AttributeType.LanguageProperty,
        Attribute.AttributeValueType.OBJECT
    )
    private val categoryVocabProperty = newAttribute(
        "urn:ngsi-ld:Apiary:TESTC",
        CATEGORY_VOCAPPROPERTY,
        Attribute.AttributeType.VocabProperty,
        Attribute.AttributeValueType.ARRAY
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
        createAttribute(incomingProperty)
        createAttribute(managedByRelationship)
        createAttribute(locationGeoProperty)
        createAttribute(outgoingProperty)
        createAttribute(luminosityJsonProperty)
        createAttribute(friendlyNameLanguageProperty)
        createAttribute(categoryVocabProperty)
    }

    @Test
    fun `it should return a list of all known entity types`() = runTest {
        val entityTypes = entityTypeService.getEntityTypeList(APIC_COMPOUND_CONTEXTS)

        assertEquals(listOf(APIARY_COMPACT_TYPE, BEEHIVE_COMPACT_TYPE, SENSOR_COMPACT_TYPE), entityTypes.typeList)
    }

    @Test
    fun `it should return an empty list of types if no entity exists`() = runTest {
        clearPreviousAttributesAndObservations()

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
        clearPreviousAttributesAndObservations()

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

    private fun createAttribute(
        attribute: Attribute
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
                .bind("id", attribute.id)
                .bind("entity_id", attribute.entityId)
                .bind("attribute_name", attribute.attributeName)
                .bind("attribute_type", attribute.attributeType.toString())
                .bind("attribute_value_type", attribute.attributeValueType.toString())
                .bind("created_at", attribute.createdAt)
                .execute()
        }

    private fun newAttribute(
        id: String,
        attributeName: String,
        attributeType: Attribute.AttributeType,
        attributeValueType: Attribute.AttributeValueType
    ): Attribute =
        Attribute(
            entityId = id.toUri(),
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
                INSERT INTO entity_payload (entity_id, types)
                VALUES (:entity_id, :types)
                """.trimIndent()
            )
                .bind("entity_id", entity.entityId)
                .bind("types", entity.types.toTypedArray())
                .execute()
        }
}
