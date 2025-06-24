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
import com.egm.stellio.shared.model.NGSILD_LOCATION_IRI
import com.egm.stellio.shared.model.NGSILD_LOCATION_TERM
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.util.APIARY_IRI
import com.egm.stellio.shared.util.APIARY_TERM
import com.egm.stellio.shared.util.APIC_COMPOUND_CONTEXTS
import com.egm.stellio.shared.util.AQUAC_COMPOUND_CONTEXT
import com.egm.stellio.shared.util.BEEHIVE_IRI
import com.egm.stellio.shared.util.BEEHIVE_TERM
import com.egm.stellio.shared.util.CATEGORY_IRI
import com.egm.stellio.shared.util.CATEGORY_TERM
import com.egm.stellio.shared.util.FRIENDLYNAME_IRI
import com.egm.stellio.shared.util.FRIENDLYNAME_TERM
import com.egm.stellio.shared.util.INCOMING_IRI
import com.egm.stellio.shared.util.INCOMING_TERM
import com.egm.stellio.shared.util.LUMINOSITY_IRI
import com.egm.stellio.shared.util.LUMINOSITY_TERM
import com.egm.stellio.shared.util.MANAGED_BY_IRI
import com.egm.stellio.shared.util.MANAGED_BY_TERM
import com.egm.stellio.shared.util.OUTGOING_IRI
import com.egm.stellio.shared.util.OUTGOING_TERM
import com.egm.stellio.shared.util.SENSOR_IRI
import com.egm.stellio.shared.util.SENSOR_TERM
import com.egm.stellio.shared.util.TEMPERATURE_IRI
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

    private val entityPayload1 = gimmeEntityPayload("urn:ngsi-ld:BeeHive:TESTA", listOf(BEEHIVE_IRI, SENSOR_IRI))
    private val entityPayload2 = gimmeEntityPayload("urn:ngsi-ld:Sensor:TESTB", listOf(SENSOR_IRI))
    private val entityPayload3 = gimmeEntityPayload("urn:ngsi-ld:Apiary:TESTC", listOf(APIARY_IRI))
    private val incomingProperty = newAttribute(
        "urn:ngsi-ld:BeeHive:TESTA",
        INCOMING_IRI,
        Attribute.AttributeType.Property,
        Attribute.AttributeValueType.NUMBER
    )
    private val managedByRelationship = newAttribute(
        "urn:ngsi-ld:BeeHive:TESTA",
        MANAGED_BY_IRI,
        Attribute.AttributeType.Relationship,
        Attribute.AttributeValueType.STRING
    )
    private val locationGeoProperty = newAttribute(
        "urn:ngsi-ld:Apiary:TESTC",
        NGSILD_LOCATION_IRI,
        Attribute.AttributeType.GeoProperty,
        Attribute.AttributeValueType.GEOMETRY
    )
    private val outgoingProperty = newAttribute(
        "urn:ngsi-ld:Sensor:TESTB",
        OUTGOING_IRI,
        Attribute.AttributeType.Property,
        Attribute.AttributeValueType.GEOMETRY
    )
    private val luminosityJsonProperty = newAttribute(
        "urn:ngsi-ld:Sensor:TESTB",
        LUMINOSITY_IRI,
        Attribute.AttributeType.JsonProperty,
        Attribute.AttributeValueType.JSON
    )
    private val friendlyNameLanguageProperty = newAttribute(
        "urn:ngsi-ld:BeeHive:TESTA",
        FRIENDLYNAME_IRI,
        Attribute.AttributeType.LanguageProperty,
        Attribute.AttributeValueType.OBJECT
    )
    private val categoryVocabProperty = newAttribute(
        "urn:ngsi-ld:Apiary:TESTC",
        CATEGORY_IRI,
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

        assertEquals(listOf(APIARY_TERM, BEEHIVE_TERM, SENSOR_TERM), entityTypes.typeList)
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
                        id = toUri(APIARY_IRI),
                        typeName = APIARY_TERM,
                        attributeNames = listOf(
                            CATEGORY_TERM,
                            NGSILD_LOCATION_TERM
                        )
                    ),
                    EntityType(
                        id = toUri(BEEHIVE_IRI),
                        typeName = BEEHIVE_TERM,
                        attributeNames = listOf(
                            FRIENDLYNAME_TERM,
                            INCOMING_TERM,
                            MANAGED_BY_TERM
                        )
                    ),
                    EntityType(
                        id = toUri(SENSOR_IRI),
                        typeName = SENSOR_TERM,
                        attributeNames = listOf(
                            FRIENDLYNAME_TERM,
                            INCOMING_TERM,
                            LUMINOSITY_TERM,
                            MANAGED_BY_TERM,
                            OUTGOING_TERM
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
        val entityTypeInfo = entityTypeService.getEntityTypeInfoByType(SENSOR_IRI, APIC_COMPOUND_CONTEXTS)

        entityTypeInfo.shouldSucceedWith {
            assertEquals(
                EntityTypeInfo(
                    id = SENSOR_IRI.toUri(),
                    typeName = SENSOR_TERM,
                    entityCount = 2,
                    attributeDetails = listOf(
                        AttributeInfo(
                            id = toUri(FRIENDLYNAME_IRI),
                            attributeName = FRIENDLYNAME_TERM,
                            attributeTypes = listOf(AttributeType.LanguageProperty)
                        ),
                        AttributeInfo(
                            id = toUri(INCOMING_IRI),
                            attributeName = INCOMING_TERM,
                            attributeTypes = listOf(AttributeType.Property)
                        ),
                        AttributeInfo(
                            id = toUri(LUMINOSITY_IRI),
                            attributeName = LUMINOSITY_TERM,
                            attributeTypes = listOf(AttributeType.JsonProperty)
                        ),
                        AttributeInfo(
                            id = toUri(MANAGED_BY_IRI),
                            attributeName = MANAGED_BY_TERM,
                            attributeTypes = listOf(AttributeType.Relationship)
                        ),
                        AttributeInfo(
                            id = toUri(OUTGOING_IRI),
                            attributeName = OUTGOING_TERM,
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
        entityTypeService.getEntityTypeInfoByType(TEMPERATURE_IRI, APIC_COMPOUND_CONTEXTS)
            .shouldFail {
                assertEquals(ResourceNotFoundException(typeNotFoundMessage(TEMPERATURE_IRI)), it)
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
                .bind("modified_at", attribute.modifiedAt)
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
