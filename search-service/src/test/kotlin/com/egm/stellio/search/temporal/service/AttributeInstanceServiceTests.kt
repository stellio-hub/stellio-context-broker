package com.egm.stellio.search.temporal.service

import com.egm.stellio.search.entity.model.Attribute
import com.egm.stellio.search.entity.model.AttributeMetadata
import com.egm.stellio.search.entity.service.EntityAttributeService
import com.egm.stellio.search.support.*
import com.egm.stellio.search.temporal.model.*
import com.egm.stellio.search.temporal.model.TemporalQuery.Timerel
import com.egm.stellio.shared.model.ExpandedAttributes
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.model.addNonReifiedTemporalProperty
import com.egm.stellio.shared.model.getSingleEntry
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_ID
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_LANGUAGE
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_VALUE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_DATE_TIME_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_DEFAULT_VOCAB
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_INSTANCE_ID_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_JSONPROPERTY_VALUE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_LANGUAGEPROPERTY_VALUE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_MODIFIED_AT_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_OBSERVED_AT_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_PROPERTY_VALUE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_VOCABPROPERTY_VALUE
import com.egm.stellio.shared.util.JsonLdUtils.buildExpandedPropertyValue
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import io.mockk.spyk
import io.mockk.verify
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.test.context.ActiveProfiles
import java.time.Instant
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.util.*

@SpringBootTest
@ActiveProfiles("test")
class AttributeInstanceServiceTests : WithTimescaleContainer, WithKafkaContainer {

    @Autowired
    private lateinit var attributeInstanceService: AttributeInstanceService

    @Autowired
    private lateinit var entityAttributeService: EntityAttributeService

    @Autowired
    private lateinit var databaseClient: DatabaseClient

    @Autowired
    private lateinit var r2dbcEntityTemplate: R2dbcEntityTemplate

    private val now = Instant.now().atZone(ZoneOffset.UTC)
    private lateinit var incomingAttribute: Attribute
    private lateinit var outgoingAttribute: Attribute
    private lateinit var jsonAttribute: Attribute
    private lateinit var languageAttribute: Attribute
    private lateinit var vocabAttribute: Attribute

    val entityId = "urn:ngsi-ld:BeeHive:TESTC".toUri()

    @BeforeAll
    fun createAttribute() {
        incomingAttribute = Attribute(
            entityId = entityId,
            attributeName = INCOMING_PROPERTY,
            attributeValueType = Attribute.AttributeValueType.NUMBER,
            createdAt = now,
            payload = EMPTY_JSON_PAYLOAD
        )

        runBlocking {
            entityAttributeService.create(incomingAttribute)
        }

        outgoingAttribute = Attribute(
            entityId = entityId,
            attributeName = OUTGOING_PROPERTY,
            attributeValueType = Attribute.AttributeValueType.NUMBER,
            createdAt = now,
            payload = EMPTY_JSON_PAYLOAD
        )

        runBlocking {
            entityAttributeService.create(outgoingAttribute)
        }

        jsonAttribute = Attribute(
            entityId = entityId,
            attributeName = LUMINOSITY_JSONPROPERTY,
            attributeValueType = Attribute.AttributeValueType.JSON,
            createdAt = now,
            payload = SAMPLE_JSON_PROPERTY_PAYLOAD
        )

        runBlocking {
            entityAttributeService.create(jsonAttribute)
        }

        languageAttribute = Attribute(
            entityId = entityId,
            attributeName = FRIENDLYNAME_LANGUAGEPROPERTY,
            attributeValueType = Attribute.AttributeValueType.ARRAY,
            createdAt = now,
            payload = SAMPLE_LANGUAGE_PROPERTY_PAYLOAD
        )

        runBlocking {
            entityAttributeService.create(languageAttribute)
        }

        vocabAttribute = Attribute(
            entityId = entityId,
            attributeName = CATEGORY_VOCAPPROPERTY,
            attributeValueType = Attribute.AttributeValueType.ARRAY,
            createdAt = now,
            payload = SAMPLE_VOCAB_PROPERTY_PAYLOAD
        )

        runBlocking {
            entityAttributeService.create(vocabAttribute)
        }
    }

    @AfterEach
    fun clearPreviousObservations() {
        r2dbcEntityTemplate.delete(AttributeInstance::class.java)
            .all()
            .block()

        r2dbcEntityTemplate.databaseClient
            .sql("delete from attribute_instance_audit")
            .fetch()
            .rowsUpdated()
            .block()
    }

    @Test
    fun `it should retrieve a full instance if temporalValues are not asked for`() = runTest {
        val observation = gimmeNumericPropertyAttributeInstance(incomingAttribute.id).copy(
            time = now,
            measuredValue = 12.4
        )
        attributeInstanceService.create(observation)

        val temporalEntitiesQuery = gimmeTemporalEntitiesQuery(
            buildDefaultTestTemporalQuery(
                timerel = Timerel.AFTER,
                timeAt = now.minusHours(1)
            )
        )
        attributeInstanceService.search(temporalEntitiesQuery, incomingAttribute)
            .shouldSucceedWith {
                assertThat(it)
                    .singleElement()
                    .hasFieldOrPropertyWithValue("attributeUuid", incomingAttribute.id)
            }
    }

    @Test
    fun `it should retrieve an instance having the corresponding time property value`() = runTest {
        val observation = gimmeNumericPropertyAttributeInstance(incomingAttribute.id).copy(
            timeProperty = AttributeInstance.TemporalProperty.CREATED_AT,
            time = now,
            measuredValue = 12.4
        )
        attributeInstanceService.create(observation)

        val temporalEntitiesQuery = gimmeTemporalEntitiesQuery(
            buildDefaultTestTemporalQuery(
                timerel = Timerel.AFTER,
                timeAt = now.minusHours(1),
                timeproperty = AttributeInstance.TemporalProperty.CREATED_AT
            )
        )
        attributeInstanceService.search(temporalEntitiesQuery, incomingAttribute)
            .shouldSucceedWith {
                assertThat(it)
                    .singleElement()
            }
    }

    @Test
    fun `it should retrieve an instance with audit info if time property is not observedAt`() = runTest {
        val observation = gimmeNumericPropertyAttributeInstance(incomingAttribute.id).copy(
            timeProperty = AttributeInstance.TemporalProperty.CREATED_AT,
            time = now,
            measuredValue = 12.4,
            sub = "sub"
        )
        attributeInstanceService.create(observation)

        val temporalEntitiesQuery = gimmeTemporalEntitiesQuery(
            buildDefaultTestTemporalQuery(
                timerel = Timerel.AFTER,
                timeAt = now.minusHours(1),
                timeproperty = AttributeInstance.TemporalProperty.CREATED_AT
            )
        )
        attributeInstanceService.search(temporalEntitiesQuery, incomingAttribute)
            .shouldSucceedWith {
                assertThat(it)
                    .singleElement()
                    .hasFieldOrPropertyWithValue("sub", "sub")
            }
    }

    @Test
    fun `it should not retrieve an instance not having the corresponding time property value`() = runTest {
        val observation = gimmeNumericPropertyAttributeInstance(incomingAttribute.id).copy(
            timeProperty = AttributeInstance.TemporalProperty.CREATED_AT,
            time = now,
            measuredValue = 12.4
        )
        attributeInstanceService.create(observation)

        val temporalEntitiesQuery = gimmeTemporalEntitiesQuery(
            buildDefaultTestTemporalQuery(
                timerel = Timerel.AFTER,
                timeAt = now.minusHours(1),
                timeproperty = AttributeInstance.TemporalProperty.MODIFIED_AT
            )
        )
        attributeInstanceService.search(temporalEntitiesQuery, incomingAttribute)
            .shouldSucceedWith {
                assertThat(it)
                    .isEmpty()
            }
    }

    @Test
    fun `it should retrieve all full instances if temporalValues are not asked for`() = runTest {
        (1..10).forEach { _ ->
            attributeInstanceService.create(gimmeNumericPropertyAttributeInstance(incomingAttribute.id))
        }

        val temporalEntitiesQuery = gimmeTemporalEntitiesQuery(
            buildDefaultTestTemporalQuery(
                timerel = Timerel.AFTER,
                timeAt = now.minusHours(1)
            )
        )
        attributeInstanceService.search(temporalEntitiesQuery, incomingAttribute)
            .shouldSucceedWith {
                assertThat(it)
                    .hasSize(10)
            }
    }

    @Test
    fun `it should retrieve all instances when no timerel and time parameters are provided`() = runTest {
        (1..10).forEach { _ ->
            attributeInstanceService.create(gimmeNumericPropertyAttributeInstance(incomingAttribute.id))
        }

        attributeInstanceService.search(
            gimmeTemporalEntitiesQuery(buildDefaultTestTemporalQuery()),
            incomingAttribute
        )
            .shouldSucceedWith {
                assertThat(it)
                    .hasSize(10)
            }
    }

    @Test
    fun `it should retrieve instances of a temporal entity attribute whose value type is Any`() = runTest {
        val attribute2 = Attribute(
            entityId = entityId,
            attributeName = "propWithStringValue",
            attributeValueType = Attribute.AttributeValueType.STRING,
            createdAt = now,
            payload = EMPTY_JSON_PAYLOAD
        )

        entityAttributeService.create(attribute2)

        (1..10).forEach { _ ->
            val observedAt = Instant.now().atZone(ZoneOffset.UTC)
            val attributeMetadata = AttributeMetadata(
                measuredValue = null,
                value = "some value",
                geoValue = null,
                valueType = Attribute.AttributeValueType.STRING,
                datasetId = null,
                type = Attribute.AttributeType.Property,
                observedAt = observedAt
            )
            val attributeInstance = AttributeInstance(
                attributeUuid = attribute2.id,
                time = observedAt,
                attributeMetadata = attributeMetadata,
                payload = buildExpandedPropertyValue(attributeMetadata.value!!)
                    .addNonReifiedTemporalProperty(NGSILD_OBSERVED_AT_PROPERTY, observedAt)
                    .getSingleEntry()
            )
            attributeInstanceService.create(attributeInstance)
        }

        attributeInstanceService.search(
            gimmeTemporalEntitiesQuery(buildDefaultTestTemporalQuery(), withTemporalValues = true),
            attribute2
        ).shouldSucceedWith { results ->
            assertThat(results)
                .hasSize(10)
                .allMatch {
                    it.attributeUuid == attribute2.id &&
                        (it as SimplifiedAttributeInstanceResult).value == "some value"
                }
        }
    }

    @Test
    fun `it should set the start time to the oldest value if asking for no timerel`() = runTest {
        (1..9).forEachIndexed { index, _ ->
            val attributeInstance =
                gimmeNumericPropertyAttributeInstance(incomingAttribute.id)
                    .copy(
                        measuredValue = index.toDouble(),
                        time = ZonedDateTime.parse("2022-07-0${index + 1}T00:00:00Z")
                    )
            attributeInstanceService.create(attributeInstance)
        }
        val temporalEntitiesQuery = gimmeTemporalEntitiesQuery(
            buildDefaultTestTemporalQuery(
                aggrPeriodDuration = "P30D",
                aggrMethods = listOf(TemporalQuery.Aggregate.MAX)
            ),
            withAggregatedValues = true
        )

        val origin = attributeInstanceService.selectOldestDate(
            temporalEntitiesQuery.temporalQuery,
            listOf(incomingAttribute)
        )

        assertNotNull(origin)
        assertEquals(ZonedDateTime.parse("2022-07-01T00:00:00Z"), origin)
    }

    @Test
    fun `it should only return the limited instances asked in the temporal query`() = runTest {
        (1..10).forEach { _ ->
            val attributeInstance = gimmeNumericPropertyAttributeInstance(incomingAttribute.id)
                .copy(measuredValue = 1.0)
            attributeInstanceService.create(attributeInstance)
        }
        val temporalEntitiesQuery = gimmeTemporalEntitiesQuery(
            buildDefaultTestTemporalQuery(
                timerel = Timerel.AFTER,
                timeAt = now.minusHours(1),
                instanceLimit = 5,
                lastN = 5
            )
        )
        attributeInstanceService.search(temporalEntitiesQuery, incomingAttribute)
            .shouldSucceedWith {
                assertThat(it)
                    .hasSize(5)
            }
    }

    @Test
    fun `it should only return the limited instances asked in an aggregated temporal query`() = runTest {
        val now = ngsiLdDateTime()
        (1..10).forEachIndexed { index, _ ->
            val attributeInstance =
                gimmeNumericPropertyAttributeInstance(incomingAttribute.id)
                    .copy(
                        measuredValue = 1.0,
                        time = now.minusSeconds(index.toLong())
                    )
            attributeInstanceService.create(attributeInstance)
        }

        val temporalEntitiesQuery = gimmeTemporalEntitiesQuery(
            buildDefaultTestTemporalQuery(
                timerel = Timerel.BEFORE,
                timeAt = now,
                aggrPeriodDuration = "PT1S",
                aggrMethods = listOf(TemporalQuery.Aggregate.SUM),
                instanceLimit = 5,
            ),
            withAggregatedValues = true
        )
        attributeInstanceService.search(temporalEntitiesQuery, incomingAttribute)
            .shouldSucceedWith {
                assertThat(it)
                    .hasSize(5)
            }
    }

    @Test
    fun `it should only retrieve the temporal evolution of the provided temporal entity attribute`() = runTest {
        val attribute2 = Attribute(
            entityId = entityId,
            attributeName = OUTGOING_COMPACT_PROPERTY,
            attributeValueType = Attribute.AttributeValueType.NUMBER,
            createdAt = now,
            payload = EMPTY_JSON_PAYLOAD
        )

        entityAttributeService.create(attribute2)

        (1..10).forEach { _ ->
            attributeInstanceService.create(gimmeNumericPropertyAttributeInstance(incomingAttribute.id))
        }
        (1..5).forEach { _ ->
            attributeInstanceService.create(gimmeNumericPropertyAttributeInstance(attribute2.id))
        }

        val temporalEntitiesQuery = gimmeTemporalEntitiesQuery(
            buildDefaultTestTemporalQuery(
                timerel = Timerel.AFTER,
                timeAt = now.minusHours(1)
            )
        )
        attributeInstanceService.search(temporalEntitiesQuery, incomingAttribute)
            .shouldSucceedWith { results ->
                assertThat(results)
                    .hasSize(10)
                    .allMatch {
                        it.attributeUuid == incomingAttribute.id
                    }
            }
    }

    @Test
    fun `it should not retrieve any instance if temporal entity does not match`() = runTest {
        (1..10).forEach { _ ->
            attributeInstanceService.create(gimmeNumericPropertyAttributeInstance(incomingAttribute.id))
        }

        val temporalEntitiesQuery = gimmeTemporalEntitiesQuery(
            buildDefaultTestTemporalQuery(
                timerel = Timerel.AFTER,
                timeAt = now.minusHours(1)
            )
        )
        attributeInstanceService.search(
            temporalEntitiesQuery,
            incomingAttribute.copy(id = UUID.randomUUID())
        ).shouldSucceedWith {
            assertThat(it)
                .isEmpty()
        }
    }

    @Test
    fun `it should not retrieve any instance if there is no value in the time interval`() = runTest {
        (1..10).forEach { _ ->
            attributeInstanceService.create(gimmeNumericPropertyAttributeInstance(incomingAttribute.id))
        }

        val temporalEntitiesQuery = gimmeTemporalEntitiesQuery(
            buildDefaultTestTemporalQuery(
                timerel = Timerel.AFTER,
                timeAt = now.plusHours(1)
            )
        )
        attributeInstanceService.search(temporalEntitiesQuery, incomingAttribute)
            .shouldSucceedWith {
                assertThat(it)
                    .isEmpty()
            }
    }

    @Test
    fun `it should update an existing attribute instance with same observation date`() = runTest {
        val attributeInstance = gimmeNumericPropertyAttributeInstance(incomingAttribute.id)

        attributeInstanceService.create(attributeInstance)

        val createResult = attributeInstanceService.create(attributeInstance.copy(measuredValue = 100.0))
        assertThat(createResult.isRight())

        attributeInstanceService.search(
            gimmeTemporalEntitiesQuery(
                buildDefaultTestTemporalQuery(
                    timerel = Timerel.AFTER,
                    timeAt = now.minusHours(1)
                ),
                withTemporalValues = true
            ),
            incomingAttribute
        ).shouldSucceedWith { results ->
            assertThat(results)
                .singleElement()
                .matches {
                    (it as SimplifiedAttributeInstanceResult).value == 100.0
                }
        }
    }

    @Test
    fun `it should create an attribute instance if it has a non null value`() = runTest {
        val attributeInstanceService = spyk(AttributeInstanceService(databaseClient), recordPrivateCalls = true)
        val attributeMetadata = AttributeMetadata(
            measuredValue = 550.0,
            value = null,
            geoValue = null,
            valueType = Attribute.AttributeValueType.NUMBER,
            datasetId = null,
            type = Attribute.AttributeType.Property,
            observedAt = ZonedDateTime.parse("2015-10-18T11:20:30.000001Z")
        )
        val attributeValues = mapOf(
            NGSILD_OBSERVED_AT_PROPERTY to listOf(
                mapOf(
                    JSONLD_VALUE to attributeMetadata.observedAt,
                    JSONLD_TYPE to NGSILD_DATE_TIME_TYPE
                )
            ),
            NGSILD_PROPERTY_VALUE to listOf(
                mapOf(
                    JSONLD_VALUE to attributeMetadata.measuredValue
                )
            )
        )

        attributeInstanceService.addAttributeInstance(
            incomingAttribute.id,
            attributeMetadata,
            attributeValues
        )

        verify {
            attributeInstanceService["create"](
                match<AttributeInstance> {
                    it.time.toString() == "2015-10-18T11:20:30.000001Z" &&
                        it.value == null &&
                        it.measuredValue == 550.0 &&
                        it.payload.asString().matchContent(
                            """
                            {
                                "https://uri.etsi.org/ngsi-ld/observedAt":[{
                                    "@value":"2015-10-18T11:20:30.000001Z",
                                    "@type":"https://uri.etsi.org/ngsi-ld/DateTime"
                                }],
                                "https://uri.etsi.org/ngsi-ld/hasValue":[{
                                    "@value":550.0
                                }],
                                "https://uri.etsi.org/ngsi-ld/instanceId":[{
                                    "@id":"${it.instanceId}"
                                }]
                            }
                            """.trimIndent()
                        )
                }
            )
        }
    }

    @Test
    fun `it should create an attribute instance with boolean value`() = runTest {
        val attributeInstanceService = spyk(AttributeInstanceService(databaseClient), recordPrivateCalls = true)
        val attributeMetadata = AttributeMetadata(
            measuredValue = null,
            value = false.toString(),
            geoValue = null,
            valueType = Attribute.AttributeValueType.BOOLEAN,
            datasetId = null,
            type = Attribute.AttributeType.Property,
            observedAt = ZonedDateTime.parse("2015-10-18T11:20:30.000001Z")
        )
        val attributeValues = mapOf(
            NGSILD_OBSERVED_AT_PROPERTY to listOf(
                mapOf(
                    JSONLD_VALUE to attributeMetadata.observedAt,
                    JSONLD_TYPE to NGSILD_DATE_TIME_TYPE
                )
            ),
            NGSILD_PROPERTY_VALUE to listOf(
                mapOf(
                    JSONLD_VALUE to false
                )
            )
        )

        attributeInstanceService.addAttributeInstance(
            incomingAttribute.id,
            attributeMetadata,
            attributeValues
        )

        verify {
            attributeInstanceService["create"](
                match<AttributeInstance> {
                    it.time.toString() == "2015-10-18T11:20:30.000001Z" &&
                        it.value == "false" &&
                        it.measuredValue == null &&
                        it.payload.asString().matchContent(
                            """
                            {
                                "https://uri.etsi.org/ngsi-ld/observedAt":[{
                                    "@value":"2015-10-18T11:20:30.000001Z",
                                    "@type":"https://uri.etsi.org/ngsi-ld/DateTime"
                                }],
                                "https://uri.etsi.org/ngsi-ld/hasValue":[{
                                    "@value":false
                                }],
                                "https://uri.etsi.org/ngsi-ld/instanceId":[{
                                    "@id":"${it.instanceId}"
                                }]
                            }
                            """.trimIndent()
                        )
                }
            )
        }
    }

    @Test
    fun `it should modify attribute instance for a property`() = runTest {
        val attributeInstance = gimmeNumericPropertyAttributeInstance(incomingAttribute.id)
        attributeInstanceService.create(attributeInstance)

        val instanceTemporalFragment =
            loadSampleData("fragments/temporal_instance_fragment.jsonld")
        val attributeInstancePayload = mapOf(INCOMING_COMPACT_PROPERTY to instanceTemporalFragment.deserializeAsMap())
        val jsonLdAttribute = JsonLdUtils.expandJsonLdFragment(
            attributeInstancePayload,
            APIC_COMPOUND_CONTEXTS
        ) as ExpandedAttributes

        val temporalEntitiesQuery = gimmeTemporalEntitiesQuery(
            buildDefaultTestTemporalQuery(
                timerel = Timerel.AFTER,
                timeAt = ZonedDateTime.parse("1970-01-01T00:00:00Z")
            )
        )

        attributeInstanceService.modifyAttributeInstance(
            entityId,
            INCOMING_PROPERTY,
            attributeInstance.instanceId,
            jsonLdAttribute.entries.first().value
        ).shouldSucceed()

        attributeInstanceService.search(temporalEntitiesQuery, incomingAttribute)
            .shouldSucceedWith {
                (it as List<FullAttributeInstanceResult>).single { result ->
                    result.time == ZonedDateTime.parse("2023-03-13T12:33:06Z") &&
                        result.payload.deserializeAsMap().containsKey(NGSILD_MODIFIED_AT_PROPERTY) ||
                        result.payload.deserializeAsMap().containsKey(NGSILD_INSTANCE_ID_PROPERTY)
                }
            }
    }

    @Test
    fun `it should modify attribute instance for a JSON property`() = runTest {
        val attributeInstance = gimmeJsonPropertyAttributeInstance(jsonAttribute.id)
        attributeInstanceService.create(attributeInstance)

        val instanceTemporalFragment =
            loadSampleData("fragments/temporal_instance_json_fragment.jsonld")
        val attributeInstancePayload =
            mapOf(LUMINOSITY_COMPACT_JSONPROPERTY to instanceTemporalFragment.deserializeAsMap())
        val jsonLdAttribute = JsonLdUtils.expandJsonLdFragment(
            attributeInstancePayload,
            APIC_COMPOUND_CONTEXTS
        ) as ExpandedAttributes

        val temporalEntitiesQuery = gimmeTemporalEntitiesQuery(
            buildDefaultTestTemporalQuery(
                timerel = Timerel.AFTER,
                timeAt = ZonedDateTime.parse("1970-01-01T00:00:00Z")
            )
        )

        attributeInstanceService.modifyAttributeInstance(
            entityId,
            LUMINOSITY_JSONPROPERTY,
            attributeInstance.instanceId,
            jsonLdAttribute.entries.first().value
        ).shouldSucceed()

        attributeInstanceService.search(temporalEntitiesQuery, jsonAttribute)
            .shouldSucceedWith {
                (it as List<FullAttributeInstanceResult>).single { result ->
                    result.time == ZonedDateTime.parse("2023-03-13T12:33:06Z") &&
                        result.payload.deserializeAsMap().containsKey(NGSILD_MODIFIED_AT_PROPERTY) &&
                        result.payload.deserializeAsMap().containsKey(NGSILD_INSTANCE_ID_PROPERTY) &&
                        result.payload.deserializeAsMap().containsKey(NGSILD_JSONPROPERTY_VALUE)
                }
            }
    }

    @Test
    fun `it should modify attribute instance for a LanguageProperty property`() = runTest {
        val attributeInstance = gimmeLanguagePropertyAttributeInstance(languageAttribute.id)
        attributeInstanceService.create(attributeInstance)

        val instanceTemporalFragment =
            loadSampleData("fragments/temporal_instance_language_fragment.jsonld")
        val attributeInstancePayload =
            mapOf(FRIENDLYNAME_COMPACT_LANGUAGEPROPERTY to instanceTemporalFragment.deserializeAsMap())
        val jsonLdAttribute = JsonLdUtils.expandJsonLdFragment(
            attributeInstancePayload,
            APIC_COMPOUND_CONTEXTS
        ) as ExpandedAttributes

        val temporalEntitiesQuery = gimmeTemporalEntitiesQuery(
            buildDefaultTestTemporalQuery(
                timerel = Timerel.AFTER,
                timeAt = ZonedDateTime.parse("1970-01-01T00:00:00Z")
            )
        )

        attributeInstanceService.modifyAttributeInstance(
            entityId,
            FRIENDLYNAME_LANGUAGEPROPERTY,
            attributeInstance.instanceId,
            jsonLdAttribute.entries.first().value
        ).shouldSucceed()

        attributeInstanceService.search(temporalEntitiesQuery, languageAttribute)
            .shouldSucceedWith {
                (it as List<FullAttributeInstanceResult>).single { result ->
                    val deserializedPayload = result.payload.deserializeAsMap()
                    result.time == ZonedDateTime.parse("2023-03-13T12:33:06Z") &&
                        deserializedPayload.containsKey(NGSILD_MODIFIED_AT_PROPERTY) &&
                        deserializedPayload.containsKey(NGSILD_INSTANCE_ID_PROPERTY) &&
                        deserializedPayload.containsKey(NGSILD_LANGUAGEPROPERTY_VALUE) &&
                        (deserializedPayload[NGSILD_LANGUAGEPROPERTY_VALUE] as List<Map<String, String>>)
                            .all { langMap ->
                                langMap[JSONLD_LANGUAGE] == "fr" || langMap[JSONLD_LANGUAGE] == "it"
                            }
                }
            }
    }

    @Test
    fun `it should modify attribute instance for a VocabProperty property`() = runTest {
        val attributeInstance = gimmeVocabPropertyAttributeInstance(vocabAttribute.id)
        attributeInstanceService.create(attributeInstance)

        val instanceTemporalFragment =
            loadSampleData("fragments/temporal_instance_vocab_fragment.jsonld")
        val attributeInstancePayload =
            mapOf(CATEGORY_COMPACT_VOCABPROPERTY to instanceTemporalFragment.deserializeAsMap())
        val jsonLdAttribute = JsonLdUtils.expandJsonLdFragment(
            attributeInstancePayload,
            APIC_COMPOUND_CONTEXTS
        ) as ExpandedAttributes

        val temporalEntitiesQuery = gimmeTemporalEntitiesQuery(
            buildDefaultTestTemporalQuery(
                timerel = Timerel.AFTER,
                timeAt = ZonedDateTime.parse("1970-01-01T00:00:00Z")
            )
        )

        attributeInstanceService.modifyAttributeInstance(
            entityId,
            CATEGORY_VOCAPPROPERTY,
            attributeInstance.instanceId,
            jsonLdAttribute.entries.first().value
        ).shouldSucceed()

        attributeInstanceService.search(temporalEntitiesQuery, vocabAttribute)
            .shouldSucceedWith {
                (it as List<FullAttributeInstanceResult>).single { result ->
                    val deserializedPayload = result.payload.deserializeAsMap()
                    result.time == ZonedDateTime.parse("2023-03-13T12:33:06Z") &&
                        deserializedPayload.containsKey(NGSILD_MODIFIED_AT_PROPERTY) &&
                        deserializedPayload.containsKey(NGSILD_INSTANCE_ID_PROPERTY) &&
                        deserializedPayload.containsKey(NGSILD_VOCABPROPERTY_VALUE) &&
                        (deserializedPayload[NGSILD_VOCABPROPERTY_VALUE] as List<Map<String, String>>)
                            .all { entry ->
                                entry[JSONLD_ID] == "${NGSILD_DEFAULT_VOCAB}stellio" ||
                                    entry[JSONLD_ID] == "${NGSILD_DEFAULT_VOCAB}egm"
                            }
                }
            }
    }

    @Test
    fun `it should delete attribute instance`() = runTest {
        val attributeInstance = gimmeNumericPropertyAttributeInstance(incomingAttribute.id)
        attributeInstanceService.create(attributeInstance).shouldSucceed()

        attributeInstanceService.deleteInstance(
            incomingAttribute.entityId,
            incomingAttribute.attributeName,
            attributeInstance.instanceId
        ).shouldSucceed()

        attributeInstanceService.search(
            gimmeTemporalEntitiesQuery(buildDefaultTestTemporalQuery()),
            incomingAttribute
        )
            .shouldSucceedWith {
                assertThat(it)
                    .isEmpty()
            }
    }

    @Test
    fun `it should not delete attribute instance if attribute name is not found`() = runTest {
        val attributeInstance = gimmeNumericPropertyAttributeInstance(incomingAttribute.id)
        attributeInstanceService.create(attributeInstance).shouldSucceed()

        attributeInstanceService.deleteInstance(
            incomingAttribute.entityId,
            outgoingAttribute.attributeName,
            attributeInstance.instanceId
        ).shouldFail {
            assertInstanceOf(ResourceNotFoundException::class.java, it)
            assertEquals(
                attributeOrInstanceNotFoundMessage(
                    outgoingAttribute.attributeName,
                    attributeInstance.instanceId.toString()
                ),
                it.message
            )
        }
    }

    @Test
    fun `it should not delete attribute instance if instanceID is not found`() = runTest {
        val attributeInstance = gimmeNumericPropertyAttributeInstance(incomingAttribute.id)
        val instanceId = "urn:ngsi-ld:Instance:notFound".toUri()
        attributeInstanceService.create(attributeInstance).shouldSucceed()

        attributeInstanceService.deleteInstance(
            incomingAttribute.entityId,
            incomingAttribute.attributeName,
            instanceId
        ).shouldFail {
            assertInstanceOf(ResourceNotFoundException::class.java, it)
            assertEquals(
                attributeOrInstanceNotFoundMessage(
                    incomingAttribute.attributeName,
                    instanceId.toString()
                ),
                it.message
            )
        }
    }

    @Test
    fun `it should delete all instances of an entity`() = runTest {
        (1..10).forEachIndexed { index, _ ->
            if (index % 2 == 0)
                attributeInstanceService.create(
                    gimmeNumericPropertyAttributeInstance(incomingAttribute.id)
                )
            else
                attributeInstanceService.create(
                    gimmeNumericPropertyAttributeInstance(
                        attributeUuid = incomingAttribute.id,
                        timeProperty = AttributeInstance.TemporalProperty.CREATED_AT
                    )
                )
        }

        attributeInstanceService.deleteInstancesOfEntity(listOf(incomingAttribute.id)).shouldSucceed()

        val temporalEntitiesQuery = gimmeTemporalEntitiesQuery(
            buildDefaultTestTemporalQuery(
                timerel = Timerel.AFTER,
                timeAt = now.minusHours(1)
            )
        )
        attributeInstanceService.search(temporalEntitiesQuery, incomingAttribute)
            .shouldSucceedWith { assertThat(it).isEmpty() }

        val temporalEntitiesAuditQuery = gimmeTemporalEntitiesQuery(
            buildDefaultTestTemporalQuery(
                timerel = Timerel.AFTER,
                timeAt = now.minusHours(1),
                timeproperty = AttributeInstance.TemporalProperty.CREATED_AT
            )
        )

        attributeInstanceService.search(temporalEntitiesAuditQuery, incomingAttribute)
            .shouldSucceedWith { assertThat(it).isEmpty() }
    }

    @Test
    fun `it should delete all instances of an attribute`() = runTest {
        (1..10).forEachIndexed { index, _ ->
            if (index % 2 == 0)
                attributeInstanceService.create(
                    gimmeNumericPropertyAttributeInstance(attributeUuid = incomingAttribute.id)
                )
            else
                attributeInstanceService.create(
                    gimmeNumericPropertyAttributeInstance(attributeUuid = outgoingAttribute.id)
                )
        }

        attributeInstanceService.deleteAllInstancesOfAttribute(entityId, INCOMING_PROPERTY).shouldSucceed()

        val temporalEntitiesQuery = gimmeTemporalEntitiesQuery(
            buildDefaultTestTemporalQuery(
                timerel = Timerel.AFTER,
                timeAt = now.minusHours(1)
            )
        )
        attributeInstanceService.search(temporalEntitiesQuery, outgoingAttribute)
            .shouldSucceedWith { assertThat(it).hasSize(5) }
        attributeInstanceService.search(temporalEntitiesQuery, incomingAttribute)
            .shouldSucceedWith { assertThat(it).isEmpty() }
    }

    @Test
    fun `it should delete all instances of an attribute on default dataset`() = runTest {
        (1..10).forEachIndexed { index, _ ->
            if (index % 2 == 0)
                attributeInstanceService.create(
                    gimmeNumericPropertyAttributeInstance(attributeUuid = incomingAttribute.id)
                )
            else
                attributeInstanceService.create(
                    gimmeNumericPropertyAttributeInstance(attributeUuid = outgoingAttribute.id)
                )
        }

        attributeInstanceService.deleteInstancesOfAttribute(entityId, INCOMING_PROPERTY, null).shouldSucceed()

        val temporalEntitiesQuery = gimmeTemporalEntitiesQuery(
            buildDefaultTestTemporalQuery(
                timerel = Timerel.AFTER,
                timeAt = now.minusHours(1)
            )
        )
        attributeInstanceService.search(temporalEntitiesQuery, outgoingAttribute)
            .shouldSucceedWith { assertThat(it).hasSize(5) }
        attributeInstanceService.search(temporalEntitiesQuery, incomingAttribute)
            .shouldSucceedWith { assertThat(it).isEmpty() }
    }
}
