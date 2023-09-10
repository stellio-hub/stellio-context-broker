package com.egm.stellio.search.service

import com.egm.stellio.search.model.*
import com.egm.stellio.search.support.*
import com.egm.stellio.shared.model.ResourceNotFoundException
import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_VALUE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_DATE_TIME_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_INSTANCE_ID_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_MODIFIED_AT_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_OBSERVED_AT_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_PROPERTY_VALUE
import com.egm.stellio.shared.util.JsonLdUtils.buildExpandedProperty
import com.egm.stellio.shared.util.JsonLdUtils.buildNonReifiedDateTime
import com.egm.stellio.shared.util.JsonUtils.deserializeAsList
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import io.mockk.spyk
import io.mockk.verify
import kotlinx.coroutines.ExperimentalCoroutinesApi
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
import java.util.UUID

@OptIn(ExperimentalCoroutinesApi::class)
@SpringBootTest
@ActiveProfiles("test")
class AttributeInstanceServiceTests : WithTimescaleContainer, WithKafkaContainer {

    @Autowired
    private lateinit var attributeInstanceService: AttributeInstanceService

    @Autowired
    private lateinit var temporalEntityAttributeService: TemporalEntityAttributeService

    @Autowired
    private lateinit var databaseClient: DatabaseClient

    @Autowired
    private lateinit var r2dbcEntityTemplate: R2dbcEntityTemplate

    private val now = Instant.now().atZone(ZoneOffset.UTC)

    private lateinit var incomingTemporalEntityAttribute: TemporalEntityAttribute
    private lateinit var outgoingTemporalEntityAttribute: TemporalEntityAttribute

    val entityId = "urn:ngsi-ld:BeeHive:TESTC".toUri()

    @BeforeAll
    fun createTemporalEntityAttribute() {
        incomingTemporalEntityAttribute = TemporalEntityAttribute(
            entityId = entityId,
            attributeName = INCOMING_PROPERTY,
            attributeValueType = TemporalEntityAttribute.AttributeValueType.NUMBER,
            createdAt = now,
            payload = EMPTY_JSON_PAYLOAD
        )

        runBlocking {
            temporalEntityAttributeService.create(incomingTemporalEntityAttribute)
        }

        outgoingTemporalEntityAttribute = TemporalEntityAttribute(
            entityId = entityId,
            attributeName = OUTGOING_PROPERTY,
            attributeValueType = TemporalEntityAttribute.AttributeValueType.NUMBER,
            createdAt = now,
            payload = EMPTY_JSON_PAYLOAD
        )

        runBlocking {
            temporalEntityAttributeService.create(outgoingTemporalEntityAttribute)
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
        val observation = gimmeAttributeInstance(incomingTemporalEntityAttribute.id).copy(
            time = now,
            measuredValue = 12.4
        )
        attributeInstanceService.create(observation)

        val temporalEntitiesQuery = gimmeTemporalEntitiesQuery(
            TemporalQuery(
                timerel = TemporalQuery.Timerel.AFTER,
                timeAt = now.minusHours(1)
            )
        )
        attributeInstanceService.search(temporalEntitiesQuery, incomingTemporalEntityAttribute)
            .shouldSucceedWith {
                assertThat(it)
                    .singleElement()
                    .hasFieldOrPropertyWithValue("temporalEntityAttribute", incomingTemporalEntityAttribute.id)
            }
    }

    @Test
    fun `it should retrieve an instance having the corresponding time property value`() = runTest {
        val observation = gimmeAttributeInstance(incomingTemporalEntityAttribute.id).copy(
            timeProperty = AttributeInstance.TemporalProperty.CREATED_AT,
            time = now,
            measuredValue = 12.4
        )
        attributeInstanceService.create(observation)

        val temporalEntitiesQuery = gimmeTemporalEntitiesQuery(
            TemporalQuery(
                timerel = TemporalQuery.Timerel.AFTER,
                timeAt = now.minusHours(1),
                timeproperty = AttributeInstance.TemporalProperty.CREATED_AT
            )
        )
        attributeInstanceService.search(temporalEntitiesQuery, incomingTemporalEntityAttribute)
            .shouldSucceedWith {
                assertThat(it)
                    .singleElement()
            }
    }

    @Test
    fun `it should retrieve an instance with audit info if time property is not observedAt`() = runTest {
        val observation = gimmeAttributeInstance(incomingTemporalEntityAttribute.id).copy(
            timeProperty = AttributeInstance.TemporalProperty.CREATED_AT,
            time = now,
            measuredValue = 12.4,
            sub = "sub"
        )
        attributeInstanceService.create(observation)

        val temporalEntitiesQuery = gimmeTemporalEntitiesQuery(
            TemporalQuery(
                timerel = TemporalQuery.Timerel.AFTER,
                timeAt = now.minusHours(1),
                timeproperty = AttributeInstance.TemporalProperty.CREATED_AT
            )
        )
        attributeInstanceService.search(temporalEntitiesQuery, incomingTemporalEntityAttribute)
            .shouldSucceedWith {
                assertThat(it)
                    .singleElement()
                    .hasFieldOrPropertyWithValue("sub", "sub")
            }
    }

    @Test
    fun `it should not retrieve an instance not having the corresponding time property value`() = runTest {
        val observation = gimmeAttributeInstance(incomingTemporalEntityAttribute.id).copy(
            timeProperty = AttributeInstance.TemporalProperty.CREATED_AT,
            time = now,
            measuredValue = 12.4
        )
        attributeInstanceService.create(observation)

        val temporalEntitiesQuery = gimmeTemporalEntitiesQuery(
            TemporalQuery(
                timerel = TemporalQuery.Timerel.AFTER,
                timeAt = now.minusHours(1),
                timeproperty = AttributeInstance.TemporalProperty.MODIFIED_AT
            )
        )
        attributeInstanceService.search(temporalEntitiesQuery, incomingTemporalEntityAttribute)
            .shouldSucceedWith {
                assertThat(it)
                    .isEmpty()
            }
    }

    @Test
    fun `it should retrieve all full instances if temporalValues are not asked for`() = runTest {
        (1..10).forEach { _ ->
            attributeInstanceService.create(gimmeAttributeInstance(incomingTemporalEntityAttribute.id))
        }

        val temporalEntitiesQuery = gimmeTemporalEntitiesQuery(
            TemporalQuery(
                timerel = TemporalQuery.Timerel.AFTER,
                timeAt = now.minusHours(1)
            )
        )
        attributeInstanceService.search(temporalEntitiesQuery, incomingTemporalEntityAttribute)
            .shouldSucceedWith {
                assertThat(it)
                    .hasSize(10)
            }
    }

    @Test
    fun `it should retrieve all instances when no timerel and time parameters are provided`() = runTest {
        (1..10).forEach { _ ->
            attributeInstanceService.create(gimmeAttributeInstance(incomingTemporalEntityAttribute.id))
        }

        attributeInstanceService.search(gimmeTemporalEntitiesQuery(TemporalQuery()), incomingTemporalEntityAttribute)
            .shouldSucceedWith {
                assertThat(it)
                    .hasSize(10)
            }
    }

    @Test
    fun `it should retrieve instances of a temporal entity attribute whose value type is Any`() = runTest {
        val temporalEntityAttribute2 = TemporalEntityAttribute(
            entityId = entityId,
            attributeName = "propWithStringValue",
            attributeValueType = TemporalEntityAttribute.AttributeValueType.STRING,
            createdAt = now,
            payload = EMPTY_JSON_PAYLOAD
        )

        temporalEntityAttributeService.create(temporalEntityAttribute2)

        (1..10).forEach { _ ->
            val observedAt = Instant.now().atZone(ZoneOffset.UTC)
            val attributeMetadata = AttributeMetadata(
                measuredValue = null,
                value = "some value",
                geoValue = null,
                valueType = TemporalEntityAttribute.AttributeValueType.STRING,
                datasetId = null,
                type = TemporalEntityAttribute.AttributeType.Property,
                observedAt = observedAt
            )
            val attributeInstance = AttributeInstance(
                temporalEntityAttribute = temporalEntityAttribute2.id,
                time = observedAt,
                attributeMetadata = attributeMetadata,
                payload = buildExpandedProperty(attributeMetadata.value!!)
                    .addSubAttribute(NGSILD_OBSERVED_AT_PROPERTY, buildNonReifiedDateTime(observedAt))
                    .getSingleEntry()
            )
            attributeInstanceService.create(attributeInstance)
        }

        attributeInstanceService.search(
            gimmeTemporalEntitiesQuery(TemporalQuery(), withTemporalValues = true),
            temporalEntityAttribute2
        ).shouldSucceedWith { results ->
            assertThat(results)
                .hasSize(10)
                .allMatch {
                    it.temporalEntityAttribute == temporalEntityAttribute2.id &&
                        (it as SimplifiedAttributeInstanceResult).value == "some value"
                }
        }
    }

    @Test
    fun `it should set the start time to the oldest value if asking for no timerel`() = runTest {
        (1..9).forEachIndexed { index, _ ->
            val attributeInstance =
                gimmeAttributeInstance(incomingTemporalEntityAttribute.id)
                    .copy(
                        measuredValue = index.toDouble(),
                        time = ZonedDateTime.parse("2022-07-0${index + 1}T00:00:00Z")
                    )
            attributeInstanceService.create(attributeInstance)
        }
        val temporalEntitiesQuery = gimmeTemporalEntitiesQuery(
            TemporalQuery(
                aggrPeriodDuration = "P30D",
                aggrMethods = listOf(TemporalQuery.Aggregate.MAX)
            ),
            withAggregatedValues = true
        )

        val origin = attributeInstanceService.selectOldestDate(
            temporalEntitiesQuery.temporalQuery,
            listOf(incomingTemporalEntityAttribute)
        )

        assertNotNull(origin)
        assertEquals(ZonedDateTime.parse("2022-07-01T00:00:00Z"), origin)
    }

    @Test
    fun `it should only return the last n instances asked in the temporal query`() = runTest {
        (1..10).forEach { _ ->
            val attributeInstance = gimmeAttributeInstance(incomingTemporalEntityAttribute.id)
                .copy(measuredValue = 1.0)
            attributeInstanceService.create(attributeInstance)
        }

        val temporalEntitiesQuery = gimmeTemporalEntitiesQuery(
            TemporalQuery(
                timerel = TemporalQuery.Timerel.AFTER,
                timeAt = now.minusHours(1),
                lastN = 5
            )
        )
        attributeInstanceService.search(temporalEntitiesQuery, incomingTemporalEntityAttribute)
            .shouldSucceedWith {
                assertThat(it)
                    .hasSize(5)
            }
    }

    @Test
    fun `it should only retrieve the temporal evolution of the provided temporal entity attribute`() = runTest {
        val temporalEntityAttribute2 = TemporalEntityAttribute(
            entityId = entityId,
            attributeName = OUTGOING_COMPACT_PROPERTY,
            attributeValueType = TemporalEntityAttribute.AttributeValueType.NUMBER,
            createdAt = now,
            payload = EMPTY_JSON_PAYLOAD
        )

        temporalEntityAttributeService.create(temporalEntityAttribute2)

        (1..10).forEach { _ ->
            attributeInstanceService.create(gimmeAttributeInstance(incomingTemporalEntityAttribute.id))
        }
        (1..5).forEach { _ ->
            attributeInstanceService.create(gimmeAttributeInstance(temporalEntityAttribute2.id))
        }

        val temporalEntitiesQuery = gimmeTemporalEntitiesQuery(
            TemporalQuery(
                timerel = TemporalQuery.Timerel.AFTER,
                timeAt = now.minusHours(1)
            )
        )
        attributeInstanceService.search(temporalEntitiesQuery, incomingTemporalEntityAttribute)
            .shouldSucceedWith { results ->
                assertThat(results)
                    .hasSize(10)
                    .allMatch {
                        it.temporalEntityAttribute == incomingTemporalEntityAttribute.id
                    }
            }
    }

    @Test
    fun `it should not retrieve any instance if temporal entity does not match`() = runTest {
        (1..10).forEach { _ ->
            attributeInstanceService.create(gimmeAttributeInstance(incomingTemporalEntityAttribute.id))
        }

        val temporalEntitiesQuery = gimmeTemporalEntitiesQuery(
            TemporalQuery(
                timerel = TemporalQuery.Timerel.AFTER,
                timeAt = now.minusHours(1)
            )
        )
        attributeInstanceService.search(
            temporalEntitiesQuery,
            incomingTemporalEntityAttribute.copy(id = UUID.randomUUID())
        ).shouldSucceedWith {
            assertThat(it)
                .isEmpty()
        }
    }

    @Test
    fun `it should not retrieve any instance if there is no value in the time interval`() = runTest {
        (1..10).forEach { _ ->
            attributeInstanceService.create(gimmeAttributeInstance(incomingTemporalEntityAttribute.id))
        }

        val temporalEntitiesQuery = gimmeTemporalEntitiesQuery(
            TemporalQuery(
                timerel = TemporalQuery.Timerel.AFTER,
                timeAt = now.plusHours(1)
            )
        )
        attributeInstanceService.search(temporalEntitiesQuery, incomingTemporalEntityAttribute)
            .shouldSucceedWith {
                assertThat(it)
                    .isEmpty()
            }
    }

    @Test
    fun `it should update an existing attribute instance with same observation date`() = runTest {
        val attributeInstance = gimmeAttributeInstance(incomingTemporalEntityAttribute.id)

        attributeInstanceService.create(attributeInstance)

        val createResult = attributeInstanceService.create(attributeInstance.copy(measuredValue = 100.0))
        assertThat(createResult.isRight())

        attributeInstanceService.search(
            gimmeTemporalEntitiesQuery(
                TemporalQuery(
                    timerel = TemporalQuery.Timerel.AFTER,
                    timeAt = now.minusHours(1)
                ),
                withTemporalValues = true
            ),
            incomingTemporalEntityAttribute
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
            valueType = TemporalEntityAttribute.AttributeValueType.NUMBER,
            datasetId = null,
            type = TemporalEntityAttribute.AttributeType.Property,
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
            incomingTemporalEntityAttribute.id,
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
            valueType = TemporalEntityAttribute.AttributeValueType.BOOLEAN,
            datasetId = null,
            type = TemporalEntityAttribute.AttributeType.Property,
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
            incomingTemporalEntityAttribute.id,
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
    fun `it should modify attribute instance`() = runTest {
        val attributeInstance = gimmeAttributeInstance(incomingTemporalEntityAttribute.id)
        attributeInstanceService.create(attributeInstance)

        val instanceTemporalFragment =
            loadSampleData("fragments/temporal_instance_fragment.jsonld")
        val attributeInstancePayload = mapOf(INCOMING_COMPACT_PROPERTY to instanceTemporalFragment.deserializeAsList())
        val jsonLdAttribute = JsonLdUtils.expandJsonLdFragment(
            attributeInstancePayload,
            listOf(APIC_COMPOUND_CONTEXT)
        ) as ExpandedAttributes

        val temporalEntitiesQuery = gimmeTemporalEntitiesQuery(
            TemporalQuery(
                timerel = TemporalQuery.Timerel.AFTER,
                timeAt = ZonedDateTime.parse("1970-01-01T00:00:00Z")
            )
        )

        attributeInstanceService.modifyAttributeInstance(
            entityId,
            INCOMING_PROPERTY,
            attributeInstance.instanceId,
            jsonLdAttribute.entries.first().value
        ).shouldSucceed()

        attributeInstanceService.search(temporalEntitiesQuery, incomingTemporalEntityAttribute)
            .shouldSucceedWith {
                (it as List<FullAttributeInstanceResult>).single { result ->
                    result.time == ZonedDateTime.parse("2023-03-13T12:33:06Z") &&
                        result.payload.deserializeAsMap().containsKey(NGSILD_MODIFIED_AT_PROPERTY) ||
                        result.payload.deserializeAsMap().containsKey(NGSILD_INSTANCE_ID_PROPERTY)
                }
            }
    }

    @Test
    fun `it should delete attribute instance`() = runTest {
        val attributeInstance = gimmeAttributeInstance(incomingTemporalEntityAttribute.id)
        attributeInstanceService.create(attributeInstance).shouldSucceed()

        attributeInstanceService.deleteInstance(
            incomingTemporalEntityAttribute.entityId,
            incomingTemporalEntityAttribute.attributeName,
            attributeInstance.instanceId
        ).shouldSucceed()

        attributeInstanceService.search(gimmeTemporalEntitiesQuery(TemporalQuery()), incomingTemporalEntityAttribute)
            .shouldSucceedWith {
                assertThat(it)
                    .isEmpty()
            }
    }

    @Test
    fun `it should not delete attribute instance if attribute name is not found`() = runTest {
        val attributeInstance = gimmeAttributeInstance(incomingTemporalEntityAttribute.id)
        attributeInstanceService.create(attributeInstance).shouldSucceed()

        attributeInstanceService.deleteInstance(
            incomingTemporalEntityAttribute.entityId,
            outgoingTemporalEntityAttribute.attributeName,
            attributeInstance.instanceId
        ).shouldFail {
            assertInstanceOf(ResourceNotFoundException::class.java, it)
            assertEquals(
                attributeOrInstanceNotFoundMessage(
                    outgoingTemporalEntityAttribute.attributeName,
                    attributeInstance.instanceId.toString()
                ),
                it.message
            )
        }
    }

    @Test
    fun `it should not delete attribute instance if instanceID is not found`() = runTest {
        val attributeInstance = gimmeAttributeInstance(incomingTemporalEntityAttribute.id)
        val instanceId = "urn:ngsi-ld:Instance:notFound".toUri()
        attributeInstanceService.create(attributeInstance).shouldSucceed()

        attributeInstanceService.deleteInstance(
            incomingTemporalEntityAttribute.entityId,
            incomingTemporalEntityAttribute.attributeName,
            instanceId
        ).shouldFail {
            assertInstanceOf(ResourceNotFoundException::class.java, it)
            assertEquals(
                attributeOrInstanceNotFoundMessage(
                    incomingTemporalEntityAttribute.attributeName,
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
                attributeInstanceService.create(gimmeAttributeInstance(incomingTemporalEntityAttribute.id))
            else
                attributeInstanceService.create(
                    gimmeAttributeInstance(
                        teaUuid = incomingTemporalEntityAttribute.id,
                        timeProperty = AttributeInstance.TemporalProperty.CREATED_AT
                    )
                )
        }

        attributeInstanceService.deleteInstancesOfEntity(listOf(incomingTemporalEntityAttribute.id)).shouldSucceed()

        val temporalEntitiesQuery = gimmeTemporalEntitiesQuery(
            TemporalQuery(
                timerel = TemporalQuery.Timerel.AFTER,
                timeAt = now.minusHours(1)
            )
        )
        attributeInstanceService.search(temporalEntitiesQuery, incomingTemporalEntityAttribute)
            .shouldSucceedWith { assertThat(it).isEmpty() }

        val temporalEntitiesAuditQuery = gimmeTemporalEntitiesQuery(
            TemporalQuery(
                timerel = TemporalQuery.Timerel.AFTER,
                timeAt = now.minusHours(1),
                timeproperty = AttributeInstance.TemporalProperty.CREATED_AT
            )
        )

        attributeInstanceService.search(temporalEntitiesAuditQuery, incomingTemporalEntityAttribute)
            .shouldSucceedWith { assertThat(it).isEmpty() }
    }

    @Test
    fun `it should delete all instances of an attribute`() = runTest {
        (1..10).forEachIndexed { index, _ ->
            if (index % 2 == 0)
                attributeInstanceService.create(
                    gimmeAttributeInstance(teaUuid = incomingTemporalEntityAttribute.id)
                )
            else
                attributeInstanceService.create(
                    gimmeAttributeInstance(teaUuid = outgoingTemporalEntityAttribute.id)
                )
        }

        attributeInstanceService.deleteAllInstancesOfAttribute(entityId, INCOMING_PROPERTY).shouldSucceed()

        val temporalEntitiesQuery = gimmeTemporalEntitiesQuery(
            TemporalQuery(
                timerel = TemporalQuery.Timerel.AFTER,
                timeAt = now.minusHours(1)
            )
        )
        attributeInstanceService.search(temporalEntitiesQuery, outgoingTemporalEntityAttribute)
            .shouldSucceedWith { assertThat(it).hasSize(5) }
        attributeInstanceService.search(temporalEntitiesQuery, incomingTemporalEntityAttribute)
            .shouldSucceedWith { assertThat(it).isEmpty() }
    }

    @Test
    fun `it should delete all instances of an attribute on default dataset`() = runTest {
        (1..10).forEachIndexed { index, _ ->
            if (index % 2 == 0)
                attributeInstanceService.create(
                    gimmeAttributeInstance(teaUuid = incomingTemporalEntityAttribute.id)
                )
            else
                attributeInstanceService.create(
                    gimmeAttributeInstance(teaUuid = outgoingTemporalEntityAttribute.id)
                )
        }

        attributeInstanceService.deleteInstancesOfAttribute(entityId, INCOMING_PROPERTY, null).shouldSucceed()

        val temporalEntitiesQuery = gimmeTemporalEntitiesQuery(
            TemporalQuery(
                timerel = TemporalQuery.Timerel.AFTER,
                timeAt = now.minusHours(1)
            )
        )
        attributeInstanceService.search(temporalEntitiesQuery, outgoingTemporalEntityAttribute)
            .shouldSucceedWith { assertThat(it).hasSize(5) }
        attributeInstanceService.search(temporalEntitiesQuery, incomingTemporalEntityAttribute)
            .shouldSucceedWith { assertThat(it).isEmpty() }
    }
}
