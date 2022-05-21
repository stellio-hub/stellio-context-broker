package com.egm.stellio.search.util

import com.egm.stellio.search.model.FullAttributeInstanceResult
import com.egm.stellio.search.model.SimplifiedAttributeInstanceResult
import com.egm.stellio.search.model.TemporalEntityAttribute
import com.egm.stellio.shared.util.BEEHIVE_TYPE
import com.egm.stellio.shared.util.loadSampleData
import com.egm.stellio.shared.util.toUri
import org.junit.jupiter.params.provider.Arguments
import java.time.ZonedDateTime
import java.util.UUID
import java.util.stream.Stream

@Suppress("unused")
class ParameterizedTests {

    companion object {
        @JvmStatic
        fun rawResultsProvider(): Stream<Arguments> {
            return Stream.of(
                Arguments.arguments(
                    mapOf(
                        TemporalEntityAttribute(
                            entityId = "urn:ngsi-ld:BeeHive:TESTC".toUri(),
                            types = listOf(BEEHIVE_TYPE),
                            attributeName = "https://ontology.eglobalmark.com/apic#incoming",
                            attributeValueType = TemporalEntityAttribute.AttributeValueType.MEASURE
                        ) to
                            listOf(
                                FullAttributeInstanceResult(
                                    temporalEntityAttribute = UUID.randomUUID(),
                                    payload = buildAttributeInstancePayload(
                                        550.0,
                                        ZonedDateTime.parse("2020-03-25T08:29:17.965206Z"),
                                        null,
                                        "urn:ngsi-ld:Instance:45678".toUri()
                                    ),
                                    sub = null
                                ),
                                FullAttributeInstanceResult(
                                    temporalEntityAttribute = UUID.randomUUID(),
                                    payload = buildAttributeInstancePayload(
                                        650.0,
                                        ZonedDateTime.parse("2020-03-25T08:33:17.965206Z"),
                                        null,
                                        "urn:ngsi-ld:Instance:45679".toUri()
                                    ),
                                    sub = null
                                )
                            )
                    ),
                    false,
                    false,
                    loadSampleData("expectations/beehive_incoming_multi_instances_without_datasetId.jsonld")
                ),
                Arguments.arguments(
                    mapOf(
                        TemporalEntityAttribute(
                            entityId = "urn:ngsi-ld:BeeHive:TESTC".toUri(),
                            types = listOf(BEEHIVE_TYPE),
                            attributeName = "https://ontology.eglobalmark.com/apic#incoming",
                            attributeType = TemporalEntityAttribute.AttributeType.Relationship,
                            attributeValueType = TemporalEntityAttribute.AttributeValueType.ANY
                        ) to
                            listOf(
                                FullAttributeInstanceResult(
                                    temporalEntityAttribute = UUID.randomUUID(),
                                    payload = buildAttributeInstancePayload(
                                        "urn:ngsi-ld:Entity:1234",
                                        ZonedDateTime.parse("2020-03-25T08:29:17.965206Z"),
                                        null,
                                        "urn:ngsi-ld:Instance:45678".toUri(),
                                        TemporalEntityAttribute.AttributeType.Relationship
                                    ),
                                    sub = null
                                ),
                                FullAttributeInstanceResult(
                                    temporalEntityAttribute = UUID.randomUUID(),
                                    payload = buildAttributeInstancePayload(
                                        "urn:ngsi-ld:Entity:5678",
                                        ZonedDateTime.parse("2020-03-25T08:33:17.965206Z"),
                                        null,
                                        "urn:ngsi-ld:Instance:45679".toUri(),
                                        TemporalEntityAttribute.AttributeType.Relationship
                                    ),
                                    sub = null
                                )
                            )
                    ),
                    false,
                    false,
                    loadSampleData("expectations/beehive_relationship_multi_instances_without_datasetId.jsonld")
                ),
                Arguments.arguments(
                    mapOf(
                        TemporalEntityAttribute(
                            entityId = "urn:ngsi-ld:BeeHive:TESTC".toUri(),
                            types = listOf(BEEHIVE_TYPE),
                            attributeName = "https://ontology.eglobalmark.com/apic#incoming",
                            attributeValueType = TemporalEntityAttribute.AttributeValueType.MEASURE,
                            datasetId = "urn:ngsi-ld:Dataset:01234".toUri()
                        ) to
                            listOf(
                                FullAttributeInstanceResult(
                                    temporalEntityAttribute = UUID.randomUUID(),
                                    payload = buildAttributeInstancePayload(
                                        550.0,
                                        ZonedDateTime.parse("2020-03-25T08:29:17.965206Z"),
                                        "urn:ngsi-ld:Dataset:01234".toUri(),
                                        "urn:ngsi-ld:Instance:01234".toUri()
                                    ),
                                    sub = null
                                ),
                                FullAttributeInstanceResult(
                                    temporalEntityAttribute = UUID.randomUUID(),
                                    payload = buildAttributeInstancePayload(
                                        650.0,
                                        ZonedDateTime.parse("2020-03-25T08:33:17.965206Z"),
                                        "urn:ngsi-ld:Dataset:01234".toUri(),
                                        "urn:ngsi-ld:Instance:01235".toUri()
                                    ),
                                    sub = null
                                )
                            ),
                        TemporalEntityAttribute(
                            entityId = "urn:ngsi-ld:BeeHive:TESTC".toUri(),
                            types = listOf(BEEHIVE_TYPE),
                            attributeName = "https://ontology.eglobalmark.com/apic#incoming",
                            attributeValueType = TemporalEntityAttribute.AttributeValueType.MEASURE,
                            datasetId = "urn:ngsi-ld:Dataset:45678".toUri()
                        ) to
                            listOf(
                                FullAttributeInstanceResult(
                                    temporalEntityAttribute = UUID.randomUUID(),
                                    payload = buildAttributeInstancePayload(
                                        487.0,
                                        ZonedDateTime.parse("2020-03-25T08:29:17.965206Z"),
                                        "urn:ngsi-ld:Dataset:45678".toUri(),
                                        "urn:ngsi-ld:Instance:45678".toUri()
                                    ),
                                    sub = null
                                ),
                                FullAttributeInstanceResult(
                                    temporalEntityAttribute = UUID.randomUUID(),
                                    payload = buildAttributeInstancePayload(
                                        698.0,
                                        ZonedDateTime.parse("2020-03-25T08:33:17.965206Z"),
                                        "urn:ngsi-ld:Dataset:45678".toUri(),
                                        "urn:ngsi-ld:Instance:45679".toUri()
                                    ),
                                    sub = null
                                )
                            )
                    ),
                    false,
                    false,
                    loadSampleData("expectations/beehive_incoming_multi_instances.jsonld")
                ),
                Arguments.arguments(
                    mapOf(
                        TemporalEntityAttribute(
                            entityId = "urn:ngsi-ld:BeeHive:TESTC".toUri(),
                            types = listOf(BEEHIVE_TYPE),
                            attributeName = "https://ontology.eglobalmark.com/apic#incoming",
                            attributeValueType = TemporalEntityAttribute.AttributeValueType.ANY,
                            datasetId = "urn:ngsi-ld:Dataset:45678".toUri()
                        ) to
                            listOf(
                                FullAttributeInstanceResult(
                                    temporalEntityAttribute = UUID.randomUUID(),
                                    payload = buildAttributeInstancePayload(
                                        "Beehive_incoming_123",
                                        ZonedDateTime.parse("2020-03-25T08:29:17.965206Z"),
                                        "urn:ngsi-ld:Dataset:45678".toUri(),
                                        "urn:ngsi-ld:Instance:45678".toUri()
                                    ),
                                    sub = null
                                ),
                                FullAttributeInstanceResult(
                                    temporalEntityAttribute = UUID.randomUUID(),
                                    payload = buildAttributeInstancePayload(
                                        "Beehive_incoming_124",
                                        ZonedDateTime.parse("2020-03-25T08:33:17.965206Z"),
                                        "urn:ngsi-ld:Dataset:45678".toUri(),
                                        "urn:ngsi-ld:Instance:45679".toUri()
                                    ),
                                    sub = null
                                )
                            )
                    ),
                    false,
                    false,
                    loadSampleData("expectations/beehive_incoming_multi_instances_string_values.jsonld")
                ),
                Arguments.arguments(
                    mapOf(
                        TemporalEntityAttribute(
                            entityId = "urn:ngsi-ld:BeeHive:TESTC".toUri(),
                            types = listOf(BEEHIVE_TYPE),
                            attributeName = "https://ontology.eglobalmark.com/apic#incoming",
                            attributeValueType = TemporalEntityAttribute.AttributeValueType.ANY,
                            datasetId = "urn:ngsi-ld:Dataset:45678".toUri()
                        ) to
                            listOf(
                                FullAttributeInstanceResult(
                                    temporalEntityAttribute = UUID.randomUUID(),
                                    payload = buildAttributeInstancePayload(
                                        "Beehive_incoming_123",
                                        ZonedDateTime.parse("2020-03-25T08:29:17.965206Z"),
                                        "urn:ngsi-ld:Dataset:45678".toUri(),
                                        "urn:ngsi-ld:Instance:45678".toUri()
                                    ),
                                    sub = "sub1"
                                ),
                                FullAttributeInstanceResult(
                                    temporalEntityAttribute = UUID.randomUUID(),
                                    payload = buildAttributeInstancePayload(
                                        "Beehive_incoming_124",
                                        ZonedDateTime.parse("2020-03-25T08:33:17.965206Z"),
                                        "urn:ngsi-ld:Dataset:45678".toUri(),
                                        "urn:ngsi-ld:Instance:45679".toUri()
                                    ),
                                    sub = "sub2"
                                )
                            )
                    ),
                    false,
                    true,
                    loadSampleData("expectations/beehive_incoming_multi_instances_string_values_with_audit.jsonld")
                ),
                Arguments.arguments(
                    mapOf(
                        TemporalEntityAttribute(
                            entityId = "urn:ngsi-ld:BeeHive:TESTC".toUri(),
                            types = listOf(BEEHIVE_TYPE),
                            attributeName = "https://ontology.eglobalmark.com/apic#incoming",
                            attributeValueType = TemporalEntityAttribute.AttributeValueType.ANY
                        ) to
                            listOf(
                                FullAttributeInstanceResult(
                                    temporalEntityAttribute = UUID.randomUUID(),
                                    payload = buildAttributeInstancePayload(
                                        "Beehive_incoming_123",
                                        ZonedDateTime.parse("2020-03-25T08:29:17.965206Z"),
                                        null,
                                        "urn:ngsi-ld:Instance:45678".toUri()
                                    ),
                                    sub = null
                                ),
                                FullAttributeInstanceResult(
                                    temporalEntityAttribute = UUID.randomUUID(),
                                    payload = buildAttributeInstancePayload(
                                        "Beehive_incoming_124",
                                        ZonedDateTime.parse("2020-03-25T08:33:17.965206Z"),
                                        null,
                                        "urn:ngsi-ld:Instance:45679".toUri()
                                    ),
                                    sub = null
                                )
                            )
                    ),
                    false,
                    false,
                    loadSampleData(
                        "expectations/beehive_incoming_multi_instances_without_datasetId_string_values.jsonld"
                    )
                ),
                Arguments.arguments(
                    mapOf(
                        TemporalEntityAttribute(
                            entityId = "urn:ngsi-ld:BeeHive:TESTC".toUri(),
                            types = listOf(BEEHIVE_TYPE),
                            attributeName = "https://ontology.eglobalmark.com/apic#incoming",
                            attributeValueType = TemporalEntityAttribute.AttributeValueType.MEASURE
                        ) to
                            listOf(
                                SimplifiedAttributeInstanceResult(
                                    temporalEntityAttribute = UUID.randomUUID(),
                                    value = 550.0,
                                    time = ZonedDateTime.parse("2020-03-25T08:29:17.965206Z")
                                ),
                                SimplifiedAttributeInstanceResult(
                                    temporalEntityAttribute = UUID.randomUUID(),
                                    value = 650.0,
                                    time = ZonedDateTime.parse("2020-03-25T08:33:17.965206Z")
                                )
                            )
                    ),
                    true,
                    false,
                    loadSampleData(
                        "expectations/beehive_incoming_multi_instances_without_datasetId_temporal_values.jsonld"
                    )
                ),
                Arguments.arguments(
                    mapOf(
                        TemporalEntityAttribute(
                            entityId = "urn:ngsi-ld:BeeHive:TESTC".toUri(),
                            types = listOf(BEEHIVE_TYPE),
                            attributeName = "https://ontology.eglobalmark.com/apic#incoming",
                            attributeValueType = TemporalEntityAttribute.AttributeValueType.MEASURE,
                            datasetId = "urn:ngsi-ld:Dataset:01234".toUri()
                        ) to
                            listOf(
                                SimplifiedAttributeInstanceResult(
                                    temporalEntityAttribute = UUID.randomUUID(),
                                    value = 550.0,
                                    time = ZonedDateTime.parse("2020-03-25T08:29:17.965206Z")
                                ),
                                SimplifiedAttributeInstanceResult(
                                    temporalEntityAttribute = UUID.randomUUID(),
                                    value = 650.0,
                                    time = ZonedDateTime.parse("2020-03-25T08:33:17.965206Z")
                                )
                            ),
                        TemporalEntityAttribute(
                            entityId = "urn:ngsi-ld:BeeHive:TESTC".toUri(),
                            types = listOf(BEEHIVE_TYPE),
                            attributeName = "https://ontology.eglobalmark.com/apic#incoming",
                            attributeValueType = TemporalEntityAttribute.AttributeValueType.MEASURE,
                            datasetId = "urn:ngsi-ld:Dataset:45678".toUri()
                        ) to
                            listOf(
                                SimplifiedAttributeInstanceResult(
                                    temporalEntityAttribute = UUID.randomUUID(),
                                    value = 487.0,
                                    time = ZonedDateTime.parse("2020-03-25T08:29:17.965206Z")
                                ),
                                SimplifiedAttributeInstanceResult(
                                    temporalEntityAttribute = UUID.randomUUID(),
                                    value = 698.0,
                                    time = ZonedDateTime.parse("2020-03-25T08:33:17.965206Z")
                                )
                            )
                    ),
                    true,
                    false,
                    loadSampleData("expectations/beehive_incoming_multi_instances_temporal_values.jsonld")
                ),
                Arguments.arguments(
                    mapOf(
                        TemporalEntityAttribute(
                            entityId = "urn:ngsi-ld:BeeHive:TESTC".toUri(),
                            types = listOf(BEEHIVE_TYPE),
                            attributeName = "https://ontology.eglobalmark.com/apic#incoming",
                            attributeValueType = TemporalEntityAttribute.AttributeValueType.ANY,
                            datasetId = "urn:ngsi-ld:Dataset:45678".toUri()
                        ) to
                            listOf(
                                SimplifiedAttributeInstanceResult(
                                    temporalEntityAttribute = UUID.randomUUID(),
                                    value = "Beehive_incoming_123",
                                    time = ZonedDateTime.parse("2020-03-25T08:29:17.965206Z")
                                ),
                                SimplifiedAttributeInstanceResult(
                                    temporalEntityAttribute = UUID.randomUUID(),
                                    value = "Beehive_incoming_124",
                                    time = ZonedDateTime.parse("2020-03-25T08:33:17.965206Z")
                                )
                            )
                    ),
                    true,
                    false,
                    loadSampleData("expectations/beehive_incoming_multi_instances_string_temporal_values.jsonld")
                ),
                Arguments.arguments(
                    mapOf(
                        TemporalEntityAttribute(
                            entityId = "urn:ngsi-ld:BeeHive:TESTC".toUri(),
                            types = listOf(BEEHIVE_TYPE),
                            attributeName = "https://ontology.eglobalmark.com/apic#incoming",
                            attributeValueType = TemporalEntityAttribute.AttributeValueType.ANY
                        ) to
                            listOf(
                                SimplifiedAttributeInstanceResult(
                                    temporalEntityAttribute = UUID.randomUUID(),
                                    value = "Beehive_incoming_123",
                                    time = ZonedDateTime.parse("2020-03-25T08:29:17.965206Z")
                                ),
                                SimplifiedAttributeInstanceResult(
                                    temporalEntityAttribute = UUID.randomUUID(),
                                    value = "Beehive_incoming_124",
                                    time = ZonedDateTime.parse("2020-03-25T08:33:17.965206Z")
                                )
                            )
                    ),
                    true,
                    false,
                    loadSampleData(
                        "expectations/beehive_incoming_multi_instances_without_datasetId_string_temporal_values.jsonld"
                    )
                ),
                Arguments.arguments(
                    mapOf(
                        TemporalEntityAttribute(
                            entityId = "urn:ngsi-ld:BeeHive:TESTC".toUri(),
                            types = listOf(BEEHIVE_TYPE),
                            attributeName = "https://ontology.eglobalmark.com/apic#incoming",
                            attributeType = TemporalEntityAttribute.AttributeType.Relationship,
                            attributeValueType = TemporalEntityAttribute.AttributeValueType.ANY,
                            datasetId = "urn:ngsi-ld:Dataset:45678".toUri()
                        ) to
                            listOf(
                                SimplifiedAttributeInstanceResult(
                                    temporalEntityAttribute = UUID.randomUUID(),
                                    value = "urn:ngsi-ld:Entity:1234",
                                    time = ZonedDateTime.parse("2020-03-25T08:29:17.965206Z")
                                ),
                                SimplifiedAttributeInstanceResult(
                                    temporalEntityAttribute = UUID.randomUUID(),
                                    value = "urn:ngsi-ld:Entity:5678",
                                    time = ZonedDateTime.parse("2020-03-25T08:33:17.965206Z")
                                )
                            )
                    ),
                    true,
                    false,
                    loadSampleData("expectations/beehive_relationship_multi_instances_temporal_values.jsonld")
                )
            )
        }
    }
}
