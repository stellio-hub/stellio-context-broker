package com.egm.stellio.search.util

import com.egm.stellio.search.model.FullAttributeInstanceResult
import com.egm.stellio.search.model.SimplifiedAttributeInstanceResult
import com.egm.stellio.search.model.TemporalEntityAttribute
import com.egm.stellio.shared.util.loadSampleData
import com.egm.stellio.shared.util.toUri
import org.junit.jupiter.params.provider.Arguments
import java.time.ZonedDateTime
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
                            type = "https://ontology.eglobalmark.com/apic#BeeHive",
                            attributeName = "https://ontology.eglobalmark.com/apic#incoming",
                            attributeValueType = TemporalEntityAttribute.AttributeValueType.MEASURE
                        ) to
                            listOf(
                                FullAttributeInstanceResult(
                                    payload = buildAttributeInstancePayload(
                                        550.0,
                                        ZonedDateTime.parse("2020-03-25T08:29:17.965206Z"),
                                        null,
                                        "urn:ngsi-ld:Instance:45678".toUri()
                                    )
                                ),
                                FullAttributeInstanceResult(
                                    payload = buildAttributeInstancePayload(
                                        650.0,
                                        ZonedDateTime.parse("2020-03-25T08:33:17.965206Z"),
                                        null,
                                        "urn:ngsi-ld:Instance:45679".toUri()
                                    )
                                )
                            )
                    ),
                    false,
                    loadSampleData("expectations/beehive_incoming_multi_instances_without_datasetId.jsonld")
                ),
                Arguments.arguments(
                    mapOf(
                        TemporalEntityAttribute(
                            entityId = "urn:ngsi-ld:BeeHive:TESTC".toUri(),
                            type = "https://ontology.eglobalmark.com/apic#BeeHive",
                            attributeName = "https://ontology.eglobalmark.com/apic#incoming",
                            attributeType = TemporalEntityAttribute.AttributeType.Relationship,
                            attributeValueType = TemporalEntityAttribute.AttributeValueType.ANY
                        ) to
                            listOf(
                                FullAttributeInstanceResult(
                                    payload = buildAttributeInstancePayload(
                                        "urn:ngsi-ld:Entity:1234",
                                        ZonedDateTime.parse("2020-03-25T08:29:17.965206Z"),
                                        null,
                                        "urn:ngsi-ld:Instance:45678".toUri(),
                                        TemporalEntityAttribute.AttributeType.Relationship
                                    )
                                ),
                                FullAttributeInstanceResult(
                                    payload = buildAttributeInstancePayload(
                                        "urn:ngsi-ld:Entity:5678",
                                        ZonedDateTime.parse("2020-03-25T08:33:17.965206Z"),
                                        null,
                                        "urn:ngsi-ld:Instance:45679".toUri(),
                                        TemporalEntityAttribute.AttributeType.Relationship
                                    )
                                )
                            )
                    ),
                    false,
                    loadSampleData("expectations/beehive_relationship_multi_instances_without_datasetId.jsonld")
                ),
                Arguments.arguments(
                    mapOf(
                        TemporalEntityAttribute(
                            entityId = "urn:ngsi-ld:BeeHive:TESTC".toUri(),
                            type = "https://ontology.eglobalmark.com/apic#BeeHive",
                            attributeName = "https://ontology.eglobalmark.com/apic#incoming",
                            datasetId = "urn:ngsi-ld:Dataset:01234".toUri(),
                            attributeValueType = TemporalEntityAttribute.AttributeValueType.MEASURE
                        ) to
                            listOf(
                                FullAttributeInstanceResult(
                                    payload = buildAttributeInstancePayload(
                                        550.0,
                                        ZonedDateTime.parse("2020-03-25T08:29:17.965206Z"),
                                        "urn:ngsi-ld:Dataset:01234".toUri(),
                                        "urn:ngsi-ld:Instance:01234".toUri()
                                    )
                                ),
                                FullAttributeInstanceResult(
                                    payload = buildAttributeInstancePayload(
                                        650.0,
                                        ZonedDateTime.parse("2020-03-25T08:33:17.965206Z"),
                                        "urn:ngsi-ld:Dataset:01234".toUri(),
                                        "urn:ngsi-ld:Instance:01235".toUri()
                                    )
                                )
                            ),
                        TemporalEntityAttribute(
                            entityId = "urn:ngsi-ld:BeeHive:TESTC".toUri(),
                            type = "https://ontology.eglobalmark.com/apic#BeeHive",
                            attributeName = "https://ontology.eglobalmark.com/apic#incoming",
                            datasetId = "urn:ngsi-ld:Dataset:45678".toUri(),
                            attributeValueType = TemporalEntityAttribute.AttributeValueType.MEASURE
                        ) to
                            listOf(
                                FullAttributeInstanceResult(
                                    payload = buildAttributeInstancePayload(
                                        487.0,
                                        ZonedDateTime.parse("2020-03-25T08:29:17.965206Z"),
                                        "urn:ngsi-ld:Dataset:45678".toUri(),
                                        "urn:ngsi-ld:Instance:45678".toUri()
                                    )
                                ),
                                FullAttributeInstanceResult(
                                    payload = buildAttributeInstancePayload(
                                        698.0,
                                        ZonedDateTime.parse("2020-03-25T08:33:17.965206Z"),
                                        "urn:ngsi-ld:Dataset:45678".toUri(),
                                        "urn:ngsi-ld:Instance:45679".toUri()
                                    )
                                )
                            )
                    ),
                    false,
                    loadSampleData("expectations/beehive_incoming_multi_instances.jsonld")
                ),
                Arguments.arguments(
                    mapOf(
                        TemporalEntityAttribute(
                            entityId = "urn:ngsi-ld:BeeHive:TESTC".toUri(),
                            type = "https://ontology.eglobalmark.com/apic#BeeHive",
                            attributeName = "https://ontology.eglobalmark.com/apic#incoming",
                            datasetId = "urn:ngsi-ld:Dataset:45678".toUri(),
                            attributeValueType = TemporalEntityAttribute.AttributeValueType.ANY
                        ) to
                            listOf(
                                FullAttributeInstanceResult(
                                    payload = buildAttributeInstancePayload(
                                        "Beehive_incoming_123",
                                        ZonedDateTime.parse("2020-03-25T08:29:17.965206Z"),
                                        "urn:ngsi-ld:Dataset:45678".toUri(),
                                        "urn:ngsi-ld:Instance:45678".toUri()
                                    )
                                ),
                                FullAttributeInstanceResult(
                                    payload = buildAttributeInstancePayload(
                                        "Beehive_incoming_124",
                                        ZonedDateTime.parse("2020-03-25T08:33:17.965206Z"),
                                        "urn:ngsi-ld:Dataset:45678".toUri(),
                                        "urn:ngsi-ld:Instance:45679".toUri()
                                    )
                                )
                            )
                    ),
                    false,
                    loadSampleData("expectations/beehive_incoming_multi_instances_string_values.jsonld")
                ),
                Arguments.arguments(
                    mapOf(
                        TemporalEntityAttribute(
                            entityId = "urn:ngsi-ld:BeeHive:TESTC".toUri(),
                            type = "https://ontology.eglobalmark.com/apic#BeeHive",
                            attributeName = "https://ontology.eglobalmark.com/apic#incoming",
                            attributeValueType = TemporalEntityAttribute.AttributeValueType.ANY
                        ) to
                            listOf(
                                FullAttributeInstanceResult(
                                    payload = buildAttributeInstancePayload(
                                        "Beehive_incoming_123",
                                        ZonedDateTime.parse("2020-03-25T08:29:17.965206Z"),
                                        null,
                                        "urn:ngsi-ld:Instance:45678".toUri()
                                    )
                                ),
                                FullAttributeInstanceResult(
                                    payload = buildAttributeInstancePayload(
                                        "Beehive_incoming_124",
                                        ZonedDateTime.parse("2020-03-25T08:33:17.965206Z"),
                                        null,
                                        "urn:ngsi-ld:Instance:45679".toUri()
                                    )
                                )
                            )
                    ),
                    false,
                    loadSampleData(
                        "expectations/beehive_incoming_multi_instances_without_datasetId_string_values.jsonld"
                    )
                ),
                Arguments.arguments(
                    mapOf(
                        TemporalEntityAttribute(
                            entityId = "urn:ngsi-ld:BeeHive:TESTC".toUri(),
                            type = "https://ontology.eglobalmark.com/apic#BeeHive",
                            attributeName = "https://ontology.eglobalmark.com/apic#incoming",
                            attributeValueType = TemporalEntityAttribute.AttributeValueType.MEASURE
                        ) to
                            listOf(
                                SimplifiedAttributeInstanceResult(
                                    value = 550.0,
                                    observedAt = ZonedDateTime.parse("2020-03-25T08:29:17.965206Z")
                                ),
                                SimplifiedAttributeInstanceResult(
                                    value = 650.0,
                                    observedAt = ZonedDateTime.parse("2020-03-25T08:33:17.965206Z")
                                )
                            )
                    ),
                    true,
                    loadSampleData(
                        "expectations/beehive_incoming_multi_instances_without_datasetId_temporal_values.jsonld"
                    )
                ),
                Arguments.arguments(
                    mapOf(
                        TemporalEntityAttribute(
                            entityId = "urn:ngsi-ld:BeeHive:TESTC".toUri(),
                            type = "https://ontology.eglobalmark.com/apic#BeeHive",
                            attributeName = "https://ontology.eglobalmark.com/apic#incoming",
                            datasetId = "urn:ngsi-ld:Dataset:01234".toUri(),
                            attributeValueType = TemporalEntityAttribute.AttributeValueType.MEASURE
                        ) to
                            listOf(
                                SimplifiedAttributeInstanceResult(
                                    value = 550.0,
                                    observedAt = ZonedDateTime.parse("2020-03-25T08:29:17.965206Z")
                                ),
                                SimplifiedAttributeInstanceResult(
                                    value = 650.0,
                                    observedAt = ZonedDateTime.parse("2020-03-25T08:33:17.965206Z")
                                )
                            ),
                        TemporalEntityAttribute(
                            entityId = "urn:ngsi-ld:BeeHive:TESTC".toUri(),
                            type = "https://ontology.eglobalmark.com/apic#BeeHive",
                            attributeName = "https://ontology.eglobalmark.com/apic#incoming",
                            datasetId = "urn:ngsi-ld:Dataset:45678".toUri(),
                            attributeValueType = TemporalEntityAttribute.AttributeValueType.MEASURE
                        ) to
                            listOf(
                                SimplifiedAttributeInstanceResult(
                                    value = 487.0,
                                    observedAt = ZonedDateTime.parse("2020-03-25T08:29:17.965206Z")
                                ),
                                SimplifiedAttributeInstanceResult(
                                    value = 698.0,
                                    observedAt = ZonedDateTime.parse("2020-03-25T08:33:17.965206Z")
                                )
                            )
                    ),
                    true,
                    loadSampleData("expectations/beehive_incoming_multi_instances_temporal_values.jsonld")
                ),
                Arguments.arguments(
                    mapOf(
                        TemporalEntityAttribute(
                            entityId = "urn:ngsi-ld:BeeHive:TESTC".toUri(),
                            type = "https://ontology.eglobalmark.com/apic#BeeHive",
                            attributeName = "https://ontology.eglobalmark.com/apic#incoming",
                            datasetId = "urn:ngsi-ld:Dataset:45678".toUri(),
                            attributeValueType = TemporalEntityAttribute.AttributeValueType.ANY
                        ) to
                            listOf(
                                SimplifiedAttributeInstanceResult(
                                    value = "Beehive_incoming_123",
                                    observedAt = ZonedDateTime.parse("2020-03-25T08:29:17.965206Z")
                                ),
                                SimplifiedAttributeInstanceResult(
                                    value = "Beehive_incoming_124",
                                    observedAt = ZonedDateTime.parse("2020-03-25T08:33:17.965206Z")
                                )
                            )
                    ),
                    true,
                    loadSampleData("expectations/beehive_incoming_multi_instances_string_temporal_values.jsonld")
                ),
                Arguments.arguments(
                    mapOf(
                        TemporalEntityAttribute(
                            entityId = "urn:ngsi-ld:BeeHive:TESTC".toUri(),
                            type = "https://ontology.eglobalmark.com/apic#BeeHive",
                            attributeName = "https://ontology.eglobalmark.com/apic#incoming",
                            attributeValueType = TemporalEntityAttribute.AttributeValueType.ANY
                        ) to
                            listOf(
                                SimplifiedAttributeInstanceResult(
                                    value = "Beehive_incoming_123",
                                    observedAt = ZonedDateTime.parse("2020-03-25T08:29:17.965206Z")
                                ),
                                SimplifiedAttributeInstanceResult(
                                    value = "Beehive_incoming_124",
                                    observedAt = ZonedDateTime.parse("2020-03-25T08:33:17.965206Z")
                                )
                            )
                    ),
                    true,
                    loadSampleData(
                        "expectations/beehive_incoming_multi_instances_without_datasetId_string_temporal_values.jsonld"
                    )
                ),
                Arguments.arguments(
                    mapOf(
                        TemporalEntityAttribute(
                            entityId = "urn:ngsi-ld:BeeHive:TESTC".toUri(),
                            type = "https://ontology.eglobalmark.com/apic#BeeHive",
                            attributeName = "https://ontology.eglobalmark.com/apic#incoming",
                            datasetId = "urn:ngsi-ld:Dataset:45678".toUri(),
                            attributeValueType = TemporalEntityAttribute.AttributeValueType.ANY,
                            attributeType = TemporalEntityAttribute.AttributeType.Relationship
                        ) to
                            listOf(
                                SimplifiedAttributeInstanceResult(
                                    value = "urn:ngsi-ld:Entity:1234",
                                    observedAt = ZonedDateTime.parse("2020-03-25T08:29:17.965206Z")
                                ),
                                SimplifiedAttributeInstanceResult(
                                    value = "urn:ngsi-ld:Entity:5678",
                                    observedAt = ZonedDateTime.parse("2020-03-25T08:33:17.965206Z")
                                )
                            )
                    ),
                    true,
                    loadSampleData("expectations/beehive_relationship_multi_instances_temporal_values.jsonld")
                )
            )
        }
    }
}
