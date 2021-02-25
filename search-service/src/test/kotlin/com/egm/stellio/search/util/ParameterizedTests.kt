package com.egm.stellio.search.util

import com.egm.stellio.search.model.AttributeInstanceResult
import com.egm.stellio.search.model.TemporalEntityAttribute
import com.egm.stellio.shared.util.buildAttributeInstancePayload
import com.egm.stellio.shared.util.loadSampleData
import com.egm.stellio.shared.util.toUri
import org.junit.jupiter.params.provider.Arguments
import java.time.ZonedDateTime
import java.util.stream.Stream

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
                                AttributeInstanceResult(
                                    attributeName = "https://ontology.eglobalmark.com/apic#incoming",
                                    instanceId = "urn:ngsi-ld:Instance:45678".toUri(),
                                    value = 550.0,
                                    datasetId = null,
                                    observedAt = ZonedDateTime.parse("2020-03-25T08:29:17.965206Z"),
                                    payload = buildAttributeInstancePayload(
                                        550.0,
                                        ZonedDateTime.parse("2020-03-25T08:29:17.965206Z"),
                                        null,
                                        "urn:ngsi-ld:Instance:45678".toUri()
                                    )
                                ),
                                AttributeInstanceResult(
                                    attributeName = "https://ontology.eglobalmark.com/apic#incoming",
                                    instanceId = "urn:ngsi-ld:Instance:45679".toUri(),
                                    value = 650.0,
                                    datasetId = null,
                                    observedAt = ZonedDateTime.parse("2020-03-25T08:33:17.965206Z"),
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
                            datasetId = "urn:ngsi-ld:Dataset:01234".toUri(),
                            attributeValueType = TemporalEntityAttribute.AttributeValueType.MEASURE
                        ) to
                            listOf(
                                AttributeInstanceResult(
                                    attributeName = "https://ontology.eglobalmark.com/apic#incoming",
                                    instanceId = "urn:ngsi-ld:Instance:01234".toUri(),
                                    value = 550.0,
                                    datasetId = "urn:ngsi-ld:Dataset:01234".toUri(),
                                    observedAt = ZonedDateTime.parse("2020-03-25T08:29:17.965206Z"),
                                    payload = buildAttributeInstancePayload(
                                        550.0,
                                        ZonedDateTime.parse("2020-03-25T08:29:17.965206Z"),
                                        "urn:ngsi-ld:Dataset:01234".toUri(),
                                        "urn:ngsi-ld:Instance:01234".toUri()
                                    )
                                ),
                                AttributeInstanceResult(
                                    attributeName = "https://ontology.eglobalmark.com/apic#incoming",
                                    instanceId = "urn:ngsi-ld:Instance:01235".toUri(),
                                    value = 650.0,
                                    datasetId = "urn:ngsi-ld:Dataset:01234".toUri(),
                                    observedAt = ZonedDateTime.parse("2020-03-25T08:33:17.965206Z"),
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
                                AttributeInstanceResult(
                                    attributeName = "https://ontology.eglobalmark.com/apic#incoming",
                                    instanceId = "urn:ngsi-ld:Instance:45678".toUri(),
                                    value = 487.0,
                                    datasetId = "urn:ngsi-ld:Dataset:45678".toUri(),
                                    observedAt = ZonedDateTime.parse("2020-03-25T08:29:17.965206Z"),
                                    payload = buildAttributeInstancePayload(
                                        487.0,
                                        ZonedDateTime.parse("2020-03-25T08:29:17.965206Z"),
                                        "urn:ngsi-ld:Dataset:45678".toUri(),
                                        "urn:ngsi-ld:Instance:45678".toUri()
                                    )
                                ),
                                AttributeInstanceResult(
                                    attributeName = "https://ontology.eglobalmark.com/apic#incoming",
                                    instanceId = "urn:ngsi-ld:Instance:45679".toUri(),
                                    value = 698.0,
                                    datasetId = "urn:ngsi-ld:Dataset:45678".toUri(),
                                    observedAt = ZonedDateTime.parse("2020-03-25T08:33:17.965206Z"),
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
                                AttributeInstanceResult(
                                    attributeName = "https://ontology.eglobalmark.com/apic#incoming",
                                    instanceId = "urn:ngsi-ld:Instance:45678".toUri(),
                                    value = "Beehive_incoming_123",
                                    datasetId = "urn:ngsi-ld:Dataset:45678".toUri(),
                                    observedAt = ZonedDateTime.parse("2020-03-25T08:29:17.965206Z"),
                                    payload = buildAttributeInstancePayload(
                                        "Beehive_incoming_123",
                                        ZonedDateTime.parse("2020-03-25T08:29:17.965206Z"),
                                        "urn:ngsi-ld:Dataset:45678".toUri(),
                                        "urn:ngsi-ld:Instance:45678".toUri()
                                    )
                                ),
                                AttributeInstanceResult(
                                    attributeName = "https://ontology.eglobalmark.com/apic#incoming",
                                    instanceId = "urn:ngsi-ld:Instance:45679".toUri(),
                                    value = "Beehive_incoming_124",
                                    datasetId = "urn:ngsi-ld:Dataset:45678".toUri(),
                                    observedAt = ZonedDateTime.parse("2020-03-25T08:33:17.965206Z"),
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
                            attributeValueType = TemporalEntityAttribute.AttributeValueType.MEASURE
                        ) to
                            listOf(
                                AttributeInstanceResult(
                                    attributeName = "https://ontology.eglobalmark.com/apic#incoming",
                                    instanceId = "urn:ngsi-ld:Instance:45678".toUri(),
                                    value = 550.0,
                                    observedAt = ZonedDateTime.parse("2020-03-25T08:29:17.965206Z")
                                ),
                                AttributeInstanceResult(
                                    attributeName = "https://ontology.eglobalmark.com/apic#incoming",
                                    instanceId = "urn:ngsi-ld:Instance:45679".toUri(),
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
                                AttributeInstanceResult(
                                    attributeName = "https://ontology.eglobalmark.com/apic#incoming",
                                    instanceId = "urn:ngsi-ld:Instance:01234".toUri(),
                                    value = 550.0,
                                    datasetId = "urn:ngsi-ld:Dataset:01234".toUri(),
                                    observedAt = ZonedDateTime.parse("2020-03-25T08:29:17.965206Z")
                                ),
                                AttributeInstanceResult(
                                    attributeName = "https://ontology.eglobalmark.com/apic#incoming",
                                    instanceId = "urn:ngsi-ld:Instance:01235".toUri(),
                                    value = 650.0,
                                    datasetId = "urn:ngsi-ld:Dataset:01234".toUri(),
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
                                AttributeInstanceResult(
                                    attributeName = "https://ontology.eglobalmark.com/apic#incoming",
                                    instanceId = "urn:ngsi-ld:Instance:45678".toUri(),
                                    value = 487.0,
                                    datasetId = "urn:ngsi-ld:Dataset:45678".toUri(),
                                    observedAt = ZonedDateTime.parse("2020-03-25T08:29:17.965206Z")
                                ),
                                AttributeInstanceResult(
                                    attributeName = "https://ontology.eglobalmark.com/apic#incoming",
                                    instanceId = "urn:ngsi-ld:Instance:45679".toUri(),
                                    value = 698.0,
                                    datasetId = "urn:ngsi-ld:Dataset:45678".toUri(),
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
                                AttributeInstanceResult(
                                    attributeName = "https://ontology.eglobalmark.com/apic#incoming",
                                    instanceId = "urn:ngsi-ld:Instance:45678".toUri(),
                                    value = "Beehive_incoming_123",
                                    datasetId = "urn:ngsi-ld:Dataset:45678".toUri(),
                                    observedAt = ZonedDateTime.parse("2020-03-25T08:29:17.965206Z")
                                ),
                                AttributeInstanceResult(
                                    attributeName = "https://ontology.eglobalmark.com/apic#incoming",
                                    instanceId = "urn:ngsi-ld:Instance:45679".toUri(),
                                    value = "Beehive_incoming_124",
                                    datasetId = "urn:ngsi-ld:Dataset:45678".toUri(),
                                    observedAt = ZonedDateTime.parse("2020-03-25T08:33:17.965206Z")
                                )
                            )
                    ),
                    true,
                    loadSampleData("expectations/beehive_incoming_multi_instances_string_temporal_values.jsonld")
                )
            )
        }
    }
}
