package com.egm.stellio.search.util

import com.egm.stellio.search.model.AttributeInstanceResult
import com.egm.stellio.shared.util.loadAndParseSampleData
import com.egm.stellio.shared.util.loadSampleData
import org.junit.jupiter.params.provider.Arguments
import java.net.URI
import java.time.ZonedDateTime
import java.util.stream.Stream

class ParameterizedTests {

    companion object {
        @JvmStatic
        fun rawResultsProvider(): Stream<Arguments> {
            return Stream.of(
                Arguments.arguments(
                    loadAndParseSampleData("beehive_multi_instance_property_with_dafault_instance.jsonld"),
                    listOf(
                        listOf(
                            AttributeInstanceResult(
                                attributeName = "https://ontology.eglobalmark.com/apic#incoming",
                                instanceId = URI.create("urn:ngsi-ld:Instance:45678"),
                                value = 550.0,
                                datasetId = null,
                                observedAt = ZonedDateTime.parse("2020-03-25T08:29:17.965206Z")
                            ),
                            AttributeInstanceResult(
                                attributeName = "https://ontology.eglobalmark.com/apic#incoming",
                                instanceId = URI.create("urn:ngsi-ld:Instance:45679"),
                                value = 650.0,
                                datasetId = null,
                                observedAt = ZonedDateTime.parse("2020-03-25T08:33:17.965206Z")
                            )
                        )
                    ),
                    false,
                    loadSampleData("expectations/beehive_incoming_multi_instances_without_datasetId.jsonld")
                ),
                Arguments.arguments(
                    loadAndParseSampleData("beehive_multi_instance_property.jsonld"),
                    listOf(
                        listOf(
                            AttributeInstanceResult(
                                attributeName = "https://ontology.eglobalmark.com/apic#incoming",
                                instanceId = URI.create("urn:ngsi-ld:Instance:01234"),
                                value = 550.0,
                                datasetId = URI.create("urn:ngsi-ld:Dataset:01234"),
                                observedAt = ZonedDateTime.parse("2020-03-25T08:29:17.965206Z")
                            ),
                            AttributeInstanceResult(
                                attributeName = "https://ontology.eglobalmark.com/apic#incoming",
                                instanceId = URI.create("urn:ngsi-ld:Instance:01235"),
                                value = 650.0,
                                datasetId = URI.create("urn:ngsi-ld:Dataset:01234"),
                                observedAt = ZonedDateTime.parse("2020-03-25T08:33:17.965206Z")
                            )
                        ),
                        listOf(
                            AttributeInstanceResult(
                                attributeName = "https://ontology.eglobalmark.com/apic#incoming",
                                instanceId = URI.create("urn:ngsi-ld:Instance:45678"),
                                value = 487.0,
                                datasetId = URI.create("urn:ngsi-ld:Dataset:45678"),
                                observedAt = ZonedDateTime.parse("2020-03-25T08:29:17.965206Z")
                            ),
                            AttributeInstanceResult(
                                attributeName = "https://ontology.eglobalmark.com/apic#incoming",
                                instanceId = URI.create("urn:ngsi-ld:Instance:45679"),
                                value = 698.0,
                                datasetId = URI.create("urn:ngsi-ld:Dataset:45678"),
                                observedAt = ZonedDateTime.parse("2020-03-25T08:33:17.965206Z")
                            )
                        )
                    ),
                    false,
                    loadSampleData("expectations/beehive_incoming_multi_instances.jsonld")
                ),
                Arguments.arguments(
                    loadAndParseSampleData("beehive_multi_instance_property.jsonld"),
                    listOf(
                        listOf(
                            AttributeInstanceResult(
                                attributeName = "https://ontology.eglobalmark.com/apic#incoming",
                                instanceId = URI.create("urn:ngsi-ld:Instance:45678"),
                                value = "Beehive_incoming_123",
                                datasetId = URI.create("urn:ngsi-ld:Dataset:45678"),
                                observedAt = ZonedDateTime.parse("2020-03-25T08:29:17.965206Z")
                            ),
                            AttributeInstanceResult(
                                attributeName = "https://ontology.eglobalmark.com/apic#incoming",
                                instanceId = URI.create("urn:ngsi-ld:Instance:45679"),
                                value = "Beehive_incoming_124",
                                datasetId = URI.create("urn:ngsi-ld:Dataset:45678"),
                                observedAt = ZonedDateTime.parse("2020-03-25T08:33:17.965206Z")
                            )
                        )
                    ),
                    false,
                    loadSampleData("expectations/beehive_incoming_multi_instances_string_values.jsonld")
                ),
                Arguments.arguments(
                    loadAndParseSampleData("beehive_multi_instance_property_with_dafault_instance.jsonld"),
                    listOf(
                        listOf(
                            AttributeInstanceResult(
                                attributeName = "https://ontology.eglobalmark.com/apic#incoming",
                                instanceId = URI.create("urn:ngsi-ld:Instance:45678"),
                                value = 550.0,
                                datasetId = null,
                                observedAt = ZonedDateTime.parse("2020-03-25T08:29:17.965206Z")
                            ),
                            AttributeInstanceResult(
                                attributeName = "https://ontology.eglobalmark.com/apic#incoming",
                                instanceId = URI.create("urn:ngsi-ld:Instance:45679"),
                                value = 650.0,
                                datasetId = null,
                                observedAt = ZonedDateTime.parse("2020-03-25T08:33:17.965206Z")
                            )
                        )
                    ),
                    true,
                    loadSampleData("expectations/beehive_incoming_multi_instances_without_datasetId_temporal_values.jsonld")
                ),
                Arguments.arguments(
                    loadAndParseSampleData("beehive_multi_instance_property.jsonld"),
                    listOf(
                        listOf(
                            AttributeInstanceResult(
                                attributeName = "https://ontology.eglobalmark.com/apic#incoming",
                                instanceId = URI.create("urn:ngsi-ld:Instance:01234"),
                                value = 550.0,
                                datasetId = URI.create("urn:ngsi-ld:Dataset:01234"),
                                observedAt = ZonedDateTime.parse("2020-03-25T08:29:17.965206Z")
                            ),
                            AttributeInstanceResult(
                                attributeName = "https://ontology.eglobalmark.com/apic#incoming",
                                instanceId = URI.create("urn:ngsi-ld:Instance:01235"),
                                value = 650.0,
                                datasetId = URI.create("urn:ngsi-ld:Dataset:01234"),
                                observedAt = ZonedDateTime.parse("2020-03-25T08:33:17.965206Z")
                            )
                        ),
                        listOf(
                            AttributeInstanceResult(
                                attributeName = "https://ontology.eglobalmark.com/apic#incoming",
                                instanceId = URI.create("urn:ngsi-ld:Instance:45678"),
                                value = 487.0,
                                datasetId = URI.create("urn:ngsi-ld:Dataset:45678"),
                                observedAt = ZonedDateTime.parse("2020-03-25T08:29:17.965206Z")
                            ),
                            AttributeInstanceResult(
                                attributeName = "https://ontology.eglobalmark.com/apic#incoming",
                                instanceId = URI.create("urn:ngsi-ld:Instance:45679"),
                                value = 698.0,
                                datasetId = URI.create("urn:ngsi-ld:Dataset:45678"),
                                observedAt = ZonedDateTime.parse("2020-03-25T08:33:17.965206Z")
                            )
                        )
                    ),
                    true,
                    loadSampleData("expectations/beehive_incoming_multi_instances_temporal_values.jsonld")
                ),
                Arguments.arguments(
                    loadAndParseSampleData("beehive_multi_instance_property.jsonld"),
                    listOf(
                        listOf(
                            AttributeInstanceResult(
                                attributeName = "https://ontology.eglobalmark.com/apic#incoming",
                                instanceId = URI.create("urn:ngsi-ld:Instance:45678"),
                                value = "Beehive_incoming_123",
                                datasetId = URI.create("urn:ngsi-ld:Dataset:45678"),
                                observedAt = ZonedDateTime.parse("2020-03-25T08:29:17.965206Z")
                            ),
                            AttributeInstanceResult(
                                attributeName = "https://ontology.eglobalmark.com/apic#incoming",
                                instanceId = URI.create("urn:ngsi-ld:Instance:45679"),
                                value = "Beehive_incoming_124",
                                datasetId = URI.create("urn:ngsi-ld:Dataset:45678"),
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