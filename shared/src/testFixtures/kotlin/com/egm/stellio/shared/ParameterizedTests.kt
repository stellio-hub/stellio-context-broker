package com.egm.stellio.shared

import com.egm.stellio.shared.util.loadSampleData
import org.junit.jupiter.params.provider.Arguments
import java.time.OffsetDateTime
import java.util.stream.Stream

class ParameterizedTests {

    companion object {
        fun rawResultsProvider(): Stream<Arguments> {
            return Stream.of(
                Arguments.arguments(
                    listOf(
                        listOf(
                            mapOf(
                                "attribute_name" to "https://ontology.eglobalmark.com/apic#incoming",
                                "instance_id" to "urn:ngsi-ld:Instance:01234",
                                "value" to 550.0,
                                "dataset_id" to "urn:ngsi-ld:Dataset:01234",
                                "observed_at" to OffsetDateTime.parse("2020-03-25T10:29:17.965206+02:00")
                            ),
                            mapOf(
                                "attribute_name" to "https://ontology.eglobalmark.com/apic#incoming",
                                "instance_id" to "urn:ngsi-ld:Instance:01235",
                                "value" to 650.0,
                                "dataset_id" to "urn:ngsi-ld:Dataset:01234",
                                "observed_at" to OffsetDateTime.parse("2020-03-25T10:33:17.965206+02:00")
                            )
                        ),
                        listOf(
                            mapOf(
                                "attribute_name" to "https://ontology.eglobalmark.com/apic#incoming",
                                "instance_id" to "urn:ngsi-ld:Instance:45678",
                                "value" to 487.0,
                                "dataset_id" to "urn:ngsi-ld:Dataset:45678",
                                "observed_at" to OffsetDateTime.parse("2020-03-25T10:29:17.965206+02:00")
                            ),
                            mapOf(
                                "attribute_name" to "https://ontology.eglobalmark.com/apic#incoming",
                                "instance_id" to "urn:ngsi-ld:Instance:45679",
                                "value" to 698.0,
                                "dataset_id" to "urn:ngsi-ld:Dataset:45678",
                                "observed_at" to OffsetDateTime.parse("2020-03-25T10:33:17.965206+02:00")
                            )
                        )
                    ),
                    loadSampleData("expectations/beehive_with_multi_instance_incoming.jsonld")
                ),
                Arguments.arguments(
                    listOf(
                        listOf(
                            mapOf(
                                "attribute_name" to "https://ontology.eglobalmark.com/apic#incoming",
                                "instance_id" to "urn:ngsi-ld:Instance:45678",
                                "value" to "Beehive_incoming_123",
                                "dataset_id" to "urn:ngsi-ld:Dataset:45678",
                                "observed_at" to OffsetDateTime.parse("2020-03-25T10:29:17.965206+02:00")
                            ),
                            mapOf(
                                "attribute_name" to "https://ontology.eglobalmark.com/apic#incoming",
                                "instance_id" to "urn:ngsi-ld:Instance:45679",
                                "value" to "Beehive_incoming_124",
                                "dataset_id" to "urn:ngsi-ld:Dataset:45678",
                                "observed_at" to OffsetDateTime.parse("2020-03-25T10:33:17.965206+02:00")
                            )
                        )
                    ),
                    loadSampleData("expectations/beehive_with_multi_instance_incoming_string_values.jsonld")
                )
            )
        }
    }
}