package com.egm.stellio.search.util

import com.egm.stellio.search.model.FullAttributeInstanceResult
import com.egm.stellio.search.model.SimplifiedAttributeInstanceResult
import com.egm.stellio.search.model.TemporalEntityAttribute
import com.egm.stellio.shared.util.loadSampleData
import com.egm.stellio.shared.util.toUri
import org.junit.jupiter.params.provider.Arguments
import java.time.ZonedDateTime
import java.util.UUID
import java.util.stream.Stream

@Suppress("unused")
class QueryParameterizedTests {

    companion object {
        private val simplifiedResultOfTwoEntitiesWithOneProperty =
            listOf(
                Pair(
                    "urn:ngsi-ld:BeeHive:TESTC".toUri(),
                    mapOf(
                        TemporalEntityAttribute(
                            entityId = "urn:ngsi-ld:BeeHive:TESTC".toUri(),
                            type = "https://ontology.eglobalmark.com/apic#BeeHive",
                            attributeName = "https://ontology.eglobalmark.com/apic#incoming",
                            attributeValueType = TemporalEntityAttribute.AttributeValueType.MEASURE
                        ) to listOf(
                            SimplifiedAttributeInstanceResult(
                                temporalEntityAttribute = UUID.randomUUID(),
                                value = 20,
                                observedAt = ZonedDateTime.parse("2020-03-25T08:33:17.965206Z")
                            )
                        )
                    )
                ),
                Pair(
                    "urn:ngsi-ld:BeeHive:TESTD".toUri(),
                    mapOf(
                        TemporalEntityAttribute(
                            entityId = "urn:ngsi-ld:BeeHive:TESTD".toUri(),
                            type = "https://ontology.eglobalmark.com/apic#BeeHive",
                            attributeName = "https://ontology.eglobalmark.com/apic#outgoing",
                            attributeValueType = TemporalEntityAttribute.AttributeValueType.MEASURE
                        ) to listOf(
                            SimplifiedAttributeInstanceResult(
                                temporalEntityAttribute = UUID.randomUUID(),
                                value = 25,
                                observedAt = ZonedDateTime.parse("2020-03-25T08:33:17.965206Z")
                            )
                        )
                    )
                )
            )

        private val resultOfTwoEntitiesWithOneProperty =
            listOf(
                Pair(
                    "urn:ngsi-ld:BeeHive:TESTC".toUri(),
                    mapOf(
                        TemporalEntityAttribute(
                            entityId = "urn:ngsi-ld:BeeHive:TESTC".toUri(),
                            type = "https://ontology.eglobalmark.com/apic#BeeHive",
                            attributeName = "https://ontology.eglobalmark.com/apic#incoming",
                            attributeValueType = TemporalEntityAttribute.AttributeValueType.MEASURE
                        ) to listOf(
                            FullAttributeInstanceResult(
                                temporalEntityAttribute = UUID.randomUUID(),
                                payload = buildAttributeInstancePayload(
                                    20,
                                    ZonedDateTime.parse("2020-03-25T08:33:17.965206Z"),
                                    null,
                                    "urn:ngsi-ld:Instance:45678".toUri()
                                )
                            )
                        )
                    )
                ),
                Pair(
                    "urn:ngsi-ld:BeeHive:TESTD".toUri(),
                    mapOf(
                        TemporalEntityAttribute(
                            entityId = "urn:ngsi-ld:BeeHive:TESTD".toUri(),
                            type = "https://ontology.eglobalmark.com/apic#BeeHive",
                            attributeName = "https://ontology.eglobalmark.com/apic#outgoing",
                            attributeValueType = TemporalEntityAttribute.AttributeValueType.MEASURE
                        ) to listOf(
                            FullAttributeInstanceResult(
                                temporalEntityAttribute = UUID.randomUUID(),
                                payload = buildAttributeInstancePayload(
                                    25,
                                    ZonedDateTime.parse("2020-03-25T08:33:17.965206Z"),
                                    null,
                                    "urn:ngsi-ld:Instance:45679".toUri()
                                )
                            )
                        )
                    )
                )
            )

        private val simplifiedResultOfTwoEntitiesWithOnePropertyAndOneRelationship =
            listOf(
                Pair(
                    "urn:ngsi-ld:BeeHive:TESTC".toUri(),
                    mapOf(
                        TemporalEntityAttribute(
                            entityId = "urn:ngsi-ld:BeeHive:TESTC".toUri(),
                            type = "https://ontology.eglobalmark.com/apic#BeeHive",
                            attributeName = "https://ontology.eglobalmark.com/apic#incoming",
                            attributeValueType = TemporalEntityAttribute.AttributeValueType.MEASURE
                        ) to listOf(
                            SimplifiedAttributeInstanceResult(
                                temporalEntityAttribute = UUID.randomUUID(),
                                value = 20,
                                observedAt = ZonedDateTime.parse("2020-03-25T08:33:17.965206Z")
                            )
                        ),
                        TemporalEntityAttribute(
                            entityId = "urn:ngsi-ld:BeeHive:TESTC".toUri(),
                            type = "https://ontology.eglobalmark.com/apic#BeeHive",
                            attributeName = "https://ontology.eglobalmark.com/egm#managedBy",
                            attributeValueType = TemporalEntityAttribute.AttributeValueType.MEASURE,
                            attributeType = TemporalEntityAttribute.AttributeType.Relationship
                        ) to listOf(
                            SimplifiedAttributeInstanceResult(
                                temporalEntityAttribute = UUID.randomUUID(),
                                value = "urn:ngsi-ld:Beekeeper:1234",
                                observedAt = ZonedDateTime.parse("2020-03-25T08:33:17.965206Z")
                            )
                        )
                    )
                ),
                Pair(
                    "urn:ngsi-ld:BeeHive:TESTD".toUri(),
                    mapOf(
                        TemporalEntityAttribute(
                            entityId = "urn:ngsi-ld:BeeHive:TESTD".toUri(),
                            type = "https://ontology.eglobalmark.com/apic#BeeHive",
                            attributeName = "https://ontology.eglobalmark.com/apic#outgoing",
                            attributeValueType = TemporalEntityAttribute.AttributeValueType.MEASURE
                        ) to listOf(
                            SimplifiedAttributeInstanceResult(
                                temporalEntityAttribute = UUID.randomUUID(),
                                value = 25,
                                observedAt = ZonedDateTime.parse("2020-03-25T08:33:17.965206Z")
                            )
                        ),
                        TemporalEntityAttribute(
                            entityId = "urn:ngsi-ld:BeeHive:TESTD".toUri(),
                            type = "https://ontology.eglobalmark.com/apic#BeeHive",
                            attributeName = "https://ontology.eglobalmark.com/egm#managedBy",
                            attributeValueType = TemporalEntityAttribute.AttributeValueType.MEASURE,
                            attributeType = TemporalEntityAttribute.AttributeType.Relationship
                        ) to listOf(
                            SimplifiedAttributeInstanceResult(
                                temporalEntityAttribute = UUID.randomUUID(),
                                value = "urn:ngsi-ld:Beekeeper:5678",
                                observedAt = ZonedDateTime.parse("2020-03-25T08:33:17.965206Z")
                            )
                        )
                    )
                )
            )

        @JvmStatic
        fun rawResultsProvider(): Stream<Arguments> {
            return Stream.of(
                Arguments.arguments(
                    resultOfTwoEntitiesWithOneProperty,
                    false,
                    loadSampleData("expectations/query/two_beehives.json")
                ),
                Arguments.arguments(
                    simplifiedResultOfTwoEntitiesWithOneProperty,
                    true,
                    loadSampleData("expectations/query/two_beehives_temporal_values.json")
                ),
                Arguments.arguments(
                    simplifiedResultOfTwoEntitiesWithOnePropertyAndOneRelationship,
                    true,
                    loadSampleData("expectations/query/two_entities_temporal_values_property_and_relationship.json")
                )
            )
        }
    }
}
