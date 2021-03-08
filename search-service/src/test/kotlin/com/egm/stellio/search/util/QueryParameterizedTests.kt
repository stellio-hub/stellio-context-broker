package com.egm.stellio.search.util

import com.egm.stellio.search.model.FullAttributeInstanceResult
import com.egm.stellio.search.model.SimplifiedAttributeInstanceResult
import com.egm.stellio.search.model.TemporalEntityAttribute
import com.egm.stellio.shared.util.toUri
import org.junit.jupiter.params.provider.Arguments
import java.time.ZonedDateTime
import java.util.stream.Stream

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

        @JvmStatic
        fun rawResultsProvider(): Stream<Arguments> {
            return Stream.of(
                Arguments.arguments(
                    resultOfTwoEntitiesWithOneProperty,
                    false,
                    "expectations/query/two_beehives.json"
                ),
                Arguments.arguments(
                    simplifiedResultOfTwoEntitiesWithOneProperty,
                    true,
                    "expectations/query/two_beehives_temporal_values.json"
                )
            )
        }
    }
}
