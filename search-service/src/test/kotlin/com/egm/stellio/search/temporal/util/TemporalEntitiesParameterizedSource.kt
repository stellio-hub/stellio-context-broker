package com.egm.stellio.search.temporal.util

import com.egm.stellio.search.entity.model.Attribute
import com.egm.stellio.search.entity.model.Entity
import com.egm.stellio.search.support.EMPTY_JSON_PAYLOAD
import com.egm.stellio.search.support.buildAttributeInstancePayload
import com.egm.stellio.search.temporal.model.EntityTemporalResult
import com.egm.stellio.search.temporal.model.FullAttributeInstanceResult
import com.egm.stellio.search.temporal.model.SimplifiedAttributeInstanceResult
import com.egm.stellio.shared.model.NGSILD_OBSERVED_AT_TERM
import com.egm.stellio.shared.util.BEEHIVE_IRI
import com.egm.stellio.shared.util.loadSampleData
import com.egm.stellio.shared.util.toUri
import org.junit.jupiter.params.provider.Arguments
import java.time.Instant
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.util.UUID
import java.util.stream.Stream

@Suppress("unused", "UtilityClassWithPublicConstructor")
class TemporalEntitiesParameterizedSource {

    companion object {
        private val now = Instant.now().atZone(ZoneOffset.UTC)

        private val simplifiedResultOfTwoEntitiesWithOneProperty =
            listOf(
                EntityTemporalResult(
                    Entity(
                        entityId = "urn:ngsi-ld:BeeHive:TESTC".toUri(),
                        types = listOf(BEEHIVE_IRI),
                        createdAt = now,
                        payload = EMPTY_JSON_PAYLOAD
                    ),
                    emptyList(),
                    mapOf(
                        Attribute(
                            entityId = "urn:ngsi-ld:BeeHive:TESTC".toUri(),
                            attributeName = "https://ontology.eglobalmark.com/apic#incoming",
                            attributeValueType = Attribute.AttributeValueType.NUMBER,
                            createdAt = now,
                            payload = EMPTY_JSON_PAYLOAD
                        ) to listOf(
                            SimplifiedAttributeInstanceResult(
                                attributeUuid = UUID.randomUUID(),
                                value = 20,
                                time = ZonedDateTime.parse("2020-03-25T08:33:17.965206Z")
                            )
                        )
                    )
                ),
                EntityTemporalResult(
                    Entity(
                        entityId = "urn:ngsi-ld:BeeHive:TESTD".toUri(),
                        types = listOf(BEEHIVE_IRI),
                        createdAt = now,
                        payload = EMPTY_JSON_PAYLOAD
                    ),
                    emptyList(),
                    mapOf(
                        Attribute(
                            entityId = "urn:ngsi-ld:BeeHive:TESTD".toUri(),
                            attributeName = "https://ontology.eglobalmark.com/apic#outgoing",
                            attributeValueType = Attribute.AttributeValueType.NUMBER,
                            createdAt = now,
                            payload = EMPTY_JSON_PAYLOAD
                        ) to listOf(
                            SimplifiedAttributeInstanceResult(
                                attributeUuid = UUID.randomUUID(),
                                value = 25,
                                time = ZonedDateTime.parse("2020-03-25T08:33:17.965206Z")
                            )
                        )
                    )
                )
            )

        private val resultOfTwoEntitiesWithOneProperty =
            listOf(
                EntityTemporalResult(
                    Entity(
                        entityId = "urn:ngsi-ld:BeeHive:TESTC".toUri(),
                        types = listOf(BEEHIVE_IRI),
                        createdAt = now,
                        payload = EMPTY_JSON_PAYLOAD
                    ),
                    emptyList(),
                    mapOf(
                        Attribute(
                            entityId = "urn:ngsi-ld:BeeHive:TESTC".toUri(),
                            attributeName = "https://ontology.eglobalmark.com/apic#incoming",
                            attributeValueType = Attribute.AttributeValueType.NUMBER,
                            createdAt = now,
                            payload = EMPTY_JSON_PAYLOAD
                        ) to listOf(
                            FullAttributeInstanceResult(
                                attributeUuid = UUID.randomUUID(),
                                payload = buildAttributeInstancePayload(
                                    20,
                                    ZonedDateTime.parse("2020-03-25T08:33:17.965206Z"),
                                    null,
                                    "urn:ngsi-ld:Instance:45678".toUri()
                                ),
                                time = ZonedDateTime.parse("2020-03-25T08:33:17.965206Z"),
                                timeproperty = NGSILD_OBSERVED_AT_TERM,
                                sub = "sub"
                            )
                        )
                    )
                ),
                EntityTemporalResult(
                    Entity(
                        entityId = "urn:ngsi-ld:BeeHive:TESTD".toUri(),
                        types = listOf(BEEHIVE_IRI),
                        createdAt = now,
                        payload = EMPTY_JSON_PAYLOAD
                    ),
                    emptyList(),
                    mapOf(
                        Attribute(
                            entityId = "urn:ngsi-ld:BeeHive:TESTD".toUri(),
                            attributeName = "https://ontology.eglobalmark.com/apic#outgoing",
                            attributeValueType = Attribute.AttributeValueType.NUMBER,
                            createdAt = now,
                            payload = EMPTY_JSON_PAYLOAD
                        ) to listOf(
                            FullAttributeInstanceResult(
                                attributeUuid = UUID.randomUUID(),
                                payload = buildAttributeInstancePayload(
                                    25,
                                    ZonedDateTime.parse("2020-03-25T08:33:17.965206Z"),
                                    null,
                                    "urn:ngsi-ld:Instance:45679".toUri()
                                ),
                                time = ZonedDateTime.parse("2020-03-25T08:33:17.965206Z"),
                                timeproperty = NGSILD_OBSERVED_AT_TERM,
                                sub = null
                            )
                        )
                    )
                )
            )

        private val simplifiedResultOfTwoEntitiesWithOnePropertyAndOneRelationship =
            listOf(
                EntityTemporalResult(
                    Entity(
                        entityId = "urn:ngsi-ld:BeeHive:TESTC".toUri(),
                        types = listOf(BEEHIVE_IRI),
                        createdAt = now,
                        payload = EMPTY_JSON_PAYLOAD
                    ),
                    emptyList(),
                    mapOf(
                        Attribute(
                            entityId = "urn:ngsi-ld:BeeHive:TESTC".toUri(),
                            attributeName = "https://ontology.eglobalmark.com/apic#incoming",
                            attributeValueType = Attribute.AttributeValueType.NUMBER,
                            createdAt = now,
                            payload = EMPTY_JSON_PAYLOAD
                        ) to listOf(
                            SimplifiedAttributeInstanceResult(
                                attributeUuid = UUID.randomUUID(),
                                value = 20,
                                time = ZonedDateTime.parse("2020-03-25T08:33:17.965206Z")
                            )
                        ),
                        Attribute(
                            entityId = "urn:ngsi-ld:BeeHive:TESTC".toUri(),
                            attributeName = "https://ontology.eglobalmark.com/egm#managedBy",
                            attributeType = Attribute.AttributeType.Relationship,
                            attributeValueType = Attribute.AttributeValueType.NUMBER,
                            createdAt = now,
                            payload = EMPTY_JSON_PAYLOAD
                        ) to listOf(
                            SimplifiedAttributeInstanceResult(
                                attributeUuid = UUID.randomUUID(),
                                value = "urn:ngsi-ld:Beekeeper:1234",
                                time = ZonedDateTime.parse("2020-03-25T08:33:17.965206Z")
                            )
                        )
                    )
                ),
                EntityTemporalResult(
                    Entity(
                        entityId = "urn:ngsi-ld:BeeHive:TESTD".toUri(),
                        types = listOf(BEEHIVE_IRI),
                        createdAt = now,
                        payload = EMPTY_JSON_PAYLOAD
                    ),
                    emptyList(),
                    mapOf(
                        Attribute(
                            entityId = "urn:ngsi-ld:BeeHive:TESTD".toUri(),
                            attributeName = "https://ontology.eglobalmark.com/apic#outgoing",
                            attributeValueType = Attribute.AttributeValueType.NUMBER,
                            createdAt = now,
                            payload = EMPTY_JSON_PAYLOAD
                        ) to listOf(
                            SimplifiedAttributeInstanceResult(
                                attributeUuid = UUID.randomUUID(),
                                value = 25,
                                time = ZonedDateTime.parse("2020-03-25T08:33:17.965206Z")
                            )
                        ),
                        Attribute(
                            entityId = "urn:ngsi-ld:BeeHive:TESTD".toUri(),
                            attributeName = "https://ontology.eglobalmark.com/egm#managedBy",
                            attributeType = Attribute.AttributeType.Relationship,
                            attributeValueType = Attribute.AttributeValueType.NUMBER,
                            createdAt = now,
                            payload = EMPTY_JSON_PAYLOAD
                        ) to listOf(
                            SimplifiedAttributeInstanceResult(
                                attributeUuid = UUID.randomUUID(),
                                value = "urn:ngsi-ld:Beekeeper:5678",
                                time = ZonedDateTime.parse("2020-03-25T08:33:17.965206Z")
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
                    TemporalRepresentation.NORMALIZED,
                    true,
                    loadSampleData("expectations/query/two_beehives.json")
                ),
                Arguments.arguments(
                    simplifiedResultOfTwoEntitiesWithOneProperty,
                    TemporalRepresentation.TEMPORAL_VALUES,
                    false,
                    loadSampleData("expectations/query/two_beehives_temporal_values.json")
                ),
                Arguments.arguments(
                    simplifiedResultOfTwoEntitiesWithOnePropertyAndOneRelationship,
                    TemporalRepresentation.TEMPORAL_VALUES,
                    false,
                    loadSampleData("expectations/query/two_entities_temporal_values_property_and_relationship.json")
                )
            )
        }
    }
}
