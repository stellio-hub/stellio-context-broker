package com.egm.stellio.search.temporal.util

import com.egm.stellio.search.entity.model.Attribute
import com.egm.stellio.search.scope.FullScopeInstanceResult
import com.egm.stellio.search.scope.ScopeInstanceResult
import com.egm.stellio.search.scope.SimplifiedScopeInstanceResult
import com.egm.stellio.search.support.EMPTY_JSON_PAYLOAD
import com.egm.stellio.search.support.SAMPLE_JSON_PROPERTY_PAYLOAD
import com.egm.stellio.search.support.SAMPLE_LANGUAGE_PROPERTY_PAYLOAD
import com.egm.stellio.search.support.SAMPLE_VOCAB_PROPERTY_PAYLOAD
import com.egm.stellio.search.support.buildAttributeInstancePayload
import com.egm.stellio.search.temporal.model.AttributeInstance
import com.egm.stellio.search.temporal.model.AttributeInstanceResult
import com.egm.stellio.search.temporal.model.FullAttributeInstanceResult
import com.egm.stellio.search.temporal.model.SimplifiedAttributeInstanceResult
import com.egm.stellio.shared.util.JsonLdUtils
import com.egm.stellio.shared.util.loadSampleData
import com.egm.stellio.shared.util.toUri
import org.junit.jupiter.params.provider.Arguments
import java.time.Instant
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.util.UUID
import java.util.stream.Stream

@Suppress("unused", "UtilityClassWithPublicConstructor")
class TemporalEntityParameterizedSource {

    companion object {
        private val now = Instant.now().atZone(ZoneOffset.UTC)

        private val entityId = "urn:ngsi-ld:BeeHive:TESTC".toUri()

        private val beehivePropertyMultiInstancesWithoutDatasetId =
            Arguments.arguments(
                emptyList<ScopeInstanceResult>(),
                mapOf(
                    Attribute(
                        entityId = entityId,
                        attributeName = "https://ontology.eglobalmark.com/apic#incoming",
                        attributeValueType = Attribute.AttributeValueType.NUMBER,
                        createdAt = now,
                        payload = EMPTY_JSON_PAYLOAD
                    ) to
                        listOf(
                            FullAttributeInstanceResult(
                                attributeUuid = UUID.randomUUID(),
                                payload = buildAttributeInstancePayload(
                                    550.0,
                                    ZonedDateTime.parse("2020-03-25T08:29:17.965206Z"),
                                    null,
                                    "urn:ngsi-ld:Instance:45678".toUri()
                                ),
                                time = ZonedDateTime.parse("2020-03-25T08:29:17.965206Z"),
                                timeproperty = JsonLdUtils.NGSILD_OBSERVED_AT_TERM,
                                sub = null
                            ),
                            FullAttributeInstanceResult(
                                attributeUuid = UUID.randomUUID(),
                                payload = buildAttributeInstancePayload(
                                    650.0,
                                    ZonedDateTime.parse("2020-03-25T08:33:17.965206Z"),
                                    null,
                                    "urn:ngsi-ld:Instance:45679".toUri()
                                ),
                                time = ZonedDateTime.parse("2020-03-25T08:33:17.965206Z"),
                                timeproperty = JsonLdUtils.NGSILD_OBSERVED_AT_TERM,
                                sub = null
                            )
                        )
                ),
                TemporalRepresentation.NORMALIZED,
                false,
                loadSampleData("expectations/beehive_incoming_multi_instances_without_datasetId.jsonld")
            )

        private val beehiveRelationshipMultiInstancesWithoutDatasetId =
            Arguments.arguments(
                emptyList<ScopeInstanceResult>(),
                mapOf(
                    Attribute(
                        entityId = entityId,
                        attributeName = "https://ontology.eglobalmark.com/apic#incoming",
                        attributeType = Attribute.AttributeType.Relationship,
                        attributeValueType = Attribute.AttributeValueType.STRING,
                        createdAt = now,
                        payload = EMPTY_JSON_PAYLOAD
                    ) to
                        listOf(
                            FullAttributeInstanceResult(
                                attributeUuid = UUID.randomUUID(),
                                payload = buildAttributeInstancePayload(
                                    "urn:ngsi-ld:Entity:1234",
                                    ZonedDateTime.parse("2020-03-25T08:29:17.965206Z"),
                                    null,
                                    "urn:ngsi-ld:Instance:45678".toUri(),
                                    Attribute.AttributeType.Relationship
                                ),
                                time = ZonedDateTime.parse("2020-03-25T08:29:17.965206Z"),
                                timeproperty = JsonLdUtils.NGSILD_OBSERVED_AT_TERM,
                                sub = null
                            ),
                            FullAttributeInstanceResult(
                                attributeUuid = UUID.randomUUID(),
                                payload = buildAttributeInstancePayload(
                                    "urn:ngsi-ld:Entity:5678",
                                    ZonedDateTime.parse("2020-03-25T08:33:17.965206Z"),
                                    null,
                                    "urn:ngsi-ld:Instance:45679".toUri(),
                                    Attribute.AttributeType.Relationship
                                ),
                                time = ZonedDateTime.parse("2020-03-25T08:33:17.965206Z"),
                                timeproperty = JsonLdUtils.NGSILD_OBSERVED_AT_TERM,
                                sub = null
                            )
                        )
                ),
                TemporalRepresentation.NORMALIZED,
                false,
                loadSampleData("expectations/beehive_relationship_multi_instances_without_datasetId.jsonld")
            )

        private val beehivePropertyMultiInstances =
            Arguments.arguments(
                emptyList<ScopeInstanceResult>(),
                mapOf(
                    Attribute(
                        entityId = entityId,
                        attributeName = "https://ontology.eglobalmark.com/apic#incoming",
                        attributeValueType = Attribute.AttributeValueType.NUMBER,
                        datasetId = "urn:ngsi-ld:Dataset:01234".toUri(),
                        createdAt = now,
                        payload = EMPTY_JSON_PAYLOAD
                    ) to
                        listOf(
                            FullAttributeInstanceResult(
                                attributeUuid = UUID.randomUUID(),
                                payload = buildAttributeInstancePayload(
                                    550.0,
                                    ZonedDateTime.parse("2020-03-25T08:29:17.965206Z"),
                                    "urn:ngsi-ld:Dataset:01234".toUri(),
                                    "urn:ngsi-ld:Instance:01234".toUri()
                                ),
                                time = ZonedDateTime.parse("2020-03-25T08:29:17.965206Z"),
                                timeproperty = JsonLdUtils.NGSILD_OBSERVED_AT_TERM,
                                sub = null
                            ),
                            FullAttributeInstanceResult(
                                attributeUuid = UUID.randomUUID(),
                                payload = buildAttributeInstancePayload(
                                    650.0,
                                    ZonedDateTime.parse("2020-03-25T08:33:17.965206Z"),
                                    "urn:ngsi-ld:Dataset:01234".toUri(),
                                    "urn:ngsi-ld:Instance:01235".toUri()
                                ),
                                time = ZonedDateTime.parse("2020-03-25T08:33:17.965206Z"),
                                timeproperty = JsonLdUtils.NGSILD_OBSERVED_AT_TERM,
                                sub = null
                            )
                        ),
                    Attribute(
                        entityId = entityId,
                        attributeName = "https://ontology.eglobalmark.com/apic#incoming",
                        attributeValueType = Attribute.AttributeValueType.NUMBER,
                        datasetId = "urn:ngsi-ld:Dataset:45678".toUri(),
                        createdAt = now,
                        payload = EMPTY_JSON_PAYLOAD
                    ) to
                        listOf(
                            FullAttributeInstanceResult(
                                attributeUuid = UUID.randomUUID(),
                                payload = buildAttributeInstancePayload(
                                    487.0,
                                    ZonedDateTime.parse("2020-03-25T08:29:17.965206Z"),
                                    "urn:ngsi-ld:Dataset:45678".toUri(),
                                    "urn:ngsi-ld:Instance:45678".toUri()
                                ),
                                time = ZonedDateTime.parse("2020-03-25T08:29:17.965206Z"),
                                timeproperty = JsonLdUtils.NGSILD_OBSERVED_AT_TERM,
                                sub = null
                            ),
                            FullAttributeInstanceResult(
                                attributeUuid = UUID.randomUUID(),
                                payload = buildAttributeInstancePayload(
                                    698.0,
                                    ZonedDateTime.parse("2020-03-25T08:33:17.965206Z"),
                                    "urn:ngsi-ld:Dataset:45678".toUri(),
                                    "urn:ngsi-ld:Instance:45679".toUri()
                                ),
                                time = ZonedDateTime.parse("2020-03-25T08:33:17.965206Z"),
                                timeproperty = JsonLdUtils.NGSILD_OBSERVED_AT_TERM,
                                sub = null
                            )
                        )
                ),
                TemporalRepresentation.NORMALIZED,
                false,
                loadSampleData("expectations/beehive_incoming_multi_instances.jsonld")
            )

        private val beehivePropertyMultiInstancesStringValues =
            Arguments.arguments(
                emptyList<ScopeInstanceResult>(),
                mapOf(
                    Attribute(
                        entityId = entityId,
                        attributeName = "https://ontology.eglobalmark.com/apic#incoming",
                        attributeValueType = Attribute.AttributeValueType.STRING,
                        datasetId = "urn:ngsi-ld:Dataset:45678".toUri(),
                        createdAt = now,
                        payload = EMPTY_JSON_PAYLOAD
                    ) to
                        listOf(
                            FullAttributeInstanceResult(
                                attributeUuid = UUID.randomUUID(),
                                payload = buildAttributeInstancePayload(
                                    "Beehive_incoming_123",
                                    ZonedDateTime.parse("2020-03-25T08:29:17.965206Z"),
                                    "urn:ngsi-ld:Dataset:45678".toUri(),
                                    "urn:ngsi-ld:Instance:45678".toUri()
                                ),
                                time = ZonedDateTime.parse("2020-03-25T08:29:17.965206Z"),
                                timeproperty = JsonLdUtils.NGSILD_OBSERVED_AT_TERM,
                                sub = null
                            ),
                            FullAttributeInstanceResult(
                                attributeUuid = UUID.randomUUID(),
                                payload = buildAttributeInstancePayload(
                                    "Beehive_incoming_124",
                                    ZonedDateTime.parse("2020-03-25T08:33:17.965206Z"),
                                    "urn:ngsi-ld:Dataset:45678".toUri(),
                                    "urn:ngsi-ld:Instance:45679".toUri()
                                ),
                                time = ZonedDateTime.parse("2020-03-25T08:33:17.965206Z"),
                                timeproperty = JsonLdUtils.NGSILD_OBSERVED_AT_TERM,
                                sub = null
                            )
                        )
                ),
                TemporalRepresentation.NORMALIZED,
                false,
                loadSampleData("expectations/beehive_incoming_multi_instances_string_values.jsonld")
            )

        private val beehivePropertyMultiInstancesStringValuesWithAudit =
            Arguments.arguments(
                emptyList<ScopeInstanceResult>(),
                mapOf(
                    Attribute(
                        entityId = entityId,
                        attributeName = "https://ontology.eglobalmark.com/apic#incoming",
                        attributeValueType = Attribute.AttributeValueType.STRING,
                        datasetId = "urn:ngsi-ld:Dataset:45678".toUri(),
                        createdAt = now,
                        payload = EMPTY_JSON_PAYLOAD
                    ) to
                        listOf(
                            FullAttributeInstanceResult(
                                attributeUuid = UUID.randomUUID(),
                                payload = buildAttributeInstancePayload(
                                    "Beehive_incoming_123",
                                    ZonedDateTime.parse("2020-03-25T08:29:17.965206Z"),
                                    "urn:ngsi-ld:Dataset:45678".toUri(),
                                    "urn:ngsi-ld:Instance:45678".toUri()
                                ),
                                time = ZonedDateTime.parse("2020-03-25T08:29:17.965206Z"),
                                timeproperty = JsonLdUtils.NGSILD_OBSERVED_AT_TERM,
                                sub = "sub1"
                            ),
                            FullAttributeInstanceResult(
                                attributeUuid = UUID.randomUUID(),
                                payload = buildAttributeInstancePayload(
                                    "Beehive_incoming_124",
                                    ZonedDateTime.parse("2020-03-25T08:33:17.965206Z"),
                                    "urn:ngsi-ld:Dataset:45678".toUri(),
                                    "urn:ngsi-ld:Instance:45679".toUri()
                                ),
                                time = ZonedDateTime.parse("2020-03-25T08:33:17.965206Z"),
                                timeproperty = JsonLdUtils.NGSILD_OBSERVED_AT_TERM,
                                sub = "sub2"
                            )
                        )
                ),
                TemporalRepresentation.NORMALIZED,
                true,
                loadSampleData("expectations/beehive_incoming_multi_instances_string_values_with_audit.jsonld")
            )

        private val beehivePropertyMultiInstancesWithoutDatasetIdStringValues =
            Arguments.arguments(
                emptyList<ScopeInstanceResult>(),
                mapOf(
                    Attribute(
                        entityId = entityId,
                        attributeName = "https://ontology.eglobalmark.com/apic#incoming",
                        attributeValueType = Attribute.AttributeValueType.STRING,
                        createdAt = now,
                        payload = EMPTY_JSON_PAYLOAD
                    ) to
                        listOf(
                            FullAttributeInstanceResult(
                                attributeUuid = UUID.randomUUID(),
                                payload = buildAttributeInstancePayload(
                                    "Beehive_incoming_123",
                                    ZonedDateTime.parse("2020-03-25T08:29:17.965206Z"),
                                    null,
                                    "urn:ngsi-ld:Instance:45678".toUri()
                                ),
                                time = ZonedDateTime.parse("2020-03-25T08:29:17.965206Z"),
                                timeproperty = JsonLdUtils.NGSILD_OBSERVED_AT_TERM,
                                sub = null
                            ),
                            FullAttributeInstanceResult(
                                attributeUuid = UUID.randomUUID(),
                                payload = buildAttributeInstancePayload(
                                    "Beehive_incoming_124",
                                    ZonedDateTime.parse("2020-03-25T08:33:17.965206Z"),
                                    null,
                                    "urn:ngsi-ld:Instance:45679".toUri()
                                ),
                                time = ZonedDateTime.parse("2020-03-25T08:33:17.965206Z"),
                                timeproperty = JsonLdUtils.NGSILD_OBSERVED_AT_TERM,
                                sub = null
                            )
                        )
                ),
                TemporalRepresentation.NORMALIZED,
                false,
                loadSampleData(
                    "expectations/beehive_incoming_multi_instances_without_datasetId_string_values.jsonld"
                )
            )

        private val beehivePropertyMultiInstancesWithoutDatasetIdTemporalValues =
            Arguments.arguments(
                emptyList<ScopeInstanceResult>(),
                mapOf(
                    Attribute(
                        entityId = entityId,
                        attributeName = "https://ontology.eglobalmark.com/apic#incoming",
                        attributeValueType = Attribute.AttributeValueType.NUMBER,
                        createdAt = now,
                        payload = EMPTY_JSON_PAYLOAD
                    ) to
                        listOf(
                            SimplifiedAttributeInstanceResult(
                                attributeUuid = UUID.randomUUID(),
                                value = 550.0,
                                time = ZonedDateTime.parse("2020-03-25T08:29:17.965206Z")
                            ),
                            SimplifiedAttributeInstanceResult(
                                attributeUuid = UUID.randomUUID(),
                                value = 650.0,
                                time = ZonedDateTime.parse("2020-03-25T08:33:17.965206Z")
                            )
                        )
                ),
                TemporalRepresentation.TEMPORAL_VALUES,
                false,
                loadSampleData(
                    "expectations/beehive_incoming_multi_instances_without_datasetId_temporal_values.jsonld"
                )
            )

        private val beehivePropertyMultiInstancesTemporalValues =
            Arguments.arguments(
                emptyList<ScopeInstanceResult>(),
                mapOf(
                    Attribute(
                        entityId = entityId,
                        attributeName = "https://ontology.eglobalmark.com/apic#incoming",
                        attributeValueType = Attribute.AttributeValueType.NUMBER,
                        datasetId = "urn:ngsi-ld:Dataset:01234".toUri(),
                        createdAt = now,
                        payload = EMPTY_JSON_PAYLOAD
                    ) to
                        listOf(
                            SimplifiedAttributeInstanceResult(
                                attributeUuid = UUID.randomUUID(),
                                value = 550.0,
                                time = ZonedDateTime.parse("2020-03-25T08:29:17.965206Z")
                            ),
                            SimplifiedAttributeInstanceResult(
                                attributeUuid = UUID.randomUUID(),
                                value = 650.0,
                                time = ZonedDateTime.parse("2020-03-25T08:33:17.965206Z")
                            )
                        ),
                    Attribute(
                        entityId = entityId,
                        attributeName = "https://ontology.eglobalmark.com/apic#incoming",
                        attributeValueType = Attribute.AttributeValueType.NUMBER,
                        datasetId = "urn:ngsi-ld:Dataset:45678".toUri(),
                        createdAt = now,
                        payload = EMPTY_JSON_PAYLOAD
                    ) to
                        listOf(
                            SimplifiedAttributeInstanceResult(
                                attributeUuid = UUID.randomUUID(),
                                value = 487.0,
                                time = ZonedDateTime.parse("2020-03-25T08:29:17.965206Z")
                            ),
                            SimplifiedAttributeInstanceResult(
                                attributeUuid = UUID.randomUUID(),
                                value = 698.0,
                                time = ZonedDateTime.parse("2020-03-25T08:33:17.965206Z")
                            )
                        )
                ),
                TemporalRepresentation.TEMPORAL_VALUES,
                false,
                loadSampleData("expectations/beehive_incoming_multi_instances_temporal_values.jsonld")
            )

        private val beehivePropertyMultiInstancesStringValuesTemporalValues =
            Arguments.arguments(
                emptyList<ScopeInstanceResult>(),
                mapOf(
                    Attribute(
                        entityId = entityId,
                        attributeName = "https://ontology.eglobalmark.com/apic#incoming",
                        attributeValueType = Attribute.AttributeValueType.STRING,
                        datasetId = "urn:ngsi-ld:Dataset:45678".toUri(),
                        createdAt = now,
                        payload = EMPTY_JSON_PAYLOAD
                    ) to
                        listOf(
                            SimplifiedAttributeInstanceResult(
                                attributeUuid = UUID.randomUUID(),
                                value = "Beehive_incoming_123",
                                time = ZonedDateTime.parse("2020-03-25T08:29:17.965206Z")
                            ),
                            SimplifiedAttributeInstanceResult(
                                attributeUuid = UUID.randomUUID(),
                                value = "Beehive_incoming_124",
                                time = ZonedDateTime.parse("2020-03-25T08:33:17.965206Z")
                            )
                        )
                ),
                TemporalRepresentation.TEMPORAL_VALUES,
                false,
                loadSampleData("expectations/beehive_incoming_multi_instances_string_temporal_values.jsonld")
            )

        private val beehivePropertyMultiInstancesWithoutDatasetIdStringValuesTemporalValues =
            Arguments.arguments(
                emptyList<ScopeInstanceResult>(),
                mapOf(
                    Attribute(
                        entityId = entityId,
                        attributeName = "https://ontology.eglobalmark.com/apic#incoming",
                        attributeValueType = Attribute.AttributeValueType.STRING,
                        createdAt = now,
                        payload = EMPTY_JSON_PAYLOAD
                    ) to
                        listOf(
                            SimplifiedAttributeInstanceResult(
                                attributeUuid = UUID.randomUUID(),
                                value = "Beehive_incoming_123",
                                time = ZonedDateTime.parse("2020-03-25T08:29:17.965206Z")
                            ),
                            SimplifiedAttributeInstanceResult(
                                attributeUuid = UUID.randomUUID(),
                                value = "Beehive_incoming_124",
                                time = ZonedDateTime.parse("2020-03-25T08:33:17.965206Z")
                            )
                        )
                ),
                TemporalRepresentation.TEMPORAL_VALUES,
                false,
                loadSampleData(
                    "expectations/beehive_incoming_multi_instances_without_datasetId_string_temporal_values.jsonld"
                )
            )

        private val beehiveRelationshipMultiInstancesTemporalValues =
            Arguments.arguments(
                emptyList<ScopeInstanceResult>(),
                mapOf(
                    Attribute(
                        entityId = entityId,
                        attributeName = "https://ontology.eglobalmark.com/apic#incoming",
                        attributeType = Attribute.AttributeType.Relationship,
                        attributeValueType = Attribute.AttributeValueType.STRING,
                        datasetId = "urn:ngsi-ld:Dataset:45678".toUri(),
                        createdAt = now,
                        payload = EMPTY_JSON_PAYLOAD
                    ) to
                        listOf(
                            SimplifiedAttributeInstanceResult(
                                attributeUuid = UUID.randomUUID(),
                                value = "urn:ngsi-ld:Entity:1234",
                                time = ZonedDateTime.parse("2020-03-25T08:29:17.965206Z")
                            ),
                            SimplifiedAttributeInstanceResult(
                                attributeUuid = UUID.randomUUID(),
                                value = "urn:ngsi-ld:Entity:5678",
                                time = ZonedDateTime.parse("2020-03-25T08:33:17.965206Z")
                            )
                        )
                ),
                TemporalRepresentation.TEMPORAL_VALUES,
                false,
                loadSampleData("expectations/beehive_relationship_multi_instances_temporal_values.jsonld")
            )

        private val beehiveScopeMultiInstancesTemporalValues =
            Arguments.arguments(
                listOf(
                    SimplifiedScopeInstanceResult(
                        entityId = entityId,
                        scopes = listOf("/A/B", "/C/D"),
                        time = ZonedDateTime.parse("2020-03-25T08:29:17.965206Z")
                    ),
                    SimplifiedScopeInstanceResult(
                        entityId = entityId,
                        scopes = listOf("/C/D"),
                        time = ZonedDateTime.parse("2020-03-25T09:29:17.965206Z")
                    )
                ),
                emptyMap<Attribute, List<AttributeInstanceResult>>(),
                TemporalRepresentation.TEMPORAL_VALUES,
                false,
                loadSampleData("expectations/beehive_scope_multi_instances_temporal_values.jsonld")
            )

        private val beehiveScopeMultiInstances =
            Arguments.arguments(
                listOf(
                    FullScopeInstanceResult(
                        entityId = entityId,
                        scopes = listOf("/A/B", "/C/D"),
                        timeproperty = AttributeInstance.TemporalProperty.MODIFIED_AT.propertyName,
                        time = ZonedDateTime.parse("2020-03-25T08:29:17.965206Z")
                    ),
                    FullScopeInstanceResult(
                        entityId = entityId,
                        scopes = listOf("/C/D"),
                        timeproperty = AttributeInstance.TemporalProperty.MODIFIED_AT.propertyName,
                        time = ZonedDateTime.parse("2020-03-25T09:29:17.965206Z")
                    )
                ),
                emptyMap<Attribute, List<AttributeInstanceResult>>(),
                TemporalRepresentation.NORMALIZED,
                false,
                loadSampleData("expectations/beehive_scope_multi_instances.jsonld")
            )

        private val beehiveJsonPropertyTemporalValues =
            Arguments.arguments(
                emptyList<ScopeInstanceResult>(),
                mapOf(
                    Attribute(
                        entityId = entityId,
                        attributeName = "https://ontology.eglobalmark.com/apic#luminosity",
                        attributeType = Attribute.AttributeType.JsonProperty,
                        attributeValueType = Attribute.AttributeValueType.JSON,
                        datasetId = null,
                        createdAt = now,
                        payload = SAMPLE_JSON_PROPERTY_PAYLOAD
                    ) to
                        listOf(
                            SimplifiedAttributeInstanceResult(
                                attributeUuid = UUID.randomUUID(),
                                value = """
                                    { "id": "123", "stringValue": "value", "nullValue": null }
                                """,
                                time = ZonedDateTime.parse("2020-03-25T08:29:17.965206Z")
                            ),
                            SimplifiedAttributeInstanceResult(
                                attributeUuid = UUID.randomUUID(),
                                value = """
                                    { "id": "456", "stringValue": "anotherValue" }
                                """,
                                time = ZonedDateTime.parse("2020-03-25T08:33:17.965206Z")
                            )
                        )
                ),
                TemporalRepresentation.TEMPORAL_VALUES,
                false,
                loadSampleData("expectations/beehive_json_property_temporal_values.jsonld")
            )

        private val beehiveLanguagePropertyTemporalValues =
            Arguments.arguments(
                emptyList<ScopeInstanceResult>(),
                mapOf(
                    Attribute(
                        entityId = entityId,
                        attributeName = "https://ontology.eglobalmark.com/apic#friendlyName",
                        attributeType = Attribute.AttributeType.LanguageProperty,
                        attributeValueType = Attribute.AttributeValueType.OBJECT,
                        datasetId = null,
                        createdAt = now,
                        payload = SAMPLE_LANGUAGE_PROPERTY_PAYLOAD
                    ) to
                        listOf(
                            SimplifiedAttributeInstanceResult(
                                attributeUuid = UUID.randomUUID(),
                                value = """
                                    [{
                                      "@value": "One beautiful beehive",
                                      "@language": "en"
                                    },
                                    {
                                      "@value": "Une belle ruche",
                                      "@language": "fr"
                                    }]
                                """,
                                time = ZonedDateTime.parse("2020-03-25T08:29:17.965206Z")
                            ),
                            SimplifiedAttributeInstanceResult(
                                attributeUuid = UUID.randomUUID(),
                                value = """
                                    [{
                                      "@value": "My beautiful beehive",
                                      "@language": "en"
                                    },
                                    {
                                      "@value": "Ma belle ruche",
                                      "@language": "fr"
                                    }]
                                """,
                                time = ZonedDateTime.parse("2020-03-25T08:33:17.965206Z")
                            )
                        )
                ),
                TemporalRepresentation.TEMPORAL_VALUES,
                false,
                loadSampleData("expectations/beehive_language_property_temporal_values.jsonld")
            )

        private val beehiveVocabPropertyTemporalValues =
            Arguments.arguments(
                emptyList<ScopeInstanceResult>(),
                mapOf(
                    Attribute(
                        entityId = entityId,
                        attributeName = "https://ontology.eglobalmark.com/apic#category",
                        attributeType = Attribute.AttributeType.VocabProperty,
                        attributeValueType = Attribute.AttributeValueType.ARRAY,
                        datasetId = null,
                        createdAt = now,
                        payload = SAMPLE_VOCAB_PROPERTY_PAYLOAD
                    ) to
                        listOf(
                            SimplifiedAttributeInstanceResult(
                                attributeUuid = UUID.randomUUID(),
                                value = """
                                    [{
                                      "@id": "https://uri.etsi.org/ngsi-ld/default-context/stellio"
                                    },
                                    {
                                      "@id": "https://uri.etsi.org/ngsi-ld/default-context/egm"
                                    }]
                                """,
                                time = ZonedDateTime.parse("2020-03-25T08:29:17.965206Z")
                            ),
                            SimplifiedAttributeInstanceResult(
                                attributeUuid = UUID.randomUUID(),
                                value = """
                                    [{
                                      "@id": "https://uri.etsi.org/ngsi-ld/default-context/stellio"
                                    }]
                                """,
                                time = ZonedDateTime.parse("2020-03-25T08:33:17.965206Z")
                            )
                        )
                ),
                TemporalRepresentation.TEMPORAL_VALUES,
                false,
                loadSampleData("expectations/beehive_vocab_property_temporal_values.jsonld")
            )

        @JvmStatic
        fun rawResultsProvider(): Stream<Arguments> {
            return Stream.of(
                beehivePropertyMultiInstancesWithoutDatasetId,
                beehiveRelationshipMultiInstancesWithoutDatasetId,
                beehivePropertyMultiInstances,
                beehivePropertyMultiInstancesStringValues,
                beehivePropertyMultiInstancesStringValuesWithAudit,
                beehivePropertyMultiInstancesWithoutDatasetIdStringValues,
                beehivePropertyMultiInstancesWithoutDatasetIdTemporalValues,
                beehivePropertyMultiInstancesTemporalValues,
                beehivePropertyMultiInstancesStringValuesTemporalValues,
                beehivePropertyMultiInstancesWithoutDatasetIdStringValuesTemporalValues,
                beehiveRelationshipMultiInstancesTemporalValues,
                beehiveScopeMultiInstancesTemporalValues,
                beehiveScopeMultiInstances,
                beehiveJsonPropertyTemporalValues,
                beehiveLanguagePropertyTemporalValues,
                beehiveVocabPropertyTemporalValues
            )
        }
    }
}
