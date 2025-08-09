package com.egm.stellio.subscription.service

import com.egm.stellio.shared.util.APIC_COMPOUND_CONTEXT
import org.junit.jupiter.params.provider.Arguments
import java.util.stream.Stream

@Suppress("unused", "UtilityClassWithPublicConstructor")
interface AttributeChangesInjectionSource {

    companion object {

        private const val APIARY_ID = "urn:ngsi-ld:Apiary:XYZ01"
        private const val APIARY_ID_2 = "urn:ngsi-ld:Apiary:XYZ02"

        private val propertyMonoInstanceSingleEntity =
            Arguments.arguments(
                """
                [{
                   "id": "$APIARY_ID",
                   "type": "Apiary",
                   "name": {
                      "type":"Property",
                      "value": "Le rucher de Nantes"
                   },
                   "@context": [ "$APIC_COMPOUND_CONTEXT" ]
                }]
                """,
                false,
                """
                {
                   "name": {
                      "type": "Property",
                      "value": "Rucher Nantais"
                   }
                }
                """,
                """
                {
                  "type":"Property",
                  "previousValue": "Rucher Nantais",
                  "value": "Le rucher de Nantes"
                }
                """
            )

        private val propertyDeletedMonoInstanceSingleEntity =
            Arguments.arguments(
                """
                [{
                   "id": "$APIARY_ID",
                   "type": "Apiary",
                   "name": {
                      "type":"Property",
                      "value": "urn:ngsi-ld:null"
                   },
                   "@context": [ "$APIC_COMPOUND_CONTEXT" ]
                }]
                """,
                false,
                """
                {
                   "name": {
                      "type": "Property",
                      "value": "Rucher Nantais"
                   }
                }
                """,
                """
                {
                  "type":"Property",
                  "previousValue": "Rucher Nantais",
                  "value": "urn:ngsi-ld:null"
                }
                """
            )

        private val propertyMultiInstanceSingleEntity =
            Arguments.arguments(
                """
                [{
                   "id": "$APIARY_ID",
                   "type": "Apiary",
                   "name": [{
                      "type":"Property",
                      "value": "Le rucher de Sophia Antipolis"
                   }, {
                      "type":"Property",
                      "value": "Le rucher de Nantes",
                      "datasetId": "urn:ngsi-ld:Dataset:1"
                   }],
                   "@context": [ "$APIC_COMPOUND_CONTEXT" ]
                }]
                """,
                true,
                """
                {
                   "name": {
                      "type": "Property",
                      "value": "Rucher Nantais",
                      "datasetId": "urn:ngsi-ld:Dataset:1"
                   }
                }
                """,
                """
                [{
                   "type":"Property",
                   "value": "Le rucher de Sophia Antipolis"
                }, {
                   "type":"Property",
                   "previousValue": "Rucher Nantais",
                   "value": "Le rucher de Nantes",
                   "datasetId": "urn:ngsi-ld:Dataset:1"
                }]
                """
            )

        private val relationshipMonoInstanceSingleEntity =
            Arguments.arguments(
                """
                [{
                   "id": "$APIARY_ID",
                   "type": "Apiary",
                   "name": {
                      "type":"Relationship",
                      "object": "urn:ngsi-ld:Device:HCMR-A"
                   },
                   "@context": [ "$APIC_COMPOUND_CONTEXT" ]
                }]
                """,
                false,
                """
                {
                   "name": {
                      "type": "Relationship",
                      "object": "urn:ngsi-ld:Device:HCMR-B"
                   }
                }
                """,
                """
                {
                  "type":"Relationship",
                  "previousObject": "urn:ngsi-ld:Device:HCMR-B",
                  "object": "urn:ngsi-ld:Device:HCMR-A"
                }
                """
            )

        private val relationshipMultiInstanceSingleEntity =
            Arguments.arguments(
                """
                [{
                   "id": "$APIARY_ID",
                   "type": "Apiary",
                   "name": [{
                      "type":"Relationship",
                      "object": "urn:ngsi-ld:Device:HCMR-A"
                   }, {
                      "type":"Relationship",
                      "object": "urn:ngsi-ld:Device:HCMR-C",
                      "datasetId": "urn:ngsi-ld:Dataset:1"
                   }],
                   "@context": [ "$APIC_COMPOUND_CONTEXT" ]
                }]
                """,
                true,
                """
                {
                   "name": {
                      "type": "Relationship",
                      "object": "urn:ngsi-ld:Device:HCMR-D",
                      "datasetId": "urn:ngsi-ld:Dataset:1"
                   }
                }
                """,
                """
                [{
                   "type":"Relationship",
                   "object": "urn:ngsi-ld:Device:HCMR-A"
                }, {
                   "type":"Relationship",
                   "previousObject": "urn:ngsi-ld:Device:HCMR-D",
                   "object": "urn:ngsi-ld:Device:HCMR-C",
                   "datasetId": "urn:ngsi-ld:Dataset:1"
                }]
                """
            )

        private val geoPropertyMonoInstanceSingleEntity =
            Arguments.arguments(
                """
                [{
                   "id": "$APIARY_ID",
                   "type": "Apiary",
                   "name": {
                      "type":"GeoProperty",
                      "value": {
                        "type": "Point",
                        "coordinates": [24.30623, 60.07966]
                      }
                   },
                   "@context": [ "$APIC_COMPOUND_CONTEXT" ]
                }]
                """,
                false,
                """
                {
                   "name": {
                      "type": "GeoProperty",
                      "value": {
                        "type": "Point",
                        "coordinates": [24.30622, 60.07965]
                      }
                   }
                }
                """,
                """
                {
                  "type":"GeoProperty",
                  "previousValue": {
                    "type": "Point",
                    "coordinates": [24.30622, 60.07965]
                  },
                  "value": {
                    "type": "Point",
                    "coordinates": [24.30623, 60.07966]
                  }
                }
                """
            )

        private val jsonPropertyMonoInstanceSingleEntity =
            Arguments.arguments(
                """
                [{
                   "id": "$APIARY_ID",
                   "type": "Apiary",
                   "name": {
                      "type":"JsonProperty",
                      "json": { "brandName": "Mercedes", "doors": 3 }
                   },
                   "@context": [ "$APIC_COMPOUND_CONTEXT" ]
                }]
                """,
                false,
                """
                {
                   "name": {
                      "type": "JsonProperty",
                      "json": { "brandName": "Mercedes", "doors": 5 }
                   }
                }
                """,
                """
                {
                  "type":"JsonProperty",
                  "previousJson": { "brandName": "Mercedes", "doors": 5 },
                  "json": { "brandName": "Mercedes", "doors": 3 }
                }
                """
            )

        private val languagePropertyMonoInstanceSingleEntity =
            Arguments.arguments(
                """
                [{
                   "id": "$APIARY_ID",
                   "type": "Apiary",
                   "name": {
                      "type":"LanguageProperty",
                      "languageMap": { "fr": "Bonjour", "en": "Hello" }
                   },
                   "@context": [ "$APIC_COMPOUND_CONTEXT" ]
                }]
                """,
                false,
                """
                {
                   "name": {
                      "type": "LanguageProperty",
                      "languageMap": { "fr": "Salut", "en": "Hi" }
                   }
                }
                """,
                """
                {
                  "type":"LanguageProperty",
                  "previousLanguageMap": { "fr": "Salut", "en": "Hi" },
                  "languageMap": { "fr": "Bonjour", "en": "Hello" }
                }
                """
            )

        private val vocabPropertyMonoInstanceSingleEntity =
            Arguments.arguments(
                """
                [{
                   "id": "$APIARY_ID",
                   "type": "Apiary",
                   "name": {
                      "type":"VocabProperty",
                      "vocab": "https://example.org/industry/Manufacturing"
                   },
                   "@context": [ "$APIC_COMPOUND_CONTEXT" ]
                }]
                """,
                false,
                """
                {
                   "name": {
                      "type": "VocabProperty",
                      "vocab": "https://example.org/industry/Retail"
                   }
                }
                """,
                """
                {
                  "type":"VocabProperty",
                  "previousVocab": "https://example.org/industry/Retail",
                  "vocab": "https://example.org/industry/Manufacturing"
                }
                """
            )

        private val propertyMonoInstanceMultiEntities =
            Arguments.arguments(
                """
                [{
                   "id": "$APIARY_ID",
                   "type": "Apiary",
                   "name": {
                      "type":"Property",
                      "value": "Le rucher de Nantes"
                   },
                   "@context": [ "$APIC_COMPOUND_CONTEXT" ]
                }, {
                   "id": "$APIARY_ID_2",
                   "type": "Apiary",
                   "name": {
                      "type": "Property",
                      "value": "Le rucher de Paris"
                   },
                   "@context": [ "$APIC_COMPOUND_CONTEXT" ]
                }]
                """,
                false,
                """
                {
                   "name": {
                      "type": "Property",
                      "value": "Rucher Nantais"
                   }
                }
                """,
                """
                {
                  "type":"Property",
                  "previousValue": "Rucher Nantais",
                  "value": "Le rucher de Nantes"
                }
                """
            )

        private val propertyMultiInstanceMultiEntities =
            Arguments.arguments(
                """
                [{
                   "id": "$APIARY_ID",
                   "type": "Apiary",
                   "name": [{
                      "type":"Property",
                      "value": "Le rucher de Sophia Antipolis"
                   }, {
                      "type":"Property",
                      "value": "Le rucher de Nantes",
                      "datasetId": "urn:ngsi-ld:Dataset:1"
                   }],
                   "@context": [ "$APIC_COMPOUND_CONTEXT" ]
                }, {
                   "id": "$APIARY_ID_2",
                   "type": "Apiary",
                   "name": {
                      "type":"Property",
                      "value": "Le rucher de Paris"
                   },
                   "@context": [ "$APIC_COMPOUND_CONTEXT" ]
                }]
                """,
                true,
                """
                {
                   "name": {
                      "type": "Property",
                      "value": "Rucher Nantais",
                      "datasetId": "urn:ngsi-ld:Dataset:1"
                   }
                }
                """,
                """
                [{
                   "type":"Property",
                   "value": "Le rucher de Sophia Antipolis"
                }, {
                   "type":"Property",
                   "previousValue": "Rucher Nantais",
                   "value": "Le rucher de Nantes",
                   "datasetId": "urn:ngsi-ld:Dataset:1"
                }]
                """
            )

        @JvmStatic
        fun showChangesDataProvider(): Stream<Arguments> {
            return Stream.of(
                propertyMonoInstanceSingleEntity,
                propertyDeletedMonoInstanceSingleEntity,
                propertyMultiInstanceSingleEntity,
                relationshipMonoInstanceSingleEntity,
                relationshipMultiInstanceSingleEntity,
                geoPropertyMonoInstanceSingleEntity,
                jsonPropertyMonoInstanceSingleEntity,
                languagePropertyMonoInstanceSingleEntity,
                vocabPropertyMonoInstanceSingleEntity,
                propertyMonoInstanceMultiEntities,
                propertyMultiInstanceMultiEntities
            )
        }
    }
}
