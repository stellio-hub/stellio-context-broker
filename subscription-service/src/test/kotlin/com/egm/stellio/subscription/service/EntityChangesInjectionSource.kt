package com.egm.stellio.subscription.service

import com.egm.stellio.shared.util.APIC_COMPOUND_CONTEXT
import org.junit.jupiter.params.provider.Arguments
import java.util.stream.Stream

@Suppress("unused", "UtilityClassWithPublicConstructor")
interface EntityChangesInjectionSource {

    companion object {

        private const val APIARY_ID = "urn:ngsi-ld:Apiary:XYZ01"

        private val entityWithSingleProperty = Arguments.arguments(
            """
            [{
               "id": "$APIARY_ID",
               "type": "Apiary",
               "deletedAt": "2025-08-15T00:00:00.000Z",
               "@context": [ "$APIC_COMPOUND_CONTEXT" ]
            }]
            """,
            """
            {
               "id": "$APIARY_ID",
               "type": "Apiary",
               "name": {
                  "type":"Property",
                  "value": "Le rucher de Nantes"
               },
               "@context": [ "$APIC_COMPOUND_CONTEXT" ]
            }                
            """,
            """
            {
               "id": "$APIARY_ID",
               "type": "Apiary",
               "deletedAt": "2025-08-15T00:00:00.000Z",
               "name": {
                  "type":"Property",
                  "previousValue": "Le rucher de Nantes",
                  "value": "urn:ngsi-ld:null"
               },
               "@context": [ "$APIC_COMPOUND_CONTEXT" ]
            }                                
            """
        )

        private val entityWithMultiInstanceProperty = Arguments.arguments(
            """
            [{
               "id": "$APIARY_ID",
               "type": "Apiary",
               "deletedAt": "2025-08-15T00:00:00.000Z",
               "@context": [ "$APIC_COMPOUND_CONTEXT" ]
            }]
            """,
            """
            {
               "id": "$APIARY_ID",
               "type": "Apiary",
               "name": [{
                  "type":"Property",
                  "value": "Le rucher de Valbonne",
                  "datasetId": "urn:ngsi-ld:Dataset:1"
               }, {
                  "type":"Property",
                  "value": "Le rucher de Nantes",
                  "datasetId": "urn:ngsi-ld:Dataset:2"
               }],
               "@context": [ "$APIC_COMPOUND_CONTEXT" ]
            }                
            """,
            """
            {
               "id": "$APIARY_ID",
               "type": "Apiary",
               "deletedAt": "2025-08-15T00:00:00.000Z",
               "name": [{
                  "type":"Property",
                  "previousValue": "Le rucher de Valbonne",
                  "datasetId": "urn:ngsi-ld:Dataset:1",
                  "value": "urn:ngsi-ld:null"
               }, {
                  "type":"Property",
                  "previousValue": "Le rucher de Nantes",
                  "value": "urn:ngsi-ld:null",
                  "datasetId": "urn:ngsi-ld:Dataset:2"
               }],
               "@context": [ "$APIC_COMPOUND_CONTEXT" ]
            }                                
            """
        )

        private val entityWithManyAttributes = Arguments.arguments(
            """
            [{
               "id": "$APIARY_ID",
               "type": "Apiary",
               "deletedAt": "2025-08-15T00:00:00.000Z",
               "@context": [ "$APIC_COMPOUND_CONTEXT" ]
            }]
            """,
            """
            {
               "id": "$APIARY_ID",
               "type": "Apiary",
               "name": {
                  "type":"Property",
                  "value": "Le rucher de Nantes"
               },
               "location": {
                  "type":"GeoProperty",
                  "value": {
                     "type": "Point",
                     "coordinates": [ 24.30623, 60.07966 ]
                  }
               },
               "belongsTo": {
                  "type":"Relationship",
                  "object": "urn:ngsi-ld:Beekeeper:1230"
               },
               "@context": [ "$APIC_COMPOUND_CONTEXT" ]
            }                
            """,
            """
            {
               "id": "$APIARY_ID",
               "type": "Apiary",
               "deletedAt": "2025-08-15T00:00:00.000Z",
               "name": {
                  "type":"Property",
                  "previousValue": "Le rucher de Nantes",
                  "value": "urn:ngsi-ld:null"
               },
               "location": {
                  "type": "GeoProperty",
                  "previousValue": {
                     "type": "Point",
                     "coordinates": [24.30623, 60.07966]
                  },
                  "value": "urn:ngsi-ld:null"
               },
               "belongsTo": {
                  "type": "Relationship",
                  "previousObject": "urn:ngsi-ld:Beekeeper:1230",
                  "object": "urn:ngsi-ld:null"
               },
               "@context": [ "$APIC_COMPOUND_CONTEXT" ]
            }                                
            """
        )

        @JvmStatic
        fun showChangesDataProvider(): Stream<Arguments> {
            return Stream.of(
                entityWithSingleProperty,
                entityWithMultiInstanceProperty,
                entityWithManyAttributes
            )
        }
    }
}
