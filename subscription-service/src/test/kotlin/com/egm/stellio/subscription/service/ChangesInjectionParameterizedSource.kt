package com.egm.stellio.subscription.service

import com.egm.stellio.shared.util.APIC_COMPOUND_CONTEXT
import org.junit.jupiter.params.provider.Arguments
import java.util.stream.Stream

@Suppress("unused", "UtilityClassWithPublicConstructor")
interface ChangesInjectionParameterizedSource {

    companion object {

        private const val APIARY_ID = "urn:ngsi-ld:Apiary:XYZ01"

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

        @JvmStatic
        fun showChangesDataProvider(): Stream<Arguments> {
            return Stream.of(
                propertyMonoInstanceSingleEntity,
                propertyMultiInstanceSingleEntity
            )
        }
    }
}
