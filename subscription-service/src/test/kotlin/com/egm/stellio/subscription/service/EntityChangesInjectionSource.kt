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
               "name": {
                  "type":"Property",
                  "value": "Le rucher de Nantes"
               },
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
                  "previousValue": "Le rucher de Nantes"
               },
               "@context": [ "$APIC_COMPOUND_CONTEXT" ]
            }                                
            """
        )

        @JvmStatic
        fun showChangesDataProvider(): Stream<Arguments> {
            return Stream.of(
                entityWithSingleProperty
            )
        }
    }
}
