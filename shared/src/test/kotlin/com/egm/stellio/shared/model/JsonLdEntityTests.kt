package com.egm.stellio.shared.model

import com.egm.stellio.shared.util.DEFAULT_CONTEXTS
import com.egm.stellio.shared.util.INCOMING_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_ID
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_NAME_PROPERTY
import com.egm.stellio.shared.util.OUTGOING_PROPERTY
import com.egm.stellio.shared.util.TEMPERATURE_PROPERTY
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.fail

class JsonLdEntityTests {

    @Test
    fun `it should find an expanded attribute contained in the entity`() {
        val jsonLdEntity = JsonLdEntity(
            mapOf(INCOMING_PROPERTY to "", OUTGOING_PROPERTY to ""),
            DEFAULT_CONTEXTS
        )

        val checkResult = jsonLdEntity.checkContainsAnyOf(setOf(TEMPERATURE_PROPERTY, INCOMING_PROPERTY))

        checkResult.fold({
            fail("it should have found one of the requested attributes")
        }, {})
    }

    @Test
    fun `it should not find an expanded attribute contained in the entity`() {
        val jsonLdEntity = JsonLdEntity(
            mapOf(INCOMING_PROPERTY to "", OUTGOING_PROPERTY to "", JSONLD_ID to "urn:ngsi-ld:Entity:01"),
            DEFAULT_CONTEXTS
        )

        val checkResult = jsonLdEntity.checkContainsAnyOf(setOf(TEMPERATURE_PROPERTY, NGSILD_NAME_PROPERTY))

        checkResult.fold({
            assertEquals(
                "Entity urn:ngsi-ld:Entity:01 does not exist or it has none of the requested attributes : " +
                    "[https://ontology.eglobalmark.com/apic#temperature, https://schema.org/name]",
                it.message
            )
        }, {
            fail("it should have not found any of the requested attributes")
        })
    }
}
