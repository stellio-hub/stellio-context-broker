package com.egm.stellio.shared.model

import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_ID
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CREATED_AT_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_NAME_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdEntity
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.fail

@OptIn(ExperimentalCoroutinesApi::class)
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

    @Test
    fun `it should get the attributes from a JSON-LD entity`() = runTest {
        val entity = """
        {
            "id": "urn:ngsi-ld:Entity:01",
            "type": "Entity",
            "name": {
                "type": "Property",
                "value": "An entity"
            },
            "@context": [ "$APIC_COMPOUND_CONTEXT" ]
        }
        """.trimIndent()

        val expandedAttributes = expandJsonLdEntity(entity).getAttributes()
        assertThat(expandedAttributes)
            .hasSize(1)
            .containsKey(NGSILD_NAME_PROPERTY)
    }

    @Test
    fun `it should populate the createdAt information at root and attribute levels`() = runTest {
        val entity = """
        {
            "id": "urn:ngsi-ld:Entity:01",
            "type": "Entity",
            "name": {
                "type": "Property",
                "value": "An entity"
            },
            "@context": [ "$APIC_COMPOUND_CONTEXT" ]
        }
        """.trimIndent()

        val jsonLdEntity = expandJsonLdEntity(entity).populateCreationTimeDate(ngsiLdDateTime())
        assertThat(jsonLdEntity.members).containsKey(NGSILD_CREATED_AT_PROPERTY)
        val nameAttributeInstances = jsonLdEntity.members[NGSILD_NAME_PROPERTY] as ExpandedAttributeInstances
        assertThat(nameAttributeInstances).hasSize(1)
        assertThat(nameAttributeInstances[0]).containsKey(NGSILD_CREATED_AT_PROPERTY)
    }
}
