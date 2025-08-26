package com.egm.stellio.shared.model

import com.egm.stellio.shared.util.APIC_COMPOUND_CONTEXT
import com.egm.stellio.shared.util.INCOMING_IRI
import com.egm.stellio.shared.util.JsonLdUtils.compactEntity
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.egm.stellio.shared.util.LUMINOSITY_IRI
import com.egm.stellio.shared.util.NAME_IRI
import com.egm.stellio.shared.util.OUTGOING_IRI
import com.egm.stellio.shared.util.TEMPERATURE_IRI
import com.egm.stellio.shared.util.assertJsonPayloadsAreEqual
import com.egm.stellio.shared.util.expandJsonLdEntity
import com.egm.stellio.shared.util.loadAndExpandSampleData
import com.egm.stellio.shared.util.ngsiLdDateTime
import com.egm.stellio.shared.util.parseAndExpandQueryParameter
import com.egm.stellio.shared.util.toUri
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.fail

class ExpandedEntityTests {
    @Test
    fun `it should find an expanded attribute contained in the entity`() {
        val expandedEntity = ExpandedEntity(
            mapOf(INCOMING_IRI to "", OUTGOING_IRI to "")
        )

        val checkResult = expandedEntity.checkContainsAnyOf(setOf(TEMPERATURE_IRI, INCOMING_IRI))

        checkResult.fold({
            fail("it should have found one of the requested attributes")
        }, {})
    }

    @Test
    fun `it should not find an expanded attribute not contained in the entity`() {
        val expandedEntity = ExpandedEntity(
            mapOf(INCOMING_IRI to "", OUTGOING_IRI to "", JSONLD_ID_KW to "urn:ngsi-ld:Entity:01".toUri())
        )

        val checkResult = expandedEntity.checkContainsAnyOf(setOf(TEMPERATURE_IRI, NAME_IRI))

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
            .containsKey(NAME_IRI)
    }

    @Test
    fun `it should get an attribute by name without datasetId`() = runTest {
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

        val expandedAttributeInstance = expandJsonLdEntity(entity).getAttribute(NAME_IRI, null)
        assertThat(expandedAttributeInstance)
            .isNotNull()
    }

    @Test
    fun `it should get an attribute by name with datasetId`() = runTest {
        val entity = """
        {
            "id": "urn:ngsi-ld:Entity:01",
            "type": "Entity",
            "name": {
                "type": "Property",
                "value": "An entity",
                "datasetId": "urn:ngsi-ld:Dataset:01"
            },
            "@context": [ "$APIC_COMPOUND_CONTEXT" ]
        }
        """.trimIndent()

        val expandedAttributeInstance = expandJsonLdEntity(entity)
            .getAttribute(NAME_IRI, "urn:ngsi-ld:Dataset:01".toUri())
        assertThat(expandedAttributeInstance)
            .isNotNull()
    }

    @Test
    fun `it should not get an attribute if name does not match`() = runTest {
        val entity = """
        {
            "id": "urn:ngsi-ld:Entity:01",
            "type": "Entity",
            "name": {
                "type": "Property",
                "value": "An entity",
                "datasetId": "urn:ngsi-ld:Dataset:01"
            },
            "@context": [ "$APIC_COMPOUND_CONTEXT" ]
        }
        """.trimIndent()

        val expandedAttributeInstance = expandJsonLdEntity(entity)
            .getAttribute(LUMINOSITY_IRI, "urn:ngsi-ld:Dataset:01".toUri())
        assertThat(expandedAttributeInstance)
            .isNull()
    }

    @Test
    fun `it should not get an attribute if datasetId does not match`() = runTest {
        val entity = """
        {
            "id": "urn:ngsi-ld:Entity:01",
            "type": "Entity",
            "name": {
                "type": "Property",
                "value": "An entity",
                "datasetId": "urn:ngsi-ld:Dataset:01"
            },
            "@context": [ "$APIC_COMPOUND_CONTEXT" ]
        }
        """.trimIndent()

        val expandedAttributeInstance = expandJsonLdEntity(entity)
            .getAttribute(NAME_IRI, "urn:ngsi-ld:Dataset:02".toUri())
        assertThat(expandedAttributeInstance)
            .isNull()
    }

    @Test
    fun `it should not get an attribute without datasetId if datasetId is provided`() = runTest {
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

        val expandedAttributeInstance = expandJsonLdEntity(entity)
            .getAttribute(NAME_IRI, "urn:ngsi-ld:Dataset:01".toUri())
        assertThat(expandedAttributeInstance)
            .isNull()
    }

    @Test
    fun `it should filter the attributes based on the attrs only`() = runTest {
        val entity = loadAndExpandSampleData("beehive_with_multi_attribute_instances.jsonld")

        val expectedEntity = """
            {
                "id": "urn:ngsi-ld:Entity:01",
                "type": "Entity",
                "managedBy": [
                    {
                        "type": "Relationship",
                        "object": "urn:ngsi-ld:Beekeeper:Default"
                    },
                    {
                        "type": "Relationship",
                        "datasetId": "urn:ngsi-ld:Dataset:managedBy",
                        "object": "urn:ngsi-ld:Beekeeper:1230"
                    },
                    {
                       "type": "Relationship",
                        "datasetId": "urn:ngsi-ld:Dataset:french-name",
                        "object": "urn:ngsi-ld:Apiculteur:1230"
                    }
                ],
                "@context" : "$APIC_COMPOUND_CONTEXT"
            }
        """.trimIndent()

        val attributesToMatch: Set<String> = parseAndExpandQueryParameter("managedBy", listOf(APIC_COMPOUND_CONTEXT))

        val filteredEntity = entity.filterAttributes(attributesToMatch, emptySet())

        val compactedEntity = compactEntity(filteredEntity, listOf(APIC_COMPOUND_CONTEXT))
        assertJsonPayloadsAreEqual(expectedEntity, serializeObject(compactedEntity))
    }

    @Test
    fun `it should filter the attributes based on the datasetId and attrs`() = runTest {
        val entity = loadAndExpandSampleData("beehive_with_multi_attribute_instances.jsonld")

        val expectedEntity = """
            {
                "id": "urn:ngsi-ld:Entity:01",
                "type": "Entity",
                "name": {
                    "type": "Property",
                    "datasetId": "urn:ngsi-ld:Dataset:english-name",
                    "value": "beehive"
                },
                "@context": "$APIC_COMPOUND_CONTEXT"
            }
        """.trimIndent()

        val attributesToMatch: Set<String> = parseAndExpandQueryParameter("name", listOf(APIC_COMPOUND_CONTEXT))
        val datasetIdToMatch: Set<String> = setOf("urn:ngsi-ld:Dataset:english-name")
        val filteredEntity = entity.filterAttributes(attributesToMatch, datasetIdToMatch)
        val compactedEntity = compactEntity(filteredEntity, listOf(APIC_COMPOUND_CONTEXT))
        assertJsonPayloadsAreEqual(expectedEntity, serializeObject(compactedEntity))
    }

    @Test
    fun `it should filter the attributes based on the datasetIds only`() = runTest {
        val entity = loadAndExpandSampleData("beehive_with_multi_attribute_instances.jsonld")

        val expectedEntity = """
            {
                "id": "urn:ngsi-ld:Entity:01",
                "type": "Entity",
                "name": {
                    "type": "Property",
                    "datasetId": "urn:ngsi-ld:Dataset:french-name",
                    "value": "ruche"
                },
                "managedBy": {
                     "type": "Relationship",
                     "datasetId": "urn:ngsi-ld:Dataset:french-name",
                     "object": "urn:ngsi-ld:Apiculteur:1230"
                },
                "@context" : "$APIC_COMPOUND_CONTEXT"
            }
        """.trimIndent()

        val datasetIdToMatch: Set<String> = setOf("urn:ngsi-ld:Dataset:french-name")
        val filteredEntity = entity.filterAttributes(emptySet(), datasetIdToMatch)
        val compactedEntity = compactEntity(filteredEntity, listOf(APIC_COMPOUND_CONTEXT))
        assertJsonPayloadsAreEqual(expectedEntity, serializeObject(compactedEntity))
    }

    @Test
    fun `it should return the default instance if @none is in the datasetId request parameter`() = runTest {
        val entity = loadAndExpandSampleData("beehive_with_multi_attribute_instances.jsonld")

        val expectedEntity = """
            {
                "id": "urn:ngsi-ld:Entity:01",
                "type": "Entity",
                "name": {
                    "type": "Property",
                    "value": "default"
                },
                "managedBy": {
                    "type": "Relationship",
                    "object": "urn:ngsi-ld:Beekeeper:Default"
                },
                "@context" : "$APIC_COMPOUND_CONTEXT"
            }
        """.trimIndent()

        val filteredEntity = entity.filterAttributes(emptySet(), setOf(JSONLD_NONE_KW))
        val compactedEntity = compactEntity(filteredEntity, listOf(APIC_COMPOUND_CONTEXT))
        assertJsonPayloadsAreEqual(expectedEntity, serializeObject(compactedEntity))
    }

    @Test
    fun `it should filter attributes on @none and attrs`() = runTest {
        val entity = loadAndExpandSampleData("beehive_with_multi_attribute_instances.jsonld")

        val expectedEntity = """
            {
                "id": "urn:ngsi-ld:Entity:01",
                "type": "Entity",
                "name": {
                    "type": "Property",
                    "value": "default"
                },
                "@context" : "$APIC_COMPOUND_CONTEXT"
            }
        """.trimIndent()

        val attributesToMatch: Set<String> = parseAndExpandQueryParameter("name", listOf(APIC_COMPOUND_CONTEXT))
        val filteredEntity = entity.filterAttributes(attributesToMatch, setOf(JSONLD_NONE_KW))
        val compactedEntity = compactEntity(filteredEntity, listOf(APIC_COMPOUND_CONTEXT))
        assertJsonPayloadsAreEqual(expectedEntity, serializeObject(compactedEntity))
    }

    @Test
    fun `it should filter the attributes based on multiple datasetIds`() = runTest {
        val entity = loadAndExpandSampleData("beehive_with_multi_attribute_instances.jsonld")

        val expectedEntity = """
            {
                "id": "urn:ngsi-ld:Entity:01",
                "type": "Entity",
                "name": [
                    {
                        "type": "Property",
                        "datasetId": "urn:ngsi-ld:Dataset:english-name",
                        "value": "beehive"
                    },
                     {
                        "type": "Property",
                        "datasetId": "urn:ngsi-ld:Dataset:french-name",
                        "value": "ruche"
                    }           
                ],
                "managedBy": {
                     "type": "Relationship",
                     "datasetId": "urn:ngsi-ld:Dataset:french-name",
                     "object": "urn:ngsi-ld:Apiculteur:1230"
                },
                "@context": "$APIC_COMPOUND_CONTEXT"
            }
        """.trimIndent()

        val datasetIdToMatch: Set<String> = setOf(
            "urn:ngsi-ld:Dataset:english-name",
            "urn:ngsi-ld:Dataset:french-name"
        )
        val filteredEntity = entity.filterAttributes(emptySet(), datasetIdToMatch)
        val compactedEntity = compactEntity(filteredEntity, listOf(APIC_COMPOUND_CONTEXT))
        assertJsonPayloadsAreEqual(expectedEntity, serializeObject(compactedEntity))
    }

    @Test
    fun `it should return no attributes if datasetIds and attrs don't match`() = runTest {
        val entity = loadAndExpandSampleData("beehive_with_multi_attribute_instances.jsonld")

        val expectedEntity = """
            {
                "id": "urn:ngsi-ld:Entity:01",
                "type": "Entity",
                "@context": "$APIC_COMPOUND_CONTEXT"
            }
        """.trimIndent()

        val attributesToMatch: Set<String> = parseAndExpandQueryParameter("name", listOf(APIC_COMPOUND_CONTEXT))
        val datasetIdToMatch: Set<String> = setOf("urn:ngsi-ld:Dataset:managedBy")
        val filteredEntity = entity.filterAttributes(attributesToMatch, datasetIdToMatch)
        val compactedEntity = compactEntity(filteredEntity, listOf(APIC_COMPOUND_CONTEXT))
        assertJsonPayloadsAreEqual(expectedEntity, serializeObject(compactedEntity))
    }

    @Test
    fun `it should get the scopes from a JSON-LD entity`() = runTest {
        val entity = """
        {
            "id": "urn:ngsi-ld:Entity:01",
            "type": "Entity",
            "scope": ["/Nantes", "/Irrigation"],
            "@context": [ "$APIC_COMPOUND_CONTEXT" ]
        }
        """.trimIndent()

        val scopes = expandJsonLdEntity(entity).getScopes()
        assertThat(scopes)
            .hasSize(2)
            .contains("/Nantes", "/Irrigation")
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

        val expandedEntity = expandJsonLdEntity(entity).populateCreationTimeDate(ngsiLdDateTime())
        assertThat(expandedEntity.members).containsKey(NGSILD_CREATED_AT_IRI)
        val nameAttributeInstances = expandedEntity.members[NAME_IRI] as ExpandedAttributeInstances
        assertThat(nameAttributeInstances).hasSize(1)
        assertThat(nameAttributeInstances[0]).containsKey(NGSILD_CREATED_AT_IRI)
    }
}
