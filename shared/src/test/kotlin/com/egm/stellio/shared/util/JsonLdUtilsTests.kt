package com.egm.stellio.shared.util

import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.JSONLD_CONTEXT_KW
import com.egm.stellio.shared.model.LdContextNotAvailableException
import com.egm.stellio.shared.model.NGSILD_DEFAULT_VOCAB
import com.egm.stellio.shared.model.NGSILD_LOCATION_IRI
import com.egm.stellio.shared.model.NGSILD_OBSERVATION_SPACE_IRI
import com.egm.stellio.shared.model.NGSILD_PROPERTY_VALUE
import com.egm.stellio.shared.model.getAttributeFromExpandedAttributes
import com.egm.stellio.shared.model.getMemberValueAsString
import com.egm.stellio.shared.util.JsonLdUtils.compactEntities
import com.egm.stellio.shared.util.JsonLdUtils.compactEntity
import com.egm.stellio.shared.util.JsonLdUtils.compactTerm
import com.egm.stellio.shared.util.JsonLdUtils.expandAttribute
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdEntity
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdEntitySafe
import com.egm.stellio.shared.util.JsonLdUtils.expandJsonLdFragment
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class JsonLdUtilsTests {

    @Test
    fun `it should throw a LdContextNotAvailable exception if the provided JSON-LD context is not available`() =
        runTest {
            val rawEntity =
                """
                {
                    "id": "urn:ngsi-ld:Device:01234",
                    "type": "Device",
                    "@context": [
                        "unknownContext"
                    ]
                }
                """.trimIndent()

            val exception = assertThrows<LdContextNotAvailableException> {
                expandJsonLdFragment(rawEntity.deserializeAsMap(), listOf("unknownContext"))
            }
            assertEquals(
                "Unable to load remote context",
                exception.message
            )
            assertEquals(
                "caused by: JsonLdError[code=There was a problem encountered " +
                    "loading a remote context [code=LOADING_REMOTE_CONTEXT_FAILED]., " +
                    "message=Context URI is not absolute [unknownContext].]",
                exception.detail
            )
        }

    @Test
    fun `it should throw a BadRequestData exception if the expanded JSON-LD fragment is empty`() = runTest {
        val rawEntity =
            """
            {
                "@context": [
                    "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.7.jsonld"
                ]
            }
            """.trimIndent()

        val exception = assertThrows<BadRequestDataException> {
            expandJsonLdFragment(rawEntity, listOf(NGSILD_TEST_CORE_CONTEXT))
        }
        assertEquals(
            "Unable to expand input payload",
            exception.message
        )
    }

    @Test
    fun `it should compact and return an entity`() = runTest {
        val entity =
            """
            {
                "id": "urn:ngsi-ld:Device:01234",
                "type": "Device"
            }
            """.trimIndent()
        val expectedEntity =
            """
            {
                "id": "urn:ngsi-ld:Device:01234",
                "type": "Device",
                "@context": "$NGSILD_TEST_CORE_CONTEXT"
            }
            """.trimIndent()

        val expandedEntity = expandJsonLdEntity(entity, NGSILD_TEST_CORE_CONTEXTS)
        val compactedEntity = compactEntity(expandedEntity, NGSILD_TEST_CORE_CONTEXTS)

        assertJsonPayloadsAreEqual(expectedEntity, mapper.writeValueAsString(compactedEntity))
    }

    @Test
    fun `it should compact and return a list of one entity`() = runTest {
        val entity =
            """
            {
                "id": "urn:ngsi-ld:Device:01234",
                "type": "Device"
            }
            """.trimIndent()
        val expectedEntity =
            """
            [{
                "id": "urn:ngsi-ld:Device:01234",
                "type": "Device",
                "@context": "$NGSILD_TEST_CORE_CONTEXT"
            }]
            """.trimIndent()

        val expandedEntity = expandJsonLdEntity(entity, NGSILD_TEST_CORE_CONTEXTS)
        val compactedEntities = compactEntities(listOf(expandedEntity), NGSILD_TEST_CORE_CONTEXTS)

        assertJsonPayloadsAreEqual(expectedEntity, mapper.writeValueAsString(compactedEntities))
    }

    @Test
    fun `it should compact and return a list of entities`() = runTest {
        val entity =
            """
            {
                "id": "urn:ngsi-ld:Device:01234",
                "type": "Device"
            }
            """.trimIndent()
        val expectedEntity =
            """
            [{
                "id": "urn:ngsi-ld:Device:01234",
                "type": "Device",
                "@context": "$NGSILD_TEST_CORE_CONTEXT"
            },{
                "id": "urn:ngsi-ld:Device:01234",
                "type": "Device",
                "@context": "$NGSILD_TEST_CORE_CONTEXT"
            }]
            """.trimIndent()

        val expandedEntity = expandJsonLdEntity(entity, NGSILD_TEST_CORE_CONTEXTS)
        val compactedEntities = compactEntities(listOf(expandedEntity, expandedEntity), NGSILD_TEST_CORE_CONTEXTS)

        assertJsonPayloadsAreEqual(expectedEntity, mapper.writeValueAsString(compactedEntities))
    }

    @Test
    fun `it should expand an attribute from a fragment`() = runTest {
        val fragment =
            """
                {
                    "attribute": {
                        "type": "Property",
                        "value": "something"
                    }
                }
            """.trimIndent()

        val expandedAttribute = expandAttribute(fragment, NGSILD_TEST_CORE_CONTEXTS)
        assertEquals(NGSILD_DEFAULT_VOCAB + "attribute", expandedAttribute.first)
        assertThat(expandedAttribute.second).hasSize(1)
    }

    @Test
    fun `it should expand an attribute from a name and a string payload`() = runTest {
        val payload =
            """
                {
                    "type": "Property",
                    "value": "something"
                }
            """.trimIndent()

        val expandedAttribute = expandAttribute("attribute", payload, NGSILD_TEST_CORE_CONTEXTS)
        assertEquals(NGSILD_DEFAULT_VOCAB + "attribute", expandedAttribute.first)
        assertThat(expandedAttribute.second).hasSize(1)
    }

    @Test
    fun `it should expand an attribute from a name and a map payload`() = runTest {
        val payload = mapOf(
            "type" to "Property",
            "value" to "something"
        )

        val expandedAttribute = expandAttribute("attribute", payload, NGSILD_TEST_CORE_CONTEXTS)
        assertEquals(NGSILD_DEFAULT_VOCAB + "attribute", expandedAttribute.first)
        assertThat(expandedAttribute.second).hasSize(1)
    }

    @Test
    fun `it should correctly transform core geoproperties`() = runTest {
        val payload =
            """
                {
                    "id": "urn:ngsi-ld:Device:01234",
                    "type": "Device",
                    "location": {
                        "type": "GeoProperty",
                        "value": {
                            "type": "Point",
                            "coordinates": [ 100.12, 0.23 ]
                        }
                    },
                    "observationSpace": {
                        "type": "GeoProperty",
                        "value": {
                            "type": "Polygon",
                            "coordinates": [[
                                [100.12, 0.23],
                                [101.12, 0.23], 
                                [101.12, 1.23],
                                [100.12, 1.23],
                                [100.12, 0.23]
                            ]]
                        }
                    }
                }
            """.trimIndent()

        val expandedEntity = expandJsonLdEntitySafe(payload.deserializeAsMap(), NGSILD_TEST_CORE_CONTEXTS)
            .shouldSucceedAndResult()

        val location = expandedEntity.getAttributes().getAttributeFromExpandedAttributes(NGSILD_LOCATION_IRI, null)
        assertNotNull(location)
        assertEquals(
            "POINT (100.12 0.23)",
            location!!.getMemberValueAsString(NGSILD_PROPERTY_VALUE)
        )

        val observationSpace = expandedEntity.getAttributes()
            .getAttributeFromExpandedAttributes(NGSILD_OBSERVATION_SPACE_IRI, null)
        assertNotNull(observationSpace)
        assertEquals(
            "POLYGON ((100.12 0.23, 101.12 0.23, 101.12 1.23, 100.12 1.23, 100.12 0.23))",
            observationSpace!!.getMemberValueAsString(NGSILD_PROPERTY_VALUE)
        )
    }

    @Test
    fun `it should correctly transform and restore core geoproperties`() = runTest {
        val payload =
            """
                {
                    "id": "urn:ngsi-ld:Device:01234",
                    "type": "Device",
                    "location": {
                        "type": "GeoProperty",
                        "value": {
                            "type": "Point",
                            "coordinates": [ 100.12, 0.23 ]
                        }
                    },
                    "observationSpace": {
                        "type": "GeoProperty",
                        "value": {
                            "type": "Polygon",
                            "coordinates": [[
                                [100.12, 0.23],
                                [101.12, 0.23], 
                                [101.12, 1.23],
                                [100.12, 1.23],
                                [100.12, 0.23]
                            ]]
                        }
                    }
                }
            """.trimIndent()

        val deserializedPayload = payload.deserializeAsMap()
        val expandedEntity = expandJsonLdEntitySafe(deserializedPayload, NGSILD_TEST_CORE_CONTEXTS)
            .shouldSucceedAndResult()
        val compactedEntity = compactEntity(expandedEntity, NGSILD_TEST_CORE_CONTEXTS)
        assertJsonPayloadsAreEqual(
            serializeObject(deserializedPayload.plus(JSONLD_CONTEXT_KW to NGSILD_TEST_CORE_CONTEXT)),
            serializeObject(compactedEntity)
        )
    }

    @Test
    fun `it should correctly transform and restore a user defined geoproperty`() = runTest {
        val payload =
            """
                {
                    "id": "urn:ngsi-ld:Device:01234",
                    "type": "Device",
                    "userDefinedGeoProperty": {
                        "type": "GeoProperty",
                        "value": {
                            "type": "Point",
                            "coordinates": [ 100.12, 0.23 ]
                        }
                    }
                }
            """.trimIndent()

        val deserializedPayload = payload.deserializeAsMap()
        val expandedEntity = expandJsonLdEntitySafe(deserializedPayload, NGSILD_TEST_CORE_CONTEXTS)
            .shouldSucceedAndResult()

        val userDefinedGeoProperty = expandedEntity.getAttributes()
            .getAttributeFromExpandedAttributes(NGSILD_DEFAULT_VOCAB + "userDefinedGeoProperty", null)
        assertNotNull(userDefinedGeoProperty)
        assertEquals(
            "POINT (100.12 0.23)",
            userDefinedGeoProperty!!.getMemberValueAsString(NGSILD_PROPERTY_VALUE)
        )

        val compactedEntity = compactEntity(expandedEntity, NGSILD_TEST_CORE_CONTEXTS)
        assertJsonPayloadsAreEqual(
            serializeObject(deserializedPayload.plus(JSONLD_CONTEXT_KW to NGSILD_TEST_CORE_CONTEXT)),
            serializeObject(compactedEntity)
        )
    }

    @Test
    fun `it should correctly compact a term if it is in the provided contexts`() = runTest {
        assertEquals(
            INCOMING_TERM,
            compactTerm(INCOMING_IRI, APIC_COMPOUND_CONTEXTS)
        )
    }

    @Test
    fun `it should return the input term if it was not able to compact it`() = runTest {
        assertEquals(
            INCOMING_IRI,
            compactTerm(INCOMING_IRI, NGSILD_TEST_CORE_CONTEXTS)
        )
    }
}
