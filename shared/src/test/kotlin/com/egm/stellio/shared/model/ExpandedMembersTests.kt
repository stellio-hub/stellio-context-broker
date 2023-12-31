package com.egm.stellio.shared.model

import com.egm.stellio.shared.util.*
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CREATED_AT_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_MODIFIED_AT_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_RELATIONSHIP_OBJECT
import com.egm.stellio.shared.util.JsonLdUtils.buildExpandedPropertyValue
import com.egm.stellio.shared.util.JsonLdUtils.buildExpandedRelationshipValue
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class ExpandedMembersTests {

    @Test
    fun `it should add createdAt information into an attribute`() {
        val attrPayload = mapOf("attribute" to buildExpandedPropertyValue(12.0))

        val attrPayloadWithSysAttrs = attrPayload.addSysAttrs(true, ngsiLdDateTime(), null)

        assertThat(attrPayloadWithSysAttrs)
            .containsKey(NGSILD_CREATED_AT_PROPERTY)
            .doesNotContainKey(NGSILD_MODIFIED_AT_PROPERTY)
    }

    @Test
    fun `it should add createdAt and modifiedAt information into an attribute`() {
        val attrPayload = mapOf("attribute" to buildExpandedPropertyValue(12.0))

        val attrPayloadWithSysAttrs = attrPayload.addSysAttrs(true, ngsiLdDateTime(), ngsiLdDateTime())

        assertThat(attrPayloadWithSysAttrs)
            .containsKey(NGSILD_CREATED_AT_PROPERTY)
            .containsKey(NGSILD_MODIFIED_AT_PROPERTY)
    }

    @Test
    fun `it should not find an unknown attribute instance in a list of attributes`() = runTest {
        val entityFragment =
            """
            {
                "brandName": {
                    "value": "a new value"
                }            
            }
            """.trimIndent()

        val expandedAttributes = JsonLdUtils.expandAttributes(entityFragment, NGSILD_TEST_CORE_CONTEXTS)
        assertNull(expandedAttributes.getAttributeFromExpandedAttributes("unknownAttribute", null))
    }

    @Test
    fun `it should find an attribute instance from a list of attributes without multi-attributes`() = runTest {
        val entityFragment =
            """
            {
                "brandName": {
                    "value": "a new value",
                    "observedAt": "2021-03-16T00:00:00.000Z"
                },
                "name": {
                    "value": 12
                }
            }
            """.trimIndent()

        val expandedAttributes = JsonLdUtils.expandAttributes(entityFragment, NGSILD_TEST_CORE_CONTEXTS)
        val expandedBrandName = JsonLdUtils.expandJsonLdTerm("brandName", NGSILD_TEST_CORE_CONTEXTS)

        assertNotNull(expandedAttributes.getAttributeFromExpandedAttributes(expandedBrandName, null))
        assertNull(
            expandedAttributes.getAttributeFromExpandedAttributes(expandedBrandName, "urn:datasetId".toUri())
        )
    }

    @Test
    fun `it should find an attribute instance from a list of attributes with multi-attributes`() = runTest {
        val entityFragment =
            """
            {
                "brandName": [{
                    "value": "a new value",
                    "observedAt": "2021-03-16T00:00:00.000Z"
                },
                {
                    "value": "a new value",
                    "observedAt": "2021-03-16T00:00:00.000Z",
                    "datasetId": "urn:datasetId:1"
                }],
                "name": {
                    "value": 12,
                    "datasetId": "urn:datasetId:1"
                }
            }
            """.trimIndent()

        val expandedAttributes = JsonLdUtils.expandAttributes(entityFragment, NGSILD_TEST_CORE_CONTEXTS)
        val expandedBrandName = JsonLdUtils.expandJsonLdTerm("brandName", NGSILD_TEST_CORE_CONTEXTS)
        val expandedName = JsonLdUtils.expandJsonLdTerm("name", NGSILD_TEST_CORE_CONTEXTS)

        assertNotNull(expandedAttributes.getAttributeFromExpandedAttributes(expandedBrandName, null))
        assertNotNull(
            expandedAttributes.getAttributeFromExpandedAttributes(expandedBrandName, "urn:datasetId:1".toUri())
        )
        assertNull(
            expandedAttributes.getAttributeFromExpandedAttributes(expandedBrandName, "urn:datasetId:2".toUri())
        )
        assertNotNull(
            expandedAttributes.getAttributeFromExpandedAttributes(expandedName, "urn:datasetId:1".toUri())
        )
        assertNull(expandedAttributes.getAttributeFromExpandedAttributes(expandedName, null))
    }

    @Test
    fun `it should return an error if a relationship has no object field`() {
        val relationshipValues = buildExpandedPropertyValue("something")[0]

        val result = relationshipValues.extractRelationshipObject("isARelationship")
        assertTrue(result.isLeft())
        result.mapLeft {
            assertEquals("Relationship isARelationship does not have an object field", it.message)
        }
    }

    @Test
    fun `it should return an error if a relationship has an empty object`() {
        val relationshipValues = mapOf(
            NGSILD_RELATIONSHIP_OBJECT to emptyList<Any>()
        )

        val result = relationshipValues.extractRelationshipObject("isARelationship")
        assertTrue(result.isLeft())
        result.mapLeft {
            assertEquals("Relationship isARelationship is empty", it.message)
        }
    }

    @Test
    fun `it should return an error if a relationship has an invalid object type`() {
        val relationshipValues = mapOf(
            NGSILD_RELATIONSHIP_OBJECT to listOf("invalid")
        )

        val result = relationshipValues.extractRelationshipObject("isARelationship")
        assertTrue(result.isLeft())
        result.mapLeft {
            assertEquals(
                "Relationship isARelationship has an invalid object type: class java.lang.String",
                it.message
            )
        }
    }

    @Test
    fun `it should return an error if a relationship has object without id`() {
        val relationshipValues = mapOf(
            NGSILD_RELATIONSHIP_OBJECT to listOf(
                mapOf("@value" to "urn:ngsi-ld:T:misplacedRelationshipObject")
            )
        )

        relationshipValues.extractRelationshipObject("isARelationship")
            .shouldFail {
                assertEquals("Relationship isARelationship has an invalid or no object id: null", it.message)
            }
    }

    @Test
    fun `it should extract the target object of a relationship`() {
        val relationshipObjectId = "urn:ngsi-ld:T:1"
        val relationshipValues = buildExpandedRelationshipValue(relationshipObjectId.toUri())

        relationshipValues[0].extractRelationshipObject("isARelationship")
            .shouldSucceedWith {
                assertEquals(relationshipObjectId.toUri(), it)
            }
    }
}
