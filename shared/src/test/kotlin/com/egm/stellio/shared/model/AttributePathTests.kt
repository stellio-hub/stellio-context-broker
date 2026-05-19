package com.egm.stellio.shared.model

import com.egm.stellio.shared.queryparameter.AttributePath
import com.egm.stellio.shared.util.APIC_COMPOUND_CONTEXTS
import com.egm.stellio.shared.util.INCOMING_IRI
import com.egm.stellio.shared.util.INCOMING_TERM
import com.egm.stellio.shared.util.NAME_IRI
import com.egm.stellio.shared.util.NAME_TERM
import com.egm.stellio.shared.util.TEMPERATURE_IRI
import com.egm.stellio.shared.util.TEMPERATURE_TERM
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import java.util.stream.Stream

class AttributePathTests {

    companion object {

        @JvmStatic
        fun attributePathProvider(): Stream<Arguments> {
            return Stream.of(
                Arguments.of(
                    INCOMING_TERM,
                    listOf(INCOMING_IRI),
                    emptyList<ExpandedTerm>()
                ),
                Arguments.of(
                    "$INCOMING_TERM.$NGSILD_MODIFIED_AT_TERM",
                    listOf(INCOMING_IRI, NGSILD_MODIFIED_AT_IRI),
                    emptyList<ExpandedTerm>()
                ),
                Arguments.of(
                    "$INCOMING_TERM[$TEMPERATURE_TERM]",
                    listOf(INCOMING_IRI),
                    listOf(TEMPERATURE_IRI)
                ),
                Arguments.of(
                    "$INCOMING_TERM[$TEMPERATURE_TERM.$NAME_TERM]",
                    listOf(INCOMING_IRI),
                    listOf(TEMPERATURE_IRI, NAME_IRI)
                ),
            )
        }
    }

    @ParameterizedTest
    @MethodSource("com.egm.stellio.shared.model.AttributePathTests#attributePathProvider")
    fun `it should correctly parse an AttributePath`(
        term: String,
        expectedMainPath: List<ExpandedTerm>,
        expectedTrailingPath: List<ExpandedTerm> = emptyList()
    ) = runTest {
        val queryTerm = AttributePath(term, APIC_COMPOUND_CONTEXTS)
        assertEquals(expectedMainPath, queryTerm.mainPath)
        assertEquals(expectedTrailingPath, queryTerm.trailingPath)
    }

    @Test
    fun `it should mark attribute as jsonKeys and keep trailing path unexpanded`() = runTest {
        val attrPath = AttributePath(
            "$INCOMING_TERM[$TEMPERATURE_TERM]",
            APIC_COMPOUND_CONTEXTS,
            jsonKeys = setOf(INCOMING_TERM)
        )
        assertTrue(attrPath.isJsonKeysAttribute)
        assertFalse(attrPath.isExpandValuesAttribute)
        assertNull(attrPath.languageTag)
        assertEquals(listOf(TEMPERATURE_TERM), attrPath.trailingPath)
    }

    @Test
    fun `it should keep composed trailing path unexpanded when attribute is in jsonKeys`() = runTest {
        val attrPath = AttributePath(
            "$INCOMING_TERM[$TEMPERATURE_TERM.$NAME_TERM]",
            APIC_COMPOUND_CONTEXTS,
            jsonKeys = setOf(INCOMING_TERM)
        )
        assertTrue(attrPath.isJsonKeysAttribute)
        assertEquals(listOf(TEMPERATURE_TERM, NAME_TERM), attrPath.trailingPath)
    }

    @Test
    fun `it should mark attribute as expandValues`() = runTest {
        val attrPath = AttributePath(
            INCOMING_TERM,
            APIC_COMPOUND_CONTEXTS,
            expandValues = setOf(INCOMING_TERM)
        )
        assertTrue(attrPath.isExpandValuesAttribute)
        assertFalse(attrPath.isJsonKeysAttribute)
        assertNull(attrPath.languageTag)
    }

    @Test
    fun `it should parse language tag from bracket notation`() = runTest {
        val attrPath = AttributePath("$INCOMING_TERM[en]", APIC_COMPOUND_CONTEXTS)
        assertEquals("en", attrPath.languageTag)
        assertEquals(listOf(INCOMING_IRI), attrPath.mainPath)
        assertTrue(attrPath.trailingPath.isEmpty())
        assertFalse(attrPath.isJsonKeysAttribute)
    }

    @Test
    fun `it should expand trailing path terms when attribute is not in jsonKeys`() = runTest {
        val attrPath = AttributePath("$INCOMING_TERM[$TEMPERATURE_TERM.$NAME_TERM]", APIC_COMPOUND_CONTEXTS)
        assertFalse(attrPath.isJsonKeysAttribute)
        assertNull(attrPath.languageTag)
        assertEquals(listOf(TEMPERATURE_IRI, NAME_IRI), attrPath.trailingPath)
    }

    @Test
    fun `it should build exists path for a simple attribute`() = runTest {
        val attrPath = AttributePath(INCOMING_TERM, APIC_COMPOUND_CONTEXTS)
        assertEquals("""$."$INCOMING_IRI"""", attrPath.buildJsonBExistsPath())
    }

    @Test
    fun `it should build exists path for a composed attribute`() = runTest {
        val attrPath = AttributePath("$INCOMING_TERM.$NGSILD_MODIFIED_AT_TERM", APIC_COMPOUND_CONTEXTS)
        assertEquals("""$."$INCOMING_IRI"."$NGSILD_MODIFIED_AT_IRI"""", attrPath.buildJsonBExistsPath())
    }

    @Test
    fun `it should build property path for a simple attribute`() = runTest {
        val attrPath = AttributePath(INCOMING_TERM, APIC_COMPOUND_CONTEXTS)
        assertEquals(
            """$."$INCOMING_IRI"."$NGSILD_PROPERTY_VALUE"."$JSONLD_VALUE_KW"""",
            attrPath.buildJsonBPropertyPath()
        )
    }

    @Test
    fun `it should build property path using a direct value access for a temporal attribute`() = runTest {
        val attrPath = AttributePath("$INCOMING_TERM.$NGSILD_MODIFIED_AT_TERM", APIC_COMPOUND_CONTEXTS)
        assertEquals(
            """$."$INCOMING_IRI"."$NGSILD_MODIFIED_AT_IRI"."$JSONLD_VALUE_KW"""",
            attrPath.buildJsonBPropertyPath()
        )
    }

    @Test
    fun `it should build property path with wildcard traversal for a composed attribute`() = runTest {
        val attrPath = AttributePath("$INCOMING_TERM.$TEMPERATURE_TERM", APIC_COMPOUND_CONTEXTS)
        assertEquals(
            """$."$INCOMING_IRI"."$TEMPERATURE_IRI".**{0 to 2}."$JSONLD_VALUE_KW"""",
            attrPath.buildJsonBPropertyPath()
        )
    }

    @Test
    fun `it should build property path with trailing path for bracket notation`() = runTest {
        val attrPath = AttributePath("$INCOMING_TERM[$TEMPERATURE_TERM]", APIC_COMPOUND_CONTEXTS)
        assertEquals(
            """$."$INCOMING_IRI"."$NGSILD_PROPERTY_VALUE"."$TEMPERATURE_IRI".**{0 to 1}."$JSONLD_VALUE_KW"""",
            attrPath.buildJsonBPropertyPath()
        )
    }

    @Test
    fun `it should build relationship path for a simple attribute`() = runTest {
        val attrPath = AttributePath(INCOMING_TERM, APIC_COMPOUND_CONTEXTS)
        assertEquals(
            """$."$INCOMING_IRI"."$NGSILD_RELATIONSHIP_OBJECT"."$JSONLD_ID_KW"""",
            attrPath.buildJsonBRelationshipPath()
        )
    }

    @Test
    fun `it should build relationship path with wildcard traversal for a composed attribute`() = runTest {
        val attrPath = AttributePath("$INCOMING_TERM.$TEMPERATURE_TERM", APIC_COMPOUND_CONTEXTS)
        assertEquals(
            """$."$INCOMING_IRI"."$TEMPERATURE_IRI".**{0 to 2}."$JSONLD_ID_KW"""",
            attrPath.buildJsonBRelationshipPath()
        )
    }

    @Test
    fun `it should build vocab path for a simple attribute`() = runTest {
        val attrPath = AttributePath(INCOMING_TERM, APIC_COMPOUND_CONTEXTS)
        assertEquals(
            """$."$INCOMING_IRI"."$NGSILD_VOCABPROPERTY_VOCAB"[*]."$JSONLD_ID_KW"""",
            attrPath.buildJsonBVocabPath()
        )
    }

    @Test
    fun `it should build vocab path for a composed attribute`() = runTest {
        val attrPath = AttributePath("$INCOMING_TERM.$TEMPERATURE_TERM", APIC_COMPOUND_CONTEXTS)
        assertEquals(
            """$."$INCOMING_IRI"."$TEMPERATURE_IRI"."$NGSILD_VOCABPROPERTY_VOCAB"[*]."$JSONLD_ID_KW"""",
            attrPath.buildJsonBVocabPath()
        )
    }

    @Test
    fun `it should build language map path for a simple attribute`() = runTest {
        val attrPath = AttributePath(INCOMING_TERM, APIC_COMPOUND_CONTEXTS)
        assertEquals(
            """$."$INCOMING_IRI"."$NGSILD_LANGUAGEPROPERTY_LANGUAGEMAP"[*]."$JSONLD_VALUE_KW"""",
            attrPath.buildJsonBLanguageMapPath()
        )
    }

    @Test
    fun `it should build language map path for a composed attribute`() = runTest {
        val attrPath = AttributePath("$INCOMING_TERM.$TEMPERATURE_TERM", APIC_COMPOUND_CONTEXTS)
        assertEquals(
            """$."$INCOMING_IRI"."$TEMPERATURE_IRI"."$NGSILD_LANGUAGEPROPERTY_LANGUAGEMAP"[*]."$JSONLD_VALUE_KW"""",
            attrPath.buildJsonBLanguageMapPath()
        )
    }

    @Test
    fun `it should build language map filter path for a simple attribute`() = runTest {
        val attrPath = AttributePath(INCOMING_TERM, APIC_COMPOUND_CONTEXTS)
        assertEquals(
            """$."$INCOMING_IRI"."$NGSILD_LANGUAGEPROPERTY_LANGUAGEMAP"[*]""",
            attrPath.buildJsonBLanguageMapFilterPath()
        )
    }

    @Test
    fun `it should build language map filter path for a composed attribute`() = runTest {
        val attrPath = AttributePath("$INCOMING_TERM.$TEMPERATURE_TERM", APIC_COMPOUND_CONTEXTS)
        assertEquals(
            """$."$INCOMING_IRI"."$TEMPERATURE_IRI"."$NGSILD_LANGUAGEPROPERTY_LANGUAGEMAP"[*]""",
            attrPath.buildJsonBLanguageMapFilterPath()
        )
    }

    @Test
    fun `it should build json property path for a jsonKeys attribute`() = runTest {
        val attrPath = AttributePath(
            "$INCOMING_TERM[$TEMPERATURE_TERM]",
            APIC_COMPOUND_CONTEXTS,
            jsonKeys = setOf(INCOMING_TERM)
        )
        assertEquals(
            """$."$INCOMING_IRI"."$NGSILD_JSONPROPERTY_JSON"."$JSONLD_VALUE_KW"."$TEMPERATURE_TERM"""",
            attrPath.buildJsonBJsonPropertyPath()
        )
    }

    @Test
    fun `it should build json property path for a jsonKeys attribute with composed main path`() = runTest {
        val attrPath = AttributePath(
            "$INCOMING_TERM.$TEMPERATURE_TERM[$NAME_TERM]",
            APIC_COMPOUND_CONTEXTS,
            jsonKeys = setOf(INCOMING_TERM)
        )
        assertEquals(
            """$."$INCOMING_IRI"."$TEMPERATURE_IRI"."$NGSILD_JSONPROPERTY_JSON"."$JSONLD_VALUE_KW"."$NAME_TERM"""",
            attrPath.buildJsonBJsonPropertyPath()
        )
    }
}
