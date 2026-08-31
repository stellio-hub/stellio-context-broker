package com.egm.stellio.shared.queryparameter

import com.egm.stellio.shared.model.JSONLD_ID_KW
import com.egm.stellio.shared.model.JSONLD_VALUE_KW
import com.egm.stellio.shared.model.NGSILD_JSONPROPERTY_JSON
import com.egm.stellio.shared.model.NGSILD_LANGUAGEPROPERTY_LANGUAGEMAP
import com.egm.stellio.shared.model.NGSILD_LISTPROPERTY_VALUE_LIST
import com.egm.stellio.shared.model.NGSILD_LISTRELATIONSHIP_OBJECT_LIST
import com.egm.stellio.shared.model.NGSILD_PROPERTY_VALUE
import com.egm.stellio.shared.model.NGSILD_RELATIONSHIP_OBJECT
import com.egm.stellio.shared.model.NGSILD_VOCABPROPERTY_VOCAB
import com.egm.stellio.shared.util.APIC_COMPOUND_CONTEXTS
import com.egm.stellio.shared.util.BEEHIVE_IRI
import com.egm.stellio.shared.util.INCOMING_IRI
import com.egm.stellio.shared.util.INCOMING_TERM
import com.egm.stellio.shared.util.TEMPERATURE_IRI
import com.egm.stellio.shared.util.TEMPERATURE_TERM
import com.egm.stellio.shared.util.removeNoise
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class QNodeSqlTranslatorTests {

    private fun buildSql(
        raw: String,
        jsonKeys: Set<String> = emptySet(),
        expandValues: Set<String> = emptySet()
    ): String {
        val node = parseQQuery(raw).getOrNull()!!
        return node.toSqlJsonPath(jsonKeys, expandValues, APIC_COMPOUND_CONTEXTS)
    }

    // jsonpath parameter placeholders — must not be interpolated by Kotlin
    private val valuePh = $$"$value"
    private val minPh = $$"$min"
    private val maxPh = $$"$max"
    private val langPh = $$"$lang"

    // shared jsonpath segments built from the same NGSI-LD constants as the production code
    private val incomingPropertyPath =
        """$."$INCOMING_IRI"."$NGSILD_PROPERTY_VALUE"."$JSONLD_VALUE_KW""""
    private val incomingRelPath =
        """$."$INCOMING_IRI"."$NGSILD_RELATIONSHIP_OBJECT"[*]."$JSONLD_ID_KW""""
    private val incomingVocabPath =
        """$."$INCOMING_IRI"."$NGSILD_VOCABPROPERTY_VOCAB"[*]."$JSONLD_ID_KW""""
    private val incomingLangMapPath =
        """$."$INCOMING_IRI"."$NGSILD_LANGUAGEPROPERTY_LANGUAGEMAP"[*]."$JSONLD_VALUE_KW""""
    private val incomingListPropertyPath =
        """$."$INCOMING_IRI"."$NGSILD_LISTPROPERTY_VALUE_LIST"."@list"."$JSONLD_VALUE_KW""""
    private val incomingListRelationshipPath =
        """$."$INCOMING_IRI"."$NGSILD_LISTRELATIONSHIP_OBJECT_LIST"."@list".""" +
            """$NGSILD_RELATIONSHIP_OBJECT"[*]."$JSONLD_ID_KW""""
    private val incomingLangFilterPath =
        """$."$INCOMING_IRI"."$NGSILD_LANGUAGEPROPERTY_LANGUAGEMAP"[*]"""
    private val incomingJsonKeyPath =
        """$."$INCOMING_IRI"."$NGSILD_JSONPROPERTY_JSON"."$JSONLD_VALUE_KW"."$TEMPERATURE_TERM""""
    private val temperaturePropertyPath =
        """$."$TEMPERATURE_IRI"."$NGSILD_PROPERTY_VALUE"."$JSONLD_VALUE_KW""""
    private val temperatureListPropertyPath =
        """$."$TEMPERATURE_IRI"."$NGSILD_LISTPROPERTY_VALUE_LIST"."@list"."$JSONLD_VALUE_KW""""

    private fun exists(path: String) =
        """jsonb_path_exists(entity_payload.payload, '$path')"""

    private fun existsWhere(path: String, filter: String, params: String) =
        """jsonb_path_exists(entity_payload.payload, '$path ? ($filter)', '$params')"""

    @Test
    fun `toSqlJsonPath should generate EXISTS check against entity_payload for a simple attribute`() {
        assertEquals(
            exists("""$."$INCOMING_IRI""""),
            buildSql(INCOMING_TERM)
        )
    }

    @Test
    fun `toSqlJsonPath should generate NOT EXISTS for negated attribute`() {
        assertEquals(
            """NOT (${exists("""$."$INCOMING_IRI"""")})""",
            buildSql("!$INCOMING_TERM")
        )
    }

    @Test
    fun `toSqlJsonPath should generate AND clause for semicolon operator`() {
        val incomingOpenSql = """
            (${existsWhere(incomingPropertyPath, """@ == $valuePh""", """{"value": "open"}""")} OR
                ${existsWhere(incomingLangMapPath, """@ == $valuePh""", """{"value": "open"}""")} OR
                ${existsWhere(incomingListPropertyPath, """@ == $valuePh""", """{"value": "open"}""")})
        """
        val temperatureGt0Sql = """
            (${existsWhere(temperaturePropertyPath, """@ > $valuePh""", """{"value": 0}""")} OR
                ${existsWhere(temperatureListPropertyPath, """@ > $valuePh""", """{"value": 0}""")})
        """
        assertEquals(
            "($incomingOpenSql) AND ($temperatureGt0Sql)".removeNoise(),
            buildSql("$INCOMING_TERM==\"open\";$TEMPERATURE_TERM>0").removeNoise()
        )
    }

    @Test
    fun `toSqlJsonPath should generate OR clause for pipe operator`() {
        val incomingOpenSql = """
            (${existsWhere(incomingPropertyPath, """@ == $valuePh""", """{"value": "open"}""")} OR
                ${existsWhere(incomingLangMapPath, """@ == $valuePh""", """{"value": "open"}""")} OR
                ${existsWhere(incomingListPropertyPath, """@ == $valuePh""", """{"value": "open"}""")})
        """
        val temperatureGt0Sql = """
            (${existsWhere(temperaturePropertyPath, """@ > $valuePh""", """{"value": 0}""")} OR
                ${existsWhere(temperatureListPropertyPath, """@ > $valuePh""", """{"value": 0}""")})
        """
        assertEquals(
            "($incomingOpenSql) OR ($temperatureGt0Sql)".removeNoise(),
            buildSql("$INCOMING_TERM==\"open\"|$TEMPERATURE_TERM>0").removeNoise()
        )
    }

    @Test
    fun `toSqlJsonPath should generate numeric comparison using hasValue`() {
        val expected = """
            (${existsWhere(temperaturePropertyPath, """@ == $valuePh""", """{"value": 42}""")} OR
                ${existsWhere(temperatureListPropertyPath, """@ == $valuePh""", """{"value": 42}""")})
        """
        assertEquals(expected.removeNoise(), buildSql("$TEMPERATURE_TERM==42").removeNoise())
    }

    @Test
    fun `toSqlJsonPath should generate string comparison targeting both hasValue and language map`() {
        val expected = """
            (${existsWhere(incomingPropertyPath, """@ == $valuePh""", """{"value": "open"}""")} OR
                ${existsWhere(incomingLangMapPath, """@ == $valuePh""", """{"value": "open"}""")} OR
                ${existsWhere(incomingListPropertyPath, """@ == $valuePh""", """{"value": "open"}""")})
        """
        assertEquals(expected.removeNoise(), buildSql("$INCOMING_TERM==\"open\"").removeNoise())
    }

    @Test
    fun `toSqlJsonPath should generate URI comparison targeting relationship, property, and vocab paths`() {
        val uri = "urn:ngsi-ld:BeeHive:001"
        val params = """{"value": "$uri"}"""
        val expected = """
            (${existsWhere(incomingRelPath, """@ == $valuePh""", params)} OR
                ${existsWhere(incomingPropertyPath, """@ == $valuePh""", params)} OR
                ${existsWhere(incomingVocabPath, """@ == $valuePh""", params)} OR
                ${existsWhere(incomingLangMapPath, """@ == $valuePh""", params)} OR
                ${existsWhere(incomingListPropertyPath, """@ == $valuePh""", params)} OR
                ${existsWhere(incomingListRelationshipPath, """@ == $valuePh""", params)})
            """
        assertEquals(expected.removeNoise(), buildSql("""$INCOMING_TERM=="$uri"""").removeNoise())
    }

    @Test
    fun `toSqlJsonPath should generate range comparison`() {
        val expected = """
            (${existsWhere(
            temperaturePropertyPath,
            """@ >= $minPh && @ <= $maxPh""",
            """{"min": 10, "max": 20}"""
        )} OR ${existsWhere(
            temperatureListPropertyPath,
            """@ >= $minPh && @ <= $maxPh""",
            """{"min": 10, "max": 20}"""
        )})
        """
        assertEquals(expected.removeNoise(), buildSql("$TEMPERATURE_TERM==10..20").removeNoise())
    }

    @Test
    fun `toSqlJsonPath should generate boolean comparison`() {
        val activePath =
            """$."https://uri.etsi.org/ngsi-ld/default-context/active"."$NGSILD_PROPERTY_VALUE"."$JSONLD_VALUE_KW""""
        val activeListPath =
            """$."https://uri.etsi.org/ngsi-ld/default-context/active"."$NGSILD_LISTPROPERTY_VALUE_LIST"."@list"."$JSONLD_VALUE_KW""""
        val expected = """(${exists("""$activePath ? (@ == true)""")} OR """ +
            """${exists("""$activeListPath ? (@ == true)""")})"""
        assertEquals(expected, buildSql("active==true"))
    }

    @Test
    fun `toSqlJsonPath should generate list comparison using inline filter`() {
        val expected = """
            (${exists("""$temperaturePropertyPath ? (@ == 10 || @ == 20 || @ == 30)""")} OR
                ${exists("""$temperatureListPropertyPath ? (@ == 10 || @ == 20 || @ == 30)""")})
        """
        assertEquals(expected.removeNoise(), buildSql("$TEMPERATURE_TERM==10,20,30").removeNoise())
    }

    @Test
    fun `toSqlJsonPath should generate NEQ range as outside-bounds filter`() {
        val params = """{"min": 10, "max": 20}"""
        val propertyNotInRange = """(${exists(temperaturePropertyPath)} AND NOT (""" +
            """${existsWhere(temperaturePropertyPath, """@ >= $minPh && @ <= $maxPh""", params)}))"""
        val listPropertyNotInRange = """(${exists(temperatureListPropertyPath)} AND NOT (""" +
            """${existsWhere(temperatureListPropertyPath, """@ >= $minPh && @ <= $maxPh""", params)}))"""
        assertEquals(
            "($propertyNotInRange OR $listPropertyNotInRange)".removeNoise(),
            buildSql("$TEMPERATURE_TERM!=10..20").removeNoise()
        )
    }

    @Test
    fun `toSqlJsonPath should generate language tag comparison using hasLanguageMap path`() {
        assertEquals(
            existsWhere(
                incomingLangFilterPath,
                """@."@language" == $langPh && @."@value" == $valuePh""",
                """{"lang": "en", "value": "hello"}"""
            ).removeNoise(),
            buildSql("""$INCOMING_TERM[en]=="hello"""").removeNoise()
        )
    }

    @Test
    fun `toSqlJsonPath should not expand trailing path for jsonKeys attributes`() {
        assertEquals(
            existsWhere(incomingJsonKeyPath, """@ == $valuePh""", """{"value": 42}"""),
            buildSql("$INCOMING_TERM[$TEMPERATURE_TERM]==42", jsonKeys = setOf(INCOMING_TERM))
        )
    }

    @Test
    fun `toSqlJsonPath should expand comparison value for expandValues attributes`() {
        val params = """{"value": "$BEEHIVE_IRI"}"""
        val expected = """
            (${existsWhere(incomingRelPath, """@ == $valuePh""", params)} OR
            ${existsWhere(incomingPropertyPath, """@ == $valuePh""", params)} OR
            ${existsWhere(incomingVocabPath, """@ == $valuePh""", params)} OR
            ${existsWhere(incomingLangMapPath, """@ == $valuePh""", params)} OR
            ${existsWhere(incomingListPropertyPath, """@ == $valuePh""", params)} OR
            ${existsWhere(incomingListRelationshipPath, """@ == $valuePh""", params)})
        """.trimIndent()
        assertEquals(
            expected.removeNoise(),
            buildSql("$INCOMING_TERM==\"BeeHive\"", expandValues = setOf(INCOMING_TERM)).removeNoise()
        )
    }

    @Test
    fun `toSqlJsonPath should generate like_regex comparison for LIKE_REGEX operator`() {
        val expected = """(${exists("""$incomingPropertyPath ? (@ like_regex "test.*")""")} OR """ +
            """${exists("""$incomingLangMapPath ? (@ like_regex "test.*")""")} OR """ +
            """${exists("""$incomingListPropertyPath ? (@ like_regex "test.*")""")})"""
        assertEquals(expected.removeNoise(), buildSql("""$INCOMING_TERM~="test.*"""").removeNoise())
    }

    @Test
    fun `toSqlJsonPath should generate NOT like_regex for NOT_LIKE_REGEX operator`() {
        val expected = """
            NOT ((${exists("""$incomingPropertyPath ? (@ like_regex "test.*")""")} OR
                ${exists("""$incomingLangMapPath ? (@ like_regex "test.*")""")} OR
                ${exists("""$incomingListPropertyPath ? (@ like_regex "test.*")""")}))
        """
        assertEquals(expected.removeNoise(), buildSql("""$INCOMING_TERM!~="test.*"""").removeNoise())
    }

    @Test
    fun `toSqlJsonPath should generate NEQ range filter for jsonKeys attribute with bracket key`() {
        val params = """{"min": 10, "max": 20}"""
        val expected = """(${exists(incomingJsonKeyPath)} AND NOT (""" +
            """${existsWhere(incomingJsonKeyPath, """@ >= $minPh && @ <= $maxPh""", params)}))"""
        assertEquals(
            expected,
            buildSql("$INCOMING_TERM[$TEMPERATURE_TERM]!=10..20", jsonKeys = setOf(INCOMING_TERM))
        )
    }

    @Test
    fun `toSqlJsonPath should query ListProperty members and negate membership for NEQ`() {
        val params = """{"value": "good"}"""
        val listPropertyNotEqual = """(${exists(incomingListPropertyPath)} AND NOT (""" +
            """${existsWhere(incomingListPropertyPath, """@ == $valuePh""", params)}))"""

        assertEquals(
            true,
            buildSql("$INCOMING_TERM!=good").removeNoise().contains(listPropertyNotEqual.removeNoise())
        )
    }

    @Test
    fun `toSqlJsonPath should query ListRelationship object members`() {
        val uri = "urn:ngsi-ld:City:Paris"
        assertEquals(
            true,
            buildSql("""$INCOMING_TERM=="$uri"""").contains(
                existsWhere(incomingListRelationshipPath, """@ == $valuePh""", """{"value": "$uri"}""")
            )
        )
    }
}
