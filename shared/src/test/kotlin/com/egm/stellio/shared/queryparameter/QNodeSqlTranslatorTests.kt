package com.egm.stellio.shared.queryparameter

import com.egm.stellio.shared.model.JSONLD_ID_KW
import com.egm.stellio.shared.model.JSONLD_VALUE_KW
import com.egm.stellio.shared.model.NGSILD_JSONPROPERTY_JSON
import com.egm.stellio.shared.model.NGSILD_LANGUAGEPROPERTY_LANGUAGEMAP
import com.egm.stellio.shared.model.NGSILD_PROPERTY_VALUE
import com.egm.stellio.shared.model.NGSILD_RELATIONSHIP_OBJECT
import com.egm.stellio.shared.model.NGSILD_VOCABPROPERTY_VOCAB
import com.egm.stellio.shared.util.APIC_COMPOUND_CONTEXTS
import com.egm.stellio.shared.util.BEEHIVE_IRI
import com.egm.stellio.shared.util.INCOMING_IRI
import com.egm.stellio.shared.util.INCOMING_TERM
import com.egm.stellio.shared.util.TEMPERATURE_IRI
import com.egm.stellio.shared.util.TEMPERATURE_TERM
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
        """$."$INCOMING_IRI"."$NGSILD_RELATIONSHIP_OBJECT"."$JSONLD_ID_KW""""
    private val incomingVocabPath =
        """$."$INCOMING_IRI"."$NGSILD_VOCABPROPERTY_VOCAB"[*]."$JSONLD_ID_KW""""
    private val incomingLangMapPath =
        """$."$INCOMING_IRI"."$NGSILD_LANGUAGEPROPERTY_LANGUAGEMAP"[*]."$JSONLD_VALUE_KW""""
    private val incomingLangFilterPath =
        """$."$INCOMING_IRI"."$NGSILD_LANGUAGEPROPERTY_LANGUAGEMAP"[*]"""
    private val incomingJsonKeyPath =
        """$."$INCOMING_IRI"."$NGSILD_JSONPROPERTY_JSON"."$JSONLD_VALUE_KW"."$TEMPERATURE_TERM""""
    private val temperaturePropertyPath =
        """$."$TEMPERATURE_IRI"."$NGSILD_PROPERTY_VALUE"."$JSONLD_VALUE_KW""""

    private fun exists(path: String) =
        """jsonb_path_exists(entity_payload.payload, '$path')"""

    private fun existsWhere(path: String, filter: String, params: String) =
        """jsonb_path_exists(entity_payload.payload, '$path ? ($filter)', '$params')"""

    @Test
    fun `it should generate EXISTS check against entity_payload for a simple attribute`() {
        assertEquals(
            exists("""$."$INCOMING_IRI""""),
            buildSql(INCOMING_TERM)
        )
    }

    @Test
    fun `it should generate NOT EXISTS for negated attribute`() {
        assertEquals(
            """NOT (${exists("""$."$INCOMING_IRI"""")})""",
            buildSql("!$INCOMING_TERM")
        )
    }

    @Test
    fun `it should generate AND clause for semicolon operator`() {
        val incomingOpenSql =
            """(${existsWhere(incomingPropertyPath, """@ == $valuePh""", """{"value": "open"}""")} OR """ +
                """${existsWhere(incomingLangMapPath, """@ == $valuePh""", """{"value": "open"}""")})"""
        val temperatureGt0Sql = existsWhere(temperaturePropertyPath, """@ > $valuePh""", """{"value": 0}""")
        assertEquals(
            "($incomingOpenSql) AND ($temperatureGt0Sql)",
            buildSql("$INCOMING_TERM==\"open\";$TEMPERATURE_TERM>0")
        )
    }

    @Test
    fun `it should generate OR clause for pipe operator`() {
        val incomingOpenSql =
            """(${existsWhere(incomingPropertyPath, """@ == $valuePh""", """{"value": "open"}""")} OR """ +
                """${existsWhere(incomingLangMapPath, """@ == $valuePh""", """{"value": "open"}""")})"""
        val temperatureGt0Sql = existsWhere(temperaturePropertyPath, """@ > $valuePh""", """{"value": 0}""")
        assertEquals(
            "($incomingOpenSql) OR ($temperatureGt0Sql)",
            buildSql("$INCOMING_TERM==\"open\"|$TEMPERATURE_TERM>0")
        )
    }

    @Test
    fun `it should generate numeric comparison using hasValue`() {
        assertEquals(
            existsWhere(temperaturePropertyPath, """@ == $valuePh""", """{"value": 42}"""),
            buildSql("$TEMPERATURE_TERM==42")
        )
    }

    @Test
    fun `it should generate string comparison targeting both hasValue and language map`() {
        val expected = """(${existsWhere(incomingPropertyPath, """@ == $valuePh""", """{"value": "open"}""")} OR """ +
            """${existsWhere(incomingLangMapPath, """@ == $valuePh""", """{"value": "open"}""")})"""
        assertEquals(expected, buildSql("$INCOMING_TERM==\"open\""))
    }

    @Test
    fun `it should generate URI comparison targeting relationship, property, and vocab paths`() {
        val uri = "urn:ngsi-ld:BeeHive:001"
        val params = """{"value": "$uri"}"""
        val expected = """(${existsWhere(incomingRelPath, """@ == $valuePh""", params)} OR """ +
            """${existsWhere(incomingPropertyPath, """@ == $valuePh""", params)} OR """ +
            """${existsWhere(incomingVocabPath, """@ == $valuePh""", params)})"""
        assertEquals(expected, buildSql("""$INCOMING_TERM=="$uri""""))
    }

    @Test
    fun `it should generate range comparison`() {
        assertEquals(
            existsWhere(
                temperaturePropertyPath,
                """@ >= $minPh && @ <= $maxPh""",
                """{"min": 10, "max": 20}"""
            ),
            buildSql("$TEMPERATURE_TERM==10..20")
        )
    }

    @Test
    fun `it should generate boolean comparison`() {
        val activePath =
            """$."https://uri.etsi.org/ngsi-ld/default-context/active"."$NGSILD_PROPERTY_VALUE"."$JSONLD_VALUE_KW""""
        assertEquals(
            exists("""$activePath ? (@ == true)"""),
            buildSql("active==true")
        )
    }

    @Test
    fun `it should generate list comparison using inline filter`() {
        assertEquals(
            exists("""$temperaturePropertyPath ? (@ == 10 || @ == 20 || @ == 30)"""),
            buildSql("$TEMPERATURE_TERM==10,20,30")
        )
    }

    @Test
    fun `it should generate NEQ range as outside-bounds filter`() {
        assertEquals(
            existsWhere(
                temperaturePropertyPath,
                """@ < $minPh || @ > $maxPh""",
                """{"min": 10, "max": 20}"""
            ),
            buildSql("$TEMPERATURE_TERM!=10..20")
        )
    }

    @Test
    fun `it should generate language tag comparison using hasLanguageMap path`() {
        assertEquals(
            existsWhere(
                incomingLangFilterPath,
                """@."@language" == $langPh && @."@value" == $valuePh""",
                """{"lang": "en", "value": "hello"}"""
            ),
            buildSql("""$INCOMING_TERM[en]=="hello"""")
        )
    }

    @Test
    fun `it should not expand trailing path for jsonKeys attributes`() {
        assertEquals(
            existsWhere(incomingJsonKeyPath, """@ == $valuePh""", """{"value": 42}"""),
            buildSql("$INCOMING_TERM[$TEMPERATURE_TERM]==42", jsonKeys = setOf(INCOMING_TERM))
        )
    }

    @Test
    fun `it should expand comparison value for expandValues attributes`() {
        val params = """{"value": "$BEEHIVE_IRI"}"""
        val expected = """(${existsWhere(incomingRelPath, """@ == $valuePh""", params)} OR """ +
            """${existsWhere(incomingPropertyPath, """@ == $valuePh""", params)} OR """ +
            """${existsWhere(incomingVocabPath, """@ == $valuePh""", params)})"""
        assertEquals(expected, buildSql("$INCOMING_TERM==\"BeeHive\"", expandValues = setOf(INCOMING_TERM)))
    }

    @Test
    fun `it should generate like_regex comparison for LIKE_REGEX operator`() {
        val expected = """(${exists("""$incomingPropertyPath ? (@ like_regex "test.*")""")} OR """ +
            """${exists("""$incomingLangMapPath ? (@ like_regex "test.*")""")})"""
        assertEquals(expected, buildSql("""$INCOMING_TERM~="test.*""""))
    }

    @Test
    fun `it should generate NOT like_regex for NOT_LIKE_REGEX operator`() {
        val expected = """NOT ((${exists("""$incomingPropertyPath ? (@ like_regex "test.*")""")} OR """ +
            """${exists("""$incomingLangMapPath ? (@ like_regex "test.*")""")}))"""
        assertEquals(expected, buildSql("""$INCOMING_TERM!~="test.*""""))
    }
}
