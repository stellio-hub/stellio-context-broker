package com.egm.stellio.shared.util

import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.JSONLD_ID_KW
import com.egm.stellio.shared.model.JSONLD_TYPE_KW
import com.egm.stellio.shared.model.NGSILD_ID_TERM
import com.egm.stellio.shared.model.NGSILD_SCOPE_IRI
import com.egm.stellio.shared.model.NGSILD_SCOPE_TERM
import com.egm.stellio.shared.model.NGSILD_TYPE_TERM
import com.egm.stellio.shared.queryparameter.QueryParameter
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.springframework.util.LinkedMultiValueMap

class ApiUtilsPickOmitTests {

    @Test
    fun `parseAndExpandPickOmitParameters should return empty sets when no pick or omit parameters are provided`() {
        val (pick, omit) = parseAndExpandPickOmitParameters(null, null, APIC_COMPOUND_CONTEXTS)
            .shouldSucceedAndResult()

        assertThat(pick).isEmpty()
        assertThat(omit).isEmpty()
    }

    @Test
    fun `parseAndExpandPickOmitParameters should parse pick and omit parameters correctly`() {
        val (pick, omit) = parseAndExpandPickOmitParameters(
            "${TEMPERATURE_TERM},${NGSILD_ID_TERM}",
            "${INCOMING_TERM},${NGSILD_TYPE_TERM}",
            APIC_COMPOUND_CONTEXTS
        ).shouldSucceedAndResult()

        assertEquals(setOf(TEMPERATURE_IRI, JSONLD_ID_KW), pick)
        assertEquals(setOf(INCOMING_IRI, JSONLD_TYPE_KW), omit)
    }

    @Test
    fun `parseAndExpandPickOmitParameters should handle reserved names correctly`() {
        val (pick, _) = parseAndExpandPickOmitParameters(
            "${NGSILD_ID_TERM},${NGSILD_TYPE_TERM},${NGSILD_SCOPE_TERM}",
            null,
            APIC_COMPOUND_CONTEXTS
        ).shouldSucceedAndResult()

        assertEquals(setOf(JSONLD_ID_KW, JSONLD_TYPE_KW, NGSILD_SCOPE_IRI), pick)
    }

    @Test
    fun `parseAndExpandPickOmitParameters should reject empty pick parameter`() {
        parseAndExpandPickOmitParameters("", null, APIC_COMPOUND_CONTEXTS).shouldFailWith {
            it is BadRequestDataException &&
                it.message == "The 'pick' parameter cannot be empty"
        }
    }

    @Test
    fun `parseAndExpandPickOmitParameters should reject invalid attribute names`() {
        val queryParams = LinkedMultiValueMap<String, String>()
        queryParams.add(QueryParameter.PICK.key, "invalid%,temperature")

        parseAndExpandPickOmitParameters("invalid%,temperature", null, APIC_COMPOUND_CONTEXTS).shouldFailWith {
            it is BadRequestDataException &&
                it.message == "The JSON-LD object contains a member with invalid characters (4.6.2): invalid%"
        }
    }
}
