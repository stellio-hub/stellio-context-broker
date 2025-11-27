package com.egm.stellio.shared.util

import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.NGSILD_ID_TERM
import com.egm.stellio.shared.model.NGSILD_SCOPE_TERM
import com.egm.stellio.shared.model.NGSILD_TYPE_TERM
import com.egm.stellio.shared.queryparameter.QueryParameter
import com.egm.stellio.shared.util.ApiUtils.parsePickOmitParameters
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.springframework.util.LinkedMultiValueMap

class ApiUtilsPickOmitTests {

    @Test
    fun `parsePickOmitParameters should return empty sets when no pick or omit parameters are provided`() {
        val (pick, omit) = parsePickOmitParameters(null, null)
            .shouldSucceedAndResult()

        assertThat(pick).isEmpty()
        assertThat(omit).isEmpty()
    }

    @Test
    fun `parsePickOmitParameters should parse pick and omit parameters correctly`() {
        val (pick, omit) = parsePickOmitParameters(
            "${TEMPERATURE_TERM},${NGSILD_ID_TERM}",
            "${INCOMING_TERM},${NGSILD_TYPE_TERM}"
        ).shouldSucceedAndResult()

        assertEquals(setOf(TEMPERATURE_TERM, NGSILD_ID_TERM), pick)
        assertEquals(setOf(INCOMING_TERM, NGSILD_TYPE_TERM), omit)
    }

    @Test
    fun `parsePickOmitParameters should handle reserved names correctly`() {
        val (pick, _) = parsePickOmitParameters(
            "${NGSILD_ID_TERM},${NGSILD_TYPE_TERM},${NGSILD_SCOPE_TERM}",
            null
        ).shouldSucceedAndResult()

        assertEquals(setOf(NGSILD_ID_TERM, NGSILD_TYPE_TERM, NGSILD_SCOPE_TERM), pick)
    }

    @Test
    fun `parseAndExpandPickOmitParameters should reject empty pick parameter`() {
        parsePickOmitParameters("", null).shouldFailWith {
            it is BadRequestDataException &&
                it.message == "The 'pick' parameter cannot be empty"
        }
    }

    @Test
    fun `parseAndExpandPickOmitParameters should reject invalid attribute names`() {
        val queryParams = LinkedMultiValueMap<String, String>()
        queryParams.add(QueryParameter.PICK.key, "invalid%,temperature")

        parsePickOmitParameters("invalid%,temperature", null).shouldFailWith {
            it is BadRequestDataException &&
                it.message == "The JSON-LD object contains a member with invalid characters (4.6.2): invalid%"
        }
    }
}
