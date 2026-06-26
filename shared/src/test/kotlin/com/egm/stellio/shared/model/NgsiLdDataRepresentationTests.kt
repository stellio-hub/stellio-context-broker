package com.egm.stellio.shared.model

import com.egm.stellio.shared.model.NgsiLdDataRepresentation.Companion.parseRepresentations
import com.egm.stellio.shared.util.shouldFail
import com.egm.stellio.shared.util.shouldSucceedAndResult
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.springframework.http.MediaType
import org.springframework.test.context.ActiveProfiles
import org.springframework.util.LinkedMultiValueMap

@ActiveProfiles("test")
class NgsiLdDataRepresentationTests {

    @Test
    fun `parseRepresentations should use the format over the options when both exist`() {
        val queryParams = LinkedMultiValueMap<String, String>()
        queryParams.add("timerel", "after")
        queryParams.add("timeAt", "2025-01-03T07:45:24Z")
        queryParams.add("options", "simplified")
        queryParams.add("format", "normalized")

        val parsedRepresentation = parseRepresentations(queryParams, MediaType.APPLICATION_JSON)
            .shouldSucceedAndResult()

        assertEquals(AttributeRepresentation.NORMALIZED, parsedRepresentation.attributeRepresentation)
    }

    @Test
    fun `parseRepresentations should use the options when no format is given`() {
        val queryParams = LinkedMultiValueMap<String, String>()
        queryParams.add("timerel", "after")
        queryParams.add("timeAt", "2025-01-03T07:45:24Z")
        queryParams.add("options", "simplified")

        val parsedRepresentation = parseRepresentations(queryParams, MediaType.APPLICATION_JSON)
            .shouldSucceedAndResult()

        assertEquals(AttributeRepresentation.SIMPLIFIED, parsedRepresentation.attributeRepresentation)
    }

    @Test
    fun `parseRepresentations should parse the options query param with more than one value`() {
        val queryParams = LinkedMultiValueMap<String, String>()
        queryParams.add("timerel", "after")
        queryParams.add("timeAt", "2025-01-03T07:45:24Z")
        queryParams.add("options", "simplified")
        queryParams.add("options", "sysAttrs,audit")
        val parsedRepresentation = parseRepresentations(queryParams, MediaType.APPLICATION_JSON)
            .shouldSucceedAndResult()

        assertEquals(AttributeRepresentation.SIMPLIFIED, parsedRepresentation.attributeRepresentation)
        assertTrue(parsedRepresentation.includeSysAttrs)
    }

    @Test
    fun `parseRepresentations should use the first format query param`() {
        val queryParams = LinkedMultiValueMap<String, String>()
        queryParams.add("timerel", "after")
        queryParams.add("timeAt", "2025-01-03T07:45:24Z")
        queryParams.add("format", "simplified")
        queryParams.add("format", "normalized")

        val parsedRepresentation = parseRepresentations(queryParams, MediaType.APPLICATION_JSON)
            .shouldSucceedAndResult()

        assertEquals(AttributeRepresentation.SIMPLIFIED, parsedRepresentation.attributeRepresentation)
    }

    @Test
    fun `parseRepresentations should return an exception when the format query param is invalid`() {
        val queryParams = LinkedMultiValueMap<String, String>()
        queryParams.add("timerel", "after")
        queryParams.add("timeAt", "2025-01-03T07:45:24Z")
        queryParams.add("format", "invalid")

        parseRepresentations(queryParams, MediaType.APPLICATION_JSON).shouldFail {
            assertInstanceOf(InvalidRequestException::class.java, it)
            assertEquals("'invalid' is not a valid value for the format query parameter", it.message)
        }
    }

    @Test
    fun `parseRepresentations should return an exception when the options query param is invalid`() {
        val queryParams = LinkedMultiValueMap<String, String>()
        queryParams.add("timerel", "after")
        queryParams.add("timeAt", "2025-01-03T07:45:24Z")
        queryParams.add("options", "invalidOptions")

        parseRepresentations(queryParams, MediaType.APPLICATION_JSON).shouldFail {
            assertInstanceOf(InvalidRequestException::class.java, it)
            assertEquals("'invalidOptions' is not a valid value for the options query parameter", it.message)
        }
    }

    @Test
    fun `parseRepresentations should return an exception when one options value is invalid`() {
        val queryParams = LinkedMultiValueMap<String, String>()
        queryParams.add("timerel", "after")
        queryParams.add("timeAt", "2025-01-03T07:45:24Z")
        queryParams.add("options", "simplified,invalidOptions")

        parseRepresentations(queryParams, MediaType.APPLICATION_JSON).shouldFail {
            assertInstanceOf(InvalidRequestException::class.java, it)
            assertEquals("'invalidOptions' is not a valid value for the options query parameter", it.message)
        }
    }
}
