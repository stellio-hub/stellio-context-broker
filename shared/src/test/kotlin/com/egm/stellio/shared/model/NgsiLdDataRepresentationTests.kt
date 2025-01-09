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
    fun `it should return the attribute representation from the format query param when both format and options exist`() {
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
    fun `it should return the attribute representation in the options query param when no format is given`() {
        val queryParams = LinkedMultiValueMap<String, String>()
        queryParams.add("timerel", "after")
        queryParams.add("timeAt", "2025-01-03T07:45:24Z")
        queryParams.add("options", "simplified")

        val parsedRepresentation = parseRepresentations(queryParams, MediaType.APPLICATION_JSON)
            .shouldSucceedAndResult()

        assertEquals(AttributeRepresentation.SIMPLIFIED, parsedRepresentation.attributeRepresentation)
    }

    @Test
    fun `it should correctly parse the options query param when it contains more than one value`() {
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
    fun `it should return the attribute representation in the first format query param`() {
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
    fun `it should return an exception if format query param is invalid`() {
        val queryParams = LinkedMultiValueMap<String, String>()
        queryParams.add("timerel", "after")
        queryParams.add("timeAt", "2025-01-03T07:45:24Z")
        queryParams.add("format", "invalid")

        parseRepresentations(queryParams, MediaType.APPLICATION_JSON).shouldFail {
            assertInstanceOf(InvalidRequestException::class.java, it)
            assertEquals("'invalid' is not a valid format value", it.message)
        }
    }

    @Test
    fun `it should return an exception if options query param is invalid`() {
        val queryParams = LinkedMultiValueMap<String, String>()
        queryParams.add("timerel", "after")
        queryParams.add("timeAt", "2025-01-03T07:45:24Z")
        queryParams.add("options", "invalidOptions")

        parseRepresentations(queryParams, MediaType.APPLICATION_JSON).shouldFail {
            assertInstanceOf(InvalidRequestException::class.java, it)
            assertEquals("'invalidOptions' is not a valid options value", it.message)
        }
    }

    @Test
    fun `it should return an exception if at least one value in options query param is invalid`() {
        val queryParams = LinkedMultiValueMap<String, String>()
        queryParams.add("timerel", "after")
        queryParams.add("timeAt", "2025-01-03T07:45:24Z")
        queryParams.add("options", "simplified,invalidOptions")

        parseRepresentations(queryParams, MediaType.APPLICATION_JSON).shouldFail {
            assertInstanceOf(InvalidRequestException::class.java, it)
            assertEquals("'invalidOptions' is not a valid options value", it.message)
        }
    }
}
