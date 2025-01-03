package com.egm.stellio.shared.model

import com.egm.stellio.shared.model.NgsiLdDataRepresentation.Companion.parseRepresentations
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.springframework.http.MediaType
import org.springframework.test.context.ActiveProfiles
import org.springframework.util.LinkedMultiValueMap

@ActiveProfiles("test")
class NgsiLdDataRepresentationTest {

    @Test
    fun `it should return the attribute representation in the format query param when options exist`() {
        val queryParams = LinkedMultiValueMap<String, String>()
        queryParams.add("timerel", "after")
        queryParams.add("timeAt", "2025-01-03T07:45:24Z")
        queryParams.add("options", "simplified")
        queryParams.add("format", "normalized")

        val parsedRepresentation = parseRepresentations(queryParams, MediaType.APPLICATION_JSON)

        assertEquals(AttributeRepresentation.NORMALIZED, parsedRepresentation.attributeRepresentation)
    }

    @Test
    fun `it should return the attribute representation in the options query param when no format is given`() {
        val queryParams = LinkedMultiValueMap<String, String>()
        queryParams.add("timerel", "after")
        queryParams.add("timeAt", "2025-01-03T07:45:24Z")
        queryParams.add("options", "simplified")

        val parsedRepresentation = parseRepresentations(queryParams, MediaType.APPLICATION_JSON)

        assertEquals(AttributeRepresentation.SIMPLIFIED, parsedRepresentation.attributeRepresentation)
    }

    @Test
    fun `it should return the attribute representation in the first format query param`() {
        val queryParams = LinkedMultiValueMap<String, String>()
        queryParams.add("timerel", "after")
        queryParams.add("timeAt", "2025-01-03T07:45:24Z")
        queryParams.add("format", "simplified")
        queryParams.add("format", "normalized")

        val parsedRepresentation = parseRepresentations(queryParams, MediaType.APPLICATION_JSON)

        assertEquals(AttributeRepresentation.SIMPLIFIED, parsedRepresentation.attributeRepresentation)
    }
}
