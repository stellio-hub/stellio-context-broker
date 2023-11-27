package com.egm.stellio.shared.util

import com.egm.stellio.shared.model.BadRequestDataException
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class UriUtilsTests {

    @Test
    fun `it should throw a BadRequestData exception if input string is not an absolute URI`() {
        val uri = "justAString"

        val exception = assertThrows<BadRequestDataException> {
            uri.toUri()
        }
        Assertions.assertEquals(
            "The supplied identifier was expected to be an URI but it is not: justAString",
            exception.message
        )
    }

    @Test
    fun `it should throw a BadRequestData exception if input string has an invalid syntax`() {
        val uri = "https://just\\AString"

        val exception = assertThrows<BadRequestDataException> {
            uri.toUri()
        }
        Assertions.assertEquals(
            "The supplied identifier was expected to be an URI but it is not: https://just\\AString " +
                "(cause was: java.net.URISyntaxException: Illegal character in authority at index 12: " +
                "https://just\\AString)",
            exception.message
        )
    }
}
