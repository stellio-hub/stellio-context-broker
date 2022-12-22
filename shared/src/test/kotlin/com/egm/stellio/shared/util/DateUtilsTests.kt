package com.egm.stellio.shared.util

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.time.ZonedDateTime

class DateUtilsTests {

    @Test
    fun `it should render a datetime with milliseconds at zero`() {
        val datetime = ZonedDateTime.parse("2020-12-26T10:54:00.000Z")
        assertEquals("2020-12-26T10:54:00Z", datetime.toNgsiLdFormat())
    }

    @Test
    fun `it should render a datetime with milliseconds`() {
        val datetime = ZonedDateTime.parse("2020-12-26T10:54:00.123Z")
        assertEquals("2020-12-26T10:54:00.123Z", datetime.toNgsiLdFormat())
    }

    @Test
    fun `it should render a datetime with nanoseconds`() {
        val datetime = ZonedDateTime.parse("2020-12-26T10:54:00.000111Z")
        assertEquals("2020-12-26T10:54:00.000111Z", datetime.toNgsiLdFormat())
    }

    @Test
    fun `it should render a datetime as a compliant NGSI-LD property`() {
        val datetime = ZonedDateTime.parse("2020-12-26T10:54:00.000Z")
        assertEquals(
            mapOf(
                "https://ontology.eglobalmark.com/apic#dateOfFirstBee" to mapOf(
                    "@type" to "https://uri.etsi.org/ngsi-ld/DateTime",
                    "@value" to "2020-12-26T10:54:00Z"
                )
            ),
            datetime.toExpandedDateTime(DATE_OF_FIRST_BEE_PROPERTY)
        )
    }

    @Test
    fun `it shoud return true if the dates or times are correct`() {
        assertTrue("2020-12-26T10:54:00.000Z".isDateTime())
        assertFalse("2020-12-26U10:54:00.000Z".isDateTime())

        assertTrue("2020-12-26".isDate())
        assertFalse("2020-12".isDate())

        assertTrue("10:54:00.000".isTime())
        assertFalse("54:00.000Z".isTime())
    }
}
