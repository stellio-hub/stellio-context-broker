package com.egm.stellio.shared.util

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.ZonedDateTime

class DateUtilsTests {

    @Test
    fun `it should correctly render a datetime with milliseconds at zero`() {
        val datetime = ZonedDateTime.parse("2020-12-26T10:54:00.000Z")
        assertEquals("2020-12-26T10:54:00Z", datetime.toNgsiLdFormat())
    }

    @Test
    fun `it should correctly render a datetime with milliseconds`() {
        val datetime = ZonedDateTime.parse("2020-12-26T10:54:00.123Z")
        assertEquals("2020-12-26T10:54:00.123Z", datetime.toNgsiLdFormat())
    }

    @Test
    fun `it should correctly render a datetime with nanoseconds`() {
        val datetime = ZonedDateTime.parse("2020-12-26T10:54:00.000111Z")
        assertEquals("2020-12-26T10:54:00.000111Z", datetime.toNgsiLdFormat())
    }

    @Test
    fun `it should correctly parse period and duration`() {
        val aggrPeriodDuration = "P1YT1M"
        assertEquals("1 years 0 months 0 days 0 hours 1 minutes 0 seconds", periodDurationToTimeBucket(aggrPeriodDuration))
    }

    @Test
    fun `it should correctly parse period`() {
        val aggrPeriodDuration = "P1Y"
        assertEquals("1 years 0 months 0 days", periodDurationToTimeBucket(aggrPeriodDuration))
    }
}
