package com.egm.stellio.shared.util

import com.egm.stellio.shared.util.OptionsParamValue.TEMPORAL_VALUES
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import java.util.*

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = [ ApiUtils::class ])
@ActiveProfiles("test")
class ApiUtilsTests {

    @Test
    fun `it should not find a value if there is no options query param`() {
        assertFalse(hasValueInOptionsParam(Optional.empty(), TEMPORAL_VALUES))
    }

    @Test
    fun `it should not find a value if it is not in a single value options query param`() {
        assertFalse(hasValueInOptionsParam(Optional.of("one"), TEMPORAL_VALUES))
    }

    @Test
    fun `it should not find a value if it is not in a multi value options query param`() {
        assertFalse(hasValueInOptionsParam(Optional.of("one,two"), TEMPORAL_VALUES))
    }

    @Test
    fun `it should find a value if it is in a single value options query param`() {
        assertTrue(hasValueInOptionsParam(Optional.of("temporalValues"), TEMPORAL_VALUES))
    }
    @Test
    fun `it should find a value if it is in a multi value options query param`() {
        assertTrue(hasValueInOptionsParam(Optional.of("one,temporalValues"), TEMPORAL_VALUES))
    }
}
