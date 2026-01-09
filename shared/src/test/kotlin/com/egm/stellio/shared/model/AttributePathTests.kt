package com.egm.stellio.shared.model

import com.egm.stellio.shared.queryparameter.AttributePath
import com.egm.stellio.shared.util.APIC_COMPOUND_CONTEXTS
import com.egm.stellio.shared.util.INCOMING_IRI
import com.egm.stellio.shared.util.INCOMING_TERM
import com.egm.stellio.shared.util.NAME_IRI
import com.egm.stellio.shared.util.NAME_TERM
import com.egm.stellio.shared.util.TEMPERATURE_IRI
import com.egm.stellio.shared.util.TEMPERATURE_TERM
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import java.util.stream.Stream

class AttributePathTests {

    companion object {

        @JvmStatic
        fun attributePathProvider(): Stream<Arguments> {
            return Stream.of(
                Arguments.of(
                    INCOMING_TERM,
                    listOf(INCOMING_IRI),
                    emptyList<ExpandedTerm>()
                ),
                Arguments.of(
                    "$INCOMING_TERM.$NGSILD_MODIFIED_AT_TERM",
                    listOf(INCOMING_IRI, NGSILD_MODIFIED_AT_IRI),
                    emptyList<ExpandedTerm>()
                ),
                Arguments.of(
                    "$INCOMING_TERM[$TEMPERATURE_TERM]",
                    listOf(INCOMING_IRI),
                    listOf(TEMPERATURE_IRI)
                ),
                Arguments.of(
                    "$INCOMING_TERM[$TEMPERATURE_TERM.$NAME_TERM]",
                    listOf(INCOMING_IRI),
                    listOf(TEMPERATURE_IRI, NAME_IRI)
                ),
            )
        }
    }

    @ParameterizedTest
    @MethodSource("com.egm.stellio.shared.model.AttributePathTests#attributePathProvider")
    fun `it should correctly parse an AttributePath`(
        term: String,
        expectedMainPath: List<ExpandedTerm>,
        expectedTrailingPath: List<ExpandedTerm> = emptyList()
    ) = runTest {
        val queryTerm = AttributePath(term, APIC_COMPOUND_CONTEXTS)
        assertEquals(expectedMainPath, queryTerm.mainPath)
        assertEquals(expectedTrailingPath, queryTerm.trailingPath)
    }
}
