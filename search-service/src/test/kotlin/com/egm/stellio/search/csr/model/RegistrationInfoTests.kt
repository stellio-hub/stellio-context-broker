package com.egm.stellio.search.csr.model

import com.egm.stellio.shared.util.APIC_COMPOUND_CONTEXTS
import com.egm.stellio.shared.util.MANAGED_BY_COMPACT_RELATIONSHIP
import com.egm.stellio.shared.util.MANAGED_BY_RELATIONSHIP
import com.egm.stellio.shared.util.NGSILD_NAME_PROPERTY
import com.egm.stellio.shared.util.NGSILD_NAME_TERM
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class RegistrationInfoTests {

    @Test
    fun `getAttributesName should merge propertyNames and relationshipNames`() = runTest {
        val information = RegistrationInfo(
            propertyNames = listOf(NGSILD_NAME_PROPERTY, MANAGED_BY_RELATIONSHIP),
            relationshipNames = listOf(MANAGED_BY_RELATIONSHIP)
        )

        val attrs = information.getAttributeNames()
        assertThat(attrs).hasSize(2)
        assertThat(attrs).contains(NGSILD_NAME_PROPERTY, MANAGED_BY_RELATIONSHIP)
    }

    @Test
    fun `getAttributesName should keep propertyNames if relationShipNames is null`() = runTest {
        val information = RegistrationInfo(
            propertyNames = null,
            relationshipNames = listOf(MANAGED_BY_RELATIONSHIP)
        )

        val attrs = information.getAttributeNames()
        assertEquals(attrs, setOf(MANAGED_BY_RELATIONSHIP))
    }

    @Test
    fun `getAttributesName should keep relationShipNames if propertyNames is null`() = runTest {
        val information = RegistrationInfo(
            propertyNames = listOf(MANAGED_BY_RELATIONSHIP),
            relationshipNames = null
        )

        val attrs = information.getAttributeNames()
        assertEquals(attrs, setOf(MANAGED_BY_RELATIONSHIP))
    }

    @Test
    fun `getAttributesName should return null if propertyNames and relationshipNames are null`() = runTest {
        val information = RegistrationInfo(
            propertyNames = null,
            relationshipNames = null
        )

        val attrs = information.getAttributeNames()
        assertEquals(attrs, null)
    }

    @Test
    fun `computeAttrsQueryParam should intersect the csf and the registration attributes`() = runTest {
        val registrationInfo = RegistrationInfo(
            propertyNames = listOf(MANAGED_BY_RELATIONSHIP, NGSILD_NAME_PROPERTY)
        )
        val csrFilters = CSRFilters(attrs = setOf(NGSILD_NAME_PROPERTY))

        val attrs = registrationInfo.computeAttrsQueryParam(csrFilters, APIC_COMPOUND_CONTEXTS)
        assertEquals(NGSILD_NAME_TERM, attrs)
    }

    @Test
    fun `computeAttrsQueryParam should return the registration attributes if the csf is empty`() = runTest {
        val registrationInfo = RegistrationInfo(
            propertyNames = listOf(MANAGED_BY_RELATIONSHIP, NGSILD_NAME_PROPERTY)
        )
        val csrFilters = CSRFilters()

        val attrs = registrationInfo.computeAttrsQueryParam(csrFilters, APIC_COMPOUND_CONTEXTS)
        assertEquals("$MANAGED_BY_COMPACT_RELATIONSHIP,$NGSILD_NAME_TERM", attrs)
    }

    @Test
    fun `computeAttrsQueryParam should return the csf attributes if the registration have no attributes`() = runTest {
        val registrationInfo = RegistrationInfo()
        val csrFilters = CSRFilters(attrs = setOf(NGSILD_NAME_PROPERTY))

        val attrs = registrationInfo.computeAttrsQueryParam(csrFilters, APIC_COMPOUND_CONTEXTS)
        assertEquals(NGSILD_NAME_TERM, attrs)
    }
}
