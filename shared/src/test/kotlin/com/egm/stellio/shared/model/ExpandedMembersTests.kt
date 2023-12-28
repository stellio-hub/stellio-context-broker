package com.egm.stellio.shared.model

import com.egm.stellio.shared.util.JsonLdUtils
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_CREATED_AT_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_MODIFIED_AT_PROPERTY
import com.egm.stellio.shared.util.ngsiLdDateTime
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class ExpandedMembersTests {

    @Test
    fun `it should add createdAt information into an attribute`() {
        val attrPayload = mapOf("attribute" to JsonLdUtils.buildExpandedPropertyValue(12.0))

        val attrPayloadWithSysAttrs = attrPayload.addSysAttrs(true, ngsiLdDateTime(), null)

        assertThat(attrPayloadWithSysAttrs)
            .containsKey(NGSILD_CREATED_AT_PROPERTY)
            .doesNotContainKey(NGSILD_MODIFIED_AT_PROPERTY)
    }

    @Test
    fun `it should add createdAt and modifiedAt information into an attribute`() {
        val attrPayload = mapOf("attribute" to JsonLdUtils.buildExpandedPropertyValue(12.0))

        val attrPayloadWithSysAttrs = attrPayload.addSysAttrs(true, ngsiLdDateTime(), ngsiLdDateTime())

        assertThat(attrPayloadWithSysAttrs)
            .containsKey(NGSILD_CREATED_AT_PROPERTY)
            .containsKey(NGSILD_MODIFIED_AT_PROPERTY)
    }
}
