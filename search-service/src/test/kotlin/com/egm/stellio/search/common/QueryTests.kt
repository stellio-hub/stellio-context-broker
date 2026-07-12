package com.egm.stellio.search.common

import com.egm.stellio.search.common.model.Query
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import com.egm.stellio.shared.util.shouldFail
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertInstanceOf

class QueryTests {

    @Test
    fun `Query constructor should not validate a Query if the type is not correct`() {
        val query = """
            {
                "type": "NotAQuery",
                "attrs": ["attr1", "attr2"]
            }
        """.trimIndent()

        Query(query.deserializeAsMap()).shouldFail {
            assertInstanceOf<BadRequestDataException>(it)
            assertEquals("Type must be 'Query'", it.message)
        }
    }

    @Test
    fun `Query constructor should not validate a Query with unexpected parameters`() {
        val query = """
            {
                "type": "Query",
                "property": "anUnexpectedProperty"
            }
        """.trimIndent()

        Query(query.deserializeAsMap()).shouldFail {
            assertInstanceOf<BadRequestDataException>(it)
            assertThat(it.message).startsWith("Query could not be parsed:")
        }
    }

    @Test
    fun `Query constructor should not validate a Query if an entity selector has no type member`() {
        val query = """
            {
                "type": "Query",
                "entities": [
                    {
                        "idPattern": "urn:ngsi-ld:BeeHive:*"
                    }
                ]
            }
        """.trimIndent()

        Query(query.deserializeAsMap()).shouldFail {
            assertInstanceOf<BadRequestDataException>(it)
            assertThat(it.message).startsWith("Query could not be parsed:")
        }
    }
}
