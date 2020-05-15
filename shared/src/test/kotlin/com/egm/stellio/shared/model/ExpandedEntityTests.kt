package com.egm.stellio.shared.model

import com.egm.stellio.shared.util.NgsiLdParsingUtils
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class ExpandedEntityTests {

    @Test
    fun `it should get relationships from ExpandedEntity`() {
        val expandedEntity = NgsiLdParsingUtils.parseEntity(
            "{\n" +
                    "  \"id\": \"urn:ngsi-ld:Vehicle:A12388\",\n" +
                    "  \"type\": \"Vehicle\",\n" +
                    "  \"connectsTo\": {\n" +
                    "    \"type\": \"Relationship\",\n" +
                    "    \"object\": \"relation1\"\n" +
                    "  },\n" +
                    "  \"@context\": [\n" +
                    "      \"https://schema.lab.fiware.org/ld/context\",\n" +
                    "      \"http://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld\"\n" +
                    "  ]\n" +
                    "}",
            listOf()
        )

        assertEquals(arrayListOf("relation1"), expandedEntity.getRelationships())
    }

    @Test
    fun `it should get relationships of properties from ExpandedEntity`() {
        val expandedEntity = NgsiLdParsingUtils.parseEntity(
            "{\r\n" +
                    " \"id\": \"urn:ngsi-ld:Vehicle:A12388\",\r\n " +
                    " \"type\": \"Vehicle\",\r\n " +
                    " \"connectsTo\": {\n" +
                    "   \"type\": \"Relationship\",\n" +
                    "   \"object\": \"relation1\"\n" +
                    " },\n" +
                    " \"speed\": {\r\n    " +
                    "   \"type\": \"Property\",\r\n " +
                    "   \"value\": 35,\r\n " +
                    "   \"flashedFrom\": {\r\n " +
                    "     \"type\": \"Relationship\",\r\n " +
                    "     \"object\": \"Radar\"\r\n " +
                    "   }\r\n  },\r\n " +
                    " \"@context\": [\r\n " +
                    "   \"https://schema.lab.fiware.org/ld/context\",\r\n " +
                    "   \"http://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld\"\r\n " +
                    " ]\r\n" +
                    "}",
            listOf()
        )

        assertEquals(listOf("Radar", "relation1"), expandedEntity.getRelationships())
    }

    @Test
    fun `it should get relationships of Relations from ExpandedEntity`() {
        val expandedEntity = NgsiLdParsingUtils.parseEntity(
            "{\r\n" +
                    " \"id\": \"urn:ngsi-ld:Vehicle:A12388\",\r\n " +
                    " \"type\": \"Vehicle\",\r\n " +
                    " \"connectsTo\": {\n" +
                    "   \"type\": \"Relationship\",\n" +
                    "   \"object\": \"relation1\",\n" +
                    "     \"createdBy\": {\n" +
                    "       \"type\": \"Relationship\",\n" +
                    "       \"object\": \"relation2\"\n" +
                    "     }\n" +
                    " },\n" +
                    " \"@context\": [\r\n " +
                    "   \"https://schema.lab.fiware.org/ld/context\",\r\n " +
                    "   \"http://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld\"\r\n " +
                    " ]\r\n" +
                    "}",
            listOf()
        )

        assertEquals(listOf("relation1", "relation2"), expandedEntity.getRelationships())
    }
}