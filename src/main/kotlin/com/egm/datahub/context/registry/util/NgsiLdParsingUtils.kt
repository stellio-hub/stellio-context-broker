package com.egm.datahub.context.registry.util

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.github.jsonldjava.core.JsonLdOptions
import com.github.jsonldjava.core.JsonLdProcessor
import com.github.jsonldjava.utils.JsonUtils
import org.slf4j.LoggerFactory

object NgsiLdParsingUtils {

    const val NGSILD_CORE_CONTEXT = "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"

    const val NGSILD_PROPERTY_TYPE = "https://uri.etsi.org/ngsi-ld/Property"
    const val NGSILD_PROPERTY_VALUE = "https://uri.etsi.org/ngsi-ld/hasValue"
    const val NGSILD_GEOPROPERTY_TYPE = "https://uri.etsi.org/ngsi-ld/GeoProperty"
    const val NGSILD_GEOPROPERTY_VALUE = "https://uri.etsi.org/ngsi-ld/hasValue"
    const val NGSILD_RELATIONSHIP_TYPE = "https://uri.etsi.org/ngsi-ld/Relationship"
    const val NGSILD_RELATIONSHIP_HAS_OBJECT = "https://uri.etsi.org/ngsi-ld/hasObject"

    const val NGSILD_ENTITY_ID = "@id"
    const val NGSILD_ENTITY_TYPE = "@type"

    const val NGSILD_CREATED_AT_PROPERTY = "https://uri.etsi.org/ngsi-ld/createdAt"
    const val NGSILD_MODIFIED_AT_PROPERTY = "https://uri.etsi.org/ngsi-ld/modifiedAt"
    const val NGSILD_OBSERVED_AT_PROPERTY = "https://uri.etsi.org/ngsi-ld/observedAt"
    const val NGSILD_UNIT_CODE_PROPERTY = "https://uri.etsi.org/ngsi-ld/unitCode"
    const val NGSILD_LOCATION_PROPERTY = "https://uri.etsi.org/ngsi-ld/location"
    const val NGSILD_COORDINATES_PROPERTY = "https://uri.etsi.org/ngsi-ld/coordinates"

    const val EGM_OBSERVED_BY = "https://ontology.eglobalmark.com/egm#observedBy"

    private val logger = LoggerFactory.getLogger(javaClass)

    fun parseEntity(input: String): Pair<Map<String, Any>, List<String>> {
        val expandedEntity = JsonLdProcessor.expand(JsonUtils.fromInputStream(input.byteInputStream()))[0]

        val expandedResult = JsonUtils.toPrettyString(expandedEntity)
        logger.debug("Expanded entity is $expandedResult")

        // TODO find a way to avoid this extra parsing
        val mapper = jacksonObjectMapper()
        val parsedInput: Map<String, Any> = mapper.readValue(input, mapper.typeFactory.constructMapLikeType(
            Map::class.java, String::class.java, Any::class.java
        ))

        // Not currently used but it could be useful later if we want to improve / optimize the parsing logic
        // and / or the way data is persisted
        // For instance, for each entity / property / relationship, we could store the compact and expanded name
        // for more efficient queries (e.g. no need to expand every parameter)
//        val compactedEntity = JsonLdProcessor.compact(JsonUtils.fromInputStream(input.byteInputStream()),
//            parsedInput["@context"], JsonLdOptions()
//        )
//        logger.debug("Compacted entity is ${JsonUtils.toPrettyString(compactedEntity)}")
//
//        val flattenedEntity = JsonLdProcessor.flatten(JsonUtils.fromInputStream(input.byteInputStream()),
//            parsedInput["@context"], JsonLdOptions())
//        logger.debug("Flattened entity is ${JsonUtils.toPrettyString(flattenedEntity)}")

        return Pair(expandedEntity as Map<String, Any>, parsedInput["@context"] as List<String>)
    }

    fun parseJsonLdFragment(input: String): Map<String, Any> {
        val mapper = jacksonObjectMapper()
        return mapper.readValue(input, mapper.typeFactory.constructMapLikeType(
            Map::class.java, String::class.java, Any::class.java
        ))
    }

    fun expandValueAsMap(value: Any): Map<String, List<Any>> =
        (value as List<Any>)[0] as Map<String, List<Any>>

    /**
     * Expects an JSON-LD expanded fragment like this :
     *
     * [{
     *    https://uri.etsi.org/ngsi-ld/hasObject=[{
     *      @id=urn:ngsi-ld:FishContainment:1234
     *    }],
     *    @type=[
     *      https://uri.etsi.org/ngsi-ld/Relationship
     *    ]
     *  }]
     *
     *  @return the raw value of the #propertyKey (typically a map or a string)
     */
    fun getRawPropertyValueFromList(value: Any, propertyKey: String): Any =
        (expandValueAsMap(value)[propertyKey]!!)[0]

    /**
     * Extract the actual value (@value) of a given property from the properties map of an expanded property.
     *
     * @param value a map similar to:
     * {
     *   https://uri.etsi.org/ngsi-ld/hasValue=[{
     *     @type=[https://uri.etsi.org/ngsi-ld/Property],
     *     @value=250
     *   }],
     *   https://uri.etsi.org/ngsi-ld/unitCode=[{
     *     @value=kg
     *   }],
     *   https://uri.etsi.org/ngsi-ld/observedAt=[{
     *     @value=2019-12-18T10:45:44.248755+01:00
     *   }]
     * }
     *
     * @return the actual value, e.g. "kg" if provided #propertyKey is https://uri.etsi.org/ngsi-ld/unitCode
     */
    fun getPropertyValueFromMap(value: Map<String, List<Any>>, propertyKey: String): Any? =
        if (value[propertyKey] != null) {
            val intermediateList = value[propertyKey] as List<Map<String, Any>>
            val firstListEntry = intermediateList[0]
            val finalValue = firstListEntry["@value"]
            finalValue
        } else
            null

    fun extractShortTypeFromPayload(payload: Map<String, Any>): String =
        // TODO is it always after a '/' ? can't it be after a '#' ? (https://redmine.eglobalmark.com/issues/852)
        // TODO do a clean implementation using info from @context
        (payload["@type"] as List<String>)[0].substringAfterLast("/").substringAfterLast("#")

    fun isOfKind(values: Map<String, List<Any>>, kind: String): Boolean =
        values.containsKey("@type") &&
            values["@type"] is List<*> &&
            values.getOrElse("@type") { emptyList() }[0] == kind

    fun expandRelationshipType(relationship: Map<String, Map<String, Any>>, contexts: List<String>): String {
        val jsonLdOptions = JsonLdOptions()
        jsonLdOptions.expandContext = mapOf("@context" to contexts)
        val expKey = JsonLdProcessor.expand(relationship, jsonLdOptions)
        return (expKey[0] as Map<String, Any>).keys.first()
    }

    fun isTypeResolvable(type: String, context: String): Boolean {
        return expandJsonLdKey(type, context) != null
    }

    fun expandJsonLdKey(type: String, context: String): String? =
        expandJsonLdKey(type, listOf(context))

    fun expandJsonLdKey(type: String, contexts: List<String>): String? {
        val jsonLdOptions = JsonLdOptions()
        jsonLdOptions.expandContext = mapOf("@context" to contexts)
        val expandedType = JsonLdProcessor.expand(mapOf(type to mapOf<String, Any>()), jsonLdOptions)
        logger.debug("Expanded type $type to $expandedType")
        return if (expandedType.isNotEmpty())
            (expandedType[0] as Map<String, Any>).keys.first()
        else
            null
    }

    fun expandJsonLdFragment(fragment: String, context: String): Map<String, Any> {
        val jsonLdOptions = JsonLdOptions()
        jsonLdOptions.expandContext = mapOf("@context" to listOf(context, NGSILD_CORE_CONTEXT))
        val expandedFragment = JsonLdProcessor.expand(JsonUtils.fromInputStream(fragment.byteInputStream()), jsonLdOptions)
        logger.debug("Expanded fragment $fragment to $expandedFragment")
        return expandedFragment[0] as Map<String, Any>
    }

    fun getTypeFromURI(uri: String): String {
        return uri.split(":")[2]
    }
}

fun String.extractShortTypeFromExpanded(): String =
    // TODO is it always after a '/' ? can't it be after a '#' ? (https://redmine.eglobalmark.com/issues/852)
    // TODO do a clean implementation using info from @context
    this.substringAfterLast("/").substringAfterLast("#")

fun String.toRelationshipTypeName(): String =
    this.extractShortTypeFromExpanded()
        .map {
            if (it.isUpperCase()) "_$it"
            else "${it.toUpperCase()}"
        }.joinToString("")

fun String.toNgsiLdRelationshipKey(): String =
    this.mapIndexed { index, c ->
        if (index > 1 && this[index - 1] == '_') "$c"
        else if (c == '_') ""
        else "${c.toLowerCase()}"
    }.joinToString("")
