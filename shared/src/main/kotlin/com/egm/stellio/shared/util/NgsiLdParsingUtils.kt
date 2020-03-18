package com.egm.stellio.shared.util

import com.egm.stellio.shared.model.Observation
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.InvalidNgsiLdPayloadException
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.github.jsonldjava.core.JsonLdOptions
import com.github.jsonldjava.core.JsonLdProcessor
import com.github.jsonldjava.utils.JsonUtils
import org.slf4j.LoggerFactory
import java.time.OffsetDateTime
import kotlin.reflect.full.safeCast

data class AttributeType(val uri: String)

object NgsiLdParsingUtils {

    const val NGSILD_CORE_CONTEXT = "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"

    val NGSILD_PROPERTY_TYPE = AttributeType("https://uri.etsi.org/ngsi-ld/Property")
    const val NGSILD_PROPERTY_VALUE = "https://uri.etsi.org/ngsi-ld/hasValue"
    val NGSILD_GEOPROPERTY_TYPE = AttributeType("https://uri.etsi.org/ngsi-ld/GeoProperty")
    const val NGSILD_GEOPROPERTY_VALUE = "https://uri.etsi.org/ngsi-ld/hasValue"
    val NGSILD_RELATIONSHIP_TYPE = AttributeType("https://uri.etsi.org/ngsi-ld/Relationship")
    const val NGSILD_RELATIONSHIP_HAS_OBJECT = "https://uri.etsi.org/ngsi-ld/hasObject"

    // TODO they are actually JSON-LD keywords, not specific to NGSI_LD, rename them
    const val NGSILD_ENTITY_ID = "@id"
    const val NGSILD_ENTITY_TYPE = "@type"

    const val JSONLD_VALUE_KW = "@value"

    const val NGSILD_CREATED_AT_PROPERTY = "https://uri.etsi.org/ngsi-ld/createdAt"
    const val NGSILD_MODIFIED_AT_PROPERTY = "https://uri.etsi.org/ngsi-ld/modifiedAt"
    const val NGSILD_OBSERVED_AT_PROPERTY = "https://uri.etsi.org/ngsi-ld/observedAt"
    const val NGSILD_UNIT_CODE_PROPERTY = "https://uri.etsi.org/ngsi-ld/unitCode"
    const val NGSILD_LOCATION_PROPERTY = "https://uri.etsi.org/ngsi-ld/location"
    const val NGSILD_COORDINATES_PROPERTY = "https://uri.etsi.org/ngsi-ld/coordinates"

    const val NGSILD_DATE_TIME_TYPE = "https://uri.etsi.org/ngsi-ld/DateTime"

    val NGSILD_ATTRIBUTES_CORE_MEMBERS = listOf(
        NGSILD_CREATED_AT_PROPERTY,
        NGSILD_MODIFIED_AT_PROPERTY,
        NGSILD_OBSERVED_AT_PROPERTY
    )

    val NGSILD_PROPERTIES_CORE_MEMBERS = listOf(
        NGSILD_PROPERTY_VALUE,
        NGSILD_UNIT_CODE_PROPERTY
    ).plus(NGSILD_ATTRIBUTES_CORE_MEMBERS)

    val NGSILD_RELATIONSHIPS_CORE_MEMBERS = listOf(
        NGSILD_RELATIONSHIP_HAS_OBJECT
    ).plus(NGSILD_ATTRIBUTES_CORE_MEMBERS)

    const val EGM_OBSERVED_BY = "https://ontology.eglobalmark.com/egm#observedBy"
    const val EGM_VENDOR_ID = "https://ontology.eglobalmark.com/egm#vendorId"

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

    fun parseEntities(entities: List<Map<String, Any>>): List<Pair<Map<String, Any>, List<String>>> {
        val mapper = jacksonObjectMapper()
        return entities.map {
            parseEntity(mapper.writeValueAsString(it))
        }
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
            if (intermediateList.size == 1) {
                val firstListEntry = intermediateList[0]
                val finalValue = firstListEntry["@value"]
                finalValue
            } else {
                intermediateList.map {
                    it["@value"]
                }
            }
        } else
            null

    fun getPropertyValueFromMapAsDateTime(values: Map<String, List<Any>>, propertyKey: String): OffsetDateTime? {
        val observedAt = String::class.safeCast(getPropertyValueFromMap(values, propertyKey))
        return observedAt?.run {
            OffsetDateTime.parse(this)
        }
    }

    fun getPropertyValueFromMapAsString(values: Map<String, List<Any>>, propertyKey: String): String? =
        String::class.safeCast(getPropertyValueFromMap(values, propertyKey))

    fun getRelationshipObjectId(relationshipValues: Map<String, List<Any>>): String {
        if (!relationshipValues.containsKey(NGSILD_RELATIONSHIP_HAS_OBJECT))
            throw BadRequestDataException("Key $NGSILD_RELATIONSHIP_HAS_OBJECT not found in $relationshipValues")

        val hasObjectEntry = relationshipValues[NGSILD_RELATIONSHIP_HAS_OBJECT]!![0]
        if (hasObjectEntry !is Map<*, *>)
            throw BadRequestDataException("hasObject entry is not a map as expected : $hasObjectEntry")

        val objectId = hasObjectEntry[NGSILD_ENTITY_ID]
        if (objectId !is String)
            throw BadRequestDataException("Expected objectId to be a string but it is instead $objectId")

        return objectId
    }

    fun extractTypeFromPayload(payload: Map<String, Any>): String =
        (payload["@type"] as List<String>)[0]

    fun extractShortTypeFromPayload(payload: Map<String, Any>): String =
        // TODO is it always after a '/' ? can't it be after a '#' ? (https://redmine.eglobalmark.com/issues/852)
        // TODO do a clean implementation using info from @context
        (payload["@type"] as List<String>)[0].substringAfterLast("/").substringAfterLast("#")

    /**
     * Given an entity's attribute, returns whether it is of the given attribute type
     * (i.e. property, geo property or relationship)
     */
    fun isAttributeOfType(values: Map<String, List<Any>>, type: AttributeType): Boolean =
        values.containsKey(NGSILD_ENTITY_TYPE) &&
            values[NGSILD_ENTITY_TYPE] is List<*> &&
            values.getOrElse(NGSILD_ENTITY_TYPE) { emptyList() }[0] == type.uri

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

    fun compactAndStringifyFragment(key: String, value: Any, context: String): String {
        val compactedFragment = JsonLdProcessor.compact(mapOf(key to value), mapOf("@context" to context), JsonLdOptions())
        return JsonUtils.toString(compactedFragment)
    }

    fun getTypeFromURI(uri: String): String {
        return uri.split(":")[2]
    }

    fun parseTemporalPropertyUpdate(content: String): Observation {
        val mapper = jacksonObjectMapper()
        val rawParsedData = mapper.readTree(content)
        val propertyName = rawParsedData.fieldNames().next()
        val propertyValues = rawParsedData[propertyName]

        logger.debug("Received property $rawParsedData (values: $propertyValues)")
        val isContentValid = listOf("type", "value", "unitCode", "observedAt", "observedBy")
            .map { propertyValues.has(it) }
            .all { it }
        if (!isContentValid)
            throw InvalidNgsiLdPayloadException("Received content misses one or more required attributes")

        val location =
            if (propertyValues["location"] != null) {
                val coordinates = propertyValues["location"]["value"]["coordinates"].asIterable()
                // as per https://tools.ietf.org/html/rfc7946#appendix-A.1, first is longitude, second is latitude
                Pair(coordinates.first().asDouble(), coordinates.last().asDouble())
            } else {
                null
            }

        return Observation(
            attributeName = propertyName,
            observedBy = propertyValues["observedBy"]["object"].asText(),
            observedAt = OffsetDateTime.parse(propertyValues["observedAt"].asText()),
            value = propertyValues["value"].asDouble(),
            unitCode = propertyValues["unitCode"].asText(),
            latitude = location?.second,
            longitude = location?.first
        )
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

fun String.isFloat(): Boolean =
    this.matches("-?\\d+(\\.\\d+)?".toRegex())
