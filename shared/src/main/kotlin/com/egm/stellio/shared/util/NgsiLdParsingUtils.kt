package com.egm.stellio.shared.util

import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.EntityEvent
import com.egm.stellio.shared.model.ExpandedEntity
import com.egm.stellio.shared.model.Observation
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.github.jsonldjava.core.JsonLdOptions
import com.github.jsonldjava.core.JsonLdProcessor
import com.github.jsonldjava.utils.JsonUtils
import org.slf4j.LoggerFactory
import org.springframework.core.io.ClassPathResource
import org.springframework.stereotype.Component
import java.net.URI
import java.time.LocalDate
import java.time.LocalTime
import java.time.ZonedDateTime
import javax.annotation.PostConstruct
import kotlin.reflect.full.safeCast

data class AttributeType(val uri: String)

@Component
object NgsiLdParsingUtils {

    const val NGSILD_CORE_CONTEXT = "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"
    const val NGSILD_EGM_CONTEXT =
        "https://raw.githubusercontent.com/easy-global-market/ngsild-api-data-models/master/shared-jsonld-contexts/egm.jsonld"

    val NGSILD_PROPERTY_TYPE = AttributeType("https://uri.etsi.org/ngsi-ld/Property")
    const val NGSILD_PROPERTY_VALUE = "https://uri.etsi.org/ngsi-ld/hasValue"
    const val NGSILD_PROPERTY_VALUES = "https://uri.etsi.org/ngsi-ld/hasValues"
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
    const val NGSILD_POINT_PROPERTY = "Point"
    const val NGSILD_INSTANCE_ID_PROPERTY = "https://uri.etsi.org/ngsi-ld/instanceId"
    const val NGSILD_DATASET_ID_PROPERTY = "https://uri.etsi.org/ngsi-ld/datasetId"

    const val NGSILD_DATE_TIME_TYPE = "https://uri.etsi.org/ngsi-ld/DateTime"
    const val NGSILD_DATE_TYPE = "https://uri.etsi.org/ngsi-ld/Date"
    const val NGSILD_TIME_TYPE = "https://uri.etsi.org/ngsi-ld/Time"

    val NGSILD_ENTITY_CORE_MEMBERS = listOf(
        NGSILD_CREATED_AT_PROPERTY,
        NGSILD_MODIFIED_AT_PROPERTY
    )

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
    const val EGM_IS_CONTAINED_IN = "https://ontology.eglobalmark.com/egm#isContainedIn"
    const val EGM_VENDOR_ID = "https://ontology.eglobalmark.com/egm#vendorId"
    const val EGM_RAISED_NOTIFICATION = "https://ontology.eglobalmark.com/egm#raised"

    private val logger = LoggerFactory.getLogger(javaClass)

    private val mapper: ObjectMapper =
        jacksonObjectMapper()
            .setSerializationInclusion(JsonInclude.Include.NON_NULL)
            .findAndRegisterModules()
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)

    private val localCoreContextPayload =
        ClassPathResource("/ngsild/ngsi-ld-core-context.jsonld").inputStream.readBytes().toString(Charsets.UTF_8)
    private var BASE_CONTEXT: Map<String, Any> = mapOf()

    @PostConstruct
    private fun loadCoreContext() {
        val coreContextPayload = HttpUtils.doGet(NGSILD_CORE_CONTEXT) ?: localCoreContextPayload
        val coreContext: Map<String, Any> = mapper.readValue(
            coreContextPayload, mapper.typeFactory.constructMapLikeType(
                Map::class.java, String::class.java, Any::class.java
            )
        )
        BASE_CONTEXT = coreContext.get("@context") as Map<String, Any>
        logger.info("Core context loaded")
    }

    private fun addCoreContext(contexts: List<String>): List<Any> =
        contexts.plus(BASE_CONTEXT)

    fun parseEntity(input: String, contexts: List<String>): ExpandedEntity {
        val usedContext = addCoreContext(contexts)
        val jsonLdOptions = JsonLdOptions()
        jsonLdOptions.expandContext = mapOf("@context" to usedContext)

        val expandedEntity =
            JsonLdProcessor.expand(JsonUtils.fromInputStream(input.byteInputStream()), jsonLdOptions)[0]

        return ExpandedEntity(expandedEntity as Map<String, Any>, contexts)
    }

    @Deprecated("Deprecated in favor of the method accepting the contexts as separate arguments, as it allow for more genericity")
    fun parseEntity(input: String): ExpandedEntity {
        val expandedEntity = JsonLdProcessor.expand(JsonUtils.fromInputStream(input.byteInputStream()))
        if (expandedEntity.isEmpty())
            throw BadRequestDataException("Could not parse entity due to invalid json-ld payload")

        // TODO find a way to avoid this extra parsing
        val parsedInput: Map<String, Any> = mapper.readValue(
            input, mapper.typeFactory.constructMapLikeType(
                Map::class.java, String::class.java, Any::class.java
            )
        )

        val contexts =
            if (parsedInput["@context"] is List<*>)
                parsedInput["@context"] as List<String>
            else
                listOf(parsedInput["@context"] as String)

        return ExpandedEntity(expandedEntity[0] as Map<String, Any>, contexts)
    }

    fun parseEntities(entities: List<Map<String, Any>>): List<ExpandedEntity> {
        return entities.map {
            parseEntity(mapper.writeValueAsString(it))
        }
    }

    fun parseEntityEvent(input: String): EntityEvent {
        return mapper.readValue(input, EntityEvent::class.java)
    }

    fun parseJsonLdFragment(input: String): Map<String, Any> {
        return mapper.readValue(
            input, mapper.typeFactory.constructMapLikeType(
                Map::class.java, String::class.java, Any::class.java
            )
        )
    }

    fun expandValueAsMap(value: Any): Map<String, List<Any>> =
        (value as List<Any>)[0] as Map<String, List<Any>>

    fun expandValueAsListOfMap(value: Any): List<Map<String, List<Any>>> =
            value as List<Map<String, List<Any>>>
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
     *     @value=2019-12-18T10:45:44.248755Z
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
                val finalValueType = firstListEntry["@type"]
                if (finalValueType != null) {
                    val finalValue = String::class.safeCast(firstListEntry["@value"])
                    when (finalValueType) {
                        NGSILD_DATE_TIME_TYPE -> ZonedDateTime.parse(finalValue)
                        NGSILD_DATE_TYPE -> LocalDate.parse(finalValue)
                        NGSILD_TIME_TYPE -> LocalTime.parse(finalValue)
                        else -> firstListEntry["@value"]
                    }
                } else if (firstListEntry["@value"] != null) {
                    firstListEntry["@value"]
                } else {
                    // Used to get the value of datasetId property, since it is mapped to "@id" key rather than "@value"
                    firstListEntry["@id"]
                }
            } else {
                intermediateList.map {
                    it["@value"]
                }
            }
        } else
            null

    fun getPropertyValueFromMapAsDateTime(values: Map<String, List<Any>>, propertyKey: String): ZonedDateTime? {
        val observedAt = ZonedDateTime::class.safeCast(getPropertyValueFromMap(values, propertyKey))
        return observedAt?.run {
            ZonedDateTime.parse(this.toString())
        }
    }

    fun getPropertyValueFromMapAsString(values: Map<String, List<Any>>, propertyKey: String): String? =
        String::class.safeCast(getPropertyValueFromMap(values, propertyKey))

    fun getPropertyValueFromMapAsUri(values: Map<String, List<Any>>, propertyKey: String): URI? =
        getPropertyValueFromMapAsString(values, propertyKey)?.let {
            URI.create(it)
        }

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

    fun extractShortTypeFromPayload(payload: Map<String, Any>): String =
        /*
         * TODO do a clean implementation using info from @context
         * TODO is it always after a '/' ? can't it be after a '#' ? (https://redmine.eglobalmark.com/issues/852)
         */
        (payload["@type"] as List<String>)[0].substringAfterLast("/").substringAfterLast("#")

    /**
     * Given an entity's attribute, returns whether it is of the given attribute type
     * (i.e. property, geo property or relationship)
     */
    fun isAttributeOfType(values: Map<String, List<Any>>, type: AttributeType): Boolean =
        // TODO move some of these checks to isValidAttribute()
        values.containsKey(NGSILD_ENTITY_TYPE) &&
            values[NGSILD_ENTITY_TYPE] is List<*> &&
            values.getOrElse(NGSILD_ENTITY_TYPE) { emptyList() }[0] == type.uri

    /**
     * Given an entity's attribute, returns whether all its instances are of the given attribute type
     * (i.e. property, relationship)
     */
    fun isAttributeOfType(values: List<Map<String, List<Any>>>, type: AttributeType): Boolean =
        values.all {
            isAttributeOfType(it, type)
        }

    /**
     * Given an entity's attribute
     * If all its instances are of the same attribute type returns true
     * Else throws BadRequestDataException
     */
    fun isValidAttribute(key: String, values: List<Map<String, List<Any>>>): Boolean {
        val firstType = values[0].getOrElse(NGSILD_ENTITY_TYPE) { throw BadRequestDataException("@type not found in one of ${key.extractShortTypeFromExpanded()} instances") }[0]
        if (!values.all { it.getOrElse(NGSILD_ENTITY_TYPE) { throw BadRequestDataException("@type not found in one of ${key.extractShortTypeFromExpanded()} instances") }[0] == firstType })
            throw BadRequestDataException("${key.extractShortTypeFromExpanded()} attribute instances must have the same type")
        return true
    }

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
        val usedContext = addCoreContext(contexts)
        val jsonLdOptions = JsonLdOptions()
        jsonLdOptions.expandContext = mapOf("@context" to usedContext)
        val expandedType = JsonLdProcessor.expand(mapOf(type to mapOf<String, Any>()), jsonLdOptions)
        logger.debug("Expanded type $type to $expandedType")
        return if (expandedType.isNotEmpty())
            (expandedType[0] as Map<String, Any>).keys.first()
        else
            null
    }

    fun expandJsonLdFragment(fragment: String, contexts: List<String>): Map<String, Any> {
        val usedContext = addCoreContext(contexts)
        val jsonLdOptions = JsonLdOptions()
        jsonLdOptions.expandContext = mapOf("@context" to usedContext)
        val expandedFragment =
            JsonLdProcessor.expand(JsonUtils.fromInputStream(fragment.byteInputStream()), jsonLdOptions)
        logger.debug("Expanded fragment $fragment to $expandedFragment")
        if (expandedFragment.isEmpty())
            throw BadRequestDataException("Unable to expand JSON-LD fragment : $fragment")
        return expandedFragment[0] as Map<String, Any>
    }

    fun expandJsonLdFragment(fragment: String, context: String): Map<String, Any> {
        return expandJsonLdFragment(fragment, listOf(context))
    }

    fun compactAndStringifyFragment(key: String, value: Any, context: List<String>): String {
        val compactedFragment =
            JsonLdProcessor.compact(mapOf(key to value), mapOf("@context" to context), JsonLdOptions())
        return mapper.writeValueAsString(compactedFragment)
    }

    fun compactAndStringifyFragment(key: String, value: Any, context: String): String {
        val compactedFragment =
            JsonLdProcessor.compact(mapOf(key to value), mapOf("@context" to context), JsonLdOptions())
        return mapper.writeValueAsString(compactedFragment)
    }

    fun compactEntities(entities: List<ExpandedEntity>): List<Map<String, Any>> =
        entities.map {
            it.compact()
        }

    fun parseTemporalPropertyUpdate(content: String): Observation? {
        val rawParsedData = mapper.readTree(content)
        val propertyName = rawParsedData.fieldNames().next()
        val propertyValues = rawParsedData[propertyName]

        logger.debug("Received property $rawParsedData (values: $propertyValues)")
        val isContentValid = listOf("type", "value", "unitCode", "observedAt", "observedBy")
            .map { propertyValues.has(it) }
            .all { it }
        if (!isContentValid) {
            logger.warn("Received content is not a temporal property, ignoring it")
            return null
        }

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
            observedAt = ZonedDateTime.parse(propertyValues["observedAt"].asText()),
            value = propertyValues["value"].asDouble(),
            unitCode = propertyValues["unitCode"].asText(),
            latitude = location?.second,
            longitude = location?.first
        )
    }

    /**
     * As per 6.3.5, extract @context from request payload. In the absence of such context, then BadRequestDataException
     * shall be raised
     */
    fun getContextOrThrowError(input: String): List<String> {
        val rawParsedData = mapper.readTree(input) as ObjectNode
        val context = rawParsedData.get("@context") ?: throw BadRequestDataException("Context not provided")

        return try {
            mapper.readValue(
                context.toString(),
                mapper.typeFactory.constructCollectionType(List::class.java, String::class.java)
            )
        } catch (e: Exception) {
            throw BadRequestDataException(e.message ?: "Unable to parse the provided context")
        }
    }

    fun getLocationFromEntity(parsedEntity: ExpandedEntity): Map<String, Any>? {
        try {
            val location = expandValueAsMap(parsedEntity.rawJsonLdProperties[NGSILD_LOCATION_PROPERTY]!!)
            val locationValue = expandValueAsMap(location[NGSILD_GEOPROPERTY_VALUE]!!)
            val geoPropertyType = locationValue["@type"]!![0] as String
            val geoPropertyValue = locationValue[NGSILD_COORDINATES_PROPERTY]!!
            if (geoPropertyType.extractShortTypeFromExpanded() == NGSILD_POINT_PROPERTY) {
                val longitude = (geoPropertyValue[0] as Map<String, Double>)["@value"]
                val latitude = (geoPropertyValue[1] as Map<String, Double>)["@value"]
                return mapOf(
                    "geometry" to geoPropertyType.extractShortTypeFromExpanded(),
                    "coordinates" to listOf(longitude, latitude)
                )
            } else {
                val res = arrayListOf<List<Double?>>()
                var count = 1
                geoPropertyValue.forEach {
                    if (count % 2 != 0) {
                        val longitude = (geoPropertyValue[count - 1] as Map<String, Double>)["@value"]
                        val latitude = (geoPropertyValue[count] as Map<String, Double>)["@value"]
                        res.add(listOf(longitude, latitude))
                    }
                    count++
                }
                return mapOf("geometry" to geoPropertyType.extractShortTypeFromExpanded(), "coordinates" to res)
            }
        } catch (e: Exception) {
            return null
        }
    }

    fun getAttributeType(attribute: String, entity: ObjectNode, separator: String): JsonNode? {
        var node: JsonNode = entity
        val attributePath = if (separator == "[")
            attribute.replace("]", "").split(separator)
        else
            attribute.split(separator)

        attributePath.forEach {
            try {
                node = node.get(it)
            } catch (e: Exception) {
                throw BadRequestDataException(e.message ?: "Unresolved value $it")
            }
        }

        return node.get("type")
    }
}

fun String.extractShortTypeFromExpanded(): String =
    /*
     * TODO is it always after a '/' ? can't it be after a '#' ? (https://redmine.eglobalmark.com/issues/852)
     * TODO do a clean implementation using info from @context
     */
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

fun String.isDateTime(): Boolean =
    try {
        ZonedDateTime.parse(this)
        true
    } catch (e: Exception) {
        false
    }

fun String.isDate(): Boolean =
    try {
        LocalDate.parse(this)
        true
    } catch (e: Exception) {
        false
    }

fun String.isTime(): Boolean =
    try {
        LocalTime.parse(this)
        true
    } catch (e: Exception) {
        false
    }
