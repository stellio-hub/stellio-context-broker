package com.egm.stellio.shared.util

import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.CompactedJsonLdEntity
import com.egm.stellio.shared.model.InvalidRequestException
import com.egm.stellio.shared.model.JsonLdEntity
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.github.jsonldjava.core.JsonLdError
import com.github.jsonldjava.core.JsonLdOptions
import com.github.jsonldjava.core.JsonLdProcessor
import com.github.jsonldjava.utils.JsonUtils
import org.slf4j.LoggerFactory
import org.springframework.core.io.ClassPathResource
import org.springframework.stereotype.Component
import java.time.LocalDate
import java.time.LocalTime
import java.time.ZonedDateTime
import javax.annotation.PostConstruct
import kotlin.reflect.full.safeCast

data class AttributeType(val uri: String)

@Component
object JsonLdUtils {

    const val NGSILD_CORE_CONTEXT = "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld"
    const val EGM_BASE_CONTEXT_URL =
        "https://raw.githubusercontent.com/easy-global-market/ngsild-api-data-models/master"
    val NGSILD_EGM_CONTEXT = "$EGM_BASE_CONTEXT_URL/shared-jsonld-contexts/egm.jsonld"

    val NGSILD_PROPERTY_TYPE = AttributeType("https://uri.etsi.org/ngsi-ld/Property")
    const val NGSILD_PROPERTY_VALUE = "https://uri.etsi.org/ngsi-ld/hasValue"
    const val NGSILD_PROPERTY_VALUES = "https://uri.etsi.org/ngsi-ld/hasValues"
    val NGSILD_GEOPROPERTY_TYPE = AttributeType("https://uri.etsi.org/ngsi-ld/GeoProperty")
    const val NGSILD_GEOPROPERTY_VALUE = "https://uri.etsi.org/ngsi-ld/hasValue"
    val NGSILD_RELATIONSHIP_TYPE = AttributeType("https://uri.etsi.org/ngsi-ld/Relationship")
    const val NGSILD_RELATIONSHIP_HAS_OBJECT = "https://uri.etsi.org/ngsi-ld/hasObject"

    const val JSONLD_ID = "@id"
    const val JSONLD_TYPE = "@type"
    const val JSONLD_VALUE_KW = "@value"
    val JSONLD_ENTITY_MANDATORY_FIELDS = setOf("id", "type", "@context")
    val JSONLD_ENTITY_CORE_PROPERTIES = setOf("id", "type", "@context")

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
            coreContextPayload,
            mapper.typeFactory.constructMapLikeType(
                Map::class.java, String::class.java, Any::class.java
            )
        )
        BASE_CONTEXT = coreContext["@context"] as Map<String, Any>
        logger.info("Core context loaded")
    }

    private fun addCoreContext(contexts: List<String>): List<Any> =
        contexts.plus(BASE_CONTEXT)

    fun expandJsonLdEntity(input: String, contexts: List<String>): JsonLdEntity {
        val usedContext = addCoreContext(contexts)
        val jsonLdOptions = JsonLdOptions()
        jsonLdOptions.expandContext = mapOf("@context" to usedContext)

        return JsonLdEntity(parseAndExpandJsonLdFragment(input, jsonLdOptions), contexts)
    }

    fun expandJsonLdEntity(input: String): JsonLdEntity {
        // TODO find a way to avoid this extra parsing
        return JsonLdEntity(parseAndExpandJsonLdFragment(input), extractContextFromInput(input))
    }

    fun expandJsonLdEntities(entities: List<Map<String, Any>>): List<JsonLdEntity> {
        return entities.map {
            expandJsonLdEntity(mapper.writeValueAsString(it))
        }
    }

    fun extractContextFromInput(input: String): List<String> {
        val parsedInput: Map<String, Any> = mapper.readValue(
            input,
            mapper.typeFactory.constructMapLikeType(
                Map::class.java, String::class.java, Any::class.java
            )
        )

        return if (!parsedInput.containsKey("@context"))
            emptyList()
        else if (parsedInput["@context"] is List<*>)
            parsedInput["@context"] as List<String>
        else if (parsedInput["@context"] is String)
            listOf(parsedInput["@context"] as String)
        else
            emptyList()
    }

    // TODO it should be replaced by proper parsing to a NGSI-LD attribute
    fun parseJsonLdFragment(input: String): Map<String, Any> {
        return mapper.readValue(
            input,
            mapper.typeFactory.constructMapLikeType(
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
        return parseAndExpandJsonLdFragment(fragment, jsonLdOptions)
    }

    fun expandJsonLdFragment(fragment: String, context: String): Map<String, Any> {
        return expandJsonLdFragment(fragment, listOf(context))
    }

    fun compactAndStringifyFragment(key: String, value: Any, context: List<String>): String {
        val compactedFragment =
            JsonLdProcessor.compact(mapOf(key to value), mapOf("@context" to context), JsonLdOptions())
        return mapper.writeValueAsString(compactedFragment.minus("@context"))
    }

    fun compactAndSerialize(jsonLdEntity: JsonLdEntity): String =
        mapper.writeValueAsString(jsonLdEntity.compact())

    fun compactEntities(entities: List<JsonLdEntity>): List<CompactedJsonLdEntity> =
        entities.map {
            it.compact()
        }

    fun filterCompactedEntityOnAttributes(
        input: CompactedJsonLdEntity,
        includedAttributes: Set<String>
    ): Map<String, Any> {
        return if (includedAttributes.isEmpty()) {
            input
        } else {
            val includedKeys = JSONLD_ENTITY_MANDATORY_FIELDS.plus(includedAttributes)
            input.filterKeys { includedKeys.contains(it) }
        }
    }
}

fun String.extractShortTypeFromExpanded(): String =
    /*
     * TODO is it always after a '/' ? can't it be after a '#' ? (https://redmine.eglobalmark.com/issues/852)
     * TODO do a clean implementation using info from @context
     */
    this.substringAfterLast("/").substringAfterLast("#")

fun CompactedJsonLdEntity.toKeyValues(): Map<String, Any> {
    return this.mapValues { (_, value) -> simplifyRepresentation(value) }
}

private fun simplifyRepresentation(value: Any): Any {
    return when (value) {
        // entity property value is always a Map
        is Map<*, *> -> simplifyValue(value)
        // we keep id, type and @context values as they are (String and List<String>)
        else -> value
    }
}

private fun simplifyValue(value: Map<*, *>): Any {
    return when (value["type"]) {
        "Property", "GeoProperty" -> value.getOrDefault("value", value)!!
        "Relationship" -> value.getOrDefault("object", value)!!
        else -> value
    }
}

private fun parseAndExpandJsonLdFragment(fragment: String, jsonLdOptions: JsonLdOptions? = null): Map<String, Any> {
    val parsedFragment = try {
        JsonUtils.fromInputStream(fragment.byteInputStream())
    } catch (e: JsonParseException) {
        throw InvalidRequestException("Unexpected error while parsing payload : ${e.message}")
    }
    val expandedFragment = try {
        if (jsonLdOptions != null)
            JsonLdProcessor.expand(parsedFragment, jsonLdOptions)
        else
            JsonLdProcessor.expand(parsedFragment)
    } catch (e: JsonLdError) {
        throw BadRequestDataException("Unexpected error while parsing payload : ${e.message}")
    }
    if (expandedFragment.isEmpty())
        throw BadRequestDataException("Unable to parse input payload")

    return expandedFragment[0] as Map<String, Any>
}
