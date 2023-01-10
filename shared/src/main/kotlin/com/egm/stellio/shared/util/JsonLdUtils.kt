package com.egm.stellio.shared.util

import arrow.core.Either
import arrow.core.flatMap
import arrow.core.left
import arrow.core.right
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_VALUE
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_GEO_PROPERTIES_TERMS
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_SYSATTRS_TERMS
import com.egm.stellio.shared.util.JsonUtils.deserializeAs
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import com.egm.stellio.shared.util.JsonUtils.deserializeListOfObjects
import com.egm.stellio.shared.util.JsonUtils.deserializeObject
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import com.github.jsonldjava.core.JsonLdError
import com.github.jsonldjava.core.JsonLdOptions
import com.github.jsonldjava.core.JsonLdProcessor
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.core.io.ClassPathResource
import org.springframework.http.MediaType
import java.net.URI
import java.time.LocalDate
import java.time.LocalTime
import java.time.ZonedDateTime
import kotlin.reflect.full.safeCast

data class AttributeType(val uri: String)

object JsonLdUtils {

    const val NGSILD_CORE_CONTEXT = "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.3.jsonld"
    const val EGM_BASE_CONTEXT_URL =
        "https://raw.githubusercontent.com/easy-global-market/ngsild-api-data-models/master"
    const val NGSILD_EGM_CONTEXT = "$EGM_BASE_CONTEXT_URL/shared-jsonld-contexts/egm.jsonld"

    val NGSILD_PROPERTY_TYPE = AttributeType("https://uri.etsi.org/ngsi-ld/Property")
    const val NGSILD_PROPERTY_VALUE = "https://uri.etsi.org/ngsi-ld/hasValue"
    val NGSILD_GEOPROPERTY_TYPE = AttributeType("https://uri.etsi.org/ngsi-ld/GeoProperty")
    const val NGSILD_GEOPROPERTY_VALUE = "https://uri.etsi.org/ngsi-ld/hasValue"
    val NGSILD_RELATIONSHIP_TYPE = AttributeType("https://uri.etsi.org/ngsi-ld/Relationship")
    const val NGSILD_RELATIONSHIP_HAS_OBJECT = "https://uri.etsi.org/ngsi-ld/hasObject"

    const val JSONLD_ID_TERM = "id"
    const val JSONLD_ID = "@id"
    const val JSONLD_TYPE_TERM = "type"
    const val JSONLD_TYPE = "@type"
    const val JSONLD_VALUE = "value"
    const val JSONLD_VALUE_KW = "@value"
    const val JSONLD_OBJECT = "object"
    const val JSONLD_CONTEXT = "@context"
    val JSONLD_EXPANDED_ENTITY_MANDATORY_FIELDS = setOf(JSONLD_ID, JSONLD_TYPE, JSONLD_CONTEXT)
    val JSONLD_COMPACTED_ENTITY_MANDATORY_FIELDS = setOf(JSONLD_ID_TERM, JSONLD_TYPE_TERM, JSONLD_CONTEXT)

    const val NGSILD_CREATED_AT_TERM = "createdAt"
    const val NGSILD_MODIFIED_AT_TERM = "modifiedAt"
    val NGSILD_SYSATTRS_TERMS = listOf(NGSILD_CREATED_AT_TERM, NGSILD_MODIFIED_AT_TERM)
    const val NGSILD_CREATED_AT_PROPERTY = "https://uri.etsi.org/ngsi-ld/createdAt"
    const val NGSILD_MODIFIED_AT_PROPERTY = "https://uri.etsi.org/ngsi-ld/modifiedAt"
    val NGSILD_SYSATTRS_PROPERTIES = listOf(NGSILD_CREATED_AT_PROPERTY, NGSILD_MODIFIED_AT_PROPERTY)
    const val NGSILD_OBSERVED_AT_TERM = "observedAt"
    const val NGSILD_OBSERVED_AT_PROPERTY = "https://uri.etsi.org/ngsi-ld/$NGSILD_OBSERVED_AT_TERM"
    const val NGSILD_UNIT_CODE_PROPERTY = "https://uri.etsi.org/ngsi-ld/unitCode"
    const val NGSILD_LOCATION_TERM = "location"
    const val NGSILD_LOCATION_PROPERTY = "https://uri.etsi.org/ngsi-ld/location"
    const val NGSILD_OBSERVATION_SPACE_TERM = "observationSpace"
    const val NGSILD_OPERATION_SPACE_TERM = "operationSpace"
    const val NGSILD_OPERATION_SPACE_PROPERTY = "https://uri.etsi.org/ngsi-ld/operationSpace"
    val NGSILD_GEO_PROPERTIES_TERMS =
        setOf(NGSILD_LOCATION_TERM, NGSILD_OBSERVATION_SPACE_TERM, NGSILD_OPERATION_SPACE_TERM)
    const val NGSILD_DATASET_ID_PROPERTY = "https://uri.etsi.org/ngsi-ld/datasetId"
    const val NGSILD_INSTANCE_ID_PROPERTY = "https://uri.etsi.org/ngsi-ld/instanceId"

    const val NGSILD_SUBSCRIPTION_TERM = "Subscription"
    const val NGSILD_SUBSCRIPTION_PROPERTY = "https://uri.etsi.org/ngsi-ld/Subscription"
    const val NGSILD_NOTIFICATION_TERM = "Notification"
    const val NGSILD_NOTIFICATION_PROPERTY = "https://uri.etsi.org/ngsi-ld/Notification"
    const val NGSILD_NOTIFICATION_ATTR_TERM = "notification"
    const val NGSILD_NOTIFICATION_ATTR_PROPERTY = "https://uri.etsi.org/ngsi-ld/notification"

    const val NGSILD_DATE_TIME_TYPE = "https://uri.etsi.org/ngsi-ld/DateTime"
    const val NGSILD_DATE_TYPE = "https://uri.etsi.org/ngsi-ld/Date"
    const val NGSILD_TIME_TYPE = "https://uri.etsi.org/ngsi-ld/Time"

    const val NGSILD_NAME_PROPERTY = "https://schema.org/name"

    const val DATASET_ID_PREFIX = "urn:ngsi-ld:Dataset:"

    val logger: Logger = LoggerFactory.getLogger(javaClass)

    private val localCoreContextPayload =
        ClassPathResource("/contexts/ngsi-ld-core-context-v1.3.jsonld").inputStream.readBytes().toString(Charsets.UTF_8)
    private var BASE_CONTEXT: Map<String, Any> = mapOf()

    init {
        val coreContext: Map<String, Any> = deserializeObject(localCoreContextPayload)
        BASE_CONTEXT = coreContext[JSONLD_CONTEXT] as Map<String, Any>
        logger.info("Core context loaded")
    }

    private fun addCoreContext(contexts: List<String>): List<Any> =
        contexts.plus(BASE_CONTEXT)

    internal fun addCoreContextIfMissing(contexts: List<String>): List<String> =
        when {
            contexts.isEmpty() -> listOf(NGSILD_CORE_CONTEXT)
            // to ensure core @context comes last
            contexts.contains(NGSILD_CORE_CONTEXT) ->
                contexts.filter { it != NGSILD_CORE_CONTEXT }.plus(NGSILD_CORE_CONTEXT)
            // to check if the core @context is included / embedded in one of the link
            canExpandJsonLdKeyFromCore(contexts) -> contexts
            else -> contexts.plus(NGSILD_CORE_CONTEXT)
        }

    private fun defaultJsonLdOptions(contexts: List<String>): JsonLdOptions =
        JsonLdOptions()
            .apply {
                expandContext = mapOf(JSONLD_CONTEXT to addCoreContext(contexts))
            }

    fun expandDeserializedPayload(deserializedPayload: Map<String, Any>, contexts: List<String>): Map<String, Any> =
        doJsonLdExpansion(deserializedPayload, contexts)

    fun expandJsonLdEntity(input: Map<String, Any>, contexts: List<String>): JsonLdEntity =
        JsonLdEntity(doJsonLdExpansion(input, contexts), contexts)

    fun expandJsonLdEntity(input: String, contexts: List<String>): JsonLdEntity =
        expandJsonLdEntity(input.deserializeAsMap(), contexts)

    fun expandJsonLdEntity(input: String): JsonLdEntity {
        val jsonInput = input.deserializeAsMap()
        return expandJsonLdEntity(jsonInput, extractContextFromInput(jsonInput))
    }

    fun expandJsonLdEntities(entities: List<Map<String, Any>>): List<JsonLdEntity> =
        entities.map {
            expandJsonLdEntity(it, extractContextFromInput(it))
        }

    fun expandJsonLdEntities(entities: List<Map<String, Any>>, contexts: List<String>): List<JsonLdEntity> =
        entities.map {
            expandJsonLdEntity(it, contexts)
        }

    fun expandJsonLdTerms(terms: List<String>, contexts: List<String>): List<ExpandedTerm> =
        terms.map {
            expandJsonLdTerm(it, contexts)
        }

    fun expandJsonLdTerm(term: String, context: String): String =
        expandJsonLdTerm(term, listOf(context))

    fun expandJsonLdTerm(term: String, contexts: List<String>): String {
        val expandedType = JsonLdProcessor.expand(mapOf(term to mapOf<String, Any>()), defaultJsonLdOptions(contexts))
        logger.debug("Expanded type $term to $expandedType")
        return if (expandedType.isNotEmpty())
            (expandedType[0] as Map<String, Any>).keys.first()
        else
            term
    }

    fun expandJsonLdFragment(fragment: Map<String, Any>, contexts: List<String>): Map<String, Any> =
        doJsonLdExpansion(fragment, contexts)

    fun expandJsonLdFragment(fragment: String, contexts: List<String>): Map<String, Any> =
        expandJsonLdFragment(fragment.deserializeAsMap(), contexts)

    fun expandJsonLdFragment(
        attributeName: String,
        attributePayload: String,
        contexts: List<String>
    ): Map<String, List<Map<String, List<Any>>>> =
        expandJsonLdFragment(mapOf(attributeName to deserializeAs(attributePayload)), contexts)
            as Map<String, List<Map<String, List<Any>>>>

    private fun doJsonLdExpansion(fragment: Map<String, Any>, contexts: List<String>): Map<String, Any> {
        // transform the GeoJSON value of geo properties into WKT format before JSON-LD expansion
        // since JSON-LD expansion breaks the data (e.g., flattening the lists of lists)
        val parsedFragment = geoPropertyToWKT(fragment)

        val usedContext = addCoreContext(contexts)
        val jsonLdOptions = JsonLdOptions().apply { expandContext = mapOf(JSONLD_CONTEXT to usedContext) }

        val expandedFragment = try {
            // ensure there is no @context in the payload since it overrides the one from JsonLdOptions
            JsonLdProcessor.expand(parsedFragment.minus(JSONLD_CONTEXT), jsonLdOptions)
        } catch (e: JsonLdError) {
            throw e.toAPIException()
        }
        if (expandedFragment.isEmpty())
            throw BadRequestDataException("Unable to expand input payload")

        return expandedFragment[0] as Map<String, Any>
    }

    fun addContextToListOfElements(listOfElements: String, contexts: List<String>): String {
        val updatedPayload = deserializeListOfObjects(listOfElements)
            .map {
                it.plus(Pair(JSONLD_CONTEXT, contexts))
            }
        return serializeObject(updatedPayload)
    }

    fun addContextToElement(element: String, contexts: List<String>): String {
        val parsedPayload = deserializeObject(element).plus(Pair(JSONLD_CONTEXT, contexts))
        return serializeObject(parsedPayload)
    }

    fun addContextsToEntity(element: CompactedJsonLdEntity, contexts: List<String>, mediaType: MediaType) =
        if (mediaType == MediaType.APPLICATION_JSON)
            element
        else
            element.plus(Pair(JSONLD_CONTEXT, contexts))

    fun extractContextFromInput(input: Map<String, Any>): List<String> {
        return if (!input.containsKey(JSONLD_CONTEXT))
            emptyList()
        else if (input[JSONLD_CONTEXT] is List<*>)
            input[JSONLD_CONTEXT] as List<String>
        else if (input[JSONLD_CONTEXT] is String)
            listOf(input[JSONLD_CONTEXT] as String)
        else
            emptyList()
    }

    fun removeContextFromInput(input: Map<String, Any>): Map<String, Any> =
        input.minus(JSONLD_CONTEXT)

    fun expandValueAsListOfMap(value: Any): List<Map<String, List<Any>>> =
        value as List<Map<String, List<Any>>>

    /**
     * Extract the actual value (@value) of a property from the properties map of an expanded property.
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
                val finalValueType = firstListEntry[JSONLD_TYPE]
                when {
                    finalValueType != null -> {
                        val finalValue = String::class.safeCast(firstListEntry[JSONLD_VALUE_KW])
                        when (finalValueType) {
                            NGSILD_DATE_TIME_TYPE -> ZonedDateTime.parse(finalValue)
                            NGSILD_DATE_TYPE -> LocalDate.parse(finalValue)
                            NGSILD_TIME_TYPE -> LocalTime.parse(finalValue)
                            else -> firstListEntry[JSONLD_VALUE_KW]
                        }
                    }

                    firstListEntry[JSONLD_VALUE_KW] != null ->
                        firstListEntry[JSONLD_VALUE_KW]

                    firstListEntry[JSONLD_ID] != null -> {
                        // Used to get the value of datasetId property,
                        // since it is mapped to "@id" key rather than "@value"
                        firstListEntry[JSONLD_ID]
                    }

                    else -> {
                        // it is a map / JSON object, keep it as is
                        // {https://uri.etsi.org/ngsi-ld/default-context/key=[{@value=value}], ...}
                        firstListEntry
                    }
                }
            } else {
                intermediateList.map {
                    it[JSONLD_VALUE_KW]
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

    fun getAttributeFromExpandedAttributes(
        expandedAttributes: Map<String, Any>,
        expandedAttributeName: String,
        datasetId: URI?
    ): ExpandedAttributePayloadEntry? {
        if (!expandedAttributes.containsKey(expandedAttributeName))
            return null

        return (expandedAttributes[expandedAttributeName]!! as List<Map<String, List<Any>>>)
            .find {
                if (datasetId == null)
                    !it.containsKey(NGSILD_DATASET_ID_PROPERTY)
                else
                    getPropertyValueFromMap(it, NGSILD_DATASET_ID_PROPERTY) == datasetId.toString()
            }
    }

    /**
     * Utility but basic method to find if given contexts can resolve a known term from the core context.
     */
    private fun canExpandJsonLdKeyFromCore(contexts: List<String>): Boolean {
        val jsonLdOptions = JsonLdOptions()
        jsonLdOptions.expandContext = mapOf(JSONLD_CONTEXT to contexts)
        val expandedType = JsonLdProcessor.expand(mapOf("datasetId" to mapOf<String, Any>()), jsonLdOptions)
        return expandedType.isNotEmpty() &&
            (expandedType[0] as? Map<*, *>)?.containsKey(NGSILD_DATASET_ID_PROPERTY) ?: false
    }

    private fun restoreGeoPropertyValue(): (Map.Entry<String, Any>) -> Any = {
        if (NGSILD_GEO_PROPERTIES_TERMS.contains(it.key)) {
            when (it.value) {
                is Map<*, *> -> {
                    val geoValues = it.value as MutableMap<String, Any>
                    if (geoValues.isNotEmpty()) {
                        geoValues[JSONLD_VALUE] = wktToGeoJson(geoValues[JSONLD_VALUE] as String)
                        geoValues
                    } else geoValues
                }
                // case of a multi-instance geoproperty or when retrieving the history of a geoproperty
                is List<*> ->
                    (it.value as List<Map<String, Any>>).map { geoInstance ->
                        val geoValues = geoInstance.toMutableMap()
                        geoValues[JSONLD_VALUE] = wktToGeoJson(geoValues[JSONLD_VALUE] as String)
                        geoValues
                    }
                else -> it.value
            }
        } else it.value
    }

    fun compact(
        jsonLdEntity: JsonLdEntity,
        context: String? = null,
        mediaType: MediaType = JSON_LD_MEDIA_TYPE
    ): CompactedJsonLdEntity {
        val contexts = addCoreContextIfMissing(listOfNotNull(context))

        return if (mediaType == MediaType.APPLICATION_JSON)
            JsonLdProcessor.compact(jsonLdEntity.properties, mapOf(JSONLD_CONTEXT to contexts), JsonLdOptions())
                .minus(JSONLD_CONTEXT)
                .mapValues(restoreGeoPropertyValue())
        else
            JsonLdProcessor.compact(jsonLdEntity.properties, mapOf(JSONLD_CONTEXT to contexts), JsonLdOptions())
                .minus(JSONLD_CONTEXT)
                .plus(JSONLD_CONTEXT to contexts)
                .mapValues(restoreGeoPropertyValue())
    }

    fun compact(
        jsonLdEntity: JsonLdEntity,
        contexts: List<String>,
        mediaType: MediaType = JSON_LD_MEDIA_TYPE
    ): CompactedJsonLdEntity {
        val allContexts = addCoreContextIfMissing(contexts)

        return if (mediaType == MediaType.APPLICATION_JSON)
            JsonLdProcessor.compact(jsonLdEntity.properties, mapOf(JSONLD_CONTEXT to allContexts), JsonLdOptions())
                .minus(JSONLD_CONTEXT)
                .mapValues(restoreGeoPropertyValue())
        else
            JsonLdProcessor.compact(jsonLdEntity.properties, mapOf(JSONLD_CONTEXT to allContexts), JsonLdOptions())
                .minus(JSONLD_CONTEXT)
                .plus(JSONLD_CONTEXT to allContexts)
                .mapValues(restoreGeoPropertyValue())
    }

    fun compactEntities(
        entities: List<JsonLdEntity>,
        useSimplifiedRepresentation: Boolean,
        context: String,
        mediaType: MediaType
    ): List<CompactedJsonLdEntity> =
        entities.map {
            if (useSimplifiedRepresentation)
                compact(it, context, mediaType).toKeyValues()
            else
                compact(it, context, mediaType)
        }

    fun compactTerms(terms: List<ExpandedTerm>, contexts: List<String>): List<String> =
        terms.map {
            compactTerm(it, contexts)
        }

    /**
     * Compact a term (type, attribute name, ...) using the provided context.
     */
    fun compactTerm(term: String, contexts: List<String>): String {
        val compactedFragment =
            JsonLdProcessor.compact(
                mapOf(term to emptyMap<String, Any>()),
                mapOf(JSONLD_CONTEXT to addCoreContextIfMissing(contexts)),
                JsonLdOptions()
            )
        return compactedFragment.keys.first()
    }

    fun compactFragment(value: Map<String, Any>, contexts: List<String>): Map<String, Any> =
        JsonLdProcessor.compact(value, mapOf(JSONLD_CONTEXT to addCoreContextIfMissing(contexts)), JsonLdOptions())
            .mapValues(restoreGeoPropertyValue())

    fun compactAndSerialize(
        jsonLdEntity: JsonLdEntity,
        contexts: List<String>,
        mediaType: MediaType = JSON_LD_MEDIA_TYPE
    ): String =
        serializeObject(compact(jsonLdEntity, contexts, mediaType))

    fun filterCompactedEntityOnAttributes(
        input: CompactedJsonLdEntity,
        includedAttributes: Set<String>
    ): Map<String, Any> {
        val identity: (CompactedJsonLdEntity) -> CompactedJsonLdEntity = { it }
        return filterEntityOnAttributes(input, identity, includedAttributes, false)
    }

    fun filterJsonLdEntityOnAttributes(
        input: JsonLdEntity,
        includedAttributes: Set<String>
    ): Map<String, Any> {
        val inputToMap = { i: JsonLdEntity -> i.properties }
        return filterEntityOnAttributes(input, inputToMap, includedAttributes, true)
    }

    private fun <T> filterEntityOnAttributes(
        input: T,
        inputToMap: (T) -> Map<String, Any>,
        includedAttributes: Set<String>,
        isExpandedForm: Boolean
    ): Map<String, Any> {
        return if (includedAttributes.isEmpty()) {
            inputToMap(input)
        } else {
            val mandatoryFields = if (isExpandedForm)
                JSONLD_EXPANDED_ENTITY_MANDATORY_FIELDS
            else
                JSONLD_COMPACTED_ENTITY_MANDATORY_FIELDS
            val includedKeys = mandatoryFields.plus(includedAttributes)
            inputToMap(input).filterKeys { includedKeys.contains(it) }
        }
    }

    fun extractRelationshipObject(name: String, values: Map<String, List<Any>>): Either<BadRequestDataException, URI> {
        return values.right()
            .flatMap {
                if (!it.containsKey(NGSILD_RELATIONSHIP_HAS_OBJECT))
                    BadRequestDataException("Relationship $name does not have an object field").left()
                else it[NGSILD_RELATIONSHIP_HAS_OBJECT]!!.right()
            }
            .flatMap {
                if (it.isEmpty())
                    BadRequestDataException("Relationship $name is empty").left()
                else it[0].right()
            }
            .flatMap {
                if (it !is Map<*, *>)
                    BadRequestDataException("Relationship $name has an invalid object type: ${it.javaClass}").left()
                else it[JSONLD_ID].right()
            }
            .flatMap {
                if (it !is String)
                    BadRequestDataException("Relationship $name has an invalid or no object id: $it").left()
                else it.toUri().right()
            }
    }

    /**
     * Build the expanded payload of a property.
     *
     * For instance:
     *
     * "https://uri.etsi.org/ngsi-ld/default-context/aProperty": [
     *   {
     *     "@type": [
     *       "https://uri.etsi.org/ngsi-ld/Property"
     *     ],
     *     "https://uri.etsi.org/ngsi-ld/hasValue": [
     *       {
     *         "@value": "Hop"
     *       }
     *     ]
     *   }
     * ]
     */
    fun buildExpandedProperty(value: Any): ExpandedAttributePayload =
        listOf(
            mapOf(
                JSONLD_TYPE to listOf(NGSILD_PROPERTY_TYPE.uri),
                NGSILD_PROPERTY_VALUE to listOf(mapOf(JSONLD_VALUE_KW to value))
            )
        )

    /**
     * Build the expanded payload of a property.
     *
     * "https://uri.etsi.org/ngsi-ld/default-context/aRelationship": [
     *   {
     *     "https://uri.etsi.org/ngsi-ld/hasObject": [
     *       {
     *         "@id": "urn:ngsi-ld:Entity02"
     *       }
     *     ],
     *     "@type": [
     *       "https://uri.etsi.org/ngsi-ld/Relationship"
     *     ]
     *   }
     * ]
     */
    fun buildExpandedRelationship(value: URI): ExpandedAttributePayload =
        listOf(
            mapOf(
                JSONLD_TYPE to listOf(NGSILD_RELATIONSHIP_TYPE.uri),
                NGSILD_RELATIONSHIP_HAS_OBJECT to listOf(mapOf(JSONLD_ID to value.toString()))
            )
        )

    /**
     * Build the expanded payload of a non-reified temporal property (createdAt, modifiedAt,...).
     *
     * For instance:
     *
     * "https://uri.etsi.org/ngsi-ld/createdAt": [
     *   {
     *     "@type": "https://uri.etsi.org/ngsi-ld/DateTime",
     *     "@value": "2022-12-01T00:00:00Z"
     *   }
     * ]
     */
    fun buildNonReifiedDateTime(value: ZonedDateTime): List<Map<String, Any>> =
        listOf(
            mapOf(
                JSONLD_TYPE to NGSILD_DATE_TIME_TYPE,
                JSONLD_VALUE_KW to value.toNgsiLdFormat()
            )
        )

    /**
     * Build the expanded payload of a non-reified core property.
     *
     * For instance:
     *
     * "https://uri.etsi.org/ngsi-ld/datasetId": [
     *   {
     *     "@id": "urn:ngsi-ld:Dataset:01"
     *   }
     * ]
     */
    fun buildNonReifiedProperty(value: Any): List<Map<String, Any>> =
        listOf(
            mapOf(JSONLD_ID to value)
        )
}

typealias ExpandedAttributePayload = List<ExpandedAttributePayloadEntry>
typealias ExpandedAttributePayloadEntry = Map<String, List<Any>>

fun ExpandedAttributePayload.addSubAttribute(
    subAttributeName: ExpandedTerm,
    subAttributePayload: List<Map<String, Any>>
): ExpandedAttributePayload {
    if (this.isEmpty() || this.size > 1)
        throw BadRequestDataException("Cannot add a sub-attribute into empty or multi-instance attribute: $this")
    return listOf(this[0].plus(subAttributeName to subAttributePayload))
}

fun ExpandedAttributePayload.getSingleEntry(): ExpandedAttributePayloadEntry {
    if (this.isEmpty() || this.size > 1)
        throw BadRequestDataException("Cannot add a sub-attribute into empty or multi-instance attribute: $this")
    return this[0]
}

fun String.extractShortTypeFromExpanded(): String =
    /*
     * TODO is it always after a '/' ? can't it be after a '#' ? (https://redmine.eglobalmark.com/issues/852)
     * TODO do a clean implementation using info from @context
     */
    this.substringAfterLast("/").substringAfterLast("#")

fun CompactedJsonLdEntity.toKeyValues(): Map<String, Any> =
    this.mapValues { (_, value) -> simplifyRepresentation(value) }

private fun simplifyRepresentation(value: Any): Any {
    return when (value) {
        // entity property value is always a Map
        is Map<*, *> -> simplifyValue(value)
        is List<*> -> value.map {
            when (it) {
                is Map<*, *> -> simplifyValue(it)
                // we keep @context value as it is (List<String>)
                else -> it
            }
        }
        // we keep id and type values as they are (String)
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

fun CompactedJsonLdEntity.withoutSysAttrs(): Map<String, Any> =
    this.filter {
        !NGSILD_SYSATTRS_TERMS.contains(it.key)
    }.mapValues {
        when (it.value) {
            is Map<*, *> -> (it.value as Map<*, *>).minus(NGSILD_SYSATTRS_TERMS)
            is List<*> -> (it.value as List<*>).map { valueInstance ->
                when (valueInstance) {
                    is Map<*, *> -> valueInstance.minus(NGSILD_SYSATTRS_TERMS)
                    // we keep @context value as it is (List<String>)
                    else -> valueInstance
                }
            }
            else -> it.value
        }
    }

fun CompactedJsonLdEntity.toFinalRepresentation(
    includeSysAttrs: Boolean,
    useSimplifiedRepresentation: Boolean
): CompactedJsonLdEntity =
    this.let {
        if (!includeSysAttrs) it.withoutSysAttrs()
        else it
    }.let {
        if (useSimplifiedRepresentation) it.toKeyValues()
        else it
    }

fun geoPropertyToWKT(jsonFragment: Map<String, Any>): Map<String, Any> {
    for (geoProperty in NGSILD_GEO_PROPERTIES_TERMS) {
        if (jsonFragment.containsKey(geoProperty)) {
            val geoAttribute = jsonFragment[geoProperty] as MutableMap<String, Any>
            val geoJsonAsString = geoAttribute[JSONLD_VALUE]
            val wktGeom = geoJsonToWkt(geoJsonAsString!! as Map<String, Any>)
            geoAttribute[JSONLD_VALUE] = wktGeom
        }
    }
    return jsonFragment
}

fun Map<String, Any>.addDateTimeProperty(propertyKey: String, dateTime: ZonedDateTime?): Map<String, Any> =
    if (dateTime != null)
        this.plus(
            propertyKey to mapOf(
                JsonLdUtils.JSONLD_TYPE to JsonLdUtils.NGSILD_DATE_TIME_TYPE,
                JsonLdUtils.JSONLD_VALUE_KW to dateTime.toNgsiLdFormat()
            )
        )
    else this
