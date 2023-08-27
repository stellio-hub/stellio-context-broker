package com.egm.stellio.shared.util

import arrow.core.Either
import arrow.core.flatMap
import arrow.core.left
import arrow.core.right
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_ID
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_TYPE
import com.egm.stellio.shared.util.JsonLdUtils.JSONLD_VALUE_TERM
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_GEO_PROPERTIES_TERMS
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_OBSERVED_AT_PROPERTY
import com.egm.stellio.shared.util.JsonLdUtils.NGSILD_SYSATTRS_TERMS
import com.egm.stellio.shared.util.JsonLdUtils.buildNonReifiedDateTime
import com.egm.stellio.shared.util.JsonLdUtils.getPropertyValueFromMapAsDateTime
import com.egm.stellio.shared.util.JsonUtils.deserializeAs
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import com.egm.stellio.shared.util.JsonUtils.deserializeObject
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

@JvmInline
value class AttributeType(val uri: String)

object JsonLdUtils {

    const val NGSILD_CORE_CONTEXT = "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.7.jsonld"
    const val EGM_BASE_CONTEXT_URL =
        "https://raw.githubusercontent.com/easy-global-market/ngsild-api-data-models/master"
    const val NGSILD_EGM_CONTEXT = "$EGM_BASE_CONTEXT_URL/shared-jsonld-contexts/egm.jsonld"

    const val NGSILD_DEFAULT_VOCAB = "https://uri.etsi.org/ngsi-ld/default-context/"

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
    const val JSONLD_VALUE_TERM = "value"
    const val JSONLD_VALUE = "@value"
    const val JSONLD_OBJECT = "object"
    const val JSONLD_CONTEXT = "@context"
    const val NGSILD_SCOPE_TERM = "scope"
    const val NGSILD_SCOPE_PROPERTY = "https://uri.etsi.org/ngsi-ld/$NGSILD_SCOPE_TERM"

    val JSONLD_EXPANDED_ENTITY_SPECIFIC_MEMBERS = setOf(JSONLD_TYPE, NGSILD_SCOPE_PROPERTY)

    // List of members that are part of a core entity base definition (i.e., without attributes)
    val JSONLD_EXPANDED_ENTITY_CORE_MEMBERS = setOf(JSONLD_ID, JSONLD_TYPE, JSONLD_CONTEXT, NGSILD_SCOPE_PROPERTY)
    val JSONLD_COMPACTED_ENTITY_CORE_MEMBERS =
        setOf(JSONLD_ID_TERM, JSONLD_TYPE_TERM, JSONLD_CONTEXT, NGSILD_SCOPE_TERM)

    const val NGSILD_CREATED_AT_TERM = "createdAt"
    const val NGSILD_MODIFIED_AT_TERM = "modifiedAt"
    val NGSILD_SYSATTRS_TERMS = setOf(NGSILD_CREATED_AT_TERM, NGSILD_MODIFIED_AT_TERM)
    const val NGSILD_CREATED_AT_PROPERTY = "https://uri.etsi.org/ngsi-ld/$NGSILD_CREATED_AT_TERM"
    const val NGSILD_MODIFIED_AT_PROPERTY = "https://uri.etsi.org/ngsi-ld/$NGSILD_MODIFIED_AT_TERM"
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
    const val NGSILD_NOTIFICATION_TERM = "Notification"

    const val NGSILD_DATE_TIME_TYPE = "https://uri.etsi.org/ngsi-ld/DateTime"
    const val NGSILD_DATE_TYPE = "https://uri.etsi.org/ngsi-ld/Date"
    const val NGSILD_TIME_TYPE = "https://uri.etsi.org/ngsi-ld/Time"

    const val NGSILD_NAME_TERM = "name"
    const val NGSILD_NAME_PROPERTY = "https://schema.org/name"

    const val DATASET_ID_PREFIX = "urn:ngsi-ld:Dataset:"

    val logger: Logger = LoggerFactory.getLogger(javaClass)

    private val localCoreContextPayload =
        ClassPathResource("/contexts/ngsi-ld-core-context-v1.7.jsonld").inputStream.readBytes().toString(Charsets.UTF_8)
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

    suspend fun expandDeserializedPayload(
        deserializedPayload: Map<String, Any>,
        contexts: List<String>
    ): Map<String, Any> =
        doJsonLdExpansion(deserializedPayload, contexts)

    suspend fun expandJsonLdEntity(input: Map<String, Any>, contexts: List<String>): JsonLdEntity =
        JsonLdEntity(doJsonLdExpansion(input, contexts), contexts)

    suspend fun expandJsonLdEntity(input: String, contexts: List<String>): JsonLdEntity =
        expandJsonLdEntity(input.deserializeAsMap(), contexts)

    suspend fun expandJsonLdEntity(input: String): JsonLdEntity {
        val jsonInput = input.deserializeAsMap()
        return expandJsonLdEntity(jsonInput, extractContextFromInput(jsonInput))
    }

    suspend fun expandJsonLdEntities(entities: List<Map<String, Any>>): List<JsonLdEntity> =
        entities.map {
            expandJsonLdEntity(it, extractContextFromInput(it))
        }

    suspend fun expandJsonLdEntities(entities: List<Map<String, Any>>, contexts: List<String>): List<JsonLdEntity> =
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
        return if (expandedType.isNotEmpty())
            (expandedType[0] as Map<String, Any>).keys.first()
        else
            term
    }

    suspend fun expandJsonLdFragment(fragment: Map<String, Any>, contexts: List<String>): Map<String, List<Any>> =
        doJsonLdExpansion(fragment, contexts) as Map<String, List<Any>>

    suspend fun expandJsonLdFragment(fragment: String, contexts: List<String>): Map<String, List<Any>> =
        expandJsonLdFragment(fragment.deserializeAsMap(), contexts)

    suspend fun expandAttribute(
        fragment: String,
        contexts: List<String>
    ): ExpandedAttribute =
        (expandJsonLdFragment(deserializeObject(fragment), contexts) as ExpandedAttributes).toList()[0]

    suspend fun expandAttribute(
        attributeName: String,
        attributePayload: String,
        contexts: List<String>
    ): ExpandedAttribute =
        (expandJsonLdFragment(mapOf(attributeName to deserializeAs(attributePayload)), contexts) as ExpandedAttributes)
            .toList().first()

    suspend fun expandAttribute(
        attributeName: String,
        attributePayload: Map<String, Any>,
        contexts: List<String>
    ): ExpandedAttribute =
        (expandJsonLdFragment(mapOf(attributeName to attributePayload), contexts) as ExpandedAttributes).toList()[0]

    suspend fun expandAttributes(
        fragment: String,
        contexts: List<String>
    ): ExpandedAttributes =
        expandAttributes(deserializeObject(fragment), contexts)

    suspend fun expandAttributes(
        fragment: Map<String, Any>,
        contexts: List<String>
    ): ExpandedAttributes =
        expandJsonLdFragment(fragment, contexts) as ExpandedAttributes

    private suspend fun doJsonLdExpansion(fragment: Map<String, Any>, contexts: List<String>): Map<String, Any> {
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

    fun castAttributeValue(value: Any): List<Map<String, List<Any>>> =
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
    fun getPropertyValueFromMap(value: ExpandedAttributeInstance, propertyKey: String): Any? =
        if (value[propertyKey] != null) {
            val intermediateList = value[propertyKey] as List<Map<String, Any>>
            if (intermediateList.size == 1) {
                val firstListEntry = intermediateList[0]
                val finalValueType = firstListEntry[JSONLD_TYPE]
                when {
                    finalValueType != null -> {
                        val finalValue = String::class.safeCast(firstListEntry[JSONLD_VALUE])
                        when (finalValueType) {
                            NGSILD_DATE_TIME_TYPE -> ZonedDateTime.parse(finalValue)
                            NGSILD_DATE_TYPE -> LocalDate.parse(finalValue)
                            NGSILD_TIME_TYPE -> LocalTime.parse(finalValue)
                            else -> firstListEntry[JSONLD_VALUE]
                        }
                    }

                    firstListEntry[JSONLD_VALUE] != null ->
                        firstListEntry[JSONLD_VALUE]

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
                    it[JSONLD_VALUE]
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
    ): ExpandedAttributeInstance? {
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
                        geoValues[JSONLD_VALUE_TERM] = wktToGeoJson(geoValues[JSONLD_VALUE_TERM] as String)
                        geoValues
                    } else geoValues
                }
                // case of a multi-instance geoproperty or when retrieving the history of a geoproperty
                is List<*> ->
                    (it.value as List<Map<String, Any>>).map { geoInstance ->
                        val geoValues = geoInstance.toMutableMap()
                        geoValues[JSONLD_VALUE_TERM] = wktToGeoJson(geoValues[JSONLD_VALUE_TERM] as String)
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
            JsonLdProcessor.compact(jsonLdEntity.members, mapOf(JSONLD_CONTEXT to contexts), JsonLdOptions())
                .minus(JSONLD_CONTEXT)
                .mapValues(restoreGeoPropertyValue())
        else
            JsonLdProcessor.compact(jsonLdEntity.members, mapOf(JSONLD_CONTEXT to contexts), JsonLdOptions())
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
            JsonLdProcessor.compact(jsonLdEntity.members, mapOf(JSONLD_CONTEXT to allContexts), JsonLdOptions())
                .minus(JSONLD_CONTEXT)
                .mapValues(restoreGeoPropertyValue())
        else
            JsonLdProcessor.compact(jsonLdEntity.members, mapOf(JSONLD_CONTEXT to allContexts), JsonLdOptions())
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
        val inputToMap = { i: JsonLdEntity -> i.members }
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
                JSONLD_EXPANDED_ENTITY_CORE_MEMBERS
            else
                JSONLD_COMPACTED_ENTITY_CORE_MEMBERS
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
    fun buildExpandedProperty(value: Any): ExpandedAttributeInstances =
        listOf(
            mapOf(
                JSONLD_TYPE to listOf(NGSILD_PROPERTY_TYPE.uri),
                NGSILD_PROPERTY_VALUE to listOf(mapOf(JSONLD_VALUE to value))
            )
        )

    /**
     * Build an expanded JSON value for a property.
     *
     * For instance:
     *
     * "[
     *   {
     *     "@type": [
     *       "https://uri.etsi.org/ngsi-ld/Property"
     *     ],
     *     "https://uri.etsi.org/ngsi-ld/hasValue": [
     *       {
     *         "https://ontology.eglobalmark.com/authorization#kind": [
     *              {
     *                  "@value": "kind"
     *               }
     *         ],
     *         "https://ontology.eglobalmark.com/authorization#username": [
     *              {
     *                  "@value": "username"
     *              }
     *         ]
     *       }
     *     ]
     *   }
     * ]
     */
    suspend fun buildExpandedPropertyMapValue(
        value: Map<String, String>,
        contexts: List<String>
    ): ExpandedAttributeInstances =
        listOf(
            mapOf(
                JSONLD_TYPE to listOf(NGSILD_PROPERTY_TYPE.uri),
                NGSILD_PROPERTY_VALUE to listOf(
                    expandJsonLdFragment(value, contexts).mapValues { it.value }
                )
            )
        )

    /**
     * Build the expanded payload of a relationship.
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
    fun buildExpandedRelationship(value: URI): ExpandedAttributeInstances =
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
                JSONLD_VALUE to value.toNgsiLdFormat()
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
    fun buildNonReifiedProperty(value: String): List<Map<String, Any>> =
        listOf(
            mapOf(JSONLD_ID to value)
        )
}

// basic alias to help identify, mainly in method calls, if the expected value is a compact or expanded one
typealias ExpandedTerm = String
typealias ExpandedAttributes = Map<ExpandedTerm, ExpandedAttributeInstances>
typealias ExpandedAttribute = Pair<ExpandedTerm, ExpandedAttributeInstances>
typealias ExpandedAttributeInstances = List<ExpandedAttributeInstance>
typealias ExpandedAttributeInstance = Map<String, List<Any>>

fun ExpandedAttribute.toExpandedAttributes() =
    mapOf(this.first to this.second)

fun ExpandedAttributeInstances.addSubAttribute(
    subAttributeName: ExpandedTerm,
    subAttributePayload: List<Map<String, Any>>
): ExpandedAttributeInstances {
    if (this.isEmpty() || this.size > 1)
        throw BadRequestDataException("Cannot add a sub-attribute into empty or multi-instance attribute: $this")
    return listOf(this[0].plus(subAttributeName to subAttributePayload))
}

fun ExpandedAttributeInstances.getSingleEntry(): ExpandedAttributeInstance {
    if (this.isEmpty() || this.size > 1)
        throw BadRequestDataException("Expected a single entry but got none or more than one: $this")
    return this[0]
}

fun CompactedJsonLdEntity.toKeyValues(): Map<String, Any> =
    this.mapValues { (_, value) -> simplifyRepresentation(value) }

private fun simplifyRepresentation(value: Any): Any {
    return when (value) {
        // entity property value is always a Map
        is Map<*, *> -> simplifyValue(value as Map<String, Any>)
        is List<*> -> value.map {
            when (it) {
                is Map<*, *> -> simplifyValue(it as Map<String, Any>)
                // we keep @context value as it is (List<String>)
                else -> it
            }
        }
        // we keep id and type values as they are (String)
        else -> value
    }
}

private fun simplifyValue(value: Map<String, Any>): Any {
    return when (value["type"]) {
        "Property", "GeoProperty" -> value.getOrDefault("value", value)
        "Relationship" -> value.getOrDefault("object", value)
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
            // when creating a temporal entity, a geoproperty can be represented as a list of instances
            val geoAttributes =
                if (jsonFragment[geoProperty] is MutableMap<*, *>)
                    listOf(jsonFragment[geoProperty] as MutableMap<String, Any>)
                else
                    jsonFragment[geoProperty] as List<MutableMap<String, Any>>
            geoAttributes.forEach { geoAttribute ->
                val geoJsonAsString = geoAttribute[JSONLD_VALUE_TERM]
                val wktGeom = geoJsonToWkt(geoJsonAsString!! as Map<String, Any>)
                    .fold({
                        throw BadRequestDataException(it.message)
                    }, { it })
                geoAttribute[JSONLD_VALUE_TERM] = wktGeom
            }
        }
    }
    return jsonFragment
}

fun Map<String, Any>.addDateTimeProperty(propertyKey: String, dateTime: ZonedDateTime?): Map<String, Any> =
    if (dateTime != null)
        this.plus(propertyKey to buildNonReifiedDateTime(dateTime))
    else this

fun Map<String, Any>.addSysAttrs(
    withSysAttrs: Boolean,
    createdAt: ZonedDateTime,
    modifiedAt: ZonedDateTime?
): Map<String, Any> =
    if (withSysAttrs)
        this.plus(JsonLdUtils.NGSILD_CREATED_AT_PROPERTY to buildNonReifiedDateTime(createdAt))
            .let {
                if (modifiedAt != null)
                    it.plus(JsonLdUtils.NGSILD_MODIFIED_AT_PROPERTY to buildNonReifiedDateTime(modifiedAt))
                else it
            }
    else this

fun ExpandedAttributes.checkTemporalAttributeInstance(): Either<APIException, Unit> =
    this.values.all { expandedInstances ->
        expandedInstances.all { expandedAttributePayloadEntry ->
            getPropertyValueFromMapAsDateTime(expandedAttributePayloadEntry, NGSILD_OBSERVED_AT_PROPERTY) != null
        }
    }.let {
        if (it) Unit.right()
        else BadRequestDataException(invalidTemporalInstanceMessage()).left()
    }

fun ExpandedAttributes.sorted(): ExpandedAttributes =
    this.mapValues {
        it.value.sortedByDescending { expandedAttributePayloadEntry ->
            getPropertyValueFromMapAsDateTime(expandedAttributePayloadEntry, NGSILD_OBSERVED_AT_PROPERTY)
        }
    }

fun ExpandedAttributes.keepFirstInstances(): ExpandedAttributes =
    this.mapValues { listOf(it.value.first()) }

fun ExpandedAttributes.removeFirstInstances(): ExpandedAttributes =
    this.mapValues {
        it.value.drop(1)
    }

fun ExpandedAttributes.addCoreMembers(
    entityId: String,
    entityTypes: List<ExpandedTerm>
): Map<String, Any> =
    this.plus(listOf(JSONLD_ID to entityId, JSONLD_TYPE to entityTypes))
