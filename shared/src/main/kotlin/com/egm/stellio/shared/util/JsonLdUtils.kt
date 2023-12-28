package com.egm.stellio.shared.util

import arrow.core.Either
import arrow.core.flatMap
import arrow.core.left
import arrow.core.right
import com.apicatalog.jsonld.JsonLd
import com.apicatalog.jsonld.JsonLdError
import com.apicatalog.jsonld.JsonLdOptions
import com.apicatalog.jsonld.context.cache.LruCache
import com.apicatalog.jsonld.document.JsonDocument
import com.egm.stellio.shared.model.*
import com.egm.stellio.shared.util.JsonUtils.deserializeAs
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import com.egm.stellio.shared.util.JsonUtils.deserializeObject
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import jakarta.json.Json
import jakarta.json.JsonObject
import jakarta.json.JsonStructure
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.http.MediaType
import java.io.ByteArrayOutputStream
import java.net.URI
import java.time.LocalDate
import java.time.LocalTime
import java.time.ZonedDateTime
import kotlin.reflect.full.safeCast

@JvmInline
value class AttributeType(val uri: String)

object JsonLdUtils {

    const val NGSILD_CORE_CONTEXT = "https://easy-global-market.github.io/ngsild-api-data-models/" +
        "shared-jsonld-contexts/ngsi-ld-core-context-v1.8.jsonld"

    const val NGSILD_PREFIX = "https://uri.etsi.org/ngsi-ld/"
    const val NGSILD_DEFAULT_VOCAB = "https://uri.etsi.org/ngsi-ld/default-context/"

    const val NGSILD_PROPERTY_TERM = "Property"
    val NGSILD_PROPERTY_TYPE = AttributeType("https://uri.etsi.org/ngsi-ld/Property")
    const val NGSILD_PROPERTY_VALUE = "https://uri.etsi.org/ngsi-ld/hasValue"
    const val NGSILD_GEOPROPERTY_TERM = "GeoProperty"
    val NGSILD_GEOPROPERTY_TYPE = AttributeType("https://uri.etsi.org/ngsi-ld/GeoProperty")
    const val NGSILD_GEOPROPERTY_VALUE = "https://uri.etsi.org/ngsi-ld/hasValue"
    const val NGSILD_JSONPROPERTY_TERM = "JsonProperty"
    val NGSILD_JSONPROPERTY_TYPE = AttributeType("https://uri.etsi.org/ngsi-ld/JsonProperty")
    const val NGSILD_JSONPROPERTY_VALUE = "https://uri.etsi.org/ngsi-ld/hasJSON"
    const val NGSILD_RELATIONSHIP_TERM = "Relationship"
    val NGSILD_RELATIONSHIP_TYPE = AttributeType("https://uri.etsi.org/ngsi-ld/Relationship")
    const val NGSILD_RELATIONSHIP_OBJECT = "https://uri.etsi.org/ngsi-ld/hasObject"

    const val NGSILD_PROPERTY_VALUES = "https://uri.etsi.org/ngsi-ld/hasValues"
    const val NGSILD_GEOPROPERTY_VALUES = "https://uri.etsi.org/ngsi-ld/hasValues"
    const val NGSILD_RELATIONSHIP_OBJECTS = "https://uri.etsi.org/ngsi-ld/hasObjects"
    const val NGSILD_JSONPROPERTY_VALUES = "https://uri.etsi.org/ngsi-ld/jsons"

    const val JSONLD_ID_TERM = "id"
    const val JSONLD_ID = "@id"
    const val JSONLD_TYPE_TERM = "type"
    const val JSONLD_TYPE = "@type"
    const val JSONLD_VALUE_TERM = "value"
    const val JSONLD_VALUE = "@value"
    const val JSONLD_OBJECT = "object"
    const val JSONLD_JSON_TERM = "json"
    const val JSONLD_JSON = "@json"
    const val JSONLD_LIST = "@list"
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
    val NGSILD_SYSATTRS_PROPERTIES = setOf(NGSILD_CREATED_AT_PROPERTY, NGSILD_MODIFIED_AT_PROPERTY)
    const val NGSILD_OBSERVED_AT_TERM = "observedAt"
    const val NGSILD_OBSERVED_AT_PROPERTY = "https://uri.etsi.org/ngsi-ld/$NGSILD_OBSERVED_AT_TERM"
    const val NGSILD_UNIT_CODE_PROPERTY = "https://uri.etsi.org/ngsi-ld/unitCode"
    const val NGSILD_LOCATION_TERM = "location"
    const val NGSILD_LOCATION_PROPERTY = "https://uri.etsi.org/ngsi-ld/location"
    const val NGSILD_OBSERVATION_SPACE_TERM = "observationSpace"
    const val NGSILD_OBSERVATION_SPACE_PROPERTY = "https://uri.etsi.org/ngsi-ld/observationSpace"
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

    private const val DEFAULT_CORE_CONTEXT_PATH = NGSILD_CORE_CONTEXT

    private const val CONTEXT_CACHE_CAPACITY = 128
    private const val DOCUMENT_CACHE_CAPACITY = 256

    private val jsonLdOptions = JsonLdOptions().apply {
        contextCache = LruCache(CONTEXT_CACHE_CAPACITY)
        documentCache = LruCache(DOCUMENT_CACHE_CAPACITY)
    }

    private fun buildContextDocument(contexts: List<String>): JsonStructure {
        val contextsArray = Json.createArrayBuilder()
        contexts.forEach { contextsArray.add(it) }
        return contextsArray.build()
    }

    private fun addCoreContext(contexts: List<String>): List<String> =
        contexts.plus(DEFAULT_CORE_CONTEXT_PATH)

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

    suspend fun expandDeserializedPayload(
        deserializedPayload: Map<String, Any>,
        contexts: List<String>
    ): Map<String, Any> =
        doJsonLdExpansion(deserializedPayload, contexts)

    suspend fun expandJsonLdEntityF(
        input: Map<String, Any>,
        contexts: List<String>
    ): Either<APIException, ExpandedEntity> =
        runCatching {
            doJsonLdExpansion(input, contexts)
        }.fold({
            ExpandedEntity(it, contexts).right()
        }, {
            if (it is APIException) it.left()
            else it.toAPIException().left()
        })

    suspend fun expandJsonLdEntityF(input: Map<String, Any>): Either<APIException, ExpandedEntity> =
        expandJsonLdEntityF(input, extractContextFromInput(input))

    suspend fun expandJsonLdEntity(input: Map<String, Any>, contexts: List<String>): ExpandedEntity =
        ExpandedEntity(doJsonLdExpansion(input, contexts), contexts)

    suspend fun expandJsonLdEntity(input: String, contexts: List<String>): ExpandedEntity =
        expandJsonLdEntity(input.deserializeAsMap(), contexts)

    suspend fun expandJsonLdEntity(input: String): ExpandedEntity {
        val jsonInput = input.deserializeAsMap()
        return expandJsonLdEntity(jsonInput, extractContextFromInput(jsonInput))
    }

    fun expandJsonLdTerms(terms: List<String>, contexts: List<String>): List<ExpandedTerm> =
        terms.map {
            expandJsonLdTerm(it, contexts)
        }

    fun expandJsonLdTerm(term: String, context: String): String =
        expandJsonLdTerm(term, listOf(context))

    fun expandJsonLdTerm(term: String, contexts: List<String>): String =
        try {
            JsonLd.expand(
                JsonDocument.of(
                    serializeObject(
                        mapOf(
                            term to mapOf<String, Any>(),
                            JSONLD_CONTEXT to addCoreContext(contexts)
                        )
                    ).byteInputStream()
                )
            )
                .options(jsonLdOptions)
                .get()
                .let {
                    if (it.isNotEmpty())
                        (it[0] as JsonObject).keys.first()
                    else term
                }
        } catch (e: JsonLdError) {
            logger.error("Unable to expand term $term with context $contexts: ${e.message}")
            throw e.toAPIException()
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

        try {
            val expandedFragment = JsonLd.expand(
                JsonDocument.of(
                    serializeObject(parsedFragment.plus(JSONLD_CONTEXT to usedContext)).byteInputStream()
                )
            )
                .options(jsonLdOptions)
                .get()

            if (expandedFragment.isEmpty())
                throw BadRequestDataException("Unable to expand input payload")

            val outputStream = ByteArrayOutputStream()
            val jsonWriter = Json.createWriter(outputStream)
            jsonWriter.write(expandedFragment.getJsonObject(0))
            return deserializeObject(outputStream.toString())
        } catch (e: JsonLdError) {
            logger.error("Unable to expand fragment with context $contexts: ${e.message}")
            throw e.toAPIException(e.cause?.cause?.message)
        }
    }

    private fun geoPropertyToWKT(jsonFragment: Map<String, Any>): Map<String, Any> {
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
        expandedAttributes: ExpandedAttributes,
        expandedAttributeName: ExpandedTerm,
        datasetId: URI?
    ): ExpandedAttributeInstance? =
        expandedAttributes[expandedAttributeName]?.let { expandedAttributeInstances ->
            expandedAttributeInstances.find { expandedAttributeInstance ->
                if (datasetId == null)
                    !expandedAttributeInstance.containsKey(NGSILD_DATASET_ID_PROPERTY)
                else
                    getPropertyValueFromMap(
                        expandedAttributeInstance,
                        NGSILD_DATASET_ID_PROPERTY
                    ) == datasetId.toString()
            }
        }

    /**
     * Utility but basic method to find if given contexts can resolve a known term from the core context.
     */
    private fun canExpandJsonLdKeyFromCore(contexts: List<String>): Boolean {
        val jsonDocument = JsonDocument.of(
            serializeObject(
                mapOf("datasetId" to mapOf<String, Any>(), JSONLD_CONTEXT to contexts)
            ).byteInputStream()
        )
        val expandedType = JsonLd.expand(jsonDocument)
            .options(jsonLdOptions)
            .get()
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

    fun compactEntity(
        expandedEntity: ExpandedEntity,
        context: String? = null,
        mediaType: MediaType = JSON_LD_MEDIA_TYPE
    ): CompactedEntity =
        compactEntity(expandedEntity, context?.let { listOf(context) } ?: emptyList(), mediaType)

    fun compactEntity(
        expandedEntity: ExpandedEntity,
        contexts: List<String>,
        mediaType: MediaType = JSON_LD_MEDIA_TYPE
    ): CompactedEntity {
        val allContexts = addCoreContextIfMissing(contexts)

        return if (mediaType == MediaType.APPLICATION_JSON)
            JsonLd.compact(
                JsonDocument.of(serializeObject(expandedEntity.members).byteInputStream()),
                JsonDocument.of(buildContextDocument(allContexts))
            )
                .options(jsonLdOptions)
                .get()
                .toPrimitiveMap()
                .minus(JSONLD_CONTEXT)
                .mapValues(restoreGeoPropertyValue())
        else
            JsonLd.compact(
                JsonDocument.of(serializeObject(expandedEntity.members).byteInputStream()),
                JsonDocument.of(buildContextDocument(allContexts))
            )
                .options(jsonLdOptions)
                .get()
                .toPrimitiveMap()
                .minus(JSONLD_CONTEXT)
                .plus(JSONLD_CONTEXT to allContexts)
                .mapValues(restoreGeoPropertyValue())
    }

    private fun JsonObject.toPrimitiveMap(): Map<String, Any> {
        val outputStream = ByteArrayOutputStream()
        val jsonWriter = Json.createWriter(outputStream)
        jsonWriter.write(this)
        return deserializeObject(outputStream.toString())
    }

    fun compactEntities(
        entities: List<ExpandedEntity>,
        context: String,
        mediaType: MediaType
    ): List<CompactedEntity> =
        entities.map {
            compactEntity(it, context, mediaType)
        }

    fun compactTerms(terms: List<ExpandedTerm>, contexts: List<String>): List<String> =
        terms.map {
            compactTerm(it, contexts)
        }

    /**
     * Compact a term (type, attribute name, ...) using the provided context.
     */
    fun compactTerm(term: String, contexts: List<String>): String =
        JsonLd.compact(
            JsonDocument.of(serializeObject(mapOf(term to emptyMap<String, Any>())).byteInputStream()),
            JsonDocument.of(buildContextDocument(addCoreContextIfMissing(contexts)))
        )
            .options(jsonLdOptions)
            .get()
            .toPrimitiveMap()
            .keys
            .elementAtOrElse(0) { _ -> term }

    fun filterCompactedEntityOnAttributes(
        input: CompactedEntity,
        includedAttributes: Set<String>
    ): CompactedEntity {
        val identity: (CompactedEntity) -> CompactedEntity = { it }
        return filterEntityOnAttributes(input, identity, includedAttributes, false)
    }

    fun filterJsonLdEntitiesOnAttributes(
        jsonLdEntities: List<ExpandedEntity>,
        includedAttributes: Set<String>
    ): List<ExpandedEntity> =
        jsonLdEntities.filter { it.containsAnyOf(includedAttributes) }
            .map {
                ExpandedEntity(filterJsonLdEntityOnAttributes(it, includedAttributes), it.contexts)
            }

    fun filterJsonLdEntityOnAttributes(
        input: ExpandedEntity,
        includedAttributes: Set<String>
    ): Map<String, Any> {
        val inputToMap = { i: ExpandedEntity -> i.members }
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
                if (!it.containsKey(NGSILD_RELATIONSHIP_OBJECT))
                    BadRequestDataException("Relationship $name does not have an object field").left()
                else it[NGSILD_RELATIONSHIP_OBJECT]!!.right()
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
     * The returned value is the following:
     *
     * [
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
    fun buildExpandedPropertyValue(value: Any): ExpandedAttributeInstances =
        listOf(
            mapOf(
                JSONLD_TYPE to listOf(NGSILD_PROPERTY_TYPE.uri),
                NGSILD_PROPERTY_VALUE to listOf(mapOf(JSONLD_VALUE to value))
            )
        )

    /**
     * Build the expanded payload of a property whose value is an object.
     *
     * The returned value is the following:
     *
     * [
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
                    expandJsonLdFragment(value, contexts)
                )
            )
        )

    /**
     * Build the expanded payload of a relationship.
     *
     * The returned value is the following:
     *
     * [
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
    fun buildExpandedRelationshipValue(value: URI): ExpandedAttributeInstances =
        listOf(
            mapOf(
                JSONLD_TYPE to listOf(NGSILD_RELATIONSHIP_TYPE.uri),
                NGSILD_RELATIONSHIP_OBJECT to listOf(mapOf(JSONLD_ID to value.toString()))
            )
        )

    /**
     * Build the expanded payload of a non-reified temporal property (createdAt, modifiedAt,...).
     *
     * The returned value is the following:
     *
     * [
     *   {
     *     "@type": "https://uri.etsi.org/ngsi-ld/DateTime",
     *     "@value": "2022-12-01T00:00:00Z"
     *   }
     * ]
     */
    fun buildNonReifiedTemporalValue(value: ZonedDateTime): ExpandedNonReifiedPropertyValue =
        listOf(
            mapOf(
                JSONLD_TYPE to NGSILD_DATE_TIME_TYPE,
                JSONLD_VALUE to value.toNgsiLdFormat()
            )
        )

    /**
     * Build the expanded payload of a non-reified core property.
     *
     * The returned value is the following:
     *
     * [
     *   {
     *     "@id": "urn:ngsi-ld:Dataset:01"
     *   }
     * ]
     */
    fun buildNonReifiedPropertyValue(value: String): ExpandedNonReifiedPropertyValue =
        listOf(
            mapOf(JSONLD_ID to value)
        )
}
