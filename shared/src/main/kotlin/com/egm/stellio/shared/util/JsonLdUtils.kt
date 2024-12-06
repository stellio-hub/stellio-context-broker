package com.egm.stellio.shared.util

import arrow.core.Either
import arrow.core.left
import arrow.core.raise.either
import arrow.core.right
import com.apicatalog.jsonld.JsonLd
import com.apicatalog.jsonld.JsonLdError
import com.apicatalog.jsonld.JsonLdOptions
import com.apicatalog.jsonld.context.cache.LruCache
import com.apicatalog.jsonld.document.JsonDocument
import com.apicatalog.jsonld.http.DefaultHttpClient
import com.apicatalog.jsonld.loader.DocumentLoaderOptions
import com.apicatalog.jsonld.loader.HttpLoader
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.model.CompactedEntity
import com.egm.stellio.shared.model.ExpandedAttribute
import com.egm.stellio.shared.model.ExpandedAttributeInstances
import com.egm.stellio.shared.model.ExpandedAttributes
import com.egm.stellio.shared.model.ExpandedEntity
import com.egm.stellio.shared.model.ExpandedNonReifiedPropertyValue
import com.egm.stellio.shared.model.ExpandedTerm
import com.egm.stellio.shared.model.toAPIException
import com.egm.stellio.shared.util.JsonUtils.deserializeAs
import com.egm.stellio.shared.util.JsonUtils.deserializeAsList
import com.egm.stellio.shared.util.JsonUtils.deserializeAsMap
import com.egm.stellio.shared.util.JsonUtils.deserializeObject
import com.egm.stellio.shared.util.JsonUtils.serializeObject
import jakarta.json.Json
import jakarta.json.JsonArray
import jakarta.json.JsonObject
import jakarta.json.JsonString
import jakarta.json.JsonStructure
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.ByteArrayOutputStream
import java.net.URI
import java.time.ZonedDateTime

@JvmInline
value class AttributeType(val uri: String)

object JsonLdUtils {

    const val NGSILD_PREFIX = "https://uri.etsi.org/ngsi-ld/"
    const val NGSILD_DEFAULT_VOCAB = "https://uri.etsi.org/ngsi-ld/default-context/"

    const val NGSILD_PROPERTY_TERM = "Property"
    val NGSILD_PROPERTY_TYPE = AttributeType("https://uri.etsi.org/ngsi-ld/Property")
    const val NGSILD_PROPERTY_VALUE = "https://uri.etsi.org/ngsi-ld/hasValue"
    const val NGSILD_GEOPROPERTY_TERM = "GeoProperty"
    val NGSILD_GEOPROPERTY_TYPE = AttributeType("https://uri.etsi.org/ngsi-ld/GeoProperty")
    const val NGSILD_GEOPROPERTY_VALUE = "https://uri.etsi.org/ngsi-ld/hasValue"
    const val NGSILD_RELATIONSHIP_TERM = "Relationship"
    val NGSILD_RELATIONSHIP_TYPE = AttributeType("https://uri.etsi.org/ngsi-ld/Relationship")
    const val NGSILD_RELATIONSHIP_OBJECT = "https://uri.etsi.org/ngsi-ld/hasObject"
    const val NGSILD_JSONPROPERTY_TERM = "JsonProperty"
    val NGSILD_JSONPROPERTY_TYPE = AttributeType("https://uri.etsi.org/ngsi-ld/JsonProperty")
    const val NGSILD_JSONPROPERTY_VALUE = "https://uri.etsi.org/ngsi-ld/hasJSON"
    const val NGSILD_LANGUAGEPROPERTY_TERM = "LanguageProperty"
    val NGSILD_LANGUAGEPROPERTY_TYPE = AttributeType("https://uri.etsi.org/ngsi-ld/LanguageProperty")
    const val NGSILD_LANGUAGEPROPERTY_VALUE = "https://uri.etsi.org/ngsi-ld/hasLanguageMap"
    const val NGSILD_VOCABPROPERTY_TERM = "VocabProperty"
    val NGSILD_VOCABPROPERTY_TYPE = AttributeType("https://uri.etsi.org/ngsi-ld/VocabProperty")
    const val NGSILD_VOCABPROPERTY_VALUE = "https://uri.etsi.org/ngsi-ld/hasVocab"

    const val NGSILD_PROPERTY_VALUES = "https://uri.etsi.org/ngsi-ld/hasValues"
    const val NGSILD_GEOPROPERTY_VALUES = "https://uri.etsi.org/ngsi-ld/hasValues"
    const val NGSILD_RELATIONSHIP_OBJECTS = "https://uri.etsi.org/ngsi-ld/hasObjects"
    const val NGSILD_JSONPROPERTY_VALUES = "https://uri.etsi.org/ngsi-ld/jsons"
    const val NGSILD_LANGUAGEPROPERTY_VALUES = "https://uri.etsi.org/ngsi-ld/hasLanguageMaps"
    const val NGSILD_VOCABPROPERTY_VALUES = "https://uri.etsi.org/ngsi-ld/hasVocabs"

    private const val JSONLD_GRAPH = "@graph"
    const val JSONLD_ID_TERM = "id"
    const val JSONLD_ID = "@id"
    const val JSONLD_TYPE_TERM = "type"
    const val JSONLD_TYPE = "@type"
    const val JSONLD_VALUE_TERM = "value"
    const val JSONLD_VALUE = "@value"
    const val JSONLD_OBJECT = "object"
    const val JSONLD_LIST = "@list"
    const val JSONLD_JSON_TERM = "json"
    const val JSONLD_LANGUAGE = "@language"
    const val JSONLD_LANGUAGEMAP_TERM = "languageMap"
    const val JSONLD_VOCAB = "@vocab"
    const val JSONLD_VOCAB_TERM = "vocab"
    const val JSONLD_JSON = "@json"
    const val JSONLD_CONTEXT = "@context"
    const val NGSILD_SCOPE_TERM = "scope"
    const val NGSILD_SCOPE_PROPERTY = "https://uri.etsi.org/ngsi-ld/$NGSILD_SCOPE_TERM"
    const val NGSILD_LANG_TERM = "lang"
    const val NGSILD_NONE_TERM = "@none"
    const val NGSILD_DATASET_TERM = "dataset"
    const val NGSILD_ENTITY_TERM = "entity"
    val JSONLD_EXPANDED_ENTITY_SPECIFIC_MEMBERS = setOf(JSONLD_TYPE, NGSILD_SCOPE_PROPERTY)

    // List of members that are part of a core entity base definition (i.e., without attributes)
    val JSONLD_EXPANDED_ENTITY_CORE_MEMBERS =
        setOf(
            JSONLD_ID,
            JSONLD_TYPE,
            JSONLD_CONTEXT,
            NGSILD_SCOPE_PROPERTY,
            NGSILD_CREATED_AT_PROPERTY,
            NGSILD_MODIFIED_AT_PROPERTY
        )
    val JSONLD_COMPACTED_ENTITY_CORE_MEMBERS =
        setOf(
            JSONLD_ID_TERM,
            JSONLD_TYPE_TERM,
            JSONLD_CONTEXT,
            NGSILD_SCOPE_TERM,
            NGSILD_CREATED_AT_TERM,
            NGSILD_MODIFIED_AT_TERM
        )

    const val NGSILD_CREATED_AT_TERM = "createdAt"
    const val NGSILD_MODIFIED_AT_TERM = "modifiedAt"
    val NGSILD_SYSATTRS_TERMS = setOf(NGSILD_CREATED_AT_TERM, NGSILD_MODIFIED_AT_TERM)
    const val NGSILD_CREATED_AT_PROPERTY = "https://uri.etsi.org/ngsi-ld/$NGSILD_CREATED_AT_TERM"
    const val NGSILD_MODIFIED_AT_PROPERTY = "https://uri.etsi.org/ngsi-ld/$NGSILD_MODIFIED_AT_TERM"
    val NGSILD_SYSATTRS_PROPERTIES = setOf(NGSILD_CREATED_AT_PROPERTY, NGSILD_MODIFIED_AT_PROPERTY)
    const val NGSILD_OBSERVED_AT_TERM = "observedAt"
    const val NGSILD_OBSERVED_AT_PROPERTY = "https://uri.etsi.org/ngsi-ld/$NGSILD_OBSERVED_AT_TERM"
    const val NGSILD_UNIT_CODE_PROPERTY = "https://uri.etsi.org/ngsi-ld/unitCode"
    const val NGSILD_UNIT_CODE_TERM = "unitCode"
    const val NGSILD_LOCATION_TERM = "location"
    const val NGSILD_LOCATION_PROPERTY = "https://uri.etsi.org/ngsi-ld/location"
    const val NGSILD_OBSERVATION_SPACE_TERM = "observationSpace"
    const val NGSILD_OBSERVATION_SPACE_PROPERTY = "https://uri.etsi.org/ngsi-ld/observationSpace"
    const val NGSILD_OPERATION_SPACE_TERM = "operationSpace"
    const val NGSILD_OPERATION_SPACE_PROPERTY = "https://uri.etsi.org/ngsi-ld/operationSpace"
    val NGSILD_GEO_PROPERTIES_TERMS =
        setOf(NGSILD_LOCATION_TERM, NGSILD_OBSERVATION_SPACE_TERM, NGSILD_OPERATION_SPACE_TERM)
    const val NGSILD_DATASET_ID_PROPERTY = "https://uri.etsi.org/ngsi-ld/datasetId"
    const val NGSILD_DATASET_ID_TERM = "datasetId"

    const val NGSILD_INSTANCE_ID_PROPERTY = "https://uri.etsi.org/ngsi-ld/instanceId"

    const val NGSILD_SUBSCRIPTION_TERM = "Subscription"
    const val NGSILD_NOTIFICATION_TERM = "Notification"
    const val NGSILD_CSR_TERM = "ContextSourceRegistration"

    const val NGSILD_DATE_TIME_TYPE = "https://uri.etsi.org/ngsi-ld/DateTime"
    const val NGSILD_DATE_TYPE = "https://uri.etsi.org/ngsi-ld/Date"
    const val NGSILD_TIME_TYPE = "https://uri.etsi.org/ngsi-ld/Time"

    val logger: Logger = LoggerFactory.getLogger(javaClass)

    private const val CONTEXT_CACHE_CAPACITY = 128
    private const val DOCUMENT_CACHE_CAPACITY = 256

    private val jsonLdOptions = JsonLdOptions().apply {
        contextCache = LruCache(CONTEXT_CACHE_CAPACITY)
        documentCache = LruCache(DOCUMENT_CACHE_CAPACITY)
    }
    private val loader = HttpLoader(DefaultHttpClient.defaultInstance())

    private fun buildContextDocument(contexts: List<String>): JsonStructure {
        val contextsArray = Json.createArrayBuilder()
        contexts.forEach { contextsArray.add(it) }
        return contextsArray.build()
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
            ExpandedEntity(it).right()
        }, {
            if (it is APIException) it.left()
            else it.toAPIException().left()
        })

    suspend fun expandJsonLdEntity(input: Map<String, Any>, contexts: List<String>): ExpandedEntity =
        ExpandedEntity(doJsonLdExpansion(input, contexts))

    suspend fun expandJsonLdEntity(input: String, contexts: List<String>): ExpandedEntity =
        expandJsonLdEntity(input.deserializeAsMap(), contexts)

    fun expandJsonLdTerms(terms: List<String>, contexts: List<String>): List<ExpandedTerm> =
        terms.map {
            expandJsonLdTerm(it, contexts)
        }

    fun expandJsonLdTerm(term: String, context: String): String =
        expandJsonLdTerm(term, listOf(context))

    fun expandJsonLdTerm(term: String, contexts: List<String>): String =
        try {
            val preparedTerm = mapOf(
                term to mapOf<String, Any>(),
                JSONLD_CONTEXT to contexts
            )
            JsonLd.expand(JsonDocument.of(serializeObject(preparedTerm).byteInputStream()))
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
        doJsonLdExpansion(fragment, contexts) as ExpandedAttributes

    private suspend fun doJsonLdExpansion(fragment: Map<String, Any>, contexts: List<String>): Map<String, Any> {
        // transform the GeoJSON value of geo properties into WKT format before JSON-LD expansion
        // since JSON-LD expansion breaks the data (e.g., flattening the lists of lists)
        val preparedFragment = fragment
            .mapValues(transformGeoPropertyToWKT())
            .plus(JSONLD_CONTEXT to contexts)

        try {
            val expandedFragment = JsonLd.expand(JsonDocument.of(serializeObject(preparedFragment).byteInputStream()))
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

    fun checkJsonldContext(context: URI): Either<APIException, Unit> = either {
        return try {
            loader.loadDocument(context, DocumentLoaderOptions())
            Unit.right()
        } catch (e: JsonLdError) {
            e.toAPIException(e.cause?.cause?.message).left()
        } catch (e: IllegalArgumentException) {
            BadRequestDataException(e.cause?.message ?: "Provided context is invalid: $context").left()
        }
    }

    private fun transformGeoPropertyToWKT(): (Map.Entry<String, Any>) -> Any = {
        if (NGSILD_GEO_PROPERTIES_TERMS.contains(it.key)) {
            when (it.value) {
                is Map<*, *> -> {
                    val geoProperty = it.value as MutableMap<String, Any>
                    val wktGeometry = throwingGeoJsonToWkt(geoProperty[JSONLD_VALUE_TERM]!! as Map<String, Any>)
                    geoProperty.plus(JSONLD_VALUE_TERM to wktGeometry)
                }
                is List<*> -> {
                    (it.value as List<Map<String, Any>>).map { geoProperty ->
                        val wktGeometry = throwingGeoJsonToWkt(geoProperty[JSONLD_VALUE_TERM] as Map<String, Any>)
                        geoProperty.plus(JSONLD_VALUE_TERM to wktGeometry)
                    }
                }
                else -> it.value
            }
        } else it.value
    }

    private fun restoreGeoPropertyFromWKT(): (Map.Entry<String, Any>) -> Any = {
        if (NGSILD_GEO_PROPERTIES_TERMS.contains(it.key)) {
            when (it.value) {
                is Map<*, *> -> {
                    val geoValues = it.value as MutableMap<String, Any>
                    // in case of an aggregated or temporalValues query, there is no "value" member
                    if (geoValues.isNotEmpty() && geoValues.containsKey(JSONLD_VALUE_TERM)) {
                        geoValues[JSONLD_VALUE_TERM] = wktToGeoJson(geoValues[JSONLD_VALUE_TERM] as String)
                        geoValues
                    } else geoValues
                }
                // case of a multi-instance geoproperty or when retrieving the history of a geoproperty
                is List<*> ->
                    (it.value as List<Map<String, Any>>).map { geoInstance ->
                        val geoValues = geoInstance.toMutableMap()
                        // in case of an aggregated or temporalValues query, there is no "value" member
                        if (geoValues.containsKey(JSONLD_VALUE_TERM)) {
                            geoValues[JSONLD_VALUE_TERM] = wktToGeoJson(geoValues[JSONLD_VALUE_TERM] as String)
                            geoValues
                        } else geoValues
                    }
                else -> it.value
            }
        } else it.value
    }

    fun compactEntity(
        expandedEntity: ExpandedEntity,
        contexts: List<String>
    ): CompactedEntity =
        JsonLd.compact(
            JsonDocument.of(serializeObject(expandedEntity.members).byteInputStream()),
            JsonDocument.of(buildContextDocument(contexts))
        )
            .options(jsonLdOptions)
            .get()
            .toPrimitiveMap()
            .mapValues(restoreGeoPropertyFromWKT())

    private fun JsonObject.toPrimitiveMap(): Map<String, Any> {
        val outputStream = ByteArrayOutputStream()
        val jsonWriter = Json.createWriter(outputStream)
        jsonWriter.write(this)
        return deserializeObject(outputStream.toString())
    }

    fun compactEntities(
        entities: List<ExpandedEntity>,
        contexts: List<String>
    ): List<CompactedEntity> {
        // do not try to compact an empty list, it will fail
        if (entities.isEmpty()) return emptyList()

        val jsonObject = JsonLd.compact(
            JsonDocument.of(serializeObject(entities.map { it.members }).byteInputStream()),
            JsonDocument.of(buildContextDocument(contexts))
        )
            .options(jsonLdOptions)
            .get()

        return if (entities.size == 1)
            listOf(jsonObject.toPrimitiveMap().mapValues(restoreGeoPropertyFromWKT()))
        else {
            // extract the context from the root of the object to inject it back in the compacted entities later
            val context: Any =
                if (jsonObject[JSONLD_CONTEXT] is JsonArray)
                    (jsonObject[JSONLD_CONTEXT] as JsonArray).map { (it as JsonString).string }
                else (jsonObject[JSONLD_CONTEXT] as JsonString).string
            // extract compacted entities from the @graph key
            (jsonObject[JSONLD_GRAPH] as JsonArray).toPrimitiveListOfObjects()
                .map {
                    it.mapValues(restoreGeoPropertyFromWKT())
                        .plus(JSONLD_CONTEXT to context)
                }
        }
    }

    private fun JsonArray.toPrimitiveListOfObjects(): List<Map<String, Any>> {
        val outputStream = ByteArrayOutputStream()
        val jsonWriter = Json.createWriter(outputStream)
        jsonWriter.write(this)
        return outputStream.toString().deserializeAsList()
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
            JsonDocument.of(buildContextDocument(contexts))
        )
            .options(jsonLdOptions)
            .get()
            .toPrimitiveMap()
            .keys
            .elementAtOrElse(0) { _ -> term }

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

    fun <T> buildExpandedTemporalValue(
        values: List<T>,
        transform: (T) -> List<Map<String, Any>>
    ): List<Map<String, List<Any>>> =
        listOf(
            mapOf(
                JSONLD_LIST to values.map { value ->
                    mapOf(JSONLD_LIST to transform(value))
                }
            )
        )
}
