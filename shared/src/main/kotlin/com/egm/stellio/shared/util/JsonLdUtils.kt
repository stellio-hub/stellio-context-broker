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
import com.egm.stellio.shared.model.COMPACTED_ENTITY_CORE_MEMBERS
import com.egm.stellio.shared.model.CompactedEntity
import com.egm.stellio.shared.model.ExpandedAttribute
import com.egm.stellio.shared.model.ExpandedAttributeInstances
import com.egm.stellio.shared.model.ExpandedAttributes
import com.egm.stellio.shared.model.ExpandedEntity
import com.egm.stellio.shared.model.ExpandedNonReifiedPropertyValue
import com.egm.stellio.shared.model.ExpandedTerm
import com.egm.stellio.shared.model.JSONLD_CONTEXT_KW
import com.egm.stellio.shared.model.JSONLD_ID_KW
import com.egm.stellio.shared.model.JSONLD_LIST_KW
import com.egm.stellio.shared.model.JSONLD_TYPE_KW
import com.egm.stellio.shared.model.JSONLD_VALUE_KW
import com.egm.stellio.shared.model.NGSILD_DATE_TIME_TYPE
import com.egm.stellio.shared.model.NGSILD_GEOPROPERTY_TERM
import com.egm.stellio.shared.model.NGSILD_NULL
import com.egm.stellio.shared.model.NGSILD_PROPERTY_TYPE
import com.egm.stellio.shared.model.NGSILD_PROPERTY_VALUE
import com.egm.stellio.shared.model.NGSILD_RELATIONSHIP_OBJECT
import com.egm.stellio.shared.model.NGSILD_RELATIONSHIP_TYPE
import com.egm.stellio.shared.model.NGSILD_TYPE_TERM
import com.egm.stellio.shared.model.NGSILD_VALUE_TERM
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
import kotlinx.coroutines.runBlocking
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.ByteArrayOutputStream
import java.net.URI
import java.time.ZonedDateTime

object JsonLdUtils {

    private const val JSONLD_GRAPH = "@graph"

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

    suspend fun expandJsonLdEntitySafe(
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

    fun expandJsonLdTerm(term: String, context: String): String =
        expandJsonLdTerm(term, listOf(context))

    fun expandJsonLdTerm(term: String, contexts: List<String>): String =
        try {
            val preparedTerm = mapOf(
                term to mapOf<String, Any>(),
                JSONLD_CONTEXT_KW to contexts
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

    fun expandGeoPropertyFragment(fragment: Map<String, Any>, contexts: List<String>): Map<String, List<Any>> =
        // as it is called from a non-corouting context (when building temporal entities), wrap it in a blocking call
        runBlocking {
            doJsonLdExpansion(fragment, contexts, doGeoPropertyTransformation = false) as Map<String, List<Any>>
        }

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

    private suspend fun doJsonLdExpansion(
        fragment: Map<String, Any>,
        contexts: List<String>,
        doGeoPropertyTransformation: Boolean = true
    ): Map<String, Any> {
        // transform the GeoJSON value of geo properties into WKT format before JSON-LD expansion
        // since JSON-LD expansion breaks the data (e.g., flattening the lists of lists)
        val preparedFragment =
            if (doGeoPropertyTransformation)
                fragment
                    .mapValues(transformGeoPropertyToWKT())
                    .plus(JSONLD_CONTEXT_KW to contexts)
            else
                fragment.plus(JSONLD_CONTEXT_KW to contexts)

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
        when {
            it.key in COMPACTED_ENTITY_CORE_MEMBERS -> it.value
            it.value is Map<*, *> -> {
                if ((it.value as Map<*, *>)[NGSILD_TYPE_TERM] == NGSILD_GEOPROPERTY_TERM) {
                    val geoProperty = it.value as Map<String, Any>
                    val wktGeometry = geoPropertyToWKTOrNull(geoProperty[NGSILD_VALUE_TERM]!!)
                    geoProperty.plus(NGSILD_VALUE_TERM to wktGeometry)
                } else it.value
            }
            it.value is List<*> -> {
                (it.value as List<Map<String, Any>>).map { geoProperty ->
                    if (geoProperty[NGSILD_TYPE_TERM] == NGSILD_GEOPROPERTY_TERM) {
                        val wktGeometry = geoPropertyToWKTOrNull(geoProperty[NGSILD_VALUE_TERM]!!)
                        geoProperty.plus(NGSILD_VALUE_TERM to wktGeometry)
                    } else geoProperty
                }
            }
            else -> it.value
        }
    }

    private fun geoPropertyToWKTOrNull(geoPropertyValue: Any): String =
        if (geoPropertyValue is String && geoPropertyValue == NGSILD_NULL)
            NGSILD_NULL
        else
            throwingGeoJsonToWkt(geoPropertyValue as Map<String, Any>)

    private fun restoreGeoPropertyFromWKT(): (Map.Entry<String, Any>) -> Any = {
        when {
            it.key in COMPACTED_ENTITY_CORE_MEMBERS -> it.value
            it.value is Map<*, *> -> {
                if ((it.value as Map<*, *>)[NGSILD_TYPE_TERM] == NGSILD_GEOPROPERTY_TERM) {
                    val geoValues = it.value as MutableMap<String, Any>
                    // in case of an aggregated or temporalValues query, there is no "value" member
                    if (geoValues.isNotEmpty() && geoValues.containsKey(NGSILD_VALUE_TERM)) {
                        geoValues[NGSILD_VALUE_TERM] = wktToGeoJson(geoValues[NGSILD_VALUE_TERM] as String)
                        geoValues
                    } else geoValues
                } else it.value
            }
            // case of a multi-instance geoproperty or when retrieving the history of a geoproperty
            it.value is List<*> ->
                (it.value as List<Map<String, Any>>).map { geoInstance ->
                    if (geoInstance[NGSILD_TYPE_TERM] == NGSILD_GEOPROPERTY_TERM) {
                        val geoValues = geoInstance.toMutableMap()
                        // in case of an aggregated or temporalValues query, there is no "value" member
                        if (geoValues.containsKey(NGSILD_VALUE_TERM)) {
                            geoValues[NGSILD_VALUE_TERM] = wktToGeoJson(geoValues[NGSILD_VALUE_TERM] as String)
                            geoValues
                        } else geoValues
                    } else geoInstance
                }
            else -> it.value
        }
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
                if (jsonObject[JSONLD_CONTEXT_KW] is JsonArray)
                    (jsonObject[JSONLD_CONTEXT_KW] as JsonArray).map { (it as JsonString).string }
                else (jsonObject[JSONLD_CONTEXT_KW] as JsonString).string
            // extract compacted entities from the @graph key
            (jsonObject[JSONLD_GRAPH] as JsonArray).toPrimitiveListOfObjects()
                .map {
                    it.mapValues(restoreGeoPropertyFromWKT())
                        .plus(JSONLD_CONTEXT_KW to context)
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
                JSONLD_TYPE_KW to listOf(NGSILD_PROPERTY_TYPE.uri),
                NGSILD_PROPERTY_VALUE to listOf(mapOf(JSONLD_VALUE_KW to value))
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
                JSONLD_TYPE_KW to listOf(NGSILD_PROPERTY_TYPE.uri),
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
                JSONLD_TYPE_KW to listOf(NGSILD_RELATIONSHIP_TYPE.uri),
                NGSILD_RELATIONSHIP_OBJECT to listOf(mapOf(JSONLD_ID_KW to value.toString()))
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
                JSONLD_TYPE_KW to NGSILD_DATE_TIME_TYPE,
                JSONLD_VALUE_KW to value.toNgsiLdFormat()
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
            mapOf(JSONLD_ID_KW to value)
        )

    fun <T> buildExpandedTemporalValue(
        values: List<T>,
        transform: (T) -> List<Map<String, Any>>
    ): List<Map<String, List<Any>>> =
        listOf(
            mapOf(
                JSONLD_LIST_KW to values.map { value ->
                    mapOf(JSONLD_LIST_KW to transform(value))
                }
            )
        )
}
