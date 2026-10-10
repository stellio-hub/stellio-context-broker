package com.egm.stellio.shared.util

import com.apicatalog.jsonld.JsonLdError
import com.apicatalog.jsonld.JsonLdErrorCode
import com.apicatalog.jsonld.JsonLdOptions
import com.apicatalog.jsonld.context.ActiveContext
import com.apicatalog.jsonld.document.Document
import com.apicatalog.jsonld.expansion.Expansion
import com.apicatalog.jsonld.json.JsonProvider
import com.apicatalog.jsonld.json.JsonUtils
import com.apicatalog.jsonld.lang.Keywords
import com.apicatalog.jsonld.processor.ProcessingRuntime
import jakarta.json.JsonArray
import jakarta.json.JsonValue
import java.net.URI
import java.util.concurrent.ConcurrentHashMap

/**
 * Expands JSON-LD documents against a cached [ActiveContext], resolved once per list of contexts.
 *
 * titanium-json-ld 1.x only caches fetched context documents and rebuilds the active context on every call,
 * which is costly (see https://github.com/filip26/titanium-json-ld/issues/292).
 *
 * Only used for expansion: compaction mutates the active context, so it cannot be shared.
 * Contexts must be passed via `options.expandContext`, an embedded "@context" bypasses the cache.
 *
 * TODO revisit when migrating to titanium-json-ld 2.x: this class relies on 1.x internals (ActiveContext,
 *  Expansion, ProcessingRuntime) and copies parts of ExpansionProcessor, which 2.x replaces with a new API.
 */
internal object CachedContextExpansion {

    // No eviction needed: only a handful of distinct context lists are used in a deployment
    private val activeContextCache = ConcurrentHashMap<ActiveContextKey, ActiveContext>()

    private data class ActiveContextKey(
        val baseUri: URI?,
        val baseUrl: URI?,
        val contexts: List<String>
    )

    /**
     * Removes every cached active context built from `context` (called when a context is reloaded).
     */
    fun invalidate(context: String) {
        activeContextCache.keys.removeIf { context in it.contexts }
    }

    /**
     * Same as titanium's ExpansionProcessor.expand(), with the active context taken from the cache.
     */
    fun expand(input: Document, contexts: List<String>, options: JsonLdOptions, frameExpansion: Boolean): JsonArray {
        val jsonStructure = input.jsonContent.orElseThrow {
            JsonLdError(JsonLdErrorCode.LOADING_DOCUMENT_FAILED, "Document is not parsed JSON.")
        }

        val baseUrl: URI? = input.documentUrl ?: options.base
        val baseUri: URI? = options.base ?: input.documentUrl

        var activeContext = resolveActiveContext(baseUri, baseUrl, contexts, options)

        if (input.contextUrl != null) {
            activeContext = activeContext.newContext()
                .create(JsonProvider.instance().createValue(input.contextUrl.toString()), input.contextUrl)
        }

        var expanded: JsonValue = Expansion.with(activeContext, jsonStructure, null, baseUrl)
            .frameExpansion(frameExpansion)
            .ordered(options.isOrdered)
            .compute()

        if (JsonUtils.isObject(expanded)) {
            val jsonObject = expanded.asJsonObject()
            if (jsonObject.size == 1 && jsonObject.containsKey(Keywords.GRAPH)) {
                expanded = jsonObject.getValue(Keywords.GRAPH)
            }
        }

        if (JsonUtils.isNull(expanded)) return JsonValue.EMPTY_JSON_ARRAY
        return JsonUtils.toJsonArray(expanded)
    }

    private fun resolveActiveContext(
        baseUri: URI?,
        baseUrl: URI?,
        contexts: List<String>,
        options: JsonLdOptions
    ): ActiveContext {
        val key = ActiveContextKey(baseUri, baseUrl, contexts)
        activeContextCache[key]?.let { return it }

        val contextValue = options.expandContext?.jsonContent?.orElse(null)
            ?: return ActiveContext(baseUri, baseUrl, ProcessingRuntime.of(options))

        val resolved = updateContext(
            ActiveContext(baseUri, baseUrl, ProcessingRuntime.of(options)),
            contextValue,
            baseUrl
        )
        activeContextCache[key] = resolved
        return resolved
    }

    // Copy of titanium's private ExpansionProcessor.updateContext()
    private fun updateContext(activeContext: ActiveContext, expandedContext: JsonValue, baseUrl: URI?): ActiveContext {
        if (JsonUtils.isArray(expandedContext)) {
            val array = expandedContext.asJsonArray()
            if (array.size == 1) {
                val value = array.iterator().next()
                if (JsonUtils.containsKey(value, Keywords.CONTEXT)) {
                    return activeContext.newContext().create(value.asJsonObject()[Keywords.CONTEXT], baseUrl)
                }
            }
            return activeContext.newContext().create(expandedContext, baseUrl)
        } else if (JsonUtils.containsKey(expandedContext, Keywords.CONTEXT)) {
            return activeContext.newContext().create(expandedContext.asJsonObject()[Keywords.CONTEXT], baseUrl)
        }
        return activeContext.newContext().create(
            JsonProvider.instance().createArrayBuilder().add(expandedContext).build(),
            baseUrl
        )
    }
}
