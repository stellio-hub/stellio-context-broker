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
 * Caches the [ActiveContext] resolved from a fixed list of context URLs, and runs JSON-LD expansion
 * against the cached context instead of letting titanium-json-ld rebuild it from scratch on every call.
 *
 * Why this exists: titanium-json-ld 1.x has no supported way to cache a *resolved* active context - only
 * the raw fetched context document (which Stellio already caches via JsonLdOptions.documentCache). Even
 * on a document-cache hit, ActiveContextBuilder.fetch() unconditionally re-runs the expensive IRI/term
 * resolution algorithm (the `.newContext().create(...)` call) that builds the active context - this is
 * confirmed by reading titanium 1.7.0's source directly (ActiveContextBuilder.java:568-577) and is the
 * dominant CPU cost measured via JFR profiling under load (java.net.URI parsing, from IRI resolution,
 * was ~45% of all top-of-stack samples). The library maintainer's own position (see
 * https://github.com/filip26/titanium-json-ld/issues/292) is that active-context caching belongs in a
 * future major version; v2 is not released (still at milestone M2, with a distinct, non-drop-in API -
 * different object model entirely, dropping jakarta.json), so this works around the gap in 1.x instead.
 *
 * This only covers expansion, not compaction: unlike expansion, compaction (UriCompaction/ValueCompaction)
 * calls ActiveContext.setBaseUri()/createInverseContext(), which mutate the context in place - verified by
 * grepping titanium 1.7.0's full source tree for every call site of those two methods. Sharing one cached
 * instance across concurrent compactions would be a data race. Expansion (Expansion/ObjectExpansion/
 * ArrayExpansion/ScalarExpansion/ValueExpansion/UriExpansion) has zero such calls anywhere in the package,
 * confirmed the same way, so sharing a cached ActiveContext across concurrent expand() calls is safe.
 *
 * For this caching to actually take effect, the caller must pass its context list via
 * `options.expandContext` (as this class expects), not embed an "@context" member inside the document
 * being expanded: an embedded "@context" is resolved by ObjectExpansion deep inside the recursive
 * expansion algorithm instead (ObjectExpansion.java:194-197), which is a private, per-call-site
 * resolution this class cannot intercept without forking the whole expansion algorithm.
 */
internal object CachedContextExpansion {

    // No LRU/eviction: the key space here is the set of distinct context-list combinations actual API
    // callers use, which for a given deployment is a handful (a core context, maybe one or two tenant-
    // specific ones) - not something that grows with request volume. A first implementation used
    // Collections.synchronizedMap over an access-order LinkedHashMap for LRU eviction, but access-order
    // means every get() also mutates the map (relinking the accessed entry), so every read - not just
    // every write - took the map's single lock. Under concurrent load that serialized every request
    // through one lock and made things *worse* than the uncached baseline. ConcurrentHashMap gives
    // lock-free reads and per-bucket write locking, which is what a read-mostly cache like this needs.
    private val activeContextCache = ConcurrentHashMap<ActiveContextKey, ActiveContext>()

    // diagnostic-only: lets callers verify the cache is actually being hit under load, without
    // depending on Micrometer/Spring from this plain Kotlin object (see JsonLdUtils.cacheStats)
    private val hits = java.util.concurrent.atomic.AtomicLong(0)
    private val misses = java.util.concurrent.atomic.AtomicLong(0)

    fun cacheHits(): Long = hits.get()
    fun cacheMisses(): Long = misses.get()

    private data class ActiveContextKey(
        val baseUri: URI?,
        val baseUrl: URI?,
        val contexts: List<String>
    )

    /**
     * Drops every cached resolved context whose context list references `context` - used by
     * JsonLdUtils.deleteAndReload() so that reloading a context (e.g. after it changed at its source)
     * doesn't keep serving a resolved ActiveContext built from the stale version. A single context URL
     * can appear in several distinct cached (baseUri, baseUrl, contexts) combinations, so this removes
     * all of them rather than a single key.
     */
    fun invalidate(context: String) {
        activeContextCache.keys.removeIf { context in it.contexts }
    }

    /**
     * Equivalent to titanium's ExpansionProcessor.expand(Document, JsonLdOptions, boolean), except the
     * ActiveContext resolved from `contexts` is cached and reused across calls sharing the same
     * (baseUri, baseUrl, contexts) key, instead of being rebuilt from scratch every time.
     */
    fun expand(input: Document, contexts: List<String>, options: JsonLdOptions, frameExpansion: Boolean): JsonArray {
        val jsonStructure = input.jsonContent.orElseThrow {
            JsonLdError(JsonLdErrorCode.LOADING_DOCUMENT_FAILED, "Document is not parsed JSON.")
        }

        var baseUri: URI? = null
        var baseUrl: URI? = input.documentUrl
        if (baseUrl != null) baseUri = baseUrl
        if (baseUrl == null) baseUrl = options.base
        if (options.base != null) baseUri = options.base

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
        activeContextCache[key]?.let {
            hits.incrementAndGet()
            return it
        }
        misses.incrementAndGet()

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

    // Mirrors titanium's private ExpansionProcessor.updateContext() (W3C JSON-LD 1.1 API "Context
    // Processing Algorithm" steps 5-7) - this is the one-time-per-key work that gets cached above.
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
