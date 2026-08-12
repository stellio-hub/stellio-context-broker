package com.egm.stellio.search.common.config

import com.egm.stellio.shared.util.JsonLdUtils
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import jakarta.annotation.PostConstruct
import org.springframework.context.annotation.Configuration

// diagnostic-only: exposes CachedContextExpansion's hit/miss counters (see its class doc in the shared
// module) as Micrometer gauges, to verify under load whether the cache is actually being hit rather than
// silently falling through on every call. Remove once the JSON-LD active-context caching investigation
// is settled - it's not meant to be a permanent metric.
@Configuration
class JsonLdCacheMetricsConfig(private val meterRegistry: MeterRegistry) {

    @PostConstruct
    fun bindGauges() {
        Gauge.builder("jsonld.context.cache.hits") { JsonLdUtils.jsonLdContextCacheStats().first.toDouble() }
            .description("Resolved active-context cache hits")
            .register(meterRegistry)
        Gauge.builder("jsonld.context.cache.misses") { JsonLdUtils.jsonLdContextCacheStats().second.toDouble() }
            .description("Resolved active-context cache misses")
            .register(meterRegistry)
    }
}
