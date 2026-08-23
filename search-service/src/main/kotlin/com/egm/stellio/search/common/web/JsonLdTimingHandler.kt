package com.egm.stellio.search.common.web

import com.egm.stellio.shared.util.JsonLdUtils
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

/**
 * Diagnostic-only endpoint (not part of the NGSI-LD API) exposing the JSON-LD expansion/compaction
 * timing gathered in JsonLdUtils, so their share of an operation's total cost can be read out directly
 * during a perf-test campaign - reset right before a run (matching the pg_stat_reset_shared workflow
 * already used for Postgres I/O stats), read after - instead of inferred from JFR or log scraping.
 * Perf-test instrumentation, not meant to ship long-term: remove once this investigation concludes.
 */
@RestController
@RequestMapping("/admin/jsonld-timing")
class JsonLdTimingHandler {

    @GetMapping
    fun stats(): JsonLdTimingStats {
        val (expansionNanos, expansionCount) = JsonLdUtils.jsonLdExpansionTimingStats()
        val (compactionNanos, compactionCount) = JsonLdUtils.jsonLdCompactionTimingStats()
        return JsonLdTimingStats(
            expansion = OperationTiming.of(expansionCount, expansionNanos),
            compaction = OperationTiming.of(compactionCount, compactionNanos)
        )
    }

    @PostMapping("/reset")
    fun reset() {
        JsonLdUtils.resetJsonLdTimingStats()
    }
}

data class JsonLdTimingStats(
    val expansion: OperationTiming,
    val compaction: OperationTiming
)

data class OperationTiming(
    val count: Long,
    val totalMs: Double,
    val avgMs: Double
) {
    companion object {
        private const val NANOS_PER_MILLI = 1_000_000.0

        fun of(count: Long, totalNanos: Long): OperationTiming {
            val totalMs = totalNanos / NANOS_PER_MILLI
            return OperationTiming(count, totalMs, if (count == 0L) 0.0 else totalMs / count)
        }
    }
}
