package com.egm.stellio.search.common.web

import com.egm.stellio.search.common.util.DiagnosticTimers
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

/**
 * Diagnostic-only endpoint (not part of the NGSI-LD API) exposing DiagnosticTimers' named timers, so a
 * specific service-layer call's total duration (business logic + DB round trips + commit) can be read
 * out directly during a perf-test campaign - same reset-then-run-then-read workflow as
 * JsonLdTimingHandler and the pg_stat_* captures in the recap. Perf-test instrumentation, not meant to
 * ship long-term: remove once this investigation concludes.
 */
@RestController
@RequestMapping("/admin/service-timing")
class DiagnosticTimingHandler {

    @GetMapping
    fun stats(): Map<String, OperationTiming> =
        DiagnosticTimers.stats().mapValues { (_, timing) -> OperationTiming.of(timing.second, timing.first) }

    @PostMapping("/reset")
    fun reset() {
        DiagnosticTimers.reset()
    }
}
