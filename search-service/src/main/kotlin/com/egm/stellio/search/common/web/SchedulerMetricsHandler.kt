package com.egm.stellio.search.common.web

import io.micrometer.core.instrument.MeterRegistry
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

/**
 * Diagnostic-only endpoint exposing Reactor scheduler executor metrics (executor.active, .queued,
 * .pool.size), summed per scheduler family (parallel, boundedElastic). Reactor's scheduler tag values
 * (e.g. parallel(8,"parallel")) contain commas, which Spring Boot Actuator's /actuator/metrics
 * ?tag=key:value query syntax splits on during parameter binding - percent-encoding the comma doesn't
 * help, since Spring decodes the query string before that split runs. Reading the MeterRegistry
 * directly in code sidesteps query-string parsing entirely. Perf-test instrumentation, not meant to
 * ship long-term: remove once this investigation concludes.
 */
@RestController
@RequestMapping("/admin/scheduler-metrics")
class SchedulerMetricsHandler(private val meterRegistry: MeterRegistry) {

    @GetMapping
    fun stats(): Map<String, Map<String, Double>> {
        val meterNames = listOf("executor.active", "executor.queued", "executor.pool.size")
        val result = mutableMapOf<String, MutableMap<String, Double>>()

        meterRegistry.meters
            .filter { it.id.name in meterNames }
            .forEach { meter ->
                val schedulerId = meter.id.getTag("reactor.scheduler.id") ?: return@forEach
                val family = when {
                    schedulerId.startsWith("parallel") -> "parallel"
                    schedulerId.startsWith("boundedElastic") -> "boundedElastic"
                    else -> schedulerId
                }
                val value = meter.measure().sumOf { it.value }
                val familyStats = result.getOrPut(family) { mutableMapOf() }
                familyStats[meter.id.name] = (familyStats[meter.id.name] ?: 0.0) + value
            }
        return result
    }
}
