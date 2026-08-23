package com.egm.stellio.search.common.util

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.LongAdder

/**
 * Diagnostic-only, in-memory named timers for perf-test instrumentation. Each name accumulates total
 * elapsed nanoseconds and call count independently, following the same pattern as
 * JsonLdUtils' expansion/compaction timing (LongAdder, no Micrometer dependency). Not a production
 * feature - intended to be removed once the current performance investigation concludes.
 */
object DiagnosticTimers {

    private class Accumulator {
        val totalNanos = LongAdder()
        val count = LongAdder()
    }

    private val timers = ConcurrentHashMap<String, Accumulator>()

    suspend inline fun <T> time(name: String, crossinline block: suspend () -> T): T {
        val startNanos = System.nanoTime()
        val result = block()
        record(name, System.nanoTime() - startNanos)
        return result
    }

    fun record(name: String, elapsedNanos: Long) {
        val accumulator = timers.computeIfAbsent(name) { Accumulator() }
        accumulator.totalNanos.add(elapsedNanos)
        accumulator.count.increment()
    }

    // name -> (totalNanos, count)
    fun stats(): Map<String, Pair<Long, Long>> =
        timers.mapValues { (_, accumulator) -> accumulator.totalNanos.sum() to accumulator.count.sum() }

    fun reset() {
        timers.clear()
    }
}
