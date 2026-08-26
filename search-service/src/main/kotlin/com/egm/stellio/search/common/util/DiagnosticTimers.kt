package com.egm.stellio.search.common.util

import kotlinx.coroutines.reactor.awaitSingle
import org.springframework.transaction.reactive.TransactionSynchronization
import org.springframework.transaction.reactive.TransactionSynchronizationManager
import reactor.core.publisher.Mono
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

    // Spring's reactive transaction manager calls commitTransaction() outside any application code,
    // between the annotated method's own return and the caller regaining control - not reachable by
    // wrapping a block with time(). beforeCommit()/afterCommit() are the framework's own hooks around
    // that call, giving a direct measurement instead of one inferred by subtracting other timers.
    suspend fun timeCommit(name: String) {
        TransactionSynchronizationManager.forCurrentTransaction().awaitSingle()
            .registerSynchronization(CommitTimingSynchronization(name))
    }

    private class CommitTimingSynchronization(private val name: String) : TransactionSynchronization {
        private var startNanos: Long = 0

        override fun beforeCommit(readOnly: Boolean): Mono<Void> =
            Mono.fromRunnable { startNanos = System.nanoTime() }

        override fun afterCommit(): Mono<Void> =
            Mono.fromRunnable { record(name, System.nanoTime() - startNanos) }
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
