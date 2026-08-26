package com.egm.stellio.search

import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.boot.context.properties.ConfigurationPropertiesScan
import org.springframework.boot.runApplication
import org.springframework.context.event.EventListener
import org.springframework.scheduling.annotation.EnableScheduling
import org.springframework.stereotype.Component
import reactor.blockhound.BlockHound
import reactor.core.scheduler.Schedulers
import reactor.util.Metrics

@SpringBootApplication(scanBasePackages = ["com.egm.stellio.search", "com.egm.stellio.shared"])
@ConfigurationPropertiesScan("com.egm.stellio.search.common.config", "com.egm.stellio.shared.config")
@EnableScheduling
class SearchServiceApplication

private val blockHoundLogger = LoggerFactory.getLogger("BlockHound")

// perf-test only: surfaces blocking calls made from Reactor/Netty non-blocking threads, which would
// explain a hard concurrency ceiling (throughput flat while latency scales with VU count - see
// perf-investigation-recap.md) that per-request CPU/DB optimizations can't move. Opt-in via
// BLOCKHOUND_ENABLED=true, and must run before any Reactor scheduler/Netty thread is created, hence
// first thing in main() rather than a Spring bean. Logs instead of throwing (blockingMethodCallback
// replaces BlockHound's default throw-on-detection behavior) so a load test run surfaces every call
// site without failing requests. Needs -XX:+AllowRedefinitionToAddDeleteMethods on JDK 13+.
private fun installBlockHoundIfEnabled() {
    if (System.getenv("BLOCKHOUND_ENABLED") != "true") return
    BlockHound.builder()
        // BlockHound.builder() does not auto-load ServiceLoader integrations (unlike the
        // BlockHound.install() static shortcut) - without this, reactor-core's own integration
        // never registers, so Schedulers.parallel()/boundedElastic() threads aren't recognized as
        // non-blocking at all and nothing is ever flagged. Verified against a standalone repro.
        .loadIntegrations()
        .blockingMethodCallback { blockingMethod ->
            blockHoundLogger.warn(
                "Blocking call detected: {}",
                blockingMethod,
                Exception("blocking call site")
            )
        }
        .install()
    blockHoundLogger.warn("BlockHound installed (diagnostic mode: logging only, not throwing)")
}

// Diagnostic-only: registers Micrometer executor metrics (executor.queued, executor.active,
// executor.pool.size, ...) on every Reactor scheduler's backing ExecutorService, tagged by scheduler
// name via the reactor.scheduler.id tag. Schedulers.enableMetrics() defaults to Micrometer's static
// global registry, not the MeterRegistry bean Spring Boot exposes via /actuator/metrics - useRegistry()
// must point it at the real bean first, or every query 404s despite the decorator being active. This
// needs a Spring bean, so it can't run in main() like BlockHound above.
//
// Known gap: Schedulers.parallel()/boundedElastic() are cached singletons, created lazily on first use
// and never re-wrapped afterward - the decorator only affects schedulers created after it's installed.
// Something during Spring's own startup touches Schedulers.parallel() before ApplicationReadyEvent
// fires, so it stays uninstrumented (boundedElastic isn't touched that early and works correctly).
// Schedulers.shutdownNow() was tried to force a fresh, instrumented recreation, but disposes the
// existing instance outright - anything already holding a direct reference to it (rather than calling
// Schedulers.parallel() fresh) then throws ReactorRejectedExecutionException("Scheduler unavailable")
// on every subsequent use, which broke entity creation. Reverted; parallel's own executor metrics are
// not available via this mechanism, only boundedElastic's.
@Component
class SchedulerMetricsInitializer(private val meterRegistry: MeterRegistry) {

    @EventListener(ApplicationReadyEvent::class)
    fun enableSchedulerMetrics() {
        Metrics.MicrometerConfiguration.useRegistry(meterRegistry)
        @Suppress("DEPRECATION")
        Schedulers.enableMetrics()
    }
}

@Suppress("SpreadOperator")
fun main(args: Array<String>) {
    installBlockHoundIfEnabled()
    runApplication<SearchServiceApplication>(*args)
}
