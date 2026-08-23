package com.egm.stellio.search

import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.ConfigurationPropertiesScan
import org.springframework.boot.runApplication
import org.springframework.scheduling.annotation.EnableScheduling
import reactor.blockhound.BlockHound

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

@Suppress("SpreadOperator")
fun main(args: Array<String>) {
    installBlockHoundIfEnabled()
    runApplication<SearchServiceApplication>(*args)
}
