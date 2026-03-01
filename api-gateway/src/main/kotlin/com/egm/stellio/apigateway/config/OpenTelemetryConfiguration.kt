package com.egm.stellio.apigateway.config

import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.Tags
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics
import io.micrometer.core.instrument.binder.jvm.convention.otel.OpenTelemetryJvmClassLoadingMeterConventions
import io.micrometer.core.instrument.binder.jvm.convention.otel.OpenTelemetryJvmCpuMeterConventions
import io.micrometer.core.instrument.binder.jvm.convention.otel.OpenTelemetryJvmMemoryMeterConventions
import io.micrometer.core.instrument.binder.jvm.convention.otel.OpenTelemetryJvmThreadMeterConventions
import io.micrometer.core.instrument.binder.system.ProcessorMetrics
import io.opentelemetry.api.OpenTelemetry
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.http.server.observation.OpenTelemetryServerRequestObservationConvention

// This class should no longer be needed in Spring Boot 4.1
// See https://github.com/spring-projects/spring-boot/pull/47935
@Configuration(proxyBeanMethods = false)
class OpenTelemetryConfiguration {

    @Bean
    fun installOpenTelemetryAppender(openTelemetry: OpenTelemetry): InstallOpenTelemetryAppender {
        return InstallOpenTelemetryAppender(openTelemetry)
    }

    @Bean
    fun openTelemetryServerRequestObservationConvention(): OpenTelemetryServerRequestObservationConvention {
        return OpenTelemetryServerRequestObservationConvention()
    }

    @Bean
    fun openTelemetryJvmCpuMeterConventions(): OpenTelemetryJvmCpuMeterConventions {
        return OpenTelemetryJvmCpuMeterConventions(Tags.empty())
    }

    @Bean
    fun processorMetrics(): ProcessorMetrics {
        return ProcessorMetrics(mutableListOf<Tag>(), OpenTelemetryJvmCpuMeterConventions(Tags.empty()))
    }

    @Bean
    fun jvmMemoryMetrics(): JvmMemoryMetrics {
        return JvmMemoryMetrics(mutableListOf<Tag>(), OpenTelemetryJvmMemoryMeterConventions(Tags.empty()))
    }

    @Bean
    fun jvmThreadMetrics(): JvmThreadMetrics {
        return JvmThreadMetrics(mutableListOf<Tag>(), OpenTelemetryJvmThreadMeterConventions(Tags.empty()))
    }

    @Bean
    fun classLoaderMetrics(): ClassLoaderMetrics {
        return ClassLoaderMetrics(OpenTelemetryJvmClassLoadingMeterConventions())
    }
}
