package com.egm.stellio.shared.queryparameter

import jakarta.validation.Constraint
import jakarta.validation.ConstraintValidator
import jakarta.validation.ConstraintValidatorContext
import jdk.internal.org.jline.utils.AttributedStringBuilder.append
import org.springframework.http.HttpStatus
import org.springframework.util.MultiValueMap
import kotlin.reflect.KClass

@Target(
    AnnotationTarget.VALUE_PARAMETER,
)
@Retention(
    AnnotationRetention.RUNTIME
)
@Constraint(validatedBy = [ AllowedParameters.ParamValidator::class])
annotation class AllowedParameters(
    val implemented: Array<QueryParameter> = [],
    val notImplemented: Array<QueryParameter> = [],
    val message: String = "Invalid parameter received",
    val groups: Array<KClass<*>> = [],
    val payload: Array<KClass<*>> = [],
) {
    class ParamValidator :
        ConstraintValidator<AllowedParameters, MultiValueMap<String, String>> {
        private var implemented: List<String> = listOf()
        private var notImplemented: List<String> = listOf()

        override fun initialize(allowedParameters: AllowedParameters) {
            this.implemented = allowedParameters.implemented.map(QueryParameter::key)
            this.notImplemented = allowedParameters.notImplemented.map(QueryParameter::key)
        }

        override fun isValid(params: MultiValueMap<String, String>, context: ConstraintValidatorContext): Boolean {
            if (implemented.containsAll(params.keys)) {
                return true
            }

            val notImplementedKeys = params.keys.intersect(notImplemented)
            val errorKeys = params.keys - notImplementedKeys - implemented

            context.disableDefaultConstraintViolation()

            val message = StringBuilder().apply {
                if (notImplementedKeys.isNotEmpty()) {
                    append(
                        "The '${notImplementedKeys.joinToString("', '")}' parameter is not implemented yet. "
                    )
                }
                if (errorKeys.isNotEmpty()) {
                    append(
                        "The '${errorKeys.joinToString("', '")}' parameter is not allowed on this endpoint. "
                    )
                }
                append(
                    "Accepted parameters are '${implemented.joinToString("', '")}'. "
                )
            }.toString()

            context.buildConstraintViolationWithTemplate(
                message
            ).addPropertyNode(
                if (notImplemented.isEmpty()) HttpStatus.BAD_REQUEST.name else HttpStatus.NOT_IMPLEMENTED.name
            ).addConstraintViolation()

            return false
        }
    }
}
