package com.egm.stellio.shared.model.parameter

import jakarta.validation.Constraint
import jakarta.validation.ConstraintValidator
import jakarta.validation.ConstraintValidatorContext
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
    val implemented: Array<QueryParameter>,
    val notImplemented: Array<QueryParameter> = [],
    val message: String = "Invalid parameter received",
    val groups: Array<KClass<*>> = [],
    val payload: Array<KClass<*>> = [],
) {
    class ParamValidator :
        ConstraintValidator<AllowedParameters, MultiValueMap<String, String>?> {
        private var implemented: List<String> = listOf()
        private var notImplemented: List<String> = listOf()

        override fun initialize(requiredIfChecked: AllowedParameters) {
            this.implemented = requiredIfChecked.implemented.map(QueryParameter::key)
            this.notImplemented = requiredIfChecked.notImplemented.map(QueryParameter::key)
        }

        override fun isValid(value: MultiValueMap<String, String>?, context: ConstraintValidatorContext): Boolean {
            if (value == null || implemented.containsAll(value.keys)) {
                return true
            }
            context.disableDefaultConstraintViolation()
            context.buildConstraintViolationWithTemplate(
                "Accepted parameters are '${implemented.joinToString("', '")}'"
            ).addConstraintViolation()

            if (value.keys.any { it in notImplemented }) {
                context.buildConstraintViolationWithTemplate(
                    "The '${notImplemented.joinToString("', '")}' parameters are not implemented yet "
                ).addPropertyNode(HttpStatus.NOT_IMPLEMENTED.name).addConstraintViolation()
                return false
            }
            return false
        }
    }
}
