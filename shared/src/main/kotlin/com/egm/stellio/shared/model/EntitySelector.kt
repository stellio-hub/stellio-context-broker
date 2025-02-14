package com.egm.stellio.shared.model

import com.fasterxml.jackson.annotation.JsonProperty
import java.net.URI

// an entity type selection as per clause 4.17
typealias EntityTypeSelection = String

/**
 * EntitySelector type as defined in 5.2.33
 */
data class EntitySelector(
    val id: URI?,
    val idPattern: String?,
    @JsonProperty(value = "type")
    val typeSelection: EntityTypeSelection
)

val orRegex = "^(.*)[|,](.*)\$".toRegex()
val andRegex = "^(.*);(.*)\$".toRegex()
val innerParanthesisRegex = "\\(([^()]*)\\)".toRegex()

fun areTypesInSelection(types: List<ExpandedTerm>, typeSelection: EntityTypeSelection): Boolean {
    var processedTypeSelection = typeSelection
    while (innerParanthesisRegex.containsMatchIn(processedTypeSelection)) {
        innerParanthesisRegex.find(processedTypeSelection)?.let { matches ->
            val groups = matches.groups.drop(1)
            groups.forEach {
                processedTypeSelection = processedTypeSelection
                    .replace("(${it!!.value})", areTypesInSelection(types, it.value).toString())
            }
        }
    }

    orRegex.find(processedTypeSelection)?.let {
        return areTypesInSelection(types, it.groups[1]!!.value) || areTypesInSelection(types, it.groups[2]!!.value)
    }
    andRegex.find(processedTypeSelection)?.let {
        return areTypesInSelection(types, it.groups[1]!!.value) && areTypesInSelection(types, it.groups[2]!!.value)
    }
    processedTypeSelection.toBooleanStrictOrNull()?.let { return it }
    return types.contains(processedTypeSelection)
}
