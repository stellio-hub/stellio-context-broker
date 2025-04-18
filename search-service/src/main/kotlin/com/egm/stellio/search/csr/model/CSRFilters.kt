package com.egm.stellio.search.csr.model

import arrow.core.Either
import arrow.core.raise.either
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.EntityTypeSelection
import com.egm.stellio.shared.model.ExpandedTerm
import com.egm.stellio.shared.queryparameter.QueryParameter
import com.egm.stellio.shared.util.expandTypeSelection
import com.egm.stellio.shared.util.parseAndExpandQueryParameter
import com.egm.stellio.shared.util.toListOfUri
import com.egm.stellio.shared.util.toTypeSelection
import com.egm.stellio.shared.util.validateIdPattern
import org.springframework.util.MultiValueMap
import java.net.URI

open class CSRFilters( // we should use a combination of EntitiesQuery TemporalQuery (when we implement all operations)
    val ids: Set<URI> = emptySet(),
    val typeSelection: EntityTypeSelection? = null,
    val idPattern: String? = null,
    val csf: String? = null,
    val attrs: Set<ExpandedTerm> = emptySet()
) {
    constructor(
        ids: Set<URI> = emptySet(),
        typeSelection: EntityTypeSelection? = null,
        idPattern: String? = null,
        operations: List<Operation>?,
        attrs: Set<String> = emptySet()
    ) :
        this(
            ids,
            typeSelection,
            idPattern,
            csf = operations?.joinToString(",") { "${ContextSourceRegistration::operations.name}==${it.key}" },
            attrs
        )

    constructor(
        ids: Set<URI> = emptySet(),
        types: Set<String>,
        idPattern: String? = null,
        operations: List<Operation>? = null,
        attrs: Set<String> = emptySet()
    ) : this(
        ids,
        types.toTypeSelection(),
        idPattern,
        operations = operations,
        attrs
    )

    companion object {
        fun fromQueryParameters(
            queryParams: MultiValueMap<String, String>,
            contexts: List<String>
        ): Either<APIException, CSRFilters> = either {
            val ids = queryParams.getFirst(QueryParameter.ID.key)?.split(",").orEmpty().toListOfUri().toSet()
            val typeSelection = expandTypeSelection(queryParams.getFirst(QueryParameter.TYPE.key), contexts)
            val idPattern = validateIdPattern(queryParams.getFirst(QueryParameter.ID_PATTERN.key)).bind()
            val attrs = parseAndExpandQueryParameter(queryParams.getFirst(QueryParameter.ATTRS.key), contexts)

            CSRFilters(ids, typeSelection, idPattern, attrs = attrs)
        }
    }
}
