package com.egm.stellio.search.util

import com.egm.stellio.shared.util.AuthContextModel.AUTH_PROP_KIND
import com.egm.stellio.shared.util.AuthContextModel.AUTH_PROP_USERNAME
import com.egm.stellio.shared.util.AuthContextModel.AUTH_TERM_KIND
import com.egm.stellio.shared.util.AuthContextModel.AUTH_TERM_USERNAME
import com.egm.stellio.shared.util.ExpandedAttributePayload
import com.egm.stellio.shared.util.JsonLdUtils

object JsonLdUtils {

    /**
     * Build the expanded subject info.
     *
     * For instance:
     *
     * "[
     *   {
     *     "@type": [
     *       "https://uri.etsi.org/ngsi-ld/Property"
     *     ],
     *     "https://uri.etsi.org/ngsi-ld/hasValue": [
     *       {
     *         "https://ontology.eglobalmark.com/authorization#kind": [
     *              {
     *                  "@value": "kind"
     *               }
     *         ],
     *         "https://ontology.eglobalmark.com/authorization#username": [
     *              {
     *                  "@value": "username"
     *              }
     *         ]
     *       }
     *     ]
     *   }
     * ]
     */
    fun buildExpandedSubjectInfo(value: Map<String, String>): ExpandedAttributePayload =
        listOf(
            mapOf(
                JsonLdUtils.JSONLD_TYPE to listOf(JsonLdUtils.NGSILD_PROPERTY_TYPE.uri),
                JsonLdUtils.NGSILD_PROPERTY_VALUE to listOf(
                    mapOf(
                        AUTH_PROP_KIND to listOf(
                            mapOf(
                                JsonLdUtils.JSONLD_VALUE_KW to value[AUTH_TERM_KIND]
                            )
                        ),
                        AUTH_PROP_USERNAME to listOf(
                            mapOf(
                                JsonLdUtils.JSONLD_VALUE_KW to value[AUTH_TERM_USERNAME]
                            )
                        )
                    )
                )
            )
        )
}
