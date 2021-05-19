package com.egm.stellio.entity.util

import java.util.regex.Pattern

object QueryUtils {

    fun buildCypherQueryToQueryEntities(rawQuery: String, pattern: Pattern): String =

        rawQuery.replace(pattern.toRegex()) { matchResult ->
            val parsedQueryTerm = extractComparisonParametersFromQuery(matchResult.value)
            if (parsedQueryTerm.third.isRelationshipTarget()) {
                """
                        EXISTS {
                            MATCH (entity)-[:HAS_OBJECT]-()-[:${parsedQueryTerm.first}]->(e)
                            WHERE e.id ${parsedQueryTerm.second} ${parsedQueryTerm.third}
                        }
                """.trimIndent()
            } else {
                val comparableValue = when {
                    parsedQueryTerm.third.isFloat() -> "toFloat('${parsedQueryTerm.third}')"
                    parsedQueryTerm.third.isDateTime() -> "datetime('${parsedQueryTerm.third}')"
                    parsedQueryTerm.third.isDate() -> "date('${parsedQueryTerm.third}')"
                    parsedQueryTerm.third.isTime() -> "localtime('${parsedQueryTerm.third}')"
                    else -> parsedQueryTerm.third
                }
                """
                       EXISTS {
                           MATCH (entity)-[:HAS_VALUE]->(p:Property)
                           WHERE p.name = '${parsedQueryTerm.first}'
                           AND p.value ${parsedQueryTerm.second} $comparableValue
                       }
                """.trimIndent()
            }
        }
            .replace(";", " AND ")
            .replace("|", " OR ")
}
