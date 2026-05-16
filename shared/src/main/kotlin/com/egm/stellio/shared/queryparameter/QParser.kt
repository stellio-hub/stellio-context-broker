package com.egm.stellio.shared.queryparameter

import arrow.core.Either
import arrow.core.left
import arrow.core.raise.either
import arrow.core.raise.ensure
import com.egm.stellio.shared.model.APIException
import com.egm.stellio.shared.model.BadRequestDataException
import com.egm.stellio.shared.util.ErrorMessages.QueryParameter.invalidQQueryMessage

/**
 * Recursive descent parser for the NGSI-LD Query Language as defined in ETSI GS CIM 009, clause 4.9.
 *
 * Grammar:
 *   query      = or-expr
 *   or-expr    = and-expr ( "|" and-expr )*
 *   and-expr   = term ( ";" term )*
 *   term       = "!" attr-path
 *              | "(" query ")"
 *              | attr-path [ comparison-op query-value ]
 *   query-value = simple-value ".." simple-value   (range)
 *              | simple-value ( "," simple-value )+ (list)
 *              | simple-value
 */
fun parseQQuery(raw: String): Either<APIException, QNode> {
    if (raw.isBlank()) return BadRequestDataException(invalidQQueryMessage("Query string is empty")).left()
    return QParserImpl(raw).parse()
        .mapLeft { BadRequestDataException(invalidQQueryMessage(it)) }
}

private class QParserImpl(private val raw: String) {
    private var pos = 0

    companion object {
        private const val OPERATOR_SNIPPET_LENGTH = 3
    }

    fun parse(): Either<String, QNode> = either {
        val node = parseOrExpr().bind()
        if (pos < raw.length) raise("Unexpected character at position $pos: '${raw[pos]}'")
        node
    }

    private fun parseOrExpr(): Either<String, QNode> = either {
        var left = parseAndExpr().bind()
        while (pos < raw.length && raw[pos] == '|') {
            pos++
            left = OrNode(left, parseAndExpr().bind())
        }
        left
    }

    private fun parseAndExpr(): Either<String, QNode> = either {
        var left = parseTerm().bind()
        while (pos < raw.length && raw[pos] == ';') {
            pos++
            ensure(pos < raw.length) { "Trailing ';' at end of query" }
            left = AndNode(left, parseTerm().bind())
        }
        left
    }

    private fun parseTerm(): Either<String, QNode> = either {
        ensure(pos < raw.length) { "Unexpected end of input" }
        when {
            raw[pos] == '(' -> parseGroupedExpr().bind()
            isNegationPrefix() -> parseNotExistsExpr().bind()
            else -> parseAttrExpr().bind()
        }
    }

    private fun parseGroupedExpr(): Either<String, QNode> = either {
        pos++
        val node = parseOrExpr().bind()
        ensure(!(pos >= raw.length || raw[pos] != ')')) { "Unbalanced parenthesis: missing closing ')'" }
        pos++
        node
    }

    private fun parseNotExistsExpr(): Either<String, QNode> = either {
        pos++
        val rawPath = parseRawAttributePath()
        ensure(rawPath.isNotEmpty()) { "Missing attribute path after '!'" }
        NotExistsNode(rawPath)
    }

    private fun parseAttrExpr(): Either<String, QNode> = either {
        val rawPath = parseRawAttributePath()
        ensure(rawPath.isNotEmpty()) { "Empty attribute path at position $pos" }
        val op = parseOperator().bind()
        if (op == null) ExistsNode(rawPath) else ComparisonNode(rawPath, op, parseValue().bind())
    }

    private fun isNegationPrefix(): Boolean {
        if (raw[pos] != '!') return false
        val next = pos + 1
        if (next >= raw.length) return false
        return raw[next] != '=' && raw[next] != '~'
    }

    private fun parseRawAttributePath(): String {
        val start = pos
        var inBracket = false
        var done = false
        while (pos < raw.length && !done) {
            val c = raw[pos]
            when {
                c == '[' -> {
                    inBracket = true
                    pos++
                }
                c == ']' -> {
                    inBracket = false
                    pos++
                    done = true
                }
                inBracket -> pos++
                c == ';' || c == '|' || c == ')' || isOperatorChar(c) -> done = true
                else -> pos++
            }
        }
        return raw.substring(start, pos)
    }

    private fun isOperatorChar(c: Char): Boolean = c in setOf('=', '!', '>', '<', '~')

    private fun parseOperator(): Either<String, ComparisonOperator?> = either {
        if (pos >= raw.length || !isOperatorChar(raw[pos])) return@either null
        when {
            tryConsume("!~=") -> ComparisonOperator.NOT_LIKE_REGEX
            tryConsume("~=") -> ComparisonOperator.LIKE_REGEX
            tryConsume("==") -> ComparisonOperator.EQ
            tryConsume("!=") -> ComparisonOperator.NEQ
            tryConsume(">=") -> ComparisonOperator.GTE
            tryConsume("<=") -> ComparisonOperator.LTE
            tryConsume(">") -> ComparisonOperator.GT
            tryConsume("<") -> ComparisonOperator.LT
            else -> raise(
                "Unknown operator at position $pos: " +
                    "'${raw.substring(pos, minOf(pos + OPERATOR_SNIPPET_LENGTH, raw.length))}'"
            )
        }
    }

    private fun tryConsume(s: String): Boolean {
        if (raw.startsWith(s, pos)) {
            pos += s.length
            return true
        }
        return false
    }

    private fun parseValue(): Either<String, QValue> = either {
        val rawVal = parseRawValue().bind()
        ensure(rawVal.isNotEmpty()) { "Empty value after operator" }

        if (!rawVal.startsWith('"') && rawVal.contains("..")) {
            val parts = rawVal.split("..")
            ensure(!(parts.size != 2 || parts[0].isEmpty() || parts[1].isEmpty())) {
                "Invalid range value: '$rawVal'"
            }
            return@either RangeValue(
                SingleValue(parts[0], ValueType.detect(parts[0])),
                SingleValue(parts[1], ValueType.detect(parts[1]))
            )
        }

        if (rawVal.isQValueList()) {
            val items = rawVal.splitQValueList().map { SingleValue(it, ValueType.detect(it)) }
            if (items.size > 1) return@either ListValue(items)
        }

        SingleValue(rawVal, ValueType.detect(rawVal))
    }

    private fun parseRawValue(): Either<String, String> = either {
        val sb = StringBuilder()
        var inQuotes = false
        while (pos < raw.length) {
            val c = raw[pos]
            when {
                c == '"' -> {
                    inQuotes = !inQuotes
                    sb.append(c)
                    pos++
                }
                inQuotes -> {
                    sb.append(c)
                    pos++
                }
                c in listOf(';', '|', ')') -> break
                else -> {
                    sb.append(c)
                    pos++
                }
            }
        }
        ensure(!inQuotes) { "Unclosed quote in value" }
        sb.toString()
    }
}

internal fun String.isQValueList(): Boolean {
    var inQuotes = false
    for (char in this) {
        when (char) {
            '"' -> inQuotes = !inQuotes
            ',' -> if (!inQuotes) return true
            else -> {}
        }
    }
    return false
}

internal fun String.splitQValueList(): List<String> {
    val values = mutableListOf<String>()
    var inQuotes = false
    val current = StringBuilder()
    for (char in this) {
        when (char) {
            '"' -> {
                inQuotes = !inQuotes
                current.append(char)
            }
            ',' if !inQuotes -> {
                values.add(current.toString())
                current.clear()
            }
            else -> current.append(char)
        }
    }
    values.add(current.toString())
    return values
}
