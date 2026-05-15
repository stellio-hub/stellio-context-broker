package com.egm.stellio.shared.queryparameter

import arrow.core.Either
import arrow.core.left
import arrow.core.right
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
    return try {
        QParserImpl(raw).parse().right()
    } catch (e: QParseException) {
        BadRequestDataException(invalidQQueryMessage(e.message ?: "Invalid query")).left()
    }
}

private class QParseException(message: String) : Exception(message)

private class QParserImpl(private val raw: String) {
    private var pos = 0

    companion object {
        private const val OPERATOR_SNIPPET_LENGTH = 3
    }

    fun parse(): QNode {
        val node = parseOrExpr()
        if (pos < raw.length) {
            throw QParseException("Unexpected character at position $pos: '${raw[pos]}'")
        }
        return node
    }

    private fun parseOrExpr(): QNode {
        var left = parseAndExpr()
        while (pos < raw.length && raw[pos] == '|') {
            pos++
            val right = parseAndExpr()
            left = OrNode(left, right)
        }
        return left
    }

    private fun parseAndExpr(): QNode {
        var left = parseTerm()
        while (pos < raw.length && raw[pos] == ';') {
            pos++
            if (pos >= raw.length) throw QParseException("Trailing ';' at end of query")
            val right = parseTerm()
            left = AndNode(left, right)
        }
        return left
    }

    private fun parseTerm(): QNode {
        if (pos >= raw.length) throw QParseException("Unexpected end of input")
        return when {
            raw[pos] == '(' -> parseGroupedExpr()
            isNegationPrefix() -> parseNotExistsExpr()
            else -> parseAttrExpr()
        }
    }

    private fun parseGroupedExpr(): QNode {
        pos++
        val node = parseOrExpr()
        if (pos >= raw.length || raw[pos] != ')') throw QParseException("Unbalanced parenthesis: missing closing ')'")
        pos++
        return node
    }

    private fun parseNotExistsExpr(): QNode {
        pos++
        val rawPath = parseRawAttributePath()
        if (rawPath.isEmpty()) throw QParseException("Missing attribute path after '!'")
        return NotExistsNode(rawPath)
    }

    private fun parseAttrExpr(): QNode {
        val rawPath = parseRawAttributePath()
        if (rawPath.isEmpty()) throw QParseException("Empty attribute path at position $pos")
        val op = parseOperator()
        return if (op == null) ExistsNode(rawPath) else ComparisonNode(rawPath, op, parseValue())
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

    private fun isOperatorChar(c: Char): Boolean = c == '=' || c == '!' || c == '>' || c == '<' || c == '~'

    private fun parseOperator(): ComparisonOperator? {
        if (pos >= raw.length || !isOperatorChar(raw[pos])) return null
        return when {
            tryConsume("!~=") -> ComparisonOperator.NOT_LIKE_REGEX
            tryConsume("~=") -> ComparisonOperator.LIKE_REGEX
            tryConsume("==") -> ComparisonOperator.EQ
            tryConsume("!=") -> ComparisonOperator.NEQ
            tryConsume(">=") -> ComparisonOperator.GTE
            tryConsume("<=") -> ComparisonOperator.LTE
            tryConsume(">") -> ComparisonOperator.GT
            tryConsume("<") -> ComparisonOperator.LT
            else -> {
                val snippetEnd = minOf(pos + OPERATOR_SNIPPET_LENGTH, raw.length)
                throw QParseException("Unknown operator at position $pos: '${raw.substring(pos, snippetEnd)}'")
            }
        }
    }

    private fun tryConsume(s: String): Boolean {
        if (raw.startsWith(s, pos)) {
            pos += s.length
            return true
        }
        return false
    }

    private fun parseValue(): QValue {
        val rawVal = parseRawValue()
        if (rawVal.isEmpty()) throw QParseException("Empty value after operator")

        if (!rawVal.startsWith('"') && rawVal.contains("..")) {
            val parts = rawVal.split("..")
            val validRange = parts.size == 2 && parts[0].isNotEmpty() && parts[1].isNotEmpty()
            if (!validRange) throw QParseException("Invalid range value: '$rawVal'")
            val low = SingleValue(parts[0], ValueType.detect(parts[0]))
            val high = SingleValue(parts[1], ValueType.detect(parts[1]))
            return RangeValue(low, high)
        }

        if (rawVal.isQValueList()) {
            val items = rawVal.splitQValueList().map { SingleValue(it, ValueType.detect(it)) }
            if (items.size > 1) return ListValue(items)
        }

        return SingleValue(rawVal, ValueType.detect(rawVal))
    }

    private fun parseRawValue(): String {
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
                c == ';' || c == '|' || c == ')' -> break
                else -> {
                    sb.append(c)
                    pos++
                }
            }
        }
        if (inQuotes) throw QParseException("Unclosed quote in value")
        return sb.toString()
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
