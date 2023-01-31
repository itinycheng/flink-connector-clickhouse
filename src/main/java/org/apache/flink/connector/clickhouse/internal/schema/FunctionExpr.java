package org.apache.flink.connector.clickhouse.internal.schema;

import javax.annotation.Nonnull;

import java.util.List;

import static java.util.stream.Collectors.joining;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.StringUtils.isNullOrWhitespaceOnly;

/** Function expression. */
public class FunctionExpr extends Expression {

    private final String functionName;

    private final List<Expression> arguments;

    private FunctionExpr(String functionName, List<Expression> arguments) {
        checkArgument(
                !isNullOrWhitespaceOnly(functionName), "functionName cannot be null or empty");
        checkNotNull(arguments, "arguments cannot be null");

        this.functionName = functionName;
        this.arguments = arguments;
    }

    public static FunctionExpr of(@Nonnull String functionName, List<Expression> arguments) {
        return new FunctionExpr(functionName, arguments);
    }

    @Override
    public String explain() {
        if (arguments.isEmpty()) {
            return functionName + "()";
        }

        return String.format(
                "%s(%s)",
                functionName, arguments.stream().map(Expression::explain).collect(joining(",")));
    }
}
