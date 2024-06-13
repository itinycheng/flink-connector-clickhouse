/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.clickhouse.util;

import java.util.function.Function;

/** SQL filters that support push down. */
public enum SqlClause {
    EQ(args -> String.format("%s = %s", args[0], args[1])),

    NOT_EQ(args -> String.format("%s <> %s", args[0], args[1])),

    GT(args -> String.format("%s > %s", args[0], args[1])),

    GT_EQ(args -> String.format("%s >= %s", args[0], args[1])),

    LT(args -> String.format("%s < %s", args[0], args[1])),

    LT_EQ(args -> String.format("%s <= %s", args[0], args[1])),

    IS_NULL(args -> String.format("%s IS NULL", args[0])),

    IS_NOT_NULL(args -> String.format("%s IS NOT NULL", args[0])),

    AND(args -> String.format("%s AND %s", args[0], args[1])),

    OR(args -> String.format("%s OR %s", args[0], args[1]));

    public final Function<String[], String> formatter;

    SqlClause(final Function<String[], String> function) {
        this.formatter = function;
    }
}
