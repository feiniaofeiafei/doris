// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.catalog;

import org.apache.doris.analysis.FunctionName;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// Used to search a function
public class FunctionSearchDesc implements Writable {
    @SerializedName("n")
    private FunctionName name;
    @SerializedName("t")
    private Type[] argTypes;
    @SerializedName("isV")
    private boolean isVariadic;

    private FunctionSearchDesc() {}

    public FunctionSearchDesc(FunctionName name, Type[] argTypes, boolean isVariadic) {
        this.name = name;
        this.argTypes = argTypes;
        this.isVariadic = isVariadic;
    }

    public FunctionName getName() {
        return name;
    }

    public Type[] getArgTypes() {
        return argTypes;
    }

    public boolean isVariadic() {
        return isVariadic;
    }

    public boolean isIdentical(Function function) {
        if (!name.equals(function.getFunctionName())) {
            return false;
        }

        if (argTypes.length != function.getArgs().length) {
            return false;
        }
        if (isVariadic != function.hasVarArgs()) {
            return false;
        }
        for (int i = 0; i < argTypes.length; ++i) {
            if (!argTypes[i].matchesType(function.getArgs()[i])) {
                return false;
            }
        }
        return true;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(name.toString()).append("(");
        int i = 0;
        for (Type type : argTypes) {
            if (i != 0) {
                sb.append(", ");
            }
            sb.append(type.toString());
            i++;
        }
        if (isVariadic) {
            if (i != 0) {
                sb.append(", ");
            }
            sb.append("...");
        }
        sb.append(")");
        return sb.toString();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static FunctionSearchDesc read(DataInput input) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(input), FunctionSearchDesc.class);
    }
}
