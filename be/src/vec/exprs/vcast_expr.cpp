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

#include "vec/exprs/vcast_expr.h"

#include <fmt/format.h>
#include <gen_cpp/Types_types.h>
#include <glog/logging.h>

#include <cstddef>
#include <memory>
#include <ostream>

#include "common/exception.h"
#include "common/status.h"
#include "runtime/runtime_state.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/columns_with_type_and_name.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/functions/simple_function_factory.h"

namespace doris {
class RowDescriptor;
class RuntimeState;
} // namespace doris

namespace doris::vectorized {
#include "common/compile_check_begin.h"

doris::Status VCastExpr::prepare(doris::RuntimeState* state, const doris::RowDescriptor& desc,
                                 VExprContext* context) {
    RETURN_IF_ERROR_OR_PREPARED(VExpr::prepare(state, desc, context));

    DCHECK_EQ(_children.size(), 1);
    auto child = _children[0];
    const auto& child_name = child->expr_name();

    // create a const string column
    _target_data_type = _data_type;
    // TODO(xy): support return struct type name
    _target_data_type_name = _target_data_type->get_name();
    // Using typeindex to indicate the datatype, not using type name because
    // type name is not stable, but type index is stable and immutable
    _cast_param_data_type = _target_data_type;

    ColumnsWithTypeAndName argument_template;
    argument_template.reserve(2);
    argument_template.emplace_back(nullptr, child->data_type(), child_name);
    argument_template.emplace_back(nullptr, _cast_param_data_type, _target_data_type_name);
    _function = SimpleFunctionFactory::instance().get_function(
            function_name, argument_template, _data_type,
            {.enable_decimal256 = state->enable_decimal256()});

    if (_function == nullptr) {
        return Status::NotSupported("Function {} is not implemented", _fn.name.function_name);
    }
    VExpr::register_function_context(state, context);
    _expr_name = fmt::format("(CAST {}({}) TO {})", child_name, child->data_type()->get_name(),
                             _target_data_type_name);
    _prepare_finished = true;
    return Status::OK();
}

const DataTypePtr& VCastExpr::get_target_type() const {
    return _target_data_type;
}

doris::Status VCastExpr::open(doris::RuntimeState* state, VExprContext* context,
                              FunctionContext::FunctionStateScope scope) {
    DCHECK(_prepare_finished);
    for (auto& i : _children) {
        RETURN_IF_ERROR(i->open(state, context, scope));
    }
    RETURN_IF_ERROR(VExpr::init_function_context(state, context, scope, _function));
    if (scope == FunctionContext::FRAGMENT_LOCAL) {
        RETURN_IF_ERROR(VExpr::get_const_col(context, nullptr));
    }
    _open_finished = true;
    return Status::OK();
}

void VCastExpr::close(VExprContext* context, FunctionContext::FunctionStateScope scope) {
    VExpr::close_function_context(context, scope, _function);
    VExpr::close(context, scope);
}

doris::Status VCastExpr::execute(VExprContext* context, doris::vectorized::Block* block,
                                 int* result_column_id) {
    DCHECK(_open_finished || _getting_const_col)
            << _open_finished << _getting_const_col << _expr_name;
    // for each child call execute
    int column_id = 0;
    RETURN_IF_ERROR(_children[0]->execute(context, block, &column_id));

    // call function
    uint32_t num_columns_without_result = block->columns();
    // prepare a column to save result
    block->insert({nullptr, _data_type, _expr_name});

    auto state = Status::OK();
    try {
        state = _function->execute(context->fn_context(_fn_context_index), *block,
                                   {static_cast<uint32_t>(column_id)}, num_columns_without_result,
                                   block->rows(), false);
        RETURN_IF_ERROR(state);
        // set the result column id only state is ok
        *result_column_id = num_columns_without_result;
    } catch (const Exception& e) {
        state = e.to_status();
    }
    if (state.ok()) {
        auto [result_column, is_const] =
                unpack_if_const(block->get_by_position(num_columns_without_result).column);

        if (result_column->is_nullable() != _data_type->is_nullable()) {
            return Status::InternalError(
                    fmt::format("CastExpr result column type mismatch, expect {}, got {} , return "
                                "column is const {}",
                                _data_type->get_name(), result_column->get_name(), is_const));
        }
    }
    return state;
}

const std::string& VCastExpr::expr_name() const {
    return _expr_name;
}

std::string VCastExpr::debug_string() const {
    std::stringstream out;
    out << "CastExpr(CAST " << get_child(0)->data_type()->get_name() << " to "
        << _target_data_type->get_name() << "){";
    bool first = true;
    for (const auto& input_expr : children()) {
        if (first) {
            first = false;
        } else {
            out << ",";
        }
        out << input_expr->debug_string();
    }
    out << "}";
    return out.str();
}

#include "common/compile_check_end.h"
} // namespace doris::vectorized
