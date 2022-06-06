/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "velox/substrait/SubstraitToVeloxExpr.h"
#include "velox/substrait/TypeUtils.h"
#include "velox/substrait/VectorCreater.h"

namespace facebook::velox::substrait {

std::shared_ptr<const core::FieldAccessTypedExpr>
SubstraitVeloxExprConverter::toVeloxExpr(
    const ::substrait::Expression::FieldReference& substraitField,
    const RowTypePtr& inputType) {
  auto typeCase = substraitField.reference_type_case();
  switch (typeCase) {
    case ::substrait::Expression::FieldReference::ReferenceTypeCase::
        kDirectReference: {
      const auto& dRef = sField.direct_reference();
      int32_t colIdx = subParser_->parseReferenceSegment(dRef);
      const auto& inputNames = inputType->names();
      const int64_t inputSize = inputNames.size();
      if (colIdx <= inputSize) {
        const auto& inputTypes = inputType->children();
        // Convert type to row.
        return std::make_shared<core::FieldAccessTypedExpr>(
            inputTypes[colIdx],
            std::make_shared<core::InputTypedExpr>(inputTypes[colIdx]),
            inputNames[colIdx]);
      } else {
        VELOX_FAIL("Missing the column with id '{}' .", colIdx);
      }
    }
    default:
      VELOX_NYI(
          "Substrait conversion not supported for Reference '{}'", typeCase);
  }
}

std::shared_ptr<const core::ITypedExpr>
SubstraitVeloxExprConverter::toAliasExpr(
    const std::vector<std::shared_ptr<const core::ITypedExpr>>& params) {
  VELOX_CHECK(params.size() == 1, "Alias expects one parameter.");
  // Alias is omitted due to name change is not needed.
  return params[0];
}

std::shared_ptr<const core::ITypedExpr>
SubstraitVeloxExprConverter::toIsNotNullExpr(
    const std::vector<std::shared_ptr<const core::ITypedExpr>>& params,
    const TypePtr& outputType) {
  // Convert is_not_null to not(is_null).
  auto isNullExpr = std::make_shared<const core::CallTypedExpr>(
      outputType, std::move(params), "is_null");
  std::vector<std::shared_ptr<const core::ITypedExpr>> notParams;
  notParams.reserve(1);
  notParams.emplace_back(isNullExpr);
  return std::make_shared<const core::CallTypedExpr>(
      outputType, std::move(notParams), "not");
}

std::shared_ptr<const core::ITypedExpr>
SubstraitVeloxExprConverter::toVeloxExpr(
    const ::substrait::Expression::ScalarFunction& substraitFunc,
    const RowTypePtr& inputType) {
  std::vector<core::TypedExprPtr> params;
  params.reserve(substraitFunc.arguments().size());
  for (const auto& sArg : substraitFunc.arguments()) {
    params.emplace_back(toVeloxExpr(sArg.value(), inputType));
  }
  const auto& veloxFunction =
      subParser_->findVeloxFunction(functionMap_, sFunc.function_reference());
  const auto& veloxType =
      toVeloxType(subParser_->parseType(sFunc.output_type())->type);

  if (veloxFunction == "alias") {
    return toAliasExpr(params);
  }
  if (veloxFunction == "is_not_null") {
    return toIsNotNullExpr(params, veloxType);
  }
  return std::make_shared<const core::CallTypedExpr>(
      toVeloxType(typeName), std::move(params), veloxFunction);
}

std::shared_ptr<const core::ConstantTypedExpr>
SubstraitVeloxExprConverter::toVeloxExpr(
    const ::substrait::Expression::Literal& substraitLit) {
  auto typeCase = substraitLit.literal_type_case();
  switch (typeCase) {
    case ::substrait::Expression_Literal::LiteralTypeCase::kBoolean:
      return std::make_shared<core::ConstantTypedExpr>(variant(sLit.boolean()));
    case ::substrait::Expression_Literal::LiteralTypeCase::kI32:
      return std::make_shared<core::ConstantTypedExpr>(variant(sLit.i32()));
    case ::substrait::Expression_Literal::LiteralTypeCase::kI64:
      return std::make_shared<core::ConstantTypedExpr>(variant(sLit.i64()));
    case ::substrait::Expression_Literal::LiteralTypeCase::kFp64:
      return std::make_shared<core::ConstantTypedExpr>(variant(sLit.fp64()));
    case ::substrait::Expression_Literal::LiteralTypeCase::kNull: {
      auto veloxType =
          toVeloxType(substraitParser_.parseType(substraitLit.null())->type);
      return std::make_shared<core::ConstantTypedExpr>(
          veloxType, variant::null(veloxType->kind()));
      case ::substrait::Expression_Literal::LiteralTypeCase::kString:
        return std::make_shared<core::ConstantTypedExpr>(
            toTypedVariant(sLit)->veloxVariant);
      case ::substrait::Expression_Literal::LiteralTypeCase::kList: {
        // List is used in 'in' expression. Will wrap a constant
        // vector with an array vector inside to create the constant expression.
        std::vector<variant> variants;
        variants.reserve(sLit.list().values().size());
        VELOX_CHECK(
            sLit.list().values().size() > 0,
            "List should have at least one item.");
        std::optional<TypePtr> literalType = std::nullopt;
        for (const auto& literal : sLit.list().values()) {
          auto typedVariant = toTypedVariant(literal);
          if (!literalType.has_value()) {
            literalType = typedVariant->variantType;
          }
          variants.emplace_back(typedVariant->veloxVariant);
        }
        VELOX_CHECK(literalType.has_value(), "Type expected.");
        // Create flat vector from the variants.
        VectorPtr vector =
            setVectorFromVariants(literalType.value(), variants, pool_);
        // Create array vector from the flat vector.
        ArrayVectorPtr arrayVector =
            toArrayVector(literalType.value(), vector, pool_);
        // Wrap the array vector into constant vector.
        auto constantVector = BaseVector::wrapInConstant(1, 0, arrayVector);
        auto constantExpr =
            std::make_shared<core::ConstantTypedExpr>(constantVector);
        return constantExpr;
      }
      default:
        VELOX_NYI(
            "Substrait conversion not supported for type case '{}'", typeCase);
    }
  }

  std::shared_ptr<const core::ITypedExpr>
  SubstraitVeloxExprConverter::toVeloxExpr(
      const ::substrait::Expression::Cast& castExpr,
      const RowTypePtr& inputType) {
    auto substraitType = substraitParser_.parseType(castExpr.type());
    auto type = toVeloxType(substraitType->type);
    // TODO add flag in substrait after. now is set false.
    bool nullOnFailure = false;

    std::vector<core::TypedExprPtr> inputs{
        toVeloxExpr(castExpr.input(), inputType)};

    return std::make_shared<core::CastTypedExpr>(type, inputs, nullOnFailure);
  }

  std::shared_ptr<const core::ITypedExpr>
  SubstraitVeloxExprConverter::toVeloxExpr(
      const ::substrait::Expression::IfThen& ifThenExpr,
      const RowTypePtr& inputType) {
    VELOX_CHECK(ifThenExpr.ifs().size() > 0, "If clause expected.");

    // Params are concatenated conditions and results with an optional "else" at
    // the end, e.g. {condition1, result1, condition2, result2,..else}
    std::vector<core::TypedExprPtr> params;
    // If and then expressions are in pairs.
    params.reserve(ifThenExpr.ifs().size() * 2);
    std::optional<TypePtr> outputType = std::nullopt;
    for (const auto& ifThen : ifThenExpr.ifs()) {
      params.emplace_back(toVeloxExpr(ifThen.if_(), inputType));
      const auto& thenExpr = toVeloxExpr(ifThen.then(), inputType);
      // Get output type from the first then expression.
      if (!outputType.has_value()) {
        outputType = thenExpr->type();
      }
      params.emplace_back(thenExpr);
    }

    if (ifThenExpr.has_else_()) {
      params.reserve(1);
      params.emplace_back(toVeloxExpr(ifThenExpr.else_(), inputType));
    }

    VELOX_CHECK(outputType.has_value(), "Output type should be set.");
    if (ifThenExpr.ifs().size() == 1) {
      // If there is only one if-then clause, use if expression.
      return std::make_shared<const core::CallTypedExpr>(
          outputType.value(), std::move(params), "if");
    }
    return std::make_shared<const core::CallTypedExpr>(
        outputType.value(), std::move(params), "switch");
  }

  std::shared_ptr<const core::ITypedExpr>
  SubstraitVeloxExprConverter::toVeloxExpr(
      const ::substrait::Expression& sExpr, const RowTypePtr& inputType) {
    std::shared_ptr<const core::ITypedExpr> veloxExpr;
    auto typeCase = substraitExpr.rex_type_case();
    switch (typeCase) {
      case ::substrait::Expression::RexTypeCase::kLiteral:
        return toVeloxExpr(substraitExpr.literal());
      case ::substrait::Expression::RexTypeCase::kScalarFunction:
        return toVeloxExpr(substraitExpr.scalar_function(), inputType);
      case ::substrait::Expression::RexTypeCase::kSelection:
        return toVeloxExpr(substraitExpr.selection(), inputType);
      case ::substrait::Expression::RexTypeCase::kCast:
        return toVeloxExpr(sExpr.cast(), inputType);
      case ::substrait::Expression::RexTypeCase::kIfThen:
        return toVeloxExpr(sExpr.if_then(), inputType);
      default:
        VELOX_NYI(
            "Substrait conversion not supported for Expression '{}'", typeCase);
    }
  }

  std::shared_ptr<SubstraitVeloxExprConverter::TypedVariant>
  SubstraitVeloxExprConverter::toTypedVariant(
      const ::substrait::Expression::Literal& literal) {
    auto typeCase = literal.literal_type_case();
    switch (typeCase) {
      case ::substrait::Expression_Literal::LiteralTypeCase::kBoolean: {
        TypedVariant typedVariant = {variant(literal.boolean()), BOOLEAN()};
        return std::make_shared<TypedVariant>(typedVariant);
      }
      case ::substrait::Expression_Literal::LiteralTypeCase::kI32: {
        TypedVariant typedVariant = {variant(literal.i32()), INTEGER()};
        return std::make_shared<TypedVariant>(typedVariant);
      }
      case ::substrait::Expression_Literal::LiteralTypeCase::kI64: {
        TypedVariant typedVariant = {variant(literal.i64()), BIGINT()};
        return std::make_shared<TypedVariant>(typedVariant);
      }
      case ::substrait::Expression_Literal::LiteralTypeCase::kFp64: {
        TypedVariant typedVariant = {variant(literal.fp64()), DOUBLE()};
        return std::make_shared<TypedVariant>(typedVariant);
      }
      case ::substrait::Expression_Literal::LiteralTypeCase::kString: {
        TypedVariant typedVariant = {variant(literal.string()), VARCHAR()};
        return std::make_shared<TypedVariant>(typedVariant);
      }
      default:
        VELOX_NYI("ToVariant not supported for type case '{}'", typeCase);
    }
  }

} // namespace facebook::velox::substrait
