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
#include "velox/exec/TableScan.h"
#include "velox/common/time/Timer.h"
#include "velox/exec/Task.h"
#include "velox/expression/Expr.h"

namespace facebook::velox::exec {

TableScan::TableScan(
    int32_t operatorId,
    DriverCtx* driverCtx,
    std::shared_ptr<const core::TableScanNode> tableScanNode)
    : SourceOperator(
          driverCtx,
          tableScanNode->outputType(),
          operatorId,
          tableScanNode->id(),
          "TableScan"),
      tableHandle_(tableScanNode->tableHandle()),
      columnHandles_(tableScanNode->assignments()),
      driverCtx_(driverCtx),
      preferredBatchSize_(driverCtx->queryConfig().preferredOutputBatchSize()) {
  connector_ = connector::getConnector(tableHandle_->connectorId());
}

RowVectorPtr TableScan::getOutput() {
  if (noMoreSplits_) {
    return nullptr;
  }

  for (;;) {
    if (needNewSplit_) {
      exec::Split split;
      blockingReason_ = driverCtx_->task->getSplitOrFuture(
          driverCtx_->splitGroupId, planNodeId(), split, blockingFuture_);
      if (blockingReason_ != BlockingReason::kNotBlocked) {
        return nullptr;
      }

      if (!split.hasConnectorSplit()) {
        noMoreSplits_ = true;

        if (dataSource_) {
          auto connectorStats = dataSource_->runtimeStats();
          auto lockedStats = stats_.wlock();
          for (const auto& [name, counter] : connectorStats) {
            if (UNLIKELY(lockedStats->runtimeStats.count(name) == 0)) {
              lockedStats->runtimeStats.insert(
                  std::make_pair(name, RuntimeMetric(counter.unit)));
            } else {
              VELOX_CHECK_EQ(
                  lockedStats->runtimeStats.at(name).unit, counter.unit);
            }
            lockedStats->runtimeStats.at(name).addValue(counter.value);
          }
        }
        return nullptr;
      }

      const auto& connectorSplit = split.connectorSplit;
      needNewSplit_ = false;

      VELOX_CHECK_EQ(
          connector_->connectorId(),
          connectorSplit->connectorId,
          "Got splits with different connector IDs");

      if (!dataSource_) {
        connectorQueryCtx_ = operatorCtx_->createConnectorQueryCtx(
            connectorSplit->connectorId, planNodeId());
        dataSource_ = connector_->createDataSource(
            outputType_,
            tableHandle_,
            columnHandles_,
            connectorQueryCtx_.get());
        for (const auto& entry : pendingDynamicFilters_) {
          dataSource_->addDynamicFilter(entry.first, entry.second);
        }
        pendingDynamicFilters_.clear();
      }

      debugString_ = fmt::format(
          "Split {} Task {}",
          connectorSplit->toString(),
          operatorCtx_->task()->taskId());
      dataSource_->addSplit(connectorSplit);
      ++stats_.wlock()->numSplits;
      setBatchSize();
    }

    const auto ioTimeStartMicros = getCurrentTimeMicro();
    ExceptionContextSetter exceptionContext(
        {[](VeloxException::Type /*exceptionType*/, auto* debugString) {
           return *static_cast<std::string*>(debugString);
         },
         &debugString_});

    auto dataOptional = dataSource_->next(readBatchSize_, blockingFuture_);
    if (!dataOptional.has_value()) {
      blockingReason_ = BlockingReason::kWaitForConnector;
      return nullptr;
    }

    {
      auto lockedStats = stats_.wlock();
      lockedStats->addRuntimeStat(
          "dataSourceWallNanos",
          RuntimeCounter(
              (getCurrentTimeMicro() - ioTimeStartMicros) * 1'000,
              RuntimeCounter::Unit::kNanos));
      lockedStats->rawInputPositions = dataSource_->getCompletedRows();
      lockedStats->rawInputBytes = dataSource_->getCompletedBytes();
      auto data = dataOptional.value();
      if (data) {
        if (data->size() > 0) {
          lockedStats->inputPositions += data->size();
          lockedStats->inputBytes += data->retainedSize();
          return data;
        }
        continue;
      }
    }

    driverCtx_->task->splitFinished();
    needNewSplit_ = true;
  }
}

bool TableScan::isFinished() {
  return noMoreSplits_;
}

void TableScan::setBatchSize() {
  if (preferredBatchSize_ != 1024) {
    // Not the default value.
    readBatchSize_ = preferredBatchSize_;
    return;
  }
  constexpr int64_t kMB = 1 << 20;
  auto estimate = dataSource_->estimatedRowSize();
  if (estimate == connector::DataSource::kUnknownRowSize) {
    readBatchSize_ = kDefaultBatchSize;
    return;
  }
  if (estimate < 1024) {
    readBatchSize_ = 10000; // No more than 10MB of data per batch.
    return;
  }
  readBatchSize_ = std::min<int64_t>(100, 10 * kMB / estimate);
}

void TableScan::addDynamicFilter(
    column_index_t outputChannel,
    const std::shared_ptr<common::Filter>& filter) {
  if (dataSource_) {
    dataSource_->addDynamicFilter(outputChannel, filter);
  } else {
    pendingDynamicFilters_.emplace(outputChannel, filter);
  }
}

} // namespace facebook::velox::exec
