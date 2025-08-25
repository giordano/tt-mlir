// SPDX-FileCopyrightText: (c) 2025 Tenstorrent AI ULC
//
// SPDX-License-Identifier: Apache-2.0

#include "tt/runtime/detail/ttnn/distributed/client/command_executor.h"
#include "tt/runtime/detail/common/common.h"
#include "tt/runtime/detail/common/logger.h"
#include "tt/runtime/detail/common/socket.h"
#include "tt/runtime/detail/common/system_desc.h"
#include "tt/runtime/detail/ttnn/distributed/client/response_factory.h"
#include "tt/runtime/detail/ttnn/ttnn.h"
#include "tt/runtime/detail/ttnn/types/types.h"
#include "tt/runtime/detail/ttnn/utils.h"
#include "tt/runtime/types.h"
#include "tt/runtime/utils.h"
#include <thread>

namespace tt::runtime::ttnn::distributed::client {

static const ::tt::target::ttnn::distributed::Command *
getCommand(const MessageBuffer &command) {
  bool isTTNNCommand =
      ::tt::target::ttnn::distributed::CommandBufferHasIdentifier(
          command.data());
  LOG_ASSERT(isTTNNCommand, "Command is not a TTNN command");
  return ::tt::target::ttnn::distributed::GetCommand(command.data());
}

void CommandExecutor::connect(const std::string &host, uint16_t port) {
  LOG_ASSERT(!clientSocket_, "ClientSocket already connected");
  clientSocket_ = std::make_unique<ClientSocket>(host, port);
}

void CommandExecutor::run() {
  launchCommandReceiver();
  while (!shutdownRequested_.load(std::memory_order_relaxed)) {
    MessageBuffer commandData = commandQueue_.popBlocking();
    const ::tt::target::ttnn::distributed::Command *command =
        getCommand(commandData);

    executeCommand(command);
  }
}

void CommandExecutor::launchCommandReceiver() {
  LOG_ASSERT(!commandReceiverThread_.joinable(),
             "Command receiver thread already running");
  commandReceiverThread_ = std::thread([this]() { receiveCommands(); });
}

// This will get run on the command receiver thread
void CommandExecutor::receiveCommands() {
  while (true) {
    if (shutdownRequested_.load(std::memory_order_relaxed)) {
      break;
    }
    if (!clientSocket_->hasDataToRead()) {
      continue;
    }
    MessageBuffer commandData = clientSocket_->sizePrefixedRead();
    LOG_ASSERT(commandData.size(), "Read null command from client socket");
    commandQueue_.push(commandData);
  }
}

void CommandExecutor::execute(
    uint64_t commandId,
    const ::tt::target::ttnn::distributed::GetSystemDescCommand *command) {

  ::flatbuffers::FlatBufferBuilder responseBuilder;

  ::tt::runtime::DispatchCoreType dispatchCoreType =
      ::tt::runtime::utils::toRuntimeDispatchCoreType(
          command->dispatch_core_type());

  std::shared_ptr<::ttnn::MeshDevice> meshDevice;
  if (!command->device()) {
    meshDevice = ::tt::runtime::common::createFullMeshDevice(dispatchCoreType);
  } else {
    meshDevice = devicePool_.at(command->device()->global_id())
                     .asSharedPtr<::ttnn::MeshDevice>(DeviceRuntime::TTNN);
  }

  ::flatbuffers::Offset<tt::target::SystemDescRoot> systemDescRoot =
      ::tt::runtime::system_desc::buildSystemDescRoot(responseBuilder,
                                                      *meshDevice);

  ResponseFactory::buildGetSystemDescResponse(responseBuilder, commandId,
                                              systemDescRoot);

  sendResponse(responseBuilder);
}

void CommandExecutor::execute(
    uint64_t commandId,
    const ::tt::target::ttnn::distributed::OpenMeshDeviceCommand *command) {

  ::flatbuffers::FlatBufferBuilder responseBuilder;

  uint64_t deviceGlobalId = command->device_global_id();
  const ::tt::target::ttnn::distributed::MeshDeviceOptions *options =
      command->options();

  ::tt::runtime::MeshDeviceOptions meshDeviceOptions;
  if (options->mesh_offset()) {
    LOG_ASSERT(options->mesh_offset()->size() == 2,
               "Currently only 2D mesh offsets are supported");
    meshDeviceOptions.meshOffset = {options->mesh_offset()->Get(0),
                                    options->mesh_offset()->Get(1)};
  }

  if (options->device_ids()) {
    meshDeviceOptions.deviceIds.resize(options->device_ids()->size());
    std::copy_n(options->device_ids()->begin(), options->device_ids()->size(),
                meshDeviceOptions.deviceIds.begin());
  }

  meshDeviceOptions.numHWCQs = options->num_hw_cqs();
  meshDeviceOptions.enableProgramCache = options->enable_program_cache();

  if (options->mesh_shape()) {
    LOG_ASSERT(options->mesh_shape()->size() == 2,
               "Currently only 2D mesh shapes are supported");
    meshDeviceOptions.meshShape = {options->mesh_shape()->Get(0),
                                   options->mesh_shape()->Get(1)};
  }

  if (options->l1_small_size().has_value()) {
    meshDeviceOptions.l1SmallSize = options->l1_small_size().value();
  }

  if (options->trace_region_size().has_value()) {
    meshDeviceOptions.traceRegionSize = options->trace_region_size().value();
  }

  if (options->dispatch_core_type().has_value()) {
    meshDeviceOptions.dispatchCoreType =
        ::tt::runtime::utils::toRuntimeDispatchCoreType(
            options->dispatch_core_type().value());
  }

  ::tt::runtime::Device device =
      ::tt::runtime::ttnn::openMeshDevice(meshDeviceOptions);

  device.setGlobalId(deviceGlobalId);

  devicePool_.insert_or_assign(deviceGlobalId, device);

  ResponseFactory::buildOpenMeshDeviceResponse(responseBuilder, commandId,
                                               device);

  sendResponse(responseBuilder);
}

void CommandExecutor::execute(
    uint64_t commandId,
    const ::tt::target::ttnn::distributed::CloseMeshDeviceCommand *command) {

  ::flatbuffers::FlatBufferBuilder responseBuilder;

  uint64_t deviceGlobalId = command->device()->global_id();

  ::tt::runtime::Device device = devicePool_.at(deviceGlobalId);

  ::tt::runtime::ttnn::closeMeshDevice(device);

  devicePool_.erase(deviceGlobalId);

  ResponseFactory::buildCloseMeshDeviceResponse(responseBuilder, commandId);

  sendResponse(responseBuilder);
}

void CommandExecutor::execute(
    uint64_t commandId,
    const ::tt::target::ttnn::distributed::CreateHostTensorCommand *command) {

  ::flatbuffers::FlatBufferBuilder responseBuilder;

  uint64_t tensorGlobalId = command->output_global_id();
  const uint8_t *tensorData = command->data()->data();
  std::vector<uint32_t> shape(command->shape()->begin(),
                              command->shape()->end());
  std::vector<uint32_t> stride(command->stride()->begin(),
                               command->stride()->end());
  uint32_t itemSize = command->item_size();
  ::tt::target::DataType dataType = command->data_type();

  ::tt::runtime::Tensor tensor = ::tt::runtime::ttnn::createOwnedHostTensor(
      tensorData, shape, stride, itemSize, dataType);

  tensor.setGlobalId(tensorGlobalId);

  tensorPool_.insert_or_assign(tensorGlobalId, tensor);

  ResponseFactory::buildCreateHostTensorResponse(responseBuilder, commandId);

  sendResponse(responseBuilder);
}

void CommandExecutor::execute(
    uint64_t commandId,
    const ::tt::target::ttnn::distributed::ToLayoutCommand *command) {

  ::flatbuffers::FlatBufferBuilder responseBuilder;

  uint64_t inputGlobalId = command->input_global_id();
  uint64_t outputGlobalId = command->output_global_id();
  ::tt::runtime::Device device = devicePool_.at(command->device()->global_id());

  ::tt::runtime::Tensor inputTensor = tensorPool_.at(inputGlobalId);

  std::shared_ptr<::tt::runtime::ttnn::LayoutDesc> layoutDesc =
      ::tt::runtime::ttnn::LayoutDesc::fromMemoryDesc(command->memory_desc());
  ::tt::runtime::Layout layout(std::static_pointer_cast<void>(layoutDesc),
                               DeviceRuntime::TTNN);

  std::optional<bool> retain = std::nullopt;
  if (command->retain().has_value()) {
    retain = command->retain().value();
  }

  ::tt::runtime::Tensor resultTensor =
      ::tt::runtime::ttnn::toLayout(inputTensor, device, layout, retain);

  resultTensor.setGlobalId(outputGlobalId);

  tensorPool_.insert_or_assign(outputGlobalId, resultTensor);

  ResponseFactory::buildToLayoutResponse(responseBuilder, commandId);

  sendResponse(responseBuilder);
}

void CommandExecutor::execute(
    uint64_t commandId,
    const ::tt::target::ttnn::distributed::SubmitCommand *command) {

  ::flatbuffers::FlatBufferBuilder responseBuilder;

  ::tt::runtime::Device device = devicePool_.at(command->device()->global_id());

  std::vector<::tt::runtime::Tensor> inputTensors;
  for (const auto &inputGlobalId : *command->input_global_ids()) {
    inputTensors.push_back(tensorPool_.at(inputGlobalId));
  }

  ::tt::runtime::Binary executable(nullptr);

  if (binaryPool_.contains(command->binary_id())) {
    executable = binaryPool_.at(command->binary_id());
  } else {
    executable = ::tt::runtime::Binary::loadFromMemory(
        command->binary()->data(), command->binary()->size());
    executable.setId(command->binary_id());
    binaryPool_.insert_or_assign(command->binary_id(), executable);
  }

  std::vector<::tt::runtime::Tensor> outputTensors =
      ::tt::runtime::ttnn::submit(device, executable, command->program_id(),
                                  inputTensors);

  LOG_ASSERT(outputTensors.size() == command->output_global_ids()->size(),
             "Output tensors from submit does not match the number of output "
             "global ids");

  for (size_t i = 0; i < outputTensors.size(); i++) {
    outputTensors[i].setGlobalId(command->output_global_ids()->Get(i));
    tensorPool_.insert_or_assign(command->output_global_ids()->Get(i),
                                 outputTensors[i]);
  }

  ResponseFactory::buildSubmitResponse(responseBuilder, commandId);

  sendResponse(responseBuilder);
}

void CommandExecutor::execute(
    uint64_t commandId,
    const ::tt::target::ttnn::distributed::ToHostCommand *command) {

  ::flatbuffers::FlatBufferBuilder responseBuilder;

  uint64_t inputGlobalId = command->input_global_id();

  ::tt::runtime::Tensor inputTensor = tensorPool_.at(inputGlobalId);

  bool untilize = command->untilize();
  bool blocking = command->blocking();

  std::vector<::tt::runtime::Tensor> outputTensors =
      ::tt::runtime::ttnn::toHost(inputTensor, untilize, blocking);

  LOG_ASSERT(outputTensors.size() == command->output_global_ids()->size(),
             "Output tensors from toHost does not match the number of output "
             "global ids");

  for (size_t i = 0; i < outputTensors.size(); i++) {
    outputTensors[i].setGlobalId(command->output_global_ids()->Get(i));
    tensorPool_.insert_or_assign(command->output_global_ids()->Get(i),
                                 outputTensors[i]);
  }

  ResponseFactory::buildToHostResponse(responseBuilder, commandId);

  sendResponse(responseBuilder);
}

void CommandExecutor::execute(
    uint64_t commandId,
    const ::tt::target::ttnn::distributed::MemcpyCommand *command) {
  ::flatbuffers::FlatBufferBuilder responseBuilder;

  uint64_t srcGlobalId = command->src_global_id();
  ::tt::runtime::Tensor srcTensor = tensorPool_.at(srcGlobalId);

  std::optional<std::vector<std::uint8_t>> dataBuffer = std::nullopt;

  if (command->dst_global_id().has_value()) {
    ::tt::runtime::Tensor dstTensor =
        tensorPool_.at(command->dst_global_id().value());
    ::tt::runtime::ttnn::memcpy(dstTensor, srcTensor);
  } else {
    const ::ttnn::Tensor &ttnnTensor =
        ::tt::runtime::ttnn::utils::getTTNNTensorFromRuntimeTensor(srcTensor);
    size_t size = ttnnTensor.physical_volume() * ttnnTensor.element_size();
    dataBuffer = std::vector<std::uint8_t>(size);

    std::optional<::tt::target::DataType> dstDataType = std::nullopt;
    if (dstDataType.has_value()) {
      dstDataType = dstDataType.value();
    }

    ::tt::runtime::ttnn::memcpy(dataBuffer.value().data(), srcTensor,
                                dstDataType);
  }

  ResponseFactory::buildMemcpyResponse(responseBuilder, commandId, dataBuffer);

  sendResponse(responseBuilder);
}

void CommandExecutor::execute(
    uint64_t commandId,
    const ::tt::target::ttnn::distributed::ShutdownCommand *command) {

  shutdownRequested_.store(true, std::memory_order_relaxed);

  ::flatbuffers::FlatBufferBuilder responseBuilder;
  ResponseFactory::buildShutdownResponse(responseBuilder, commandId);

  sendResponse(responseBuilder);

  handleShutdown();
}

void CommandExecutor::executeCommand(
    const ::tt::target::ttnn::distributed::Command *command) {
  switch (command->type_type()) {
  case ::tt::target::ttnn::distributed::CommandType::GetSystemDescCommand: {
    return execute(command->command_id(),
                   command->type_as_GetSystemDescCommand());
  }
  case ::tt::target::ttnn::distributed::CommandType::OpenMeshDeviceCommand: {
    return execute(command->command_id(),
                   command->type_as_OpenMeshDeviceCommand());
  }
  case ::tt::target::ttnn::distributed::CommandType::CloseMeshDeviceCommand: {
    return execute(command->command_id(),
                   command->type_as_CloseMeshDeviceCommand());
  }
  case ::tt::target::ttnn::distributed::CommandType::CreateHostTensorCommand: {
    return execute(command->command_id(),
                   command->type_as_CreateHostTensorCommand());
  }
  case ::tt::target::ttnn::distributed::CommandType::ToLayoutCommand: {
    return execute(command->command_id(), command->type_as_ToLayoutCommand());
  }
  case ::tt::target::ttnn::distributed::CommandType::SubmitCommand: {
    return execute(command->command_id(), command->type_as_SubmitCommand());
  }
  case ::tt::target::ttnn::distributed::CommandType::ToHostCommand: {
    return execute(command->command_id(), command->type_as_ToHostCommand());
  }
  case ::tt::target::ttnn::distributed::CommandType::MemcpyCommand: {
    return execute(command->command_id(), command->type_as_MemcpyCommand());
  }
  case ::tt::target::ttnn::distributed::CommandType::ShutdownCommand: {
    return execute(command->command_id(), command->type_as_ShutdownCommand());
  }
  default: {
    LOG_FATAL("Unhandled command type: ",
              ::tt::target::ttnn::distributed::EnumNameCommandType(
                  command->type_type()));
  }
  }
}

void CommandExecutor::sendResponse(
    ::flatbuffers::FlatBufferBuilder &responseBuilder) {
  LOG_ASSERT(responseBuilder.GetSize() > 0,
             "Expected response from command execution");
  size_t responseSize = responseBuilder.GetSize();
  clientSocket_->sizePrefixedWrite(responseBuilder.GetBufferPointer(),
                                   responseSize);
}

void CommandExecutor::handleShutdown() {
  LOG_INFO("Shutdown command received, shutting down command executor");
  commandReceiverThread_.join();
}

} // namespace tt::runtime::ttnn::distributed::client
