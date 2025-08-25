// SPDX-FileCopyrightText: (c) 2025 Tenstorrent AI ULC
//
// SPDX-License-Identifier: Apache-2.0

#ifndef TT_RUNTIME_DETAIL_TTNN_DISTRIBUTED_CLIENT_COMMAND_EXECUTOR_H
#define TT_RUNTIME_DETAIL_TTNN_DISTRIBUTED_CLIENT_COMMAND_EXECUTOR_H

#include "tt/runtime/detail/common/socket.h"
#include "tt/runtime/detail/ttnn/distributed/types/spsc_queue.h"
#include "tt/runtime/detail/ttnn/types/types.h"
#include "ttmlir/Target/TTNN/Target.h"

namespace tt::runtime::ttnn::distributed::client {

class CommandExecutor {
public:
  CommandExecutor() = default;
  ~CommandExecutor() = default;

  CommandExecutor(const CommandExecutor &) = delete;
  CommandExecutor &operator=(const CommandExecutor &) = delete;
  CommandExecutor(CommandExecutor &&) = delete;
  CommandExecutor &operator=(CommandExecutor &&) = delete;

  void connect(const std::string &host, uint16_t port);

  void run();

private:
  std::atomic<bool> shutdownRequested_{false};
  std::unique_ptr<ClientSocket> clientSocket_;
  SPSCQueue<MessageBuffer> commandQueue_;
  std::thread commandReceiverThread_;
  std::unordered_map<uint32_t, ::tt::runtime::Device> devicePool_;
  std::unordered_map<uint64_t, ::tt::runtime::Binary> binaryPool_;
  std::unordered_map<uint64_t, ::tt::runtime::Tensor> tensorPool_;

  void launchCommandReceiver();
  void receiveCommands();

  void
  execute(uint64_t commandId,
          const ::tt::target::ttnn::distributed::GetSystemDescCommand *command);
  void execute(
      uint64_t commandId,
      const ::tt::target::ttnn::distributed::OpenMeshDeviceCommand *command);
  void execute(
      uint64_t commandId,
      const ::tt::target::ttnn::distributed::CloseMeshDeviceCommand *command);
  void execute(
      uint64_t commandId,
      const ::tt::target::ttnn::distributed::CreateHostTensorCommand *command);
  void execute(uint64_t commandId,
               const ::tt::target::ttnn::distributed::ToLayoutCommand *command);
  void execute(uint64_t commandId,
               const ::tt::target::ttnn::distributed::SubmitCommand *command);
  void execute(uint64_t commandId,
               const ::tt::target::ttnn::distributed::ToHostCommand *command);
  void execute(uint64_t commandId,
               const ::tt::target::ttnn::distributed::MemcpyCommand *command);
  void execute(uint64_t commandId,
               const ::tt::target::ttnn::distributed::ShutdownCommand *command);

  void executeCommand(const ::tt::target::ttnn::distributed::Command *command);

  void sendResponse(::flatbuffers::FlatBufferBuilder &responseBuilder);
  void handleShutdown();
};

} // namespace tt::runtime::ttnn::distributed::client
#endif // TT_RUNTIME_DETAIL_TTNN_DISTRIBUTED_CLIENT_COMMAND_EXECUTOR_H
