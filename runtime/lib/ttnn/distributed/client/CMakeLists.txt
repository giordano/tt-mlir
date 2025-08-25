if (NOT TT_RUNTIME_ENABLE_TTNN)
  add_library(TTRuntimeTTNNDistributedClient INTERFACE)
  return()
endif()

add_library(TTRuntimeTTNNDistributedClient
  STATIC
  response_factory.cpp
  command_executor.cpp
)

set_target_properties(TTRuntimeTTNNDistributedClient PROPERTIES CXX_STANDARD 20)

target_include_directories(TTRuntimeTTNNDistributedClient PRIVATE
  ${PROJECT_SOURCE_DIR}/runtime/include
  ${PROJECT_BINARY_DIR}/include/ttmlir/Target/Common
)
target_include_directories(TTRuntimeTTNNDistributedClient SYSTEM PRIVATE "$<BUILD_INTERFACE:${TTMETAL_INCLUDE_DIRS}>")

add_dependencies(TTRuntimeTTNNDistributedClient FBS_GENERATION)
target_link_libraries(TTRuntimeTTNNDistributedClient PRIVATE TTRuntimeSocket TTRuntimeTypes TTRuntimeUtils TTRuntimeSysDesc TTRuntimeTTNN)


#
# Runtime Client Executable
#
add_executable(ttnn-runtime-client runtime_client.cpp)

set_target_properties(ttnn-runtime-client PROPERTIES CXX_STANDARD 20)

target_include_directories(ttnn-runtime-client PRIVATE
  ${PROJECT_SOURCE_DIR}/runtime/include
  ${PROJECT_BINARY_DIR}/include/ttmlir/Target/Common
)
target_include_directories(ttnn-runtime-client SYSTEM PRIVATE "$<BUILD_INTERFACE:${TTMETAL_INCLUDE_DIRS}>")

add_dependencies(ttnn-runtime-client FBS_GENERATION)

target_link_libraries(ttnn-runtime-client PRIVATE
  TTMLIRRuntime
  TTRuntimeTTNNDistributedClient
  TTNN_LIBRARY
  TTMETAL_LIBRARY
  DEVICE_LIBRARY
  TT_STL_LIBRARY
)
