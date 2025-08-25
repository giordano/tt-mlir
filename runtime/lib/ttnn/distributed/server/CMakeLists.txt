if (NOT TT_RUNTIME_ENABLE_TTNN)
  add_library(TTRuntimeTTNNDistributedServer INTERFACE)
  return()
endif()

add_library(TTRuntimeTTNNDistributedServer
  STATIC
  command_factory.cpp
)

set_target_properties(TTRuntimeTTNNDistributedServer PROPERTIES CXX_STANDARD 20)

target_include_directories(TTRuntimeTTNNDistributedServer PRIVATE
  ${PROJECT_SOURCE_DIR}/runtime/include
  ${PROJECT_BINARY_DIR}/include/ttmlir/Target/Common
)
target_include_directories(TTRuntimeTTNNDistributedServer SYSTEM PRIVATE "$<BUILD_INTERFACE:${TTMETAL_INCLUDE_DIRS}>")

add_dependencies(TTRuntimeTTNNDistributedServer FBS_GENERATION)
target_link_libraries(TTRuntimeTTNNDistributedServer PRIVATE TTBinary TTRuntimeTypes TTRuntimeTTNN flatbuffers)
