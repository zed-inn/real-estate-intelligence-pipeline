#!/bin/bash
set -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR="$DIR/.."

echo "Generating Node.js Protobuf schemas..."
cd "$ROOT_DIR/services/gateway"
npx protoc \
  --plugin=protoc-gen-ts_proto=./node_modules/.bin/protoc-gen-ts_proto \
  --ts_proto_out=./src/generated \
  --ts_proto_opt=outputServices=grpc-js,env=node,useOptionals=all \
  -I="$ROOT_DIR/shared/proto" \
  "$ROOT_DIR/shared/proto/"*.proto
echo "Node.js Protobuf schemas generated successfully!"

echo "----------------------------------------"

echo "Generating Python Protobuf schemas..."
cd "$ROOT_DIR/services/engine"
uv run python -m grpc_tools.protoc \
  -I "$ROOT_DIR/shared/proto" \
  --python_betterproto_out=./generated \
  "$ROOT_DIR/shared/proto/"*.proto
echo "Python Protobuf schemas generated successfully!"

echo "----------------------------------------"
echo "All schemas generated and synced!"
