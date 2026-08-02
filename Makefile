MAKEFILE_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))


generate_protos_nodejs:
	@echo "Generating Node.js protos..."
	@cd ${MAKEFILE_DIR}services/gateway && npx -y @bufbuild/buf generate ../../shared/proto --template buf.gen.yaml --include-imports
	@mkdir -p ${MAKEFILE_DIR}services/gateway/src/gen/real-estate
	@cp ${MAKEFILE_DIR}shared/proto/real-estate/real-estate-listing-section-startings.json ${MAKEFILE_DIR}services/gateway/src/gen/real-estate/real-estate-listing-section-startings.json
	@echo "Node.js protos generated successfully!"


generate_protos_python:
	@echo "Generating Python protos..."
	@cd ${MAKEFILE_DIR}services/engine && uv run buf generate
	@mkdir -p ${MAKEFILE_DIR}services/engine/src/gen/real_estate
	@cp ${MAKEFILE_DIR}shared/proto/real-estate/real-estate-listing-section-startings.json ${MAKEFILE_DIR}services/engine/src/gen/real_estate/real_estate_listing_section_startings.json
	@sed -i 's/import semantic_search_pb2/from . import semantic_search_pb as semantic_search_pb2/g' ${MAKEFILE_DIR}services/engine/src/gen/semantic_search/semantic_search_grpc.py
	@echo "Python protos generated successfully!"


generate_zod_schema_real_estate:
	@echo "Generating Zod Schema from Real Estate Schema..."
	@cd ${MAKEFILE_DIR}services/gateway && npx tsx src/scripts/build-zod-schema.ts
	@echo "Zod Schema generated successfully!"

generate_drizzle_schema_real_estate:
	@echo "Generating Drizzle Schema from Real Estate Schema..."
	@cd ${MAKEFILE_DIR}services/gateway && npx tsx src/scripts/build-drizzle-schema.ts
	@echo "Drizzle Schema generated successfully!"


all:
	@echo "Generating all protos..."
	make generate_protos_nodejs
	make generate_zod_schema_real_estate
	make generate_drizzle_schema_real_estate
	make generate_protos_python
	@echo "All protos generated successfully!"


clean:
	@echo "Cleaning all protos..."
	rm -rf ${MAKEFILE_DIR}services/gateway/src/gen
	rm -rf ${MAKEFILE_DIR}services/engine/src/gen
	@echo "All protos cleaned successfully!"