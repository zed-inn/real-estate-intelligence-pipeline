MAKEFILE_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))


generate_protos_nodejs:
	@echo "Generating Node.js protos..."
	@cd ${MAKEFILE_DIR}services/gateway && npx -y @bufbuild/buf generate ../../shared/proto --template buf.gen.yaml --include-imports
	@cp ${MAKEFILE_DIR}shared/proto/property/property_section_startings.json ${MAKEFILE_DIR}services/gateway/src/gen/property/property_section_startings.json
	@echo "Node.js protos generated successfully!"


generate_protos_python:
	@echo "Generating Python protos..."
	@cd ${MAKEFILE_DIR}services/engine && uv run buf generate
	@cp ${MAKEFILE_DIR}shared/proto/property/property_section_startings.json ${MAKEFILE_DIR}services/engine/src/gen/property/property_section_startings.json
	@echo "Python protos generated successfully!"


generate_zod_schema_property:
	@echo "Generating Zod Schema from Property Schema..."
	@cd ${MAKEFILE_DIR}services/gateway && npx tsx src/scripts/generate-zod-schema.ts
	@echo "Zod Schema generated successfully!"

generate_drizzle_schema_property:
	@echo "Generating Drizzle Schema from Property Schema..."
	@cd ${MAKEFILE_DIR}services/gateway && npx tsx src/scripts/generate-drizzle-schema.ts
	@echo "Drizzle Schema generated successfully!"


all:
	@echo "Generating all protos..."
	make generate_protos_nodejs
	make generate_zod_schema_property
	make generate_drizzle_schema_property
	make generate_protos_python
	@echo "All protos generated successfully!"


clean:
	@echo "Cleaning all protos..."
	rm -rf ${MAKEFILE_DIR}services/gateway/src/gen
	rm -rf ${MAKEFILE_DIR}services/engine/src/gen
	@echo "All protos cleaned successfully!"