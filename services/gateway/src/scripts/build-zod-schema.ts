import { getOption, hasOption, ScalarType } from "@bufbuild/protobuf";
import { RealEstateListingSchema } from "@/gen/real-estate/real-estate-listing_pb";
import { info, validation } from "@/gen/real-estate/metadata_pb";
import { FeatureSet_FieldPresence } from "@bufbuild/protobuf/wkt";
import { appendFileSync, mkdirSync, writeFileSync } from "fs";
import { fileURLToPath } from "url";
import { dirname, resolve } from "path";

function createRealEstateListingSchema() {
  const schemas: Record<string, { schema: string; searchable: boolean }> = {};

  for (const field of RealEstateListingSchema.fields) {
    const isOptional =
      field.presence === FeatureSet_FieldPresence.LEGACY_REQUIRED ||
      field.presence === FeatureSet_FieldPresence.EXPLICIT;

    const metadata = hasOption(field, info)
      ? getOption(field, info)
      : undefined;

    const rules = hasOption(field, validation)
      ? getOption(field, validation)
      : undefined;

    let schema = "";

    switch (field.fieldKind) {
      case "scalar":
        switch (field.scalar) {
          case ScalarType.BOOL:
            schema = "z.boolean()";
            break;

          case ScalarType.DOUBLE:
            schema = "z.float64()";
            break;

          case ScalarType.STRING:
            schema = "z.string()";
            break;

          case ScalarType.UINT32:
            schema = "z.int32().nonnegative()";
            break;
        }

        break;

      case "enum":
        schema = `z.enum([${field.enum.values.map((x) => '"' + x.name + '"')}])`;
        break;

      case "list":
      case "map":
      case "message":
        break;
    }

    if (rules && field.fieldKind !== "enum") {
      let { min, max } = rules;
      schema += min ? `.min(${min})` : "";
      schema += max ? `.max(${max})` : "";
    }

    schema += isOptional ? ".nullable().default(null)" : "";
    schema += metadata?.label ? `.describe("${metadata.label}")` : "";

    schemas[field.localName] = {
      schema,
      searchable: metadata?.searchable ?? false,
    };
  }

  return schemas;
}

function writeToFile(
  schema: ReturnType<typeof createRealEstateListingSchema>,
  filename: string,
) {
  const __dirname = dirname(fileURLToPath(import.meta.url));
  const absDirname = resolve(`${__dirname}/../gen/zod/`);
  mkdirSync(absDirname, { recursive: true });

  const absFilename = resolve(absDirname + "/" + filename);

  writeFileSync(absFilename, "");
  appendFileSync(absFilename, `import { z } from "zod";\n\n`);

  for (const [fieldName, schemaOpts] of Object.entries(schema))
    appendFileSync(absFilename, `const ${fieldName} = ${schemaOpts.schema};\n`);

  appendFileSync(
    absFilename,
    `\n\nexport const RealEstateListingSchema = z.object({${Object.keys(schema)}})`
  );

  appendFileSync(
    absFilename,
    `\n\nexport const RealEstateListingSearchableSchema = z.object({${Object.entries(
      schema,
    )
      .filter((x) => x[1].searchable)
      .map((x) => x[0])}})`
  );
}

const FILENAME = "real-estate-zod-schema.ts";

writeToFile(createRealEstateListingSchema(), FILENAME);
