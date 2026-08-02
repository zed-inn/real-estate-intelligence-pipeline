import { info, PropertySchema, validation } from "@/gen/property/property_pb";
import { getOption, hasOption, ScalarType } from "@bufbuild/protobuf";
import { FeatureSet_FieldPresence } from "@bufbuild/protobuf/wkt";
import { appendFileSync, mkdirSync, writeFileSync } from "fs";
import { dirname, resolve } from "path";
import { fileURLToPath } from "url";

function createPropertyDatabaseSchema() {
  const schemas: Record<string, string> = {};

  schemas["id"] = "uuid('id').default(sql`uuidv7()`).primaryKey()";

  for (const field of PropertySchema.fields) {
    const isOptional =
      field.presence === FeatureSet_FieldPresence.LEGACY_REQUIRED ||
      field.presence === FeatureSet_FieldPresence.EXPLICIT;

    const metadata = hasOption(field, info)
      ? getOption(field, info)
      : undefined;

    const rules = hasOption(field, validation)
      ? getOption(field, validation)
      : undefined;

    let schema: string = "";

    if (metadata?.searchable) {
      switch (field.fieldKind) {
        case "scalar":
          switch (field.scalar) {
            case ScalarType.STRING:
              schema += `varchar('${field.name}'${rules?.max ? `, { length: ${rules.max} }` : ""})`;
              break;

            case ScalarType.DOUBLE:
              schema += `numeric('${field.name}', {mode: "number"})`;
              break;

            case ScalarType.BOOL:
              schema += `boolean('${field.name}')`;
              break;

            case ScalarType.UINT32:
              schema += `integer('${field.name}')`;
              break;
          }
          break;

        case "enum":
          schema += `pgEnum('${field.name}', [${field.enum.values.map((x) => '"' + x.name + '"')}])('${field.name}')`;
          break;

        case "list":
        case "message":
        case "map":
          break;
      }

      if (!isOptional) schema += ".notNull()";

      schemas[field.localName] = schema;
    }
  }

  schemas["rawFeatures"] = "jsonb('raw_features').notNull()";
  schemas["intelligenceContext"] = "text('intelligence_context')";
  schemas["embedding"] = "vector('embedding', { dimensions: 384 })";
  schemas["createdAt"] = "timestamp('created_at').defaultNow().notNull()";

  return schemas;
}

function writeToFile(schema: Record<string, string>, filename: string) {
  const __dirname = dirname(fileURLToPath(import.meta.url));
  const absDirname = resolve(`${__dirname}/../gen/drizzle/`);
  mkdirSync(absDirname, { recursive: true });

  const absFilename = resolve(absDirname + "/" + filename);

  writeFileSync(absFilename, "");
  appendFileSync(
    absFilename,
    `import { pgTable, pgEnum, uuid, varchar, timestamp, jsonb, boolean, integer, numeric, text } from "drizzle-orm/pg-core";\nimport { vector } from 'drizzle-orm/pg-core';\nimport { sql } from 'drizzle-orm';\n\n`,
  );

  appendFileSync(
    absFilename,
    `\n\nexport const properties = pgTable('properties', {\n`,
  );

  for (const [fieldName, drizzleSchema] of Object.entries(schema))
    appendFileSync(absFilename, `${fieldName}: ${drizzleSchema},\n`);

  appendFileSync(absFilename, `})\n\n`);
}

const FILENAME = "property_drizzle_schema.ts";

writeToFile(createPropertyDatabaseSchema(), FILENAME);
