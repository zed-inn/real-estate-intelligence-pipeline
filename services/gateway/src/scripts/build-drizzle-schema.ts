import { RealEstateListingSchema } from "@/gen/real-estate/real-estate-listing_pb";
import { info, validation } from "@/gen/real-estate/metadata_pb";
import { getOption, hasOption, ScalarType } from "@bufbuild/protobuf";
import { FeatureSet_FieldPresence } from "@bufbuild/protobuf/wkt";
import { appendFileSync, mkdirSync, writeFileSync } from "fs";
import { dirname, resolve } from "path";
import { fileURLToPath } from "url";

function createRealEstateListingDatabaseSchema() {
  const schemas: Record<string, {type: string, definition: string, isEnum?: boolean, enumValues?: string[], enumName?: string}> = {};

  schemas["id"] = { type: "uuid", definition: "uuid('id').default(sql`uuidv7()`).primaryKey()" };

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

    let definition: string = "";
    let isEnum = false;
    let enumValues: string[] | undefined;
    let enumName: string | undefined;

    if (metadata?.searchable) {
      switch (field.fieldKind) {
        case "scalar":
          switch (field.scalar) {
            case ScalarType.STRING:
              definition = `text('${field.name}')`;
              break;

            case ScalarType.DOUBLE:
              definition = `numeric('${field.name}', {mode: "number"})`;
              break;

            case ScalarType.BOOL:
              definition = `boolean('${field.name}')`;
              break;

            case ScalarType.UINT32:
              definition = `integer('${field.name}')`;
              break;
          }
          break;

        case "enum":
          isEnum = true;
          enumName = field.name;
          enumValues = field.enum.values.map((x) => x.name);
          definition = `pgEnum('${field.name}', [${field.enum.values.map((x) => '"' + x.name + '"')}])('${field.name}')`;
          break;

        case "list":
        case "message":
        case "map":
          break;
      }

      if (!isOptional) definition += ".notNull()";

      schemas[field.localName] = { type: "field", definition, isEnum, enumValues, enumName };
    }
  }

  schemas["rawFeatures"] = { type: "jsonb", definition: "jsonb('raw_features').notNull()" };
  schemas["intelligenceContext"] = { type: "text", definition: "text('intelligence_context')" };
  schemas["embedding"] = { type: "vector", definition: "vector('embedding', { dimensions: 384 })" };
  schemas["createdAt"] = { type: "timestamp", definition: "timestamp('created_at').defaultNow().notNull()" };

  return schemas;
}

function writeToFile(schema: Record<string, {type: string, definition: string, isEnum?: boolean, enumValues?: string[], enumName?: string}>, filename: string) {
  const __dirname = dirname(fileURLToPath(import.meta.url));
  const absDirname = resolve(`${__dirname}/../gen/drizzle/`);
  mkdirSync(absDirname, { recursive: true });

  const absFilename = resolve(absDirname + "/" + filename);

  writeFileSync(absFilename, "");
  appendFileSync(
    absFilename,
    `import { pgTable, pgEnum, uuid, varchar, timestamp, jsonb, boolean, integer, numeric, text, index } from "drizzle-orm/pg-core";\nimport { vector } from 'drizzle-orm/pg-core';\nimport { sql } from 'drizzle-orm';\n\n`,
  );

  for (const [fieldName, fieldObj] of Object.entries(schema)) {
    if (fieldObj.isEnum && fieldObj.enumName && fieldObj.enumValues) {
      appendFileSync(absFilename, `export const ${fieldObj.enumName}Enum = pgEnum('${fieldObj.enumName}', [${fieldObj.enumValues.map(x => '"'+x+'"').join(", ")}]);\n`);
    }
  }

  appendFileSync(
    absFilename,
    `\n\nexport const realEstateListings = pgTable('real_estate_listings', {\n`,
  );

  for (const [fieldName, fieldObj] of Object.entries(schema)) {
    if (fieldObj.isEnum && fieldObj.enumName) {
      appendFileSync(absFilename, `${fieldName}: ${fieldObj.enumName}Enum('${fieldObj.enumName}'),\n`);
    } else {
      appendFileSync(absFilename, `${fieldName}: ${fieldObj.definition},\n`);
    }
  }

  appendFileSync(absFilename, `}, (table) => [\n  index('embedding_index').using('hnsw', table.embedding.op('vector_cosine_ops')),\n  index('fts_idx').using('gin', sql\`to_tsvector('english', \${table.intelligenceContext})\`)\n])\n\n`);
}

const FILENAME = "real-estate-drizzle-schema.ts";

writeToFile(createRealEstateListingDatabaseSchema(), FILENAME);
