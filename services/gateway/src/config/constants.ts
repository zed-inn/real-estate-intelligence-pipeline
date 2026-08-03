export const EVENT_RELAY_BATCH_SIZE = 50;

export const KAFKA_TOPICS = {
  REAL_ESTATE_LISTING_INGESTED: 'real_estate.listing.ingested',
  REAL_ESTATE_LISTING_EMBEDDED: 'real_estate.listing.embedded',
  REAL_ESTATE_LISTING_DLQ: 'real_estate.listing.dlq',
} as const;
