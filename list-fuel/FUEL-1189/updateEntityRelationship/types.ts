/**
 * @file types.ts
 * @description This file defines the types and interfaces used for searching organizations.
 * @module updateEntityRelationship/types
 */

/**
 * Represents the response to a search query for entities.
 *
 * @typedef {Object} GetEntitiesResponse
 *
 * @property {EntityTypesAggregate[]} data - The array of search responses.
 * @property {string} result - The result message of the search query.
 * @property {boolean} success - Indicates whether the search was successful.
 *
 * @example
 * {
 *   data: [...],
 *   result: 'Organization found.',
 *   success: true
 * }
 */

export type UpdateEntityRelationshipResponse = {
  /**
   * The response to the search query.
   * @example result: 'Organization found.'
   * @example success: true
   * @type {string}
   */
  success: boolean;
  result: AttributeUpdateResult[];
};

export type EntityTypesAggregate = {
  labels?: EntityLabels[] | null;
  mostRelevantLabel: string;
  _profileComplete?: boolean | null;
  allowAccolades?: boolean | null;
  altDescription?: string | null;
  cars?: string | null;
  ceoCompensations?: number | null;
  ceoName?: string | null;
  ceoTitle?: string | null;
  cfoName?: string | null;
  championships?: string | null;
  changeBy: string | null;
  changeDate: number | null;
  city?: string | null;
  coachImageUrl?: string | null;
  coachName?: string | null;
  collegeMediaWebsite?: string | null;
  country?: string | null;
  crewChiefImageUrl?: string | null;
  crewChiefName?: string | null;
  dailyContentCount?: bigint | null;
  daylifeId?: string | null;
  description?: string | null;
  email?: string | null;
  embargo?: boolean | null;
  employees?: bigint | null;
  exchange?: string | null;
  fuelId?: string | null;
  geoLocationLatitude?: number | null;
  geoLocationLongitude?: number | null;
  image?: string | null;
  industries?: string | null;
  industry?: string | null;
  keyPlayerImageUrl?: string | null;
  keyPlayerName?: string | null;
  landscapeImage?: string | null;
  latestContentDate?: bigint | null;
  latitude?: number | null;
  league?: string | null;
  longitude?: number | null;
  manufacturer?: string | null;
  name?: string | null;
  naturalId?: string | null;
  parentOrganizationNaturalId?: string | null;
  partnerBrandSlug?: string | null;
  phoneNumber?: string | null;
  placeUri?: string | null;
  portraitImage?: string | null;
  premiumProfile?: boolean | null;
  quantalyticsBasket?: string | null;
  quantalyticsExchange?: string | null;
  quantalyticsFactorScore?: string | null;
  quantalyticsGrowth?: string | null;
  quantalyticsMomentumVolatility?: string | null;
  quantalyticsQualityValue?: string | null;
  quantalyticsTechnical?: string | null;
  quantalyticsTicker?: string | null;
  quantalyticsTimestamp?: bigint | null;
  recentContentCount?: bigint | null;
  relatedVisible?: boolean | null;
  squareImage?: string | null;
  state?: string | null;
  stateCode?: string | null;
  street?: string | null;
  street2?: string | null;
  street3?: string | null;
  subType?: string | null;
  ticker?: string | null;
  typeLabel?: string[] | null;
  typeSlug?: string[] | null;
  uri?: string | null;
  uris?: string | null;
  venueAvgTicketPrice?: number | null;
  venueCapacity?: bigint | null;
  venueConcessionaire?: string | null;
  venueCostToBuild?: number | null;
  venueImageUrl?: string | null;
  venueName?: string | null;
  venueOwner?: string | null;
  venueRenovated?: boolean | null;
  venueYearOpened?: bigint | null;
  visible?: boolean | null;
  webSite?: string | null;
  yearFounded?: bigint | null;
  youTubePlayList?: string | null;
  zipCode?: string | null;
  companiesHasRelatedEntities?: any[];
};

enum EntityLabels {
  College,
  Company,
  Organization,
  Person,
  Team
}

export type UpdateEntityRelationshipInput = {
  sourceFuelId: string;
  relationshipTag: string;
  targetFuelId: string;
  attributes?: AttributeUpdate[];
};

export type AttributeUpdate = {
  attributeName: string
  updateRootEntity: boolean
  removeAttribute: boolean
  attributeValue: any
};

export type AttributeUpdateResult = {
  attributeName: string;
  result: string;
};
