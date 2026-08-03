import http from 'k6/http';
import { check } from 'k6';

export const options = {
  vus: 50,
  iterations: 10000,
};

function generatePayload() {
  return JSON.stringify({
    city: ["Mumbai", "Delhi", "Bangalore", "Pune"][Math.floor(Math.random() * 4)],
    state: ["Maharashtra", "Delhi", "Karnataka", "Maharashtra"][Math.floor(Math.random() * 4)],
    pinCode: Math.floor(100000 + Math.random() * 899999),
    address: `Street ${Math.floor(Math.random() * 100)}`,
    societyName: `Society ${Math.floor(Math.random() * 100)}`,
    locality: `Locality ${Math.floor(Math.random() * 100)}`,
    cityTier: ["TIER_1","TIER_2","TIER_3"][Math.floor(Math.random() * 3)],
    zone: ["North", "South", "East", "West"][Math.floor(Math.random() * 4)],
    landmark: "Near Station",
    latitudeDegrees: 18.0 + Math.random() * 10,
    longitudeDegrees: 72.0 + Math.random() * 10,
    priceCrore: parseFloat((Math.random() * 20 + 1).toFixed(2)),
    isNegotiable: Math.random() > 0.5,
    priceRsPerSqft: Math.floor(5000 + Math.random() * 20000),
    maintenanceMonthlyRs: Math.floor(2000 + Math.random() * 10000),
    maintenanceCycleMonths: 12,
    propertyTaxYearlyRs: Math.floor(10000 + Math.random() * 50000),
    stampDutyEstRs: Math.floor(500000 + Math.random() * 2000000),
    registrationFeesEstRs: Math.floor(30000 + Math.random() * 100000),
    bookingAmountRs: Math.floor(100000 + Math.random() * 500000),
    expectedRentalYieldPercent: parseFloat((2 + Math.random() * 5).toFixed(2)),
    bhk: Math.floor(1 + Math.random() * 5),
    propertyType: ["APARTMENT","VILLA","INDEPENDENT_HOUSE","BUILDER_FLOOR","PENTHOUSE","STUDIO_APARTMENT","DUPLEX","TRIPLEX","FARM_HOUSE","AGRICULTURAL_LAND","RESIDENTIAL_PLOT","COMMERCIAL_OFFICE","RETAIL_SHOP","SERVICED_APARTMENT","CO_WORKING_SPACE"][Math.floor(Math.random() * 15)],
    carpetAreaSqft: Math.floor(800 + Math.random() * 2000),
    bathrooms: Math.floor(1 + Math.random() * 4),
    balconies: Math.floor(Math.random() * 4),
    superBuiltUpAreaSqft: Math.floor(1000 + Math.random() * 2500),
    plotAreaSqft: Math.floor(1500 + Math.random() * 3000),
    facingDirection: ["NORTH","EAST","NORTH_EAST","SOUTH","WEST","NORTH_WEST","SOUTH_EAST","SOUTH_WEST"][Math.floor(Math.random() * 8)],
    furnishingStatus: ["UNFURNISHED","SEMI_FURNISHED","FULLY_FURNISHED","BARE_SHELL","WARM_SHELL","CUSTOM_FURNISHED"][Math.floor(Math.random() * 6)],
    flooringType: ["VITRIFIED","MARBLE","WOODEN","GRANITE","MOSAIC","CEMENT"][Math.floor(Math.random() * 6)],
    possessionStatus: ["READY_TO_MOVE","UNDER_CONSTRUCTION","NEW_LAUNCH"][Math.floor(Math.random() * 3)],
    ownershipType: ["FREEHOLD","LEASEHOLD","CO_OPERATIVE_SOCIETY","POWER_OF_ATTORNEY"][Math.floor(Math.random() * 4)],
    reraStatus: ["RERA_APPROVED","RERA_PENDING","NOT_APPLICABLE"][Math.floor(Math.random() * 3)],
    khataType: "A Khata",
    isDuplex: Math.random() > 0.8,
    isCornerPlot: Math.random() > 0.8,
    waterSupply: ["MUNICIPAL","BOREWELL","MUNICIPAL_AND_BOREWELL","WATER_TANKER","RO_PLANT","TREATED_WATER","GRAM_PANCHAYAT"][Math.floor(Math.random() * 7)],
    powerBackup: ["NONE","PARTIAL","FULL_24X7"][Math.floor(Math.random() * 3)],
    coveredParkingSpots: Math.floor(Math.random() * 3),
    openParkingSpots: Math.floor(Math.random() * 2),
    hasEvCharging: Math.random() > 0.5,
    hasVisitorParking: Math.random() > 0.5,
    numberOfLifts: Math.floor(Math.random() * 4),
    serviceLifts: Math.floor(Math.random() * 2),
    hasStp: Math.random() > 0.5,
    hasRainwaterHarvesting: Math.random() > 0.5,
    hasSolarPanels: Math.random() > 0.5,
    hasPipedGas: Math.random() > 0.5,
    hasPoojaRoom: Math.random() > 0.5,
    hasServantRoom: Math.random() > 0.5,
    hasStudyRoom: Math.random() > 0.5,
    hasStoreRoom: Math.random() > 0.5,
    hasPrivateTerrace: Math.random() > 0.5,
    hasPrivateGarden: Math.random() > 0.5,
    hasBasement: Math.random() > 0.5,
    hasModularKitchen: Math.random() > 0.5,
    hasSwimmingPool: Math.random() > 0.5,
    hasGymnasium: Math.random() > 0.5,
    hasClubhouse: Math.random() > 0.5,
    hasKidsPlayArea: Math.random() > 0.5,
    hasJoggingTrack: Math.random() > 0.5,
    hasTennisCourt: Math.random() > 0.5,
    hasBadmintonCourt: Math.random() > 0.5,
    hasSquashCourt: Math.random() > 0.5,
    hasIndoorGames: Math.random() > 0.5,
    hasMiniTheatre: Math.random() > 0.5,
    hasPharmacy: Math.random() > 0.5,
    hasAtm: Math.random() > 0.5,
    hasGroceryStore: Math.random() > 0.5,
    hasClinic: Math.random() > 0.5,
    hasCreche: Math.random() > 0.5,
    hasSalon: Math.random() > 0.5,
    hasDogPark: Math.random() > 0.5,
    hasLibrary: Math.random() > 0.5,
    hasBanquetHall: Math.random() > 0.5,
    hasTemple: Math.random() > 0.5,
    has24x7Security: Math.random() > 0.5,
    hasCctv: Math.random() > 0.5,
    hasIntercom: Math.random() > 0.5,
    hasVideoDoorPhone: Math.random() > 0.5,
    hasBiometricAccess: Math.random() > 0.5,
    hasFireAlarm: Math.random() > 0.5,
    hasFireExtinguishers: Math.random() > 0.5,
    isGatedCommunity: Math.random() > 0.5,
    hasGuardRoom: Math.random() > 0.5,
    hasBoomBarrier: Math.random() > 0.5,
    distanceMetroKm: parseFloat((Math.random() * 10).toFixed(1)),
    distanceAirportKm: parseFloat((Math.random() * 30).toFixed(1)),
    distanceHospitalKm: parseFloat((Math.random() * 5).toFixed(1)),
    distanceSchoolKm: parseFloat((Math.random() * 5).toFixed(1)),
    distanceMallKm: parseFloat((Math.random() * 10).toFixed(1)),
    distanceBusStopKm: parseFloat((Math.random() * 2).toFixed(1)),
    walkabilityScore: parseFloat((Math.random() * 10).toFixed(1)),
    safetyScore: parseFloat((Math.random() * 10).toFixed(1)),
    livabilityScore: parseFloat((Math.random() * 10).toFixed(1)),
    greenCoverScore: parseFloat((Math.random() * 10).toFixed(1))
  });
}

function getRandomIP() {
  return `${Math.floor(Math.random() * 255)}.${Math.floor(Math.random() * 255)}.${Math.floor(Math.random() * 255)}.${Math.floor(Math.random() * 255)}`;
}

export default function () {
  const ip = getRandomIP();
  
  if (Math.random() < 0.8) {
    const res = http.post('http://gateway:3000/api/ingest', generatePayload(), { 
      headers: { 
        'Content-Type': 'application/json',
        'X-Forwarded-For': ip
      } 
    });
    check(res, { 'ingest status 201': (r) => r.status === 201 });
  } else {
    const res = http.get('http://gateway:3000/api/search?q=luxury+apartment&limit=5&page=1', {
      headers: { 'X-Forwarded-For': ip }
    });
    check(res, { 'search status 200': (r) => r.status === 200 });
  }
}
