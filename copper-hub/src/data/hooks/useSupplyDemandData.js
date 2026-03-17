import {
  staticSdBalance,
  staticCountryProduction,
  staticDemandSectors,
  staticChinaIndicators,
  sdNarrative,
} from '../static/supplyDemand'

export function useSupplyDemandData() {
  // PHASE 1: static data
  return {
    balance: staticSdBalance,
    countryProduction: staticCountryProduction,
    demandSectors: staticDemandSectors,
    chinaIndicators: staticChinaIndicators,
    narrative: sdNarrative,
    loading: false,
    error: null,
  }

  // PHASE 2: NBS data feed for China indicators
}
