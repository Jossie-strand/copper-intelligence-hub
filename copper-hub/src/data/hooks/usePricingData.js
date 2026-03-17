import {
  staticForwardCurve,
  staticPriceBenchmarks,
  staticPhysicalPremiums,
  staticTcrcHistory,
  TC_BEAR_THRESHOLD,
} from '../static/pricing'

export function usePricingData() {
  // PHASE 1: static data
  return {
    forwardCurve: staticForwardCurve,
    benchmarks: staticPriceBenchmarks,
    physicalPremiums: staticPhysicalPremiums,
    tcrcHistory: staticTcrcHistory,
    tcBearThreshold: TC_BEAR_THRESHOLD,
    loading: false,
    error: null,
  }

  // PHASE 2: Barchart API — symbols pending confirmation
}
