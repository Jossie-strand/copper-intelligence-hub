import {
  staticDisruptionsData,
  disruptionsSummary,
  DISRUPTION_BULL_THRESHOLD_KT,
} from '../static/disruptions'

export function useDisruptionsData() {
  // PHASE 1: static data
  return {
    disruptions: staticDisruptionsData,
    summary: disruptionsSummary,
    bullThresholdKt: DISRUPTION_BULL_THRESHOLD_KT,
    loading: false,
    error: null,
  }

  // PHASE 2: connect to live disruption tracking feed
}
