import { staticMinesData, minesSummary } from '../static/mines'

export function useMinesData() {
  // PHASE 1: static data
  return { mines: staticMinesData, summary: minesSummary, loading: false, error: null }

  // PHASE 2: connect to live mine production database
}
