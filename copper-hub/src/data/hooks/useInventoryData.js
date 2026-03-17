import {
  staticComexData, staticComexHistory,
  staticLmeData, staticLmeHistory,
  staticShfeData, staticShfeHistory,
} from '../static/inventories'

export function useInventoryData() {
  // PHASE 1: static data
  return {
    comex: staticComexData,
    comexHistory: staticComexHistory,
    lme: staticLmeData,
    lmeHistory: staticLmeHistory,
    shfe: staticShfeData,
    shfeHistory: staticShfeHistory,
    loading: false,
    error: null,
  }

  // PHASE 2 (Google Sheets): fetch COMEX, LME, SHFE tabs separately
}
