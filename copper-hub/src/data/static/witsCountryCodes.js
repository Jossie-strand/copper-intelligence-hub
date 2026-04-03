// Maps WITS/Comtrade numeric country codes → ISO 3166-1 alpha-3 codes
// NOTE: Comtrade uses several codes that differ from ISO 3166-1 numeric:
//   251 = France (includes overseas departments, ISO=250)
//   699 = India (ISO=356)
//   757 = Switzerland (includes Liechtenstein, ISO=756)
//   842 = USA (ISO=840)
export const WITS_TO_ISO3 = {
  4: 'AFG', 8: 'ALB', 12: 'DZA', 24: 'AGO', 32: 'ARG',
  36: 'AUS', 40: 'AUT', 44: 'BHS', 48: 'BHR', 31: 'AZE',
  50: 'BGD', 51: 'ARM', 56: 'BEL', 64: 'BTN', 68: 'BOL',
  70: 'BIH', 72: 'BWA', 76: 'BRA', 100: 'BGR', 104: 'MMR',
  108: 'BDI', 112: 'BLR', 116: 'KHM', 120: 'CMR', 124: 'CAN',
  132: 'CPV', 140: 'CAF', 144: 'LKA', 148: 'TCD', 152: 'CHL',
  156: 'CHN', 170: 'COL', 178: 'COG', 180: 'COD', 188: 'CRI',
  191: 'HRV', 192: 'CUB', 196: 'CYP', 203: 'CZE', 208: 'DNK',
  214: 'DOM', 218: 'ECU', 222: 'SLV', 231: 'ETH', 233: 'EST',
  246: 'FIN',
  // Comtrade-specific: France (250=ISO, 251=Comtrade incl. overseas depts)
  250: 'FRA', 251: 'FRA',
  266: 'GAB', 268: 'GEO', 276: 'DEU', 288: 'GHA', 300: 'GRC',
  308: 'GRD', 320: 'GTM', 328: 'GUY', 332: 'HTI', 340: 'HND',
  344: 'HKG', 348: 'HUN', 352: 'ISL', 356: 'IND', 360: 'IDN',
  364: 'IRN', 368: 'IRQ', 372: 'IRL', 376: 'ISR', 380: 'ITA',
  384: 'CIV', 388: 'JAM', 392: 'JPN', 398: 'KAZ', 400: 'JOR',
  404: 'KEN', 408: 'PRK', 410: 'KOR', 414: 'KWT', 418: 'LAO',
  422: 'LBN', 426: 'LSO', 428: 'LVA', 430: 'LBR', 440: 'LTU',
  442: 'LUX', 446: 'MAC', 454: 'MWI', 458: 'MYS', 466: 'MLI',
  470: 'MLT', 478: 'MRT', 480: 'MUS', 484: 'MEX', 496: 'MNG',
  504: 'MAR', 508: 'MOZ', 516: 'NAM', 524: 'NPL', 528: 'NLD',
  554: 'NZL', 558: 'NIC', 562: 'NER', 566: 'NGA', 578: 'NOR',
  586: 'PAK', 591: 'PAN', 598: 'PNG', 604: 'PER', 608: 'PHL',
  616: 'POL', 620: 'PRT', 630: 'PRI', 634: 'QAT', 642: 'ROU',
  643: 'RUS', 682: 'SAU', 686: 'SEN', 688: 'SRB', 694: 'SLE',
  690: 'SYC', 702: 'SGP', 703: 'SVK', 704: 'VNM', 705: 'SVN',
  706: 'SOM', 710: 'ZAF', 716: 'ZWE', 724: 'ESP', 729: 'SDN',
  748: 'SWZ', 752: 'SWE',
  // Comtrade-specific: Switzerland (756=ISO, 757=Comtrade incl. Liechtenstein)
  756: 'CHE', 757: 'CHE',
  762: 'TJK', 764: 'THA', 768: 'TGO', 780: 'TTO', 784: 'ARE',
  788: 'TUN', 792: 'TUR', 800: 'UGA', 804: 'UKR', 807: 'MKD',
  818: 'EGY', 826: 'GBR',
  // Comtrade-specific: USA (840=ISO, 842=Comtrade)
  840: 'USA', 842: 'USA',
  // Comtrade-specific: India (356=ISO, 699=Comtrade)
  356: 'IND', 699: 'IND',
  834: 'TZA', 854: 'BFA', 858: 'URY', 860: 'UZB',
  862: 'VEN', 887: 'YEM', 894: 'ZMB',
}

// Reverse lookup: ISO A3 → the WITS/Comtrade numeric code actually used in the DB
// Where Comtrade differs from ISO, we prefer the Comtrade code so DB queries work.
const COMTRADE_OVERRIDES = {
  'FRA': '251', // Comtrade France
  'IND': '699', // Comtrade India
  'CHE': '757', // Comtrade Switzerland
  'USA': '842', // Comtrade USA
}

export const ISO3_TO_WITS = (() => {
  const map = {}
  for (const [wits, iso3] of Object.entries(WITS_TO_ISO3)) {
    if (!map[iso3]) map[iso3] = String(wits)
  }
  // Apply Comtrade overrides so DB queries use the right code
  Object.assign(map, COMTRADE_OVERRIDES)
  return map
})()
