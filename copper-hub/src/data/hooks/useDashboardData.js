import { staticDashboardData, latestDashboardRow } from '../static/dashboard'

export function useDashboardData() {
  // PHASE 1: static data
  return { data: staticDashboardData, latest: latestDashboardRow, loading: false, error: null }

  // PHASE 2 (Google Sheets): uncomment and remove above
  // const [data, setData] = useState(null)
  // const [loading, setLoading] = useState(true)
  // const [error, setError] = useState(null)
  // useEffect(() => {
  //   fetchGoogleSheet('Dashboard')
  //     .then(rows => { setData(rows); setLoading(false) })
  //     .catch(err => { setError(err.message); setLoading(false) })
  // }, [])
  // return { data, latest: data?.[data.length - 1], loading, error }
}
