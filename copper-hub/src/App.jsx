import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom'
import Layout from './components/layout/Layout'
import Dashboard from './pages/Dashboard'
import Inventories from './pages/Inventories'
import Pricing from './pages/Pricing'
import GlobalMines from './pages/GlobalMines'
import SupplyDemand from './pages/SupplyDemand'
import Disruptions from './pages/Disruptions'
import TradeFlows from './pages/TradeFlows'

export default function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<Layout />}>
          <Route index element={<Navigate to="/dashboard" replace />} />
          <Route path="dashboard" element={<Dashboard />} />
          <Route path="inventories" element={<Inventories />} />
          <Route path="pricing" element={<Pricing />} />
          <Route path="mines" element={<GlobalMines />} />
          <Route path="supply-demand" element={<SupplyDemand />} />
          <Route path="trade-flows" element={<TradeFlows />} />
          <Route path="disruptions" element={<Disruptions />} />
        </Route>
      </Routes>
    </BrowserRouter>
  )
}
