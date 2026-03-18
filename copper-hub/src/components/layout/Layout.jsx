import { Outlet } from 'react-router-dom'
import Sidebar from './Sidebar'
import TickerBar from '../ui/TickerBar'

export default function Layout() {
  return (
    <div className="flex h-screen bg-bg-primary overflow-hidden">
      <Sidebar />
      <main className="flex-1 ml-60 overflow-y-auto pb-10">
        <div className="p-6 min-h-full">
          <Outlet />
        </div>
      </main>
      <TickerBar />
    </div>
  )
}
