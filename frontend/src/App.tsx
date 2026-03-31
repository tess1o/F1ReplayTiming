import { Navigate, Route, Routes } from "react-router-dom";
import AuthGate from "@/components/AuthGate";
import HomePage from "@/app/page";
import DownloadsPage from "@/app/downloads/page";
import AboutPage from "@/app/about/page";
import FeaturesPage from "@/app/features/page";
import ReplayPage from "@/app/replay/[year]/[round]/page";
import LivePage from "@/app/live/[year]/[round]/page";
import ResultsPage from "@/app/results/[year]/[round]/page";

export default function App() {
  return (
    <div className="bg-f1-dark text-f1-text antialiased min-h-screen">
      <AuthGate>
        <Routes>
          <Route path="/" element={<HomePage />} />
          <Route path="/downloads" element={<DownloadsPage />} />
          <Route path="/about" element={<AboutPage />} />
          <Route path="/features" element={<FeaturesPage />} />
          <Route path="/replay/:year/:round" element={<ReplayPage />} />
          <Route path="/live/:year/:round" element={<LivePage />} />
          <Route path="/results/:year/:round" element={<ResultsPage />} />
          <Route path="*" element={<Navigate to="/" replace />} />
        </Routes>
      </AuthGate>
    </div>
  );
}
