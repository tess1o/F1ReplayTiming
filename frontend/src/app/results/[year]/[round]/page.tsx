"use client";

import { useParams, useSearchParams } from "react-router-dom";
import { useApi } from "@/hooks/useApi";
import SessionBanner from "@/components/SessionBanner";

interface RaceResult {
  position: number | null;
  driver: string;
  abbreviation: string;
  team: string;
  team_color: string;
  grid_position: number | null;
  status: string;
  points: number;
  fastest_lap: string | null;
  gap_to_leader: string | null;
}

interface ResultsResponse {
  results: RaceResult[];
}

interface SessionData {
  year: number;
  round_number: number;
  event_name: string;
  circuit: string;
  country: string;
  session_type: string;
  drivers: unknown[];
}

export default function ResultsPage() {
  const params = useParams<{ year: string; round: string }>();
  const [searchParams] = useSearchParams();
  const year = Number(params.year);
  const round = Number(params.round);
  const sessionType = searchParams.get("type") || "R";

  const { data: sessionData } = useApi<SessionData>(
    `/api/sessions/${year}/${round}?type=${sessionType}`,
  );

  const { data: resultsData, loading } = useApi<ResultsResponse>(
    `/api/sessions/${year}/${round}/results?type=${sessionType}`,
  );

  const results = resultsData?.results || [];

  return (
    <div className="min-h-screen bg-f1-dark">
      {sessionData && (
        <SessionBanner
          eventName={sessionData.event_name}
          circuit={sessionData.circuit}
          country={sessionData.country}
          sessionType={sessionType}
          year={year}
        />
      )}

      <div className="max-w-5xl mx-auto px-6 py-8">
        <div className="flex items-center justify-between mb-6">
          <h2 className="text-xl font-bold text-white">Race Results</h2>
          <a
            href={`/replay/${year}/${round}?type=${sessionType}`}
            className="px-4 py-2 bg-f1-red text-white text-sm font-bold rounded hover:bg-red-700 transition-colors"
          >
            Watch Replay
          </a>
        </div>

        {loading ? (
          <div className="text-center py-20 text-f1-muted">Loading results...</div>
        ) : (
          <div className="bg-f1-card rounded-xl border border-f1-border overflow-hidden">
            <table className="w-full">
              <thead>
                <tr className="border-b border-f1-border text-f1-muted text-xs uppercase tracking-wider">
                  <th className="px-4 py-3 text-left">Pos</th>
                  <th className="px-4 py-3 text-left">Driver</th>
                  <th className="px-4 py-3 text-left">Team</th>
                  <th className="px-4 py-3 text-center">Grid</th>
                  <th className="px-4 py-3 text-center">+/-</th>
                  <th className="px-4 py-3 text-left">Status</th>
                  <th className="px-4 py-3 text-right">Points</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-f1-border">
                {results.map((r) => {
                  const gained =
                    r.grid_position && r.position
                      ? r.grid_position - r.position
                      : null;
                  return (
                    <tr key={r.abbreviation} className="hover:bg-white/5">
                      <td className="px-4 py-3 font-bold text-white">
                        {r.position ?? "-"}
                      </td>
                      <td className="px-4 py-3">
                        <div className="flex items-center gap-2">
                          <span
                            className="w-1 h-5 rounded-full"
                            style={{ backgroundColor: r.team_color }}
                          />
                          <span className="font-bold text-white">
                            {r.abbreviation}
                          </span>
                          <span className="text-f1-muted text-sm">
                            {r.driver}
                          </span>
                        </div>
                      </td>
                      <td className="px-4 py-3 text-sm text-f1-muted">
                        {r.team}
                      </td>
                      <td className="px-4 py-3 text-center text-sm text-f1-muted">
                        {r.grid_position ?? "-"}
                      </td>
                      <td className="px-4 py-3 text-center text-sm font-bold">
                        {gained === null ? (
                          "-"
                        ) : gained > 0 ? (
                          <span className="text-green-400">+{gained}</span>
                        ) : gained < 0 ? (
                          <span className="text-red-400">{gained}</span>
                        ) : (
                          <span className="text-f1-muted">0</span>
                        )}
                      </td>
                      <td className="px-4 py-3 text-sm text-f1-muted">
                        {r.status}
                      </td>
                      <td className="px-4 py-3 text-right font-bold text-white">
                        {r.points}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}

        <div className="mt-6 text-center">
          <a href="/" className="text-f1-muted hover:text-white transition-colors text-sm">
            Back to session picker
          </a>
        </div>
      </div>
    </div>
  );
}
