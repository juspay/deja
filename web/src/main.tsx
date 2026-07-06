import React from "react";
import ReactDOM from "react-dom/client";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import {
  createBrowserRouter,
  Link,
  Outlet,
  RouterProvider,
  useLocation,
} from "react-router-dom";

import "@fontsource/inter/latin-400.css";
import "@fontsource/inter/latin-500.css";
import "@fontsource/inter/latin-600.css";
import "json-diff-kit/dist/viewer.css";
import "./styles.css";
import { actor, setActor } from "./lib/api";
import RecordingsPage from "./pages/RecordingsPage";
import NewRunPage from "./pages/NewRunPage";
import RunsPage from "./pages/RunsPage";
import RunDetailPage from "./pages/RunDetailPage";
import ScorecardPage from "./pages/ScorecardPage";
import AuditPage from "./pages/AuditPage";

const queryClient = new QueryClient({
  defaultOptions: { queries: { refetchOnWindowFocus: false, retry: 1 } },
});

function ActorBox() {
  const [name, setName] = React.useState(actor());
  return (
    <input
      className="actor"
      placeholder="your name (audit actor)"
      value={name}
      onChange={(e) => {
        setName(e.target.value);
        setActor(e.target.value);
      }}
      title="Recorded as the audit actor on every action you take"
    />
  );
}

function Shell() {
  const loc = useLocation();
  const tab = (path: string, label: string) => (
    <Link className={loc.pathname.startsWith(path) ? "tab active" : "tab"} to={path}>
      {label}
    </Link>
  );
  return (
    <>
      <header>
        <span className="logo">déjà</span>
        <nav>
          {tab("/runs", "Runs")}
          {tab("/recordings", "Recordings")}
          {tab("/replays/new", "New run")}
          {tab("/audit", "Audit")}
        </nav>
        <ActorBox />
      </header>
      <main>
        <Outlet />
      </main>
    </>
  );
}

const router = createBrowserRouter([
  {
    element: <Shell />,
    children: [
      { path: "/", element: <RunsPage /> },
      { path: "/runs", element: <RunsPage /> },
      { path: "/runs/:runId", element: <RunDetailPage /> },
      { path: "/runs/:runId/scorecard", element: <ScorecardPage /> },
      { path: "/recordings", element: <RecordingsPage /> },
      { path: "/replays/new", element: <NewRunPage /> },
      { path: "/audit", element: <AuditPage /> },
    ],
  },
]);

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <QueryClientProvider client={queryClient}>
      <RouterProvider router={router} />
    </QueryClientProvider>
  </React.StrictMode>,
);
