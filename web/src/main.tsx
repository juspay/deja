import React from "react";
import ReactDOM from "react-dom/client";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import {
  createBrowserRouter,
  Link,
  Navigate,
  Outlet,
  RouterProvider,
  useLocation,
  useParams,
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
import ReportPage from "./pages/ReportPage";
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
  const tab = (path: string, label: string, exact = false) => {
    const on = exact ? loc.pathname === path : loc.pathname.startsWith(path);
    return (
      <Link className={on ? "tab active" : "tab"} to={path}>
        {label}
      </Link>
    );
  };
  return (
    <>
      <header>
        <Link className="logo" to="/">déjà</Link>
        <nav>
          {tab("/", "New run", true)}
          {tab("/runs", "Runs")}
          {tab("/recordings", "Recordings")}
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

/**
 * `/runs/:id` and `/runs/:id/scorecard` are the URLs already pasted into PR
 * comments and chat. They redirect to the report rather than 404-ing, and the
 * query string (`?debug=1`) survives the hop.
 */
function LegacyRunRedirect() {
  const { runId = "" } = useParams();
  const loc = useLocation();
  return <Navigate to={`/r/${runId}${loc.search}`} replace />;
}

const router = createBrowserRouter([
  {
    element: <Shell />,
    children: [
      // HOME is the form. The list is a destination you go to, not the thing you
      // land on when you want to start a run.
      { path: "/", element: <NewRunPage /> },
      { path: "/replays/new", element: <Navigate to="/" replace /> },
      { path: "/runs", element: <RunsPage /> },
      { path: "/r/:runId", element: <ReportPage /> },
      { path: "/runs/:runId", element: <LegacyRunRedirect /> },
      { path: "/runs/:runId/scorecard", element: <LegacyRunRedirect /> },
      { path: "/recordings", element: <RecordingsPage /> },
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
