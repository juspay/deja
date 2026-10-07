import React from "react";
import ReactDOM from "react-dom/client";
import { QueryClient, QueryClientProvider, useQueryClient } from "@tanstack/react-query";
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
import { actor, api, setActor } from "./lib/api";
import { loginUrl, useMe } from "./lib/me";
import RecordingsPage from "./pages/RecordingsPage";
import NewRunPage from "./pages/NewRunPage";
import RunsPage from "./pages/RunsPage";
import ReportPage from "./pages/ReportPage";
import DeltaPage from "./pages/DeltaPage";
import AcknowledgePage from "./pages/AcknowledgePage";
import AuditPage from "./pages/AuditPage";
import LoginPage from "./pages/LoginPage";

const queryClient = new QueryClient({
  defaultOptions: { queries: { refetchOnWindowFocus: false, retry: 1 } },
});

/** The identity in the header: the signed-in account when sign-in is on,
 *  a sign-in link when it is on and there is no session, the typed name
 *  when it is off. */
function Identity() {
  const me = useMe();
  const qc = useQueryClient();
  if (me.isLoading) return null;
  if (me.data?.configured) {
    if (!me.data.authenticated) {
      return (
        <a className="btn signin" href={loginUrl()}>
          Sign in
        </a>
      );
    }
    const roles = (me.data.roles ?? []).filter((r) => r !== "viewer");
    return (
      <span className="whoami" title={me.data.name || me.data.email}>
        {me.data.picture && <img src={me.data.picture} alt="" />}
        <span className="email">{me.data.email}</span>
        {roles.length > 0 && <span className="roles">{roles.join(" · ")}</span>}
        <button
          className="btn quiet"
          onClick={async () => {
            await api.logout();
            await qc.invalidateQueries({ queryKey: ["me"] });
            window.location.href = "/";
          }}
        >
          Sign out
        </button>
      </span>
    );
  }
  return <ActorBox />;
}

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
        <Identity />
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
      { path: "/login", element: <LoginPage /> },
      { path: "/replays/new", element: <Navigate to="/" replace /> },
      { path: "/runs", element: <RunsPage /> },
      { path: "/r/:runId", element: <ReportPage /> },
      { path: "/r/:runId/delta", element: <DeltaPage /> },
      { path: "/r/:runId/acknowledge", element: <AcknowledgePage /> },
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
