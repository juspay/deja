import { Navigate, useSearchParams } from "react-router-dom";
import { ERROR_MESSAGES, loginUrl, useMe } from "../lib/me";

/**
 * `/login`: the one page a signed-out person is sent to. The handshake is
 * the server's (`/auth/login/start` and `/auth/callback`); this renders the
 * button and, after a failed attempt, the reason. With sign-in off there is
 * nothing to do here, and the page sends the browser on to where it was going.
 */
export default function LoginPage() {
  const [params] = useSearchParams();
  const me = useMe();
  const raw = params.get("return_url") ?? "/";
  const returnUrl = raw.startsWith("/") && !raw.startsWith("//") ? raw : "/";
  const error = params.get("error") ?? "";
  const message = ERROR_MESSAGES[error] ?? (error ? "Sign-in failed. Try again." : null);
  if (me.data && !me.data.configured) return <Navigate to={returnUrl} replace />;
  if (me.data?.authenticated) return <Navigate to={returnUrl} replace />;
  return (
    <div className="login">
      <h1>Sign in to déjà</h1>
      <p className="hint">Acknowledging a divergence and upgrading an environment need a person behind them. Reading does not.</p>
      {message && <p className="err">{message}</p>}
      <a className="btn primary" href={loginUrl(returnUrl)}>
        Continue with Google
      </a>
      <p className="hint">Juspay accounts only.</p>
    </div>
  );
}
