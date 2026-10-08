import { useQuery } from "@tanstack/react-query";
import { Me, actor, api, askToSignIn, rememberMe } from "./api";

/** Before a gated action: when sign-in is configured and the person is not
 *  signed in, open the sign-in dialog and report that the action must wait.
 *  With sign-in off, or signed in, nothing happens and the action proceeds. */
export function needsSignIn(me: Me | undefined): boolean {
  if (!me?.configured || me.authenticated) return false;
  askToSignIn();
  return true;
}

/** The who-am-I probe, once per page load, remembered for `actor()`. */
export function useMe() {
  return useQuery({
    queryKey: ["me"],
    queryFn: async () => {
      const m = await api.me();
      rememberMe(m);
      return m;
    },
    staleTime: 60_000,
  });
}

/** The actor as a hook, so a component re-renders when the probe answers. */
export function useActor(): string {
  const me = useMe();
  if (me.data?.authenticated && me.data.email) return me.data.email;
  return actor();
}

export function useHasRole(role: string): boolean {
  const me = useMe();
  // With sign-in off there are no roles; everyone may do what the typed
  // name allows, as before.
  if (!me.data?.configured) return true;
  return (me.data.roles ?? []).includes(role);
}

export function loginUrl(returnTo?: string): string {
  const back = returnTo ?? window.location.pathname + window.location.search;
  return "/auth/login/start?return_url=" + encodeURIComponent(back);
}

export const ERROR_MESSAGES: Record<string, string> = {
  state_mismatch: "The sign-in took too long or was tampered with. Try again.",
  exchange: "Google did not complete the sign-in. Try again.",
  verify: "The identity Google returned could not be verified. Try again.",
  domain: "This account is not allowed to use déjà.",
};
