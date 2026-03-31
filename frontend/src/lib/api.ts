import { getToken, clearToken } from "./auth";

const PLACEHOLDER = "__NEXT_PUBLIC_API_URL__";
const rawApiUrl = process.env.NEXT_PUBLIC_API_URL?.trim() || "";
const explicitApiUrl = rawApiUrl && rawApiUrl !== PLACEHOLDER ? rawApiUrl.replace(/\/+$/, "") : "";

// Empty means "use same-origin frontend proxy" (/api and /ws).
export const API_URL = explicitApiUrl;

export function apiUrl(path: string): string {
  return API_URL ? `${API_URL}${path}` : path;
}

export function wsUrl(path: string): string {
  const base = API_URL
    ? API_URL.replace(/^http/, "ws")
    : `${window.location.protocol === "https:" ? "wss" : "ws"}://${window.location.host}`;
  const token = getToken();
  const separator = path.includes("?") ? "&" : "?";
  const tokenParam = token ? `${separator}token=${encodeURIComponent(token)}` : "";
  return `${base}${path}${tokenParam}`;
}

export async function apiRequest<T>(path: string, init: RequestInit = {}): Promise<T> {
  const headers = new Headers(init.headers || {});
  const token = getToken();
  if (token) {
    headers.set("Authorization", `Bearer ${token}`);
  }
  if (init.body && !headers.has("Content-Type")) {
    headers.set("Content-Type", "application/json");
  }
  const res = await fetch(apiUrl(path), {
    ...init,
    headers,
  });
  if (res.status === 401) {
    clearToken();
    window.location.reload();
    throw new Error("Unauthorized");
  }
  if (!res.ok) {
    let detail = "";
    try {
      detail = (await res.text()).trim();
    } catch {
      detail = "";
    }
    throw new Error(`API error ${res.status}: ${detail || res.statusText}`);
  }
  if (res.status === 204) {
    return undefined as T;
  }
  const contentType = res.headers.get("content-type") || "";
  if (contentType.includes("application/json")) {
    return res.json();
  }
  return (await res.text()) as T;
}

export async function apiFetch<T>(path: string): Promise<T> {
  return apiRequest<T>(path);
}
