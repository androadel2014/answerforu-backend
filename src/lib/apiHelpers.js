// src/lib/apiHelpers.js

export function classNames(...a) {
  return a.filter(Boolean).join(" ");
}

/* =========================
   Auth
========================= */

export function getToken() {
  return localStorage.getItem("token") || "";
}

export function authHeaders() {
  const token = getToken();
  return token ? { Authorization: `Bearer ${token}` } : {};
}

/* =========================
   Fetch helper (fallback support)
========================= */

export async function tryFetchJSON(urls, options = {}) {
  const list = Array.isArray(urls) ? urls : [urls];
  let lastErr = null;

  for (const url of list) {
    try {
      const opts = { ...(options || {}) };
      const headers = { ...(opts.headers || {}) };

      const hasBody =
        opts.body !== undefined &&
        opts.body !== null &&
        opts.method &&
        opts.method !== "GET";

      if (hasBody && !headers["Content-Type"] && !headers["content-type"]) {
        headers["Content-Type"] = "application/json";
      }

      opts.headers = headers;

      const res = await fetch(url, opts);
      const text = await res.text().catch(() => "");

      let data = null;

      try {
        data = text ? JSON.parse(text) : null;
      } catch {
        data = text || null;
      }

      if (res.ok) {
        return { ok: true, status: res.status, data };
      }

      lastErr = {
        ok: false,
        status: res.status,
        data,
        error: data?.error || data?.message || `HTTP ${res.status}`,
        url,
      };
    } catch (e) {
      lastErr = {
        ok: false,
        status: 0,
        data: null,
        error: e?.message || String(e),
        url,
      };
    }
  }

  return (
    lastErr || {
      ok: false,
      status: 0,
      data: null,
      error: "Network error",
    }
  );
}
