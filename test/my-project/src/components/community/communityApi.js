const API_BASE =
  import.meta.env.VITE_API_BASE_URL ||
  import.meta.env.VITE_API_BASE ||
  "http://localhost:5000";

const authHeaders = () => {
  const token = localStorage.getItem("token");
  return token ? { Authorization: `Bearer ${token}` } : {};
};

async function safeJson(res) {
  const txt = await res.text();
  try {
    return JSON.parse(txt);
  } catch {
    return { raw: txt };
  }
}

export async function listCommunity(type, q = "") {
  const url = new URL(`${API_BASE}/api/community/${type}`);
  if (q) url.searchParams.set("q", q);
  const res = await fetch(url.toString());
  const data = await safeJson(res);
  if (!res.ok) throw new Error(data?.error || "Failed");
  return data;
}

export async function createCommunity(type, payload) {
  const res = await fetch(`${API_BASE}/api/community/${type}`, {
    method: "POST",
    headers: { "Content-Type": "application/json", ...authHeaders() },
    body: JSON.stringify(payload),
  });
  const data = await safeJson(res);
  if (!res.ok) throw new Error(data?.error || "Failed");
  return data;
}

export async function deleteCommunity(type, id) {
  const res = await fetch(`${API_BASE}/api/community/${type}/${id}`, {
    method: "DELETE",
    headers: { ...authHeaders() },
  });
  const data = await safeJson(res);
  if (!res.ok) throw new Error(data?.error || "Failed");
  return data;
}
