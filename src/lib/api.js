const API_BASE =
  import.meta.env.VITE_API_BASE_URL ||
  import.meta.env.VITE_API_BASE ||
  "http://localhost:5000";

export async function apiFetch(path, options = {}) {
  const token =
    localStorage.getItem("token") ||
    localStorage.getItem("jwt") ||
    localStorage.getItem("authToken") ||
    ""; // احتياط لو اسم التوكن مختلف

  const headers = {
    ...(options.headers || {}),
    ...(options.body ? { "Content-Type": "application/json" } : {}),
  };

  if (token) headers.Authorization = `Bearer ${token}`;

  const res = await fetch(`${API_BASE}${path}`, {
    ...options,
    headers,
  });

  // لو 401 رجّع سبب واضح بدل "no data"
  if (res.status === 401) {
    const msg = (await res.json().catch(() => null))?.message || "Unauthorized";
    throw new Error(`401 Unauthorized: ${msg}`);
  }

  return res;
}
