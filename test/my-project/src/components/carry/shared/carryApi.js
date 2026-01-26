// src/components/carry/shared/carryApi.js
import { tryFetchJSON, authHeaders } from "../../../lib/apiHelpers";

export const API_BASE =
  import.meta.env.VITE_API_BASE_URL ||
  import.meta.env.VITE_API_BASE ||
  "http://localhost:5000";

/* =====================
   Helpers
===================== */
function normalizeDetailsResponse(raw) {
  const root = raw?.data ?? raw ?? {};
  const item = root?.item ?? null;

  let messages = Array.isArray(root?.messages) ? root.messages.slice() : [];

  if (messages.length >= 2) {
    const a0 = messages[0];
    const a1 = messages[messages.length - 1];

    const t0 = String(a0?.created_at || a0?.createdAt || "");
    const t1 = String(a1?.created_at || a1?.createdAt || "");

    if (t0 && t1 && t0 > t1) {
      messages = messages.slice().reverse();
    } else if (!t0 || !t1) {
      const id0 = Number(a0?.id || 0);
      const id1 = Number(a1?.id || 0);
      if (id0 && id1 && id0 > id1) messages = messages.slice().reverse();
    }
  }

  return { ...root, item, messages };
}

function normalizeRequestStatus(v) {
  const s = String(v || "")
    .toLowerCase()
    .trim();
  if (!s) return null;
  if (s === "sent") return "pending";
  if (s === "approved" || s === "matched") return "accepted";
  return s; // pending | accepted | rejected | cancelled | ...
}

function extractStatusFromRequestResponse(raw) {
  const root = raw?.data ?? raw ?? {};
  const status =
    root?.my_request_status ||
    root?.myRequestStatus ||
    root?.request_status ||
    root?.requestStatus ||
    root?.status ||
    root?.request?.status ||
    root?.request_state ||
    root?.requestState ||
    root?.my_request?.status ||
    null;

  return normalizeRequestStatus(status) || "pending";
}

/* =====================
   TRIPS (carry_listings)
===================== */
export async function fetchCarryListings({
  q,
  role,
  from_country,
  to_country,
}) {
  const qs = new URLSearchParams();
  const qq = (q || "").trim();
  if (qq) qs.set("q", qq);
  if (role) qs.set("role", role);
  if ((from_country || "").trim()) qs.set("from_country", from_country.trim());
  if ((to_country || "").trim()) qs.set("to_country", to_country.trim());

  const url = `${API_BASE}/api/carry/listings?${qs.toString()}`;
  return tryFetchJSON(url, { headers: { ...authHeaders() } });
}

export async function fetchCarryDetails(id) {
  const url = `${API_BASE}/api/carry/listings/${id}`;
  const r = await tryFetchJSON(url, { headers: { ...authHeaders() } });
  if (r?.ok) {
    return { ...r, data: normalizeDetailsResponse(r.data ?? r) };
  }
  return r;
}

export async function upsertCarryListing({ payload, editId }) {
  const isEdit = !!editId;
  const url = isEdit
    ? `${API_BASE}/api/carry/listings/${editId}`
    : `${API_BASE}/api/carry/listings`;

  return tryFetchJSON(url, {
    method: isEdit ? "PATCH" : "POST",
    headers: { "Content-Type": "application/json", ...authHeaders() },
    body: JSON.stringify(payload),
  });
}

export async function deleteCarryListing(id) {
  return tryFetchJSON(`${API_BASE}/api/carry/listings/${id}`, {
    method: "DELETE",
    headers: { ...authHeaders() },
  });
}

/* =====================
   SHIPMENTS (carry_shipments)
===================== */
export async function fetchCarryShipments() {
  return tryFetchJSON(`${API_BASE}/api/carry/shipments`, {
    headers: { ...authHeaders() },
  });
}

export async function createCarryShipment(payload) {
  return tryFetchJSON(`${API_BASE}/api/carry/shipments`, {
    method: "POST",
    headers: { "Content-Type": "application/json", ...authHeaders() },
    body: JSON.stringify(payload),
  });
}

export async function updateCarryShipment(id, payload) {
  return tryFetchJSON(`${API_BASE}/api/carry/shipments/${id}`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json", ...authHeaders() },
    body: JSON.stringify(payload),
  });
}

export async function deleteCarryShipment(id) {
  return tryFetchJSON(`${API_BASE}/api/carry/shipments/${id}`, {
    method: "DELETE",
    headers: { ...authHeaders() },
  });
}

/* =====================
   REQUEST (match shipment -> trip)
   ✅ backend requires: shipment_id
===================== */
export async function requestCarryMatch(listingId, payload = {}) {
  // payload: { shipment_id, offer_amount, offer_currency, note }
  const r = await tryFetchJSON(
    `${API_BASE}/api/carry/listings/${listingId}/request`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json", ...authHeaders() },
      body: JSON.stringify(payload),
    }
  );

  if (r?.ok) {
    const status = extractStatusFromRequestResponse(r);
    return { ...r, data: { ...(r.data ?? {}), status } };
  }
  return r;
}

/* =====================
   MESSAGES
===================== */
export async function sendCarryMessage(listingId, message) {
  return tryFetchJSON(`${API_BASE}/api/carry/listings/${listingId}/messages`, {
    method: "POST",
    headers: { "Content-Type": "application/json", ...authHeaders() },
    body: JSON.stringify({ message }),
  });
}

/* =====================
   OWNER: Requests list + accept/reject
===================== */
export async function fetchCarryRequests(listingId) {
  return tryFetchJSON(`${API_BASE}/api/carry/listings/${listingId}/requests`, {
    headers: { ...authHeaders() },
  });
}

export async function acceptCarryRequest(requestId) {
  return tryFetchJSON(`${API_BASE}/api/carry/requests/${requestId}/accept`, {
    method: "POST",
    headers: { ...authHeaders() },
  });
}

export async function rejectCarryRequest(requestId) {
  return tryFetchJSON(`${API_BASE}/api/carry/requests/${requestId}/reject`, {
    method: "POST",
    headers: { ...authHeaders() },
  });
}

export async function cancelCarryRequest(requestId) {
  return tryFetchJSON(`${API_BASE}/api/carry/requests/${requestId}`, {
    method: "DELETE",
    headers: { ...authHeaders() },
  });
}

/* =====================
   Airports
===================== */
export async function searchAirports(q) {
  const text = (q || "").trim();
  const url = `${API_BASE}/api/airports/search?q=${encodeURIComponent(
    text
  )}&limit=25`;

  try {
    return await tryFetchJSON(url);
  } catch {
    return await tryFetchJSON(url, {
      headers: { "Content-Type": "application/json" },
    });
  }
}
