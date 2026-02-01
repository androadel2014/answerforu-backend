export const cn = (...a) => a.filter(Boolean).join(" ");

export function airportLabel(a) {
  const code = a.code || a.iata || a.icao || "";
  const city = a.city || "";
  const country = a.country || a.country_code || "";
  const name = a.name || "";
  return `${city} (${code}) — ${country} • ${name}`.trim();
}

export const TABS = [
  { key: "explore", label: "Explore" },
  { key: "create", label: "Create" },
  { key: "my", label: "My" },
];

export const emptyForm = {
  role: "traveler",

  from_airport: "",
  to_airport: "",
  from_airport_name: "",
  to_airport_name: "",

  from_country: "",
  from_city: "",
  to_country: "",
  to_city: "",

  travel_date: "",
  arrival_date: "",

  available_weight: "",
  item_type: "",
  description: "",

  reward_amount: "",
  currency: "USD",

  // traveler extras
  airline: "",
  flight_number: "",
  max_item_size: "medium",

  // sender extras
  item_weight: "",
  item_value: "",
  deadline: "",
  fragile: false,
  meet_pref: "airport",
};

function safeJson(v) {
  try {
    return JSON.parse(v);
  } catch {
    return null;
  }
}

function decodeJwtPayload(token) {
  try {
    const part = token.split(".")[1];
    if (!part) return null;
    const base64 = part.replace(/-/g, "+").replace(/_/g, "/");
    const json = decodeURIComponent(
      atob(base64)
        .split("")
        .map((c) => "%" + ("00" + c.charCodeAt(0).toString(16)).slice(-2))
        .join("")
    );
    return JSON.parse(json);
  } catch {
    return null;
  }
}

export function getCurrentUserId() {
  const u1 = safeJson(localStorage.getItem("user"));
  const u2 = safeJson(localStorage.getItem("me"));
  const u3 = safeJson(localStorage.getItem("profile"));
  const fromObj = u1?.id ?? u2?.id ?? u3?.id ?? null;
  if (fromObj) return Number(fromObj) || null;

  const token = localStorage.getItem("token") || "";
  const p = decodeJwtPayload(token);
  const id = p?.id ?? p?.userId ?? p?.uid ?? p?.sub;
  return id ? Number(id) || null : null;
}
