// src/components/carry/CarryMegaView.jsx
import React, { useEffect, useMemo, useRef, useState } from "react";
import toast from "react-hot-toast";
import { CarryCard, Pill } from "./card";
import { useLocation, useNavigate } from "react-router-dom";
import {
  isTripLocked,
  normalizeTrip,
  normalizeShipment,
  MyGrid,
  ShipmentsGrid,
  TripsGrid,
  ExploreGrid,
} from "./carryMega.shared";

import {
  Package,
  Plus,
  RefreshCw,
  Lock,
  Search,
  Filter,
  Plane,
  LogIn,
  User,
  SendHorizontal,
} from "lucide-react";

import { tryFetchJSON, authHeaders } from "../../lib/apiHelpers";
import { toastConfirm } from "../../lib/notify";

// Put it here: src/data/airports.json (OBJECT keyed by ICAO)
import AIRPORTS_RAW from "../../../backend/src/data/airports.json";

const API_BASE =
  import.meta.env.VITE_API_BASE_URL ||
  import.meta.env.VITE_API_BASE ||
  "http://localhost:5000";

const cn = (...a) => a.filter(Boolean).join(" ");

/* =========================
   Auth helpers (stable)
========================= */
function getTokenLocal() {
  try {
    const t =
      localStorage.getItem("token") ||
      localStorage.getItem("access_token") ||
      localStorage.getItem("auth_token") ||
      localStorage.getItem("jwt") ||
      "";
    return String(t || "").trim();
  } catch {
    return "";
  }
}
function isAuthed() {
  return !!getTokenLocal();
}
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
function getCurrentUserId() {
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

/* =========================
   Security helpers
========================= */
function normalizeText(v) {
  return String(v ?? "").trim();
}
function hasPhoneLike(text) {
  const s = normalizeText(text);
  if (!s) return false;
  const digits = s.replace(/\D/g, "");
  if (digits.length >= 7) return true;
  const re = /(\+?\d[\s().-]?){7,}\d/;
  return re.test(s);
}
function validateNoPhones(fields) {
  for (const [k, v] of Object.entries(fields || {})) {
    if (hasPhoneLike(v)) return k;
  }
  return null;
}

/* =========================
   Airports helpers (OBJECT keyed by ICAO) ✅ NO HOOKS HERE
========================= */
function normalizeAirportFromObj(key, raw) {
  const a = raw || {};
  const icao = String(a.icao || key || "").toUpperCase();
  const iata = String(a.iata || "").toUpperCase();
  const code = iata || icao; // prefer IATA, fallback ICAO
  const name = String(a.name || "").trim();
  const country = String(a.country || "").toUpperCase();
  const state = String(a.state || "").trim();
  const city = String(a.city || "").trim();

  return {
    icao,
    iata,
    code,
    name,
    country,
    state,
    city,
    lat: a.lat,
    lon: a.lon,
    tz: a.tz,
  };
}

const AIRPORTS_LIST = (() => {
  const src = AIRPORTS_RAW || {};
  if (Array.isArray(src)) {
    return src
      .map((x) => normalizeAirportFromObj(x?.icao || x?.iata || "", x))
      .filter((x) => x.code && x.name);
  }
  if (src && typeof src === "object") {
    return Object.entries(src)
      .map(([k, v]) => normalizeAirportFromObj(k, v))
      .filter((x) => x.code && x.name);
  }
  return [];
})();

const AIRPORT_SEARCH_ALIASES = {
  cairo: ["CAI", "HECA"],
};

function airportLabel(a) {
  if (!a) return "";
  const code = a.code || "";
  const name = a.name || "";
  const country = a.country || "";
  const state = a.state ? `, ${a.state}` : "";
  return `${code} — ${name}${country ? ` • ${country}${state}` : ""}`;
}
function findAirportByCode(code) {
  const c = String(code || "")
    .toUpperCase()
    .trim();
  if (!c) return null;
  return AIRPORTS_LIST.find((a) => a.code === c) || null;
}

function AirportPicker({ label, valueCode, onPick }) {
  const [open, setOpen] = useState(false);
  const [q, setQ] = useState("");
  const wrapRef = useRef(null);

  const picked = useMemo(() => findAirportByCode(valueCode), [valueCode]);

  const results = useMemo(() => {
    const term = String(q || "")
      .trim()
      .toLowerCase();
    if (!term) return AIRPORTS_LIST.slice(0, 15);

    const tokens = term.split(/\s+/).filter(Boolean);

    const aliasCodes = new Set();
    for (const [k, codes] of Object.entries(AIRPORT_SEARCH_ALIASES)) {
      if (term.includes(k))
        (codes || []).forEach((c) => aliasCodes.add(String(c).toUpperCase()));
    }

    const scored = AIRPORTS_LIST.map((a) => {
      const code = String(a.code || "").toUpperCase();
      const hay =
        `${a.code} ${a.iata} ${a.icao} ${a.name} ${a.city} ${a.state} ${a.country}`.toLowerCase();
      let score = 0;

      if (aliasCodes.has(code)) score += 500;

      for (const t of tokens) {
        if (!t) continue;
        if (a.code?.toLowerCase() === t) score += 140;
        if (a.iata?.toLowerCase() === t) score += 140;
        if (a.icao?.toLowerCase() === t) score += 130;

        if (a.code?.toLowerCase().startsWith(t)) score += 80;
        if (a.name?.toLowerCase().startsWith(t)) score += 35;
        if (a.city?.toLowerCase().startsWith(t)) score += 35;

        if (hay.includes(t)) score += 12;
      }

      return { a, score };
    })
      .filter((x) => x.score > 0)
      .sort((x, y) => y.score - x.score)
      .slice(0, 15)
      .map((x) => x.a);

    return scored.length ? scored : AIRPORTS_LIST.slice(0, 15);
  }, [q]);

  useEffect(() => {
    function onDoc(e) {
      if (!wrapRef.current) return;
      if (!wrapRef.current.contains(e.target)) setOpen(false);
    }
    document.addEventListener("mousedown", onDoc);
    return () => document.removeEventListener("mousedown", onDoc);
  }, []);

  return (
    <div ref={wrapRef} className="relative">
      <div className="text-xs font-extrabold text-slate-600">{label}</div>

      <button
        type="button"
        onClick={() => setOpen((v) => !v)}
        className={cn(
          "mt-2 w-full px-4 py-4 rounded-2xl border bg-white text-left font-extrabold",
          "border-slate-200 hover:bg-slate-50"
        )}
      >
        {picked ? airportLabel(picked) : "Search airport…"}
      </button>

      {open ? (
        <div className="absolute z-[95] mt-2 w-full rounded-2xl border border-slate-200 bg-white shadow-xl overflow-hidden">
          <div className="p-2 border-b border-slate-100">
            <input
              value={q}
              onChange={(e) => setQ(e.target.value)}
              placeholder="Search by IATA/ICAO or airport name…"
              className="w-full px-4 py-3 rounded-2xl border border-slate-200 bg-white focus:outline-none focus:ring-2 focus:ring-indigo-200 text-sm font-bold"
              autoFocus
            />
          </div>

          <div className="max-h-80 overflow-y-auto">
            {results.map((a) => (
              <button
                key={`${a.code}_${a.icao}_${a.iata}`}
                type="button"
                onClick={() => {
                  onPick(a);
                  setOpen(false);
                }}
                className="w-full px-4 py-3 text-left hover:bg-slate-50 border-b border-slate-100"
              >
                <div className="text-sm font-extrabold text-slate-900">
                  {a.code} — {a.name}
                </div>
                <div className="text-xs text-slate-500 font-bold truncate">
                  {a.country}
                  {a.state ? ` • ${a.state}` : ""}
                  {a.city ? ` • ${a.city}` : ""}
                  {a.icao ? ` • ICAO ${a.icao}` : ""}
                </div>
              </button>
            ))}
          </div>

          <div className="p-2">
            <button
              type="button"
              onClick={() => {
                onPick(null);
                setOpen(false);
              }}
              className="w-full px-3 py-2.5 rounded-2xl border border-slate-200 hover:bg-slate-50 text-xs font-extrabold"
            >
              Clear
            </button>
          </div>
        </div>
      ) : null}
    </div>
  );
}

function readFileAsDataURL(file) {
  return new Promise((resolve, reject) => {
    const fr = new FileReader();
    fr.onerror = () => reject(new Error("Failed to read file"));
    fr.onload = () => resolve(String(fr.result || ""));
    fr.readAsDataURL(file);
  });
}

/* =========================
   Categories
========================= */
const PROHIBITED_CATEGORIES = [
  { id: "weapons", label: "Weapons / ammo" },
  { id: "explosives", label: "Explosives / fireworks" },
  { id: "alcohol", label: "Alcohol" },
  { id: "illegal_drugs", label: "Illegal drugs" },
  { id: "vapes", label: "Vapes / nicotine" },
  { id: "hazmat", label: "Hazardous materials (hazmat)" },
  { id: "liquids_bulk", label: "Large liquids (bulk)" },
  { id: "lithium_loose", label: "Loose lithium batteries" },
  { id: "aerosols", label: "Perfume / aerosols" },
  { id: "perishable_food", label: "Perishable food" },
  { id: "cash_giftcards", label: "Cash / gift cards" },
  { id: "sensitive_docs", label: "Sensitive documents" },
  { id: "prescription_meds", label: "Prescription meds (restricted)" },
];

const ITEM_CATEGORIES = [
  { id: "mobile_accessories", label: "Mobile accessories" },
  { id: "phones_tablets", label: "Phones & tablets" },
  { id: "laptops", label: "Laptops" },
  { id: "computer_accessories", label: "Computer accessories" },
  { id: "electronics_other", label: "Electronics (other)" },

  { id: "clothing", label: "Clothing" },
  { id: "shoes", label: "Shoes" },
  { id: "bags", label: "Bags" },

  { id: "cosmetics", label: "Cosmetics / skincare" },
  { id: "supplements", label: "Supplements (OTC)" },
  { id: "medicines_otc", label: "Medicines (OTC)" },

  { id: "books", label: "Books" },
  { id: "documents", label: "Documents" },
  { id: "toys", label: "Toys" },
  { id: "gifts", label: "Gifts" },
  { id: "other", label: "Other" },
];

function TogglePills({ value, onChange }) {
  const set = new Set(Array.isArray(value) ? value : []);
  return (
    <div className="flex flex-wrap gap-2">
      {PROHIBITED_CATEGORIES.map((c) => {
        const on = set.has(c.id);
        return (
          <button
            key={c.id}
            type="button"
            onClick={() => {
              const next = new Set(set);
              if (next.has(c.id)) next.delete(c.id);
              else next.add(c.id);
              onChange(Array.from(next));
            }}
            className={cn(
              "px-3 py-2 rounded-2xl border text-xs font-extrabold",
              on
                ? "bg-slate-900 text-white border-slate-900"
                : "bg-white text-slate-800 border-slate-200 hover:bg-slate-50"
            )}
          >
            {c.label}
          </button>
        );
      })}
    </div>
  );
}

/* =========================
   API (inline)
========================= */
async function fetchTrips({
  q,
  from_airport_code,
  to_airport_code,
  date_from,
  date_to,
}) {
  const qs = new URLSearchParams();
  if ((q || "").trim()) qs.set("q", q.trim());
  qs.set("role", "traveler");

  if ((from_airport_code || "").trim())
    qs.set("from_airport_code", String(from_airport_code).trim().toUpperCase());
  if ((to_airport_code || "").trim())
    qs.set("to_airport_code", String(to_airport_code).trim().toUpperCase());

  if ((date_from || "").trim()) qs.set("date_from", date_from.trim());
  if ((date_to || "").trim()) qs.set("date_to", date_to.trim());

  return tryFetchJSON(`${API_BASE}/api/carry/listings?${qs.toString()}`, {
    headers: { ...authHeaders() },
  });
}

async function createOrUpdateTrip({ payload, editId }) {
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

async function deleteTripApi(id) {
  return tryFetchJSON(`${API_BASE}/api/carry/listings/${id}`, {
    method: "DELETE",
    headers: { ...authHeaders() },
  });
}

async function fetchMyShipments() {
  return tryFetchJSON(`${API_BASE}/api/carry/shipments`, {
    headers: { ...authHeaders() },
  });
}
async function createShipment(payload) {
  return tryFetchJSON(`${API_BASE}/api/carry/shipments`, {
    method: "POST",
    headers: { "Content-Type": "application/json", ...authHeaders() },
    body: JSON.stringify(payload || {}),
  });
}
async function updateShipment(id, payload) {
  return tryFetchJSON(`${API_BASE}/api/carry/shipments/${id}`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json", ...authHeaders() },
    body: JSON.stringify(payload || {}),
  });
}
async function deleteShipment(id) {
  return tryFetchJSON(`${API_BASE}/api/carry/shipments/${id}`, {
    method: "DELETE",
    headers: { ...authHeaders() },
  });
}

async function fetchExploreShipments({
  q,
  from_airport_code,
  to_airport_code,
  date_from,
  date_to,
}) {
  const qs = new URLSearchParams();
  if ((q || "").trim()) qs.set("q", q.trim());

  if ((from_airport_code || "").trim())
    qs.set("from_airport_code", String(from_airport_code).trim().toUpperCase());
  if ((to_airport_code || "").trim())
    qs.set("to_airport_code", String(to_airport_code).trim().toUpperCase());

  if ((date_from || "").trim()) qs.set("date_from", date_from.trim());
  if ((date_to || "").trim()) qs.set("date_to", date_to.trim());

  const paths = [
    `${API_BASE}/api/carry/shipments/public?${qs.toString()}`,
    `${API_BASE}/api/carry/shipments/explore?${qs.toString()}`,
    `${API_BASE}/api/carry/shipments?public=1&${qs.toString()}`,
    `${API_BASE}/api/carry/explore/shipments?${qs.toString()}`,
  ];

  let last = null;
  for (const url of paths) {
    // eslint-disable-next-line no-await-in-loop
    const r = await tryFetchJSON(url, { headers: { ...authHeaders() } });
    last = r;
    if (r?.ok) return r;
  }
  return last || { ok: true, data: { items: [] } };
}

async function requestMatch(tripId, { shipment_id, offer_amount, note }) {
  return tryFetchJSON(`${API_BASE}/api/carry/listings/${tripId}/request`, {
    method: "POST",
    headers: { "Content-Type": "application/json", ...authHeaders() },
    body: JSON.stringify({
      shipment_id,
      offer_amount,
      offer_currency: "USD",
      note: note || "",
    }),
  });
}

/* =========================
   UI primitives
========================= */
function Field({ label, hint, children }) {
  return (
    <div>
      <div className="flex items-baseline justify-between gap-2">
        <div className="text-xs font-extrabold text-slate-600">{label}</div>
        {hint ? (
          <div className="text-[11px] text-slate-400 font-bold">{hint}</div>
        ) : null}
      </div>
      <div className="mt-2">{children}</div>
    </div>
  );
}

function Modal({ open, title, children, onClose, wide }) {
  if (!open) return null;

  return (
    <div
      className="fixed inset-0 z-[90]"
      onWheelCapture={(e) => e.stopPropagation()}
      onTouchMoveCapture={(e) => e.stopPropagation()}
    >
      <div className="absolute inset-0 bg-black/40" onClick={onClose} />
      <div className="absolute inset-0 p-3 sm:p-6 flex items-center justify-center">
        <div
          className={cn(
            "relative w-full rounded-3xl border bg-white shadow-xl overflow-hidden",
            "max-h-[92vh] flex flex-col",
            wide ? "max-w-5xl" : "max-w-xl"
          )}
          onClick={(e) => e.stopPropagation()}
        >
          <div className="px-6 py-5 border-b flex items-center justify-between shrink-0">
            <div className="font-black text-slate-900">{title}</div>
            <button
              className="px-3 py-2 rounded-xl hover:bg-slate-100 font-extrabold"
              onClick={onClose}
              aria-label="Close"
              type="button"
            >
              ✕
            </button>
          </div>

          <div className="p-6 overflow-y-auto overscroll-contain">
            {children}
          </div>
        </div>
      </div>
    </div>
  );
}

/* =========================
   helpers
========================= */
function toNum(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function shipmentIsUsableForRequest(s) {
  const st = String(s?.status || "")
    .trim()
    .toLowerCase();
  if (["matched", "completed", "cancelled", "canceled"].includes(st))
    return false;
  return true;
}

function shipmentIsVisibleInExplore(s) {
  const st = String(s?.status || "")
    .trim()
    .toLowerCase();
  if (["matched", "completed", "cancelled", "canceled"].includes(st))
    return false;
  return true;
}

// ✅ NEW: Shipment lock reason (deal made => status != open)
function shipmentLockReason(s) {
  const st =
    String(s?.status || "")
      .trim()
      .toLowerCase() || "open";
  if (st === "open") return "";
  if (["cancelled", "canceled"].includes(st))
    return "Locked: This shipment is cancelled.";
  return "Locked: A deal has been made on this shipment.";
}

/* =========================
   Saved (local only)
========================= */
const LS_SAVED_KEY = "carry_saved_v1";
function loadSavedSet() {
  try {
    const raw = safeJson(localStorage.getItem(LS_SAVED_KEY));
    const arr = Array.isArray(raw) ? raw : [];
    return new Set(arr.map(String));
  } catch {
    return new Set();
  }
}
function persistSavedSet(setObj) {
  try {
    const arr = Array.from(setObj || new Set()).map(String);
    localStorage.setItem(LS_SAVED_KEY, JSON.stringify(arr));
  } catch {}
}

/* =========================
   Tabs
========================= */
const TABS = [
  { key: "explore", label: "Explore", icon: Search },
  { key: "trips", label: "Trips", icon: Plane },
  { key: "shipments", label: "Shipments", icon: Package },
  { key: "my", label: "My", icon: User },
];

/* =========================
   Mega View
========================= */
export default function CarryMegaView() {
  const nav = useNavigate();
  const loc = useLocation();

  const [currentUserId, setCurrentUserId] = useState(getCurrentUserId());
  useEffect(() => {
    const sync = () => setCurrentUserId(getCurrentUserId());
    window.addEventListener("storage", sync);
    window.addEventListener("focus", sync);
    return () => {
      window.removeEventListener("storage", sync);
      window.removeEventListener("focus", sync);
    };
  }, []);

  const authed = useMemo(() => isAuthed(), [currentUserId]);

  const [tab, setTab] = useState("explore");
  const [loading, setLoading] = useState(false);
  const [filtersOpen, setFiltersOpen] = useState(false);

  const [q, setQ] = useState("");
  const [dateFrom, setDateFrom] = useState("");
  const [dateTo, setDateTo] = useState("");
  const [minWeight, setMinWeight] = useState("");

  const [fromAirport, setFromAirport] = useState("");
  const [toAirport, setToAirport] = useState("");

  const [trips, setTrips] = useState([]);
  const [exploreShipments, setExploreShipments] = useState([]);
  const [myShipments, setMyShipments] = useState([]);

  const [saved, setSaved] = useState(() => loadSavedSet());

  const emptyTrip = useMemo(
    () => ({
      role: "traveler",
      from_airport_code: "",
      to_airport_code: "",
      travel_date: "",
      arrival_date: "",
      available_weight: "",
      airline: "",
      flight_number: "",
      meet_pref: "airport",
      meet_place: "",
      description: "",
      no_carry: [],
      passport_image: "",
      visa_image: "",
    }),
    []
  );

  const [tripFormOpen, setTripFormOpen] = useState(false);
  const [tripForm, setTripForm] = useState(emptyTrip);
  const [tripEditId, setTripEditId] = useState(null);

  const emptyShipment = useMemo(
    () => ({
      category: "mobile_accessories",
      from_airport_code: "",
      to_airport_code: "",
      from_country: "",
      from_city: "",
      to_country: "",
      to_city: "",
      deadline: "",
      item_title: "",
      item_desc: "",
      item_weight: "",
      budget_amount: "",
      product_url: "",
      item_image: "",
    }),
    []
  );

  const [shipFormOpen, setShipFormOpen] = useState(false);
  const [shipForm, setShipForm] = useState(emptyShipment);
  const [shipEditId, setShipEditId] = useState(null);
  const [shipLoading, setShipLoading] = useState(false);

  const [reqOpen, setReqOpen] = useState(false);
  const [reqTripId, setReqTripId] = useState(null);
  const [reqShipmentId, setReqShipmentId] = useState("");
  const [reqOfferAmount, setReqOfferAmount] = useState("");
  const [reqNote, setReqNote] = useState("");

  useEffect(() => {
    const anyOpen = filtersOpen || tripFormOpen || shipFormOpen || reqOpen;
    if (!anyOpen) return;

    const prevOverflow = document.body.style.overflow;
    const prevPad = document.body.style.paddingRight;
    const scrollBarW = window.innerWidth - document.documentElement.clientWidth;

    document.body.style.overflow = "hidden";
    if (scrollBarW > 0) document.body.style.paddingRight = `${scrollBarW}px`;

    return () => {
      document.body.style.overflow = prevOverflow || "";
      document.body.style.paddingRight = prevPad || "";
    };
  }, [filtersOpen, tripFormOpen, shipFormOpen, reqOpen]);

  const abortRef = useRef({
    trips: null,
    exploreShipments: null,
    myShipments: null,
  });

  function goLogin(nextPath) {
    nav("/auth", { state: { next: nextPath || loc.pathname } });
  }
  function requireAuth(nextPath) {
    if (isAuthed()) return true;
    toast.error("Please login first.");
    goLogin(nextPath);
    return false;
  }

  function goTripDetails(id) {
    nav(`/carry/trips/${id}`);
  }
  function goShipmentDetails(id) {
    nav(`/carry/shipments/${id}`);
  }

  function toggleSaved(id) {
    if (!requireAuth("/carry")) return;
    const next = new Set(saved);
    const key = String(id);
    if (next.has(key)) next.delete(key);
    else next.add(key);
    setSaved(next);
    persistSavedSet(next);
  }

  function getRequestState(trip) {
    const raw =
      trip.my_request_status ||
      trip.myRequestStatus ||
      trip.request_status ||
      trip.requestStatus ||
      null;
    const s = String(raw || "").toLowerCase();
    if (s === "pending" || s === "sent") return "pending";
    if (s === "accepted" || s === "approved" || s === "matched")
      return "accepted";
    if (s === "rejected" || s === "declined") return "rejected";
    if (s === "canceled" || s === "cancelled") return "canceled";
    if (s === "counter_offer" || s === "counter") return "counter_offer";
    return "none";
  }

  async function loadTripsNow() {
    if (abortRef.current.trips) abortRef.current.trips.abort();
    const ac = new AbortController();
    abortRef.current.trips = ac;

    setLoading(true);
    try {
      const r = await fetchTrips({
        q,
        from_airport_code: fromAirport,
        to_airport_code: toAirport,
        date_from: dateFrom,
        date_to: dateTo,
      });

      if (ac.signal.aborted) return;
      if (!r?.ok) throw new Error(r?.error || "Failed");

      const root = r?.data ?? r;
      let arr = Array.isArray(root?.items)
        ? root.items
        : Array.isArray(root)
        ? root
        : [];
      arr = arr.map(normalizeTrip);

      const minW = minWeight === "" ? null : Number(minWeight);
      if (Number.isFinite(minW)) {
        arr = arr.filter((x) => {
          const aw = toNum(x.available_weight);
          return aw === null ? true : aw >= minW;
        });
      }

      setTrips(arr);
    } catch (e) {
      if (!String(e?.name || "").includes("Abort"))
        toast.error(String(e?.message || e));
    } finally {
      if (!ac.signal.aborted) setLoading(false);
    }
  }

  async function loadExploreShipmentsNow() {
    if (abortRef.current.exploreShipments)
      abortRef.current.exploreShipments.abort();
    const ac = new AbortController();
    abortRef.current.exploreShipments = ac;

    try {
      const r = await fetchExploreShipments({
        q,
        from_airport_code: fromAirport,
        to_airport_code: toAirport,
        date_from: dateFrom,
        date_to: dateTo,
      });

      if (ac.signal.aborted) return;

      const root = r?.data ?? r;
      let arr = Array.isArray(root?.items)
        ? root.items
        : Array.isArray(root)
        ? root
        : [];
      arr = arr.map(normalizeShipment);

      // ✅ hide matched/closed shipments in Explore (so nobody can request them again)
      arr = arr.filter((s) => shipmentIsVisibleInExplore(s));

      const minW = minWeight === "" ? null : Number(minWeight);
      if (Number.isFinite(minW)) {
        arr = arr.filter((x) => {
          const w = toNum(x.item_weight);
          return w === null ? true : w >= minW;
        });
      }

      setExploreShipments(arr);
    } catch {
      setExploreShipments([]);
    }
  }

  async function loadMyShipmentsNow({ silent } = {}) {
    if (!requireAuth("/carry?tab=shipments")) return [];
    if (abortRef.current.myShipments) abortRef.current.myShipments.abort();
    const ac = new AbortController();
    abortRef.current.myShipments = ac;

    if (!silent) setShipLoading(true);
    try {
      const r = await fetchMyShipments();
      if (ac.signal.aborted) return [];
      if (!r?.ok) throw new Error(r?.error || "Failed");

      const root = r?.data ?? r;
      const arr = (
        Array.isArray(root?.items)
          ? root.items
          : Array.isArray(root)
          ? root
          : []
      ).map(normalizeShipment);

      setMyShipments(arr);
      return arr;
    } catch (e) {
      if (!silent) toast.error(String(e?.message || e));
      return [];
    } finally {
      if (!silent && !ac.signal.aborted) setShipLoading(false);
    }
  }

  function startCreateTrip() {
    if (!requireAuth("/carry")) return;
    setTripEditId(null);
    setTripForm(emptyTrip);
    setTripFormOpen(true);
  }

  function startEditTrip(itOrId) {
    if (!requireAuth("/carry")) return;

    const raw =
      itOrId && typeof itOrId === "object"
        ? itOrId
        : (trips || []).find((t) => String(t.id) === String(itOrId)) || null;

    if (!raw)
      return toast.error("Trip not found in list. Refresh and try again.");

    const mergedRaw =
      raw && raw.data && typeof raw.data === "object"
        ? { ...raw, ...raw.data }
        : raw;

    const n = normalizeTrip(mergedRaw);

    if (isTripLocked(n)) {
      toast.error(
        "Locked: A deal/request exists on this trip. You can’t edit it."
      );
      return;
    }

    setTripEditId(n.id);

    setTripForm({
      ...emptyTrip,
      ...n,
      data: raw?.data && typeof raw.data === "object" ? raw.data : n.data || {},
      role: "traveler",
      available_weight:
        n.available_weight === null || n.available_weight === undefined
          ? ""
          : String(n.available_weight),
      no_carry: Array.isArray(n.no_carry) ? n.no_carry : [],
    });

    setTripFormOpen(true);
  }

  async function submitTrip() {
    if (!requireAuth("/carry")) return;

    if (!String(tripForm.from_airport_code || "").trim())
      return toast.error("From airport is required.");
    if (!String(tripForm.to_airport_code || "").trim())
      return toast.error("To airport is required.");
    if (!String(tripForm.travel_date || "").trim())
      return toast.error("Travel date is required.");
    if (!String(tripForm.arrival_date || "").trim())
      return toast.error("Arrival date is required.");

    const airlineV = String(tripForm.airline || "").trim();
    const flightV = String(tripForm.flight_number || "").trim();
    if (!airlineV) return toast.error("Airline is required.");
    if (!flightV) return toast.error("Flight number is required.");

    const w = Number(tripForm.available_weight);
    if (!Number.isFinite(w) || w <= 0)
      return toast.error("Enter a valid weight.");

    if (tripForm.meet_pref === "nearby") {
      const mp = String(tripForm.meet_place || "").trim();
      if (!mp) return toast.error("Enter nearby meeting place(s).");
    }

    const badKey = validateNoPhones({
      airline: airlineV,
      flight_number: flightV,
      meet_place: tripForm.meet_place,
      description: tripForm.description,
    });
    if (badKey)
      return toast.error(`Phone numbers are not allowed (${badKey}).`);

    const payload = {
      ...tripForm,
      role: "traveler",
      from_airport_code: String(tripForm.from_airport_code || "").toUpperCase(),
      to_airport_code: String(tripForm.to_airport_code || "").toUpperCase(),
      airline: airlineV,
      flight_number: flightV,
      traveler_airline: airlineV,
      traveler_flight_number: flightV,
      flight_no: flightV,
      available_weight: w,
      meet_pref: tripForm.meet_pref || "airport",
      meet_place: String(tripForm.meet_place || "").trim() || "",
      no_carry: Array.isArray(tripForm.no_carry) ? tripForm.no_carry : [],
    };

    try {
      setLoading(true);
      const r = await createOrUpdateTrip({ payload, editId: tripEditId });
      if (!r?.ok) throw new Error(r?.error || "Failed");
      toast.success(tripEditId ? "Updated ✅" : "Created ✅");

      setTripEditId(null);
      setTripForm(emptyTrip);
      setTripFormOpen(false);

      await loadTripsNow();
    } catch (e) {
      toast.error(String(e?.message || e));
    } finally {
      setLoading(false);
    }
  }

  async function deleteTrip(id) {
    if (!requireAuth("/carry")) return;

    const trip = (trips || []).find((t) => String(t.id) === String(id));
    if (trip && isTripLocked(trip)) {
      toast.error(
        "Locked: A deal/request exists on this trip. You can’t delete it."
      );
      return;
    }

    const ok = await toastConfirm("Delete this trip?");
    if (!ok) return;

    try {
      setLoading(true);
      const r = await deleteTripApi(id);
      if (!r?.ok) throw new Error(r?.error || "Failed");
      toast.success("Deleted ✅");
      await loadTripsNow();
    } catch (e) {
      toast.error(String(e?.message || e));
    } finally {
      setLoading(false);
    }
  }

  function startCreateShipment() {
    if (!requireAuth("/carry?tab=shipments")) return;
    setShipEditId(null);
    setShipForm(emptyShipment);
    setShipFormOpen(true);
  }

  // ✅ LOCK shipment edit if not OPEN (deal made)
  function startEditShipment(sOrId) {
    if (!requireAuth("/carry?tab=shipments")) return;

    const raw =
      sOrId && typeof sOrId === "object"
        ? sOrId
        : (myShipments || []).find((x) => String(x.id) === String(sOrId)) ||
          null;

    if (!raw) return toast.error("Shipment not found. Refresh and try again.");

    const n = normalizeShipment(raw);

    const lockMsg = shipmentLockReason(n);
    if (lockMsg) {
      toast.error(lockMsg);
      return;
    }

    setShipEditId(n.id);
    setShipForm({
      from_airport_code: String(n.from_airport_code || "").toUpperCase(),
      to_airport_code: String(n.to_airport_code || "").toUpperCase(),
      from_country: n.from_country || "",
      from_city: n.from_city || "",
      to_country: n.to_country || "",
      to_city: n.to_city || "",
      deadline: n.deadline || "",
      item_title: n.item_title || "",
      item_desc: n.item_desc || "",
      category: n.category || "other",
      product_url: n.product_url || n.productUrl || "",
      item_weight:
        n.item_weight === null || n.item_weight === undefined
          ? ""
          : String(n.item_weight),
      budget_amount:
        n.budget_amount === null || n.budget_amount === undefined
          ? ""
          : String(n.budget_amount),
      item_image: n.image || n.item_image || "",
    });

    setShipFormOpen(true);
  }

  async function submitShipment() {
    if (!requireAuth("/carry?tab=shipments")) return;

    if (!String(shipForm.from_airport_code || "").trim())
      return toast.error("From airport is required.");
    if (!String(shipForm.to_airport_code || "").trim())
      return toast.error("To airport is required.");
    if (!String(shipForm.deadline || "").trim())
      return toast.error("Deadline is required.");

    if (!String(shipForm.item_title || "").trim())
      return toast.error("Title is required.");
    if (!String(shipForm.product_url || "").trim())
      return toast.error("Product link is required.");
    if (!String(shipForm.item_desc || "").trim())
      return toast.error("Description is required.");
    if (!String(shipForm.category || "").trim())
      return toast.error("Category is required.");

    const w = Number(shipForm.item_weight);
    if (!Number.isFinite(w) || w <= 0)
      return toast.error("Enter a valid weight.");

    const b = Number(shipForm.budget_amount);
    if (!Number.isFinite(b) || b <= 0)
      return toast.error("Enter a valid budget.");

    if (!String(shipForm.item_image || "").trim())
      return toast.error("Item image is required (URL or upload).");

    const badKey = validateNoPhones({
      item_title: shipForm.item_title,
      item_desc: shipForm.item_desc,
      category: shipForm.category || "other",
      product_url: shipForm.product_url,
    });
    if (badKey)
      return toast.error(`Phone numbers are not allowed (${badKey}).`);

    const payload = {
      ...shipForm,
      category: shipForm.category || "other",
      item_weight: w,
      budget_amount: b,
      product_url: String(shipForm.product_url || "").trim(),
      item_image: shipForm.item_image || "",
    };

    try {
      setShipLoading(true);
      const r = shipEditId
        ? await updateShipment(shipEditId, payload)
        : await createShipment(payload);
      if (!r?.ok) throw new Error(r?.error || "Failed");
      toast.success(shipEditId ? "Shipment updated ✅" : "Shipment created ✅");

      setShipEditId(null);
      setShipForm(emptyShipment);
      setShipFormOpen(false);

      await loadMyShipmentsNow({ silent: true });
      await loadExploreShipmentsNow();
    } catch (e) {
      toast.error(String(e?.message || e));
    } finally {
      setShipLoading(false);
    }
  }

  // ✅ LOCK shipment delete if not OPEN (deal made)
  async function removeShipment(id) {
    if (!requireAuth("/carry?tab=shipments")) return;

    const raw = (myShipments || []).find((s) => String(s.id) === String(id));
    const n = raw ? normalizeShipment(raw) : null;

    const lockMsg = shipmentLockReason(n);
    if (lockMsg) {
      toast.error(lockMsg);
      return;
    }

    const ok = await toastConfirm(
      "Delete this shipment? (Only OPEN shipments can be deleted)"
    );
    if (!ok) return;

    try {
      setShipLoading(true);
      const r = await deleteShipment(id);
      if (!r?.ok) throw new Error(r?.error || "Failed");
      toast.success("Deleted ✅");

      await loadMyShipmentsNow({ silent: true });
      await loadExploreShipmentsNow();
    } catch (e) {
      toast.error(String(e?.message || e));
    } finally {
      setShipLoading(false);
    }
  }

  const usableMyShipments = useMemo(() => {
    return (myShipments || []).filter((s) => shipmentIsUsableForRequest(s));
  }, [myShipments]);

  async function openRequest(tripId) {
    if (!requireAuth(`/carry`)) return;

    const list = await loadMyShipmentsNow({ silent: true });
    const usable = (list || []).filter((s) => shipmentIsUsableForRequest(s));

    if (!usable.length) {
      toast.error("You have no usable shipments. Create a new shipment first.");
      setTab("shipments");
      startCreateShipment();
      return;
    }

    setReqTripId(tripId);
    setReqShipmentId(String(usable[0]?.id || ""));
    setReqOfferAmount("");
    setReqNote("");
    setReqOpen(true);
  }

  async function submitRequest() {
    if (!reqTripId) return;

    const shipment_id = Number(reqShipmentId);
    if (!shipment_id) return toast.error("Choose a shipment.");

    const offer_amount = Number(reqOfferAmount);
    if (!Number.isFinite(offer_amount) || offer_amount <= 0)
      return toast.error("Enter a valid offer.");

    const badKey = validateNoPhones({ note: reqNote });
    if (badKey) return toast.error("Phone numbers are not allowed.");

    try {
      setLoading(true);
      const r = await requestMatch(reqTripId, {
        shipment_id,
        offer_amount,
        note: reqNote || "",
      });

      if (!r?.ok) throw new Error(r?.error || r?.data?.error || "Failed");

      // ✅ reflect instantly on this page
      setTrips((prev) =>
        (prev || []).map((t) =>
          String(t.id) === String(reqTripId)
            ? {
                ...t,
                my_request_status: "pending",
                my_request: r?.request || r?.data?.request || null,
                my_request_id:
                  r?.request?.id ||
                  r?.data?.request?.id ||
                  t.my_request_id ||
                  null,
              }
            : t
        )
      );

      toast.success(r?.already ? "Updated offer ✅" : "Request sent ✅");
      setReqOpen(false);

      if (authed) await loadMyShipmentsNow({ silent: true });
      await loadTripsNow();

      nav(`/carry/trips/${reqTripId}`);
    } catch (e) {
      toast.error(String(e?.message || e));
    } finally {
      setLoading(false);
    }
  }

  // initial load
  useEffect(() => {
    loadTripsNow();
    loadExploreShipmentsNow();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // load my shipments on tabs
  useEffect(() => {
    if (tab === "shipments" || tab === "my") {
      if (isAuthed()) loadMyShipmentsNow({ silent: tab === "my" });
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tab]);

  // auto-open request if navigated with state
  useEffect(() => {
    const tripId = loc?.state?.requestTripId;
    if (!tripId) return;
    try {
      window.history.replaceState({}, document.title);
    } catch {}
    openRequest(Number(tripId));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [loc?.state]);

  const exploreItems = useMemo(
    () => [...(trips || []), ...(exploreShipments || [])],
    [trips, exploreShipments]
  );

  const myTrips = useMemo(() => {
    if (!currentUserId) return [];
    return (trips || []).filter(
      (t) => String(t._ownerId ?? "") === String(currentUserId)
    );
  }, [trips, currentUserId]);

  const mySavedList = useMemo(() => {
    const s = saved || new Set();
    const all = [...(trips || []), ...(exploreShipments || [])];
    return all.filter((x) => s.has(String(x.id)));
  }, [saved, trips, exploreShipments]);

  const stats = useMemo(() => {
    const tripsCount = (trips || []).length;
    const shipCount = (exploreShipments || []).length;
    const savedCount = (saved || new Set()).size;
    return { tripsCount, shipCount, savedCount };
  }, [trips, exploreShipments, saved]);

  return (
    <div className="max-w-7xl mx-auto px-4 sm:px-6 py-6">
      <div className="rounded-3xl overflow-hidden border border-slate-200 bg-white shadow-sm">
        {/* HERO */}
        <div className="relative p-6 sm:p-8 bg-gradient-to-r from-slate-950 via-slate-900 to-indigo-950 text-white">
          <div className="flex items-start justify-between gap-4">
            <div className="flex items-center gap-3">
              <div className="w-11 h-11 rounded-2xl bg-white/10 border border-white/15 flex items-center justify-center">
                <Package size={22} />
              </div>
              <div>
                <div className="text-2xl sm:text-3xl font-black tracking-tight">
                  Carry
                </div>
                <div className="text-sm text-white/75 mt-1">
                  Explore • Trips • Shipments • Request → Accept → Chat
                </div>
              </div>
            </div>

            <div className="hidden sm:flex items-center gap-2">
              <button
                disabled={loading}
                onClick={async () => {
                  await loadTripsNow();
                  await loadExploreShipmentsNow();
                  if (authed) await loadMyShipmentsNow({ silent: true });
                }}
                className="px-4 py-2 rounded-2xl bg-white/10 hover:bg-white/15 border border-white/15 text-sm font-extrabold flex items-center gap-2 disabled:opacity-60"
                type="button"
              >
                <RefreshCw size={16} />
                Refresh
              </button>

              <button
                onClick={startCreateTrip}
                className="px-4 py-2 rounded-2xl bg-white text-slate-900 hover:bg-slate-100 text-sm font-extrabold flex items-center gap-2"
                type="button"
              >
                <Plus size={16} />
                New Trip
              </button>

              <button
                onClick={() => {
                  if (!requireAuth("/carry?tab=shipments")) return;
                  startCreateShipment();
                  setTab("shipments");
                }}
                className="px-4 py-2 rounded-2xl bg-white/10 hover:bg-white/15 border border-white/15 text-sm font-extrabold flex items-center gap-2"
                type="button"
              >
                <Package size={16} />
                New Shipment
              </button>

              {!authed ? (
                <button
                  onClick={() => goLogin("/carry")}
                  className="px-4 py-2 rounded-2xl bg-white/10 hover:bg-white/15 border border-white/15 text-sm font-extrabold flex items-center gap-2"
                  type="button"
                >
                  <Lock size={16} />
                  Login
                </button>
              ) : null}
            </div>
          </div>

          <div className="mt-6 grid grid-cols-2 sm:grid-cols-4 gap-3">
            <div className="rounded-2xl bg-white/10 border border-white/15 p-3">
              <div className="text-[11px] text-white/70 font-bold">Trips</div>
              <div className="text-lg font-black">{stats.tripsCount}</div>
            </div>
            <div className="rounded-2xl bg-white/10 border border-white/15 p-3">
              <div className="text-[11px] text-white/70 font-bold">
                Shipments
              </div>
              <div className="text-lg font-black">{stats.shipCount}</div>
            </div>
            <div className="rounded-2xl bg-white/10 border border-white/15 p-3">
              <div className="text-[11px] text-white/70 font-bold">Saved</div>
              <div className="text-lg font-black">{stats.savedCount}</div>
            </div>
            <div className="rounded-2xl bg-white/10 border border-white/15 p-3">
              <div className="text-[11px] text-white/70 font-bold">Account</div>
              <div className="text-lg font-black">
                {currentUserId ? `#${currentUserId}` : "Guest"}
              </div>
            </div>
          </div>

          <div className="mt-6 flex flex-wrap gap-2">
            {TABS.map((t) => {
              const Icon = t.icon;
              return (
                <button
                  key={t.key}
                  onClick={() => setTab(t.key)}
                  className={cn(
                    "px-4 py-2 rounded-2xl text-sm font-extrabold border transition-all inline-flex items-center gap-2",
                    tab === t.key
                      ? "bg-white text-slate-900 border-white"
                      : "bg-white/10 text-white border-white/15 hover:bg-white/15"
                  )}
                  type="button"
                >
                  <Icon size={16} />
                  {t.label}
                </button>
              );
            })}
          </div>
        </div>

        {/* SEARCH / FILTER BAR */}
        <div className="p-4 sm:p-6 border-b border-slate-100 bg-white">
          <div className="flex flex-col lg:flex-row gap-3 lg:items-center lg:justify-between">
            <div className="flex flex-col sm:flex-row gap-2 flex-1">
              <div className="flex-1 relative">
                <Search
                  size={18}
                  className="absolute left-3 top-1/2 -translate-y-1/2 text-slate-400"
                />
                <input
                  value={q}
                  onChange={(e) => setQ(e.target.value)}
                  placeholder="Search (airport/flight)…"
                  className="w-full pl-10 pr-3 py-2.5 rounded-2xl border border-slate-200 bg-white focus:outline-none focus:ring-2 focus:ring-indigo-200"
                />
              </div>

              <button
                onClick={() => setFiltersOpen(true)}
                className="px-4 py-2.5 rounded-2xl border border-slate-200 bg-white hover:bg-slate-50 text-sm font-extrabold flex items-center gap-2"
                type="button"
              >
                <Filter size={16} />
                Filters
              </button>

              <button
                disabled={loading}
                onClick={async () => {
                  await loadTripsNow();
                  await loadExploreShipmentsNow();
                }}
                className="px-5 py-2.5 rounded-2xl bg-slate-900 text-white hover:bg-slate-800 text-sm font-extrabold disabled:opacity-60"
                type="button"
              >
                Search
              </button>
            </div>

            <div className="flex gap-2">
              <button
                onClick={() => {
                  if (!requireAuth("/carry")) return;
                  startCreateTrip();
                }}
                className="px-4 py-2.5 rounded-2xl border border-slate-200 bg-white hover:bg-slate-50 text-sm font-extrabold flex items-center gap-2"
                type="button"
              >
                <Plus size={16} />
                Trip
              </button>
              <button
                onClick={() => {
                  if (!requireAuth("/carry?tab=shipments")) return;
                  startCreateShipment();
                  setTab("shipments");
                }}
                className="px-4 py-2.5 rounded-2xl border border-slate-200 bg-white hover:bg-slate-50 text-sm font-extrabold flex items-center gap-2"
                type="button"
              >
                <Package size={16} />
                Shipment
              </button>
            </div>
          </div>

          {!authed ? (
            <div className="mt-3 rounded-2xl border border-amber-200 bg-amber-50 p-3 text-sm text-amber-900 flex items-center justify-between gap-3">
              <div className="flex items-center gap-2">
                <Lock size={18} />
                Guest can browse only. Login to request, save, create, edit,
                delete or chat.
              </div>
              <button
                onClick={() => goLogin("/carry")}
                className="px-4 py-2 rounded-xl bg-slate-900 text-white font-extrabold inline-flex items-center gap-2"
                type="button"
              >
                <LogIn size={16} />
                Login
              </button>
            </div>
          ) : null}
        </div>

        {/* BODY */}
        <div className="p-4 sm:p-6">
          {tab === "explore" ? (
            <ExploreGrid
              loading={loading}
              items={exploreItems}
              saved={saved}
              currentUserId={currentUserId}
              getRequestState={getRequestState}
              onOpenTrip={(id) => goTripDetails(id)}
              onOpenShipment={(sh) => goShipmentDetails(sh.id)}
              onRequest={(tripId) => openRequest(tripId)}
              onSave={(id) => toggleSaved(id)}
              onEditShipment={(s) => startEditShipment(s)}
              onDeleteShipment={(id) => removeShipment(id)}
              onEditTrip={(t) => startEditTrip(t)}
              onDeleteTrip={(id) => deleteTrip(id)}
              onLogin={() => goLogin("/carry")}
            />
          ) : null}

          {tab === "trips" ? (
            <TripsGrid
              loading={loading}
              trips={trips}
              saved={saved}
              currentUserId={currentUserId}
              getRequestState={getRequestState}
              onOpenTrip={(id) => goTripDetails(id)}
              onRequest={(tripId) => openRequest(tripId)}
              onSave={(id) => toggleSaved(id)}
              onEdit={(it) => startEditTrip(it)}
              onDelete={(id) => deleteTrip(id)}
              onCreate={startCreateTrip}
              onLogin={() => goLogin("/carry")}
              authed={authed}
            />
          ) : null}

          {tab === "shipments" ? (
            <ShipmentsGrid
              authed={authed}
              shipLoading={shipLoading}
              myShipments={myShipments}
              currentUserId={currentUserId}
              onCreate={startCreateShipment}
              onEdit={(s) => startEditShipment(s)}
              onDelete={(id) => removeShipment(id)}
              onOpenShipment={(id) => goShipmentDetails(id)}
              onLogin={() => goLogin("/carry?tab=shipments")}
            />
          ) : null}

          {tab === "my" ? (
            <MyGrid
              authed={authed}
              myTrips={myTrips}
              myShipments={myShipments}
              savedList={mySavedList}
              currentUserId={currentUserId}
              saved={saved}
              getRequestState={getRequestState}
              onOpenTrip={(id) => goTripDetails(id)}
              onOpenShipment={(sh) => goShipmentDetails(sh.id)}
              onRequest={(tripId) => openRequest(tripId)}
              onSave={(id) => toggleSaved(id)}
              onEditTrip={(it) => startEditTrip(it)}
              onDeleteTrip={(id) => deleteTrip(id)}
              onCreateTrip={startCreateTrip}
              onCreateShipment={() => {
                startCreateShipment();
                setTab("shipments");
              }}
              onEditShipment={(s) => startEditShipment(s)}
              onDeleteShipment={(id) => removeShipment(id)}
              onLogin={() => goLogin("/carry")}
            />
          ) : null}
        </div>
      </div>

      {/* FILTERS MODAL */}
      <Modal
        open={filtersOpen}
        title="Filters"
        onClose={() => setFiltersOpen(false)}
      >
        <div className="space-y-3">
          <div className="grid sm:grid-cols-2 gap-3">
            <AirportPicker
              label="From airport"
              valueCode={fromAirport}
              onPick={(a) => setFromAirport(a?.code || "")}
            />
            <AirportPicker
              label="To airport"
              valueCode={toAirport}
              onPick={(a) => setToAirport(a?.code || "")}
            />
          </div>

          <div className="grid sm:grid-cols-2 gap-3">
            <Field label="Date from" hint="optional">
              <input
                type="date"
                value={dateFrom}
                onChange={(e) => setDateFrom(e.target.value)}
                className="w-full px-4 py-4 rounded-2xl border border-slate-200 bg-white"
              />
            </Field>
            <Field label="Date to" hint="optional">
              <input
                type="date"
                value={dateTo}
                onChange={(e) => setDateTo(e.target.value)}
                className="w-full px-4 py-4 rounded-2xl border border-slate-200 bg-white"
              />
            </Field>
          </div>

          <Field label="Min weight (kg)">
            <input
              type="number"
              value={minWeight}
              onChange={(e) => setMinWeight(e.target.value)}
              className="w-full px-4 py-4 rounded-2xl border border-slate-200 bg-white"
            />
          </Field>

          <div className="flex gap-2 pt-2">
            <button
              onClick={() => {
                setFromAirport("");
                setToAirport("");
                setDateFrom("");
                setDateTo("");
                setMinWeight("");
              }}
              className="flex-1 px-4 py-4 rounded-2xl border border-slate-200 bg-white hover:bg-slate-50 font-extrabold"
              type="button"
            >
              Reset
            </button>
            <button
              onClick={async () => {
                setFiltersOpen(false);
                await loadTripsNow();
                await loadExploreShipmentsNow();
              }}
              className="flex-1 px-4 py-4 rounded-2xl bg-slate-900 text-white hover:bg-slate-800 font-extrabold"
              type="button"
            >
              Apply
            </button>
          </div>
        </div>
      </Modal>

      {/* TRIP FORM MODAL */}
      <Modal
        open={tripFormOpen}
        wide
        title={tripEditId ? `Edit Trip #${tripEditId}` : "Create Trip"}
        onClose={() => setTripFormOpen(false)}
      >
        <div className="grid lg:grid-cols-2 gap-8">
          <div className="space-y-4">
            <div className="text-sm font-black text-slate-900">
              Route (Airports)
            </div>

            <div className="grid grid-cols-1 gap-4">
              <AirportPicker
                label="From airport"
                valueCode={tripForm.from_airport_code}
                onPick={(a) =>
                  setTripForm((s) => ({
                    ...s,
                    from_airport_code: a?.code || "",
                  }))
                }
              />
              <AirportPicker
                label="To airport"
                valueCode={tripForm.to_airport_code}
                onPick={(a) =>
                  setTripForm((s) => ({ ...s, to_airport_code: a?.code || "" }))
                }
              />
            </div>

            <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
              <Field label="Travel date (departure)">
                <input
                  type="datetime-local"
                  value={tripForm.travel_date}
                  onChange={(e) =>
                    setTripForm((s) => ({ ...s, travel_date: e.target.value }))
                  }
                  className="w-full px-4 py-4 rounded-2xl border border-slate-200 bg-white"
                />
              </Field>
              <Field label="Arrival date (required)">
                <input
                  type="datetime-local"
                  value={tripForm.arrival_date}
                  onChange={(e) =>
                    setTripForm((s) => ({ ...s, arrival_date: e.target.value }))
                  }
                  className="w-full px-4 py-4 rounded-2xl border border-slate-200 bg-white"
                />
              </Field>
            </div>

            <div className="rounded-3xl border border-slate-200 bg-slate-50 p-5 space-y-4">
              <div className="text-sm font-black text-slate-900">
                Traveler details (Required)
              </div>

              <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
                <Field label="Airline">
                  <input
                    value={
                      tripForm?.airline ??
                      tripForm?.data?.airline ??
                      tripForm?.traveler_airline ??
                      ""
                    }
                    onChange={(e) => {
                      const v = e.target.value;
                      setTripForm((s) => ({
                        ...s,
                        airline: v,
                        traveler_airline: v,
                        data: {
                          ...(s?.data || {}),
                          airline: v,
                          traveler_airline: v,
                        },
                      }));
                    }}
                    className="w-full px-4 py-4 rounded-2xl border border-slate-200 bg-white"
                  />
                </Field>
                <Field label="Flight number">
                  <input
                    value={tripForm.flight_number}
                    onChange={(e) =>
                      setTripForm((s) => ({
                        ...s,
                        flight_number: e.target.value,
                      }))
                    }
                    className="w-full px-4 py-4 rounded-2xl border border-slate-200 bg-white"
                  />
                </Field>
              </div>

              <Field label="Meet preference">
                <select
                  value={tripForm.meet_pref}
                  onChange={(e) =>
                    setTripForm((s) => ({
                      ...s,
                      meet_pref: e.target.value,
                      meet_place:
                        e.target.value === "nearby" ? s.meet_place : "",
                    }))
                  }
                  className="w-full px-4 py-4 rounded-2xl border border-slate-200 bg-white text-sm font-bold"
                >
                  <option value="airport">At airport</option>
                  <option value="nearby">Nearby</option>
                  <option value="city">In city</option>
                </select>
              </Field>

              {tripForm.meet_pref === "nearby" ? (
                <Field label="Nearby meeting places" hint="required">
                  <input
                    value={tripForm.meet_place}
                    onChange={(e) =>
                      setTripForm((s) => ({ ...s, meet_place: e.target.value }))
                    }
                    className="w-full px-4 py-4 rounded-2xl border border-slate-200 bg-white"
                    placeholder="e.g. Tysons Corner, Pentagon City…"
                  />
                </Field>
              ) : null}
            </div>

            <div className="rounded-3xl border border-slate-200 bg-slate-50 p-5 space-y-4">
              <div className="text-sm font-black text-slate-900">
                Admin-only documents
              </div>

              <Field label="Passport photo" hint="image">
                <input
                  type="file"
                  accept="image/*"
                  onChange={async (e) => {
                    const f = e.target.files?.[0] || null;
                    if (!f) return;
                    if (f.size > 2.2 * 1024 * 1024) {
                      toast.error("Passport image too large (max ~2MB).");
                      e.target.value = "";
                      return;
                    }
                    try {
                      const url = await readFileAsDataURL(f);
                      setTripForm((s) => ({ ...s, passport_image: url }));
                    } catch {
                      toast.error("Failed to read passport image.");
                    }
                  }}
                  className="w-full px-4 py-4 rounded-2xl border border-slate-200 bg-white"
                />
              </Field>

              <Field label="Visa photo" hint="image">
                <input
                  type="file"
                  accept="image/*"
                  onChange={async (e) => {
                    const f = e.target.files?.[0] || null;
                    if (!f) return;
                    if (f.size > 2.2 * 1024 * 1024) {
                      toast.error("Visa image too large (max ~2MB).");
                      e.target.value = "";
                      return;
                    }
                    try {
                      const url = await readFileAsDataURL(f);
                      setTripForm((s) => ({ ...s, visa_image: url }));
                    } catch {
                      toast.error("Failed to read visa image.");
                    }
                  }}
                  className="w-full px-4 py-4 rounded-2xl border border-slate-200 bg-white"
                />
              </Field>
            </div>
          </div>

          <div className="space-y-4">
            <div className="text-sm font-black text-slate-900">Capacity</div>

            <Field label="Available weight (kg)">
              <input
                type="number"
                value={tripForm.available_weight}
                onChange={(e) =>
                  setTripForm((s) => ({
                    ...s,
                    available_weight: e.target.value,
                  }))
                }
                className="w-full px-4 py-4 rounded-2xl border border-slate-200 bg-white"
              />
            </Field>

            <div className="rounded-3xl border border-slate-200 bg-slate-50 p-5 space-y-4">
              <div className="text-sm font-black text-slate-900">
                Items I won’t carry
              </div>

              <TogglePills
                value={tripForm.no_carry}
                onChange={(arr) =>
                  setTripForm((s) => ({ ...s, no_carry: arr }))
                }
              />
            </div>

            <Field label="Description" hint="no phone numbers">
              <textarea
                value={tripForm.description}
                onChange={(e) =>
                  setTripForm((s) => ({ ...s, description: e.target.value }))
                }
                className="w-full px-4 py-4 rounded-2xl border border-slate-200 bg-white min-h-[260px]"
                placeholder="Rules, restrictions, meet notes…"
              />
            </Field>

            <div className="flex gap-3 pt-2">
              <button
                disabled={loading}
                onClick={() => {
                  setTripEditId(null);
                  setTripForm(emptyTrip);
                }}
                className="flex-1 px-5 py-4 rounded-2xl border border-slate-200 bg-white hover:bg-slate-50 font-extrabold disabled:opacity-60"
                type="button"
              >
                Reset
              </button>

              <button
                disabled={loading}
                onClick={submitTrip}
                className="flex-1 px-5 py-4 rounded-2xl bg-slate-900 text-white hover:bg-slate-800 font-extrabold disabled:opacity-60"
                type="button"
              >
                {tripEditId ? "Update Trip" : "Create Trip"}
              </button>
            </div>
          </div>
        </div>
      </Modal>

      {/* SHIPMENT FORM MODAL */}
      <Modal
        open={shipFormOpen}
        wide
        title={shipEditId ? `Edit Shipment #${shipEditId}` : "Create Shipment"}
        onClose={() => setShipFormOpen(false)}
      >
        <div className="grid lg:grid-cols-2 gap-8">
          <div className="space-y-4">
            <div className="text-sm font-black text-slate-900">
              Route (Airports)
            </div>

            <div className="grid grid-cols-1 gap-4">
              <AirportPicker
                label="From airport"
                valueCode={shipForm.from_airport_code}
                onPick={(a) =>
                  setShipForm((s) => ({
                    ...s,
                    from_airport_code: a?.code || "",
                    from_country: a?.country || s.from_country || "",
                    from_city: a?.city || s.from_city || "",
                  }))
                }
              />
              <AirportPicker
                label="To airport"
                valueCode={shipForm.to_airport_code}
                onPick={(a) =>
                  setShipForm((s) => ({
                    ...s,
                    to_airport_code: a?.code || "",
                    to_country: a?.country || s.to_country || "",
                    to_city: a?.city || s.to_city || "",
                  }))
                }
              />
            </div>

            <div className="grid grid-cols-2 gap-3">
              <div className="rounded-2xl border border-slate-200 bg-slate-50 p-3">
                <div className="text-[11px] font-extrabold text-slate-500">
                  From city
                </div>
                <div dir="ltr" className="text-sm font-black text-slate-900">
                  {shipForm.from_city || "—"}
                </div>
              </div>
              <div className="rounded-2xl border border-slate-200 bg-slate-50 p-3">
                <div className="text-[11px] font-extrabold text-slate-500">
                  To city
                </div>
                <div dir="ltr" className="text-sm font-black text-slate-900">
                  {shipForm.to_city || "—"}
                </div>
              </div>
            </div>

            <Field label="Deliver before (deadline)" hint="required">
              <input
                type="date"
                value={shipForm.deadline}
                onChange={(e) =>
                  setShipForm((s) => ({ ...s, deadline: e.target.value }))
                }
                className="w-full px-4 py-4 rounded-2xl border border-slate-200 bg-white"
              />
            </Field>

            <div className="rounded-3xl border border-slate-200 bg-slate-50 p-5 space-y-4">
              <div className="text-sm font-black text-slate-900">Item</div>

              <Field label="Title" hint="required">
                <input
                  value={shipForm.item_title}
                  onChange={(e) =>
                    setShipForm((s) => ({ ...s, item_title: e.target.value }))
                  }
                  className="w-full px-4 py-4 rounded-2xl border border-slate-200 bg-white"
                  placeholder="e.g. Shoes, iPhone, Documents…"
                />
              </Field>

              <Field label="Product link" hint="required">
                <input
                  value={shipForm.product_url}
                  onChange={(e) =>
                    setShipForm((s) => ({ ...s, product_url: e.target.value }))
                  }
                  className="w-full px-4 py-4 rounded-2xl border border-slate-200 bg-white"
                  placeholder="https://example.com/product"
                  dir="ltr"
                />
              </Field>

              <Field label="Description" hint="required • no phone numbers">
                <textarea
                  value={shipForm.item_desc}
                  onChange={(e) =>
                    setShipForm((s) => ({ ...s, item_desc: e.target.value }))
                  }
                  className="w-full px-4 py-4 rounded-2xl border border-slate-200 bg-white min-h-[160px]"
                />
              </Field>

              <Field label="Category" hint="required">
                <select
                  value={shipForm.category || "other"}
                  onChange={(e) =>
                    setShipForm((s) => ({ ...s, category: e.target.value }))
                  }
                  className="w-full px-4 py-4 rounded-2xl border border-slate-200 bg-white text-sm font-bold"
                >
                  {ITEM_CATEGORIES.map((c) => (
                    <option key={c.id} value={c.id}>
                      {c.label}
                    </option>
                  ))}
                </select>
              </Field>

              <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
                <Field label="Weight (kg)" hint="required">
                  <input
                    type="number"
                    value={shipForm.item_weight}
                    dir="ltr"
                    onChange={(e) =>
                      setShipForm((s) => ({
                        ...s,
                        item_weight: e.target.value,
                      }))
                    }
                    className="w-full px-4 py-4 rounded-2xl border border-slate-200 bg-white"
                  />
                </Field>

                <Field label="Budget (USD)" hint="required">
                  <input
                    type="number"
                    value={shipForm.budget_amount}
                    dir="ltr"
                    onChange={(e) =>
                      setShipForm((s) => ({
                        ...s,
                        budget_amount: e.target.value,
                      }))
                    }
                    className="w-full px-4 py-4 rounded-2xl border border-slate-200 bg-white"
                    placeholder="Amount"
                  />
                </Field>
              </div>

              <Field label="Item image" hint="required (URL or upload)">
                <div className="space-y-2">
                  <input
                    value={shipForm.item_image}
                    onChange={(e) =>
                      setShipForm((s) => ({ ...s, item_image: e.target.value }))
                    }
                    className="w-full px-4 py-4 rounded-2xl border border-slate-200 bg-white"
                    placeholder="https://..."
                    dir="ltr"
                  />
                  <input
                    type="file"
                    accept="image/*"
                    onChange={async (e) => {
                      const f = e.target.files?.[0] || null;
                      if (!f) return;
                      if (f.size > 2.2 * 1024 * 1024) {
                        toast.error("Image too large (max ~2MB).");
                        e.target.value = "";
                        return;
                      }
                      try {
                        const url = await readFileAsDataURL(f);
                        setShipForm((s) => ({ ...s, item_image: url }));
                      } catch {
                        toast.error("Failed to read image.");
                      }
                    }}
                    className="w-full px-4 py-4 rounded-2xl border border-slate-200 bg-white"
                  />
                </div>
              </Field>
            </div>
          </div>

          <div className="space-y-4">
            <div className="rounded-3xl border border-slate-200 bg-white p-5 space-y-3">
              <div className="text-sm font-black text-slate-900">
                Quick preview
              </div>

              <div className="text-xs text-slate-500 font-bold">
                This is just a preview card (same component used in lists).
              </div>

              <CarryCard
                kind="shipment"
                it={normalizeShipment({
                  ...shipForm,
                  from_airport_code: shipForm.from_airport_code,
                  to_airport_code: shipForm.to_airport_code,
                  item_image: shipForm.item_image,
                  product_url: shipForm.product_url,
                })}
                saved={new Set()}
                currentUserId={currentUserId}
                loading={shipLoading}
                flags={{
                  showSave: false,
                  showPrimary: false,
                  showDetails: false,
                  showOwnerActions: false,
                  showImage: true,
                }}
                onOpen={() => {}}
              />
            </div>

            <div className="flex gap-3 pt-2">
              <button
                disabled={shipLoading}
                onClick={() => {
                  setShipEditId(null);
                  setShipForm(emptyShipment);
                }}
                className="flex-1 px-5 py-4 rounded-2xl border border-slate-200 bg-white hover:bg-slate-50 font-extrabold disabled:opacity-60"
                type="button"
              >
                Reset
              </button>

              <button
                disabled={shipLoading}
                onClick={submitShipment}
                className="flex-1 px-5 py-4 rounded-2xl bg-slate-900 text-white hover:bg-slate-800 font-extrabold disabled:opacity-60"
                type="button"
              >
                {shipEditId ? "Update Shipment" : "Create Shipment"}
              </button>
            </div>
          </div>
        </div>
      </Modal>

      {/* REQUEST MODAL */}
      <Modal
        open={reqOpen}
        title="Send request"
        onClose={() => setReqOpen(false)}
      >
        <div className="space-y-4">
          <div className="rounded-2xl border border-slate-200 bg-slate-50 p-4">
            <div className="text-sm font-black text-slate-900">
              Step 1: Choose shipment
            </div>
            <div className="text-xs text-slate-500 font-bold mt-1">
              Only <span className="font-black">OPEN</span> shipments can be
              used. Matched/Completed shipments are hidden.
            </div>

            <div className="mt-3">
              <select
                value={reqShipmentId}
                onChange={(e) => setReqShipmentId(e.target.value)}
                className="w-full px-4 py-4 rounded-2xl border border-slate-200 bg-white text-sm font-bold"
              >
                {(usableMyShipments || []).map((s) => (
                  <option key={s.id} value={String(s.id)}>
                    #{s.id} — {normalizeShipment(s).item_title || "Shipment"}
                  </option>
                ))}
              </select>
            </div>
          </div>

          <div className="rounded-2xl border border-slate-200 bg-white p-4">
            <div className="text-sm font-black text-slate-900">
              Step 2: Offer
            </div>

            <div className="mt-3">
              <Field label="Offer (USD)">
                <input
                  type="number"
                  value={reqOfferAmount}
                  onChange={(e) => setReqOfferAmount(e.target.value)}
                  className="w-full px-4 py-4 rounded-2xl border border-slate-200 bg-white"
                  placeholder="Amount in USD"
                />
              </Field>
            </div>

            <div className="mt-3">
              <textarea
                value={reqNote}
                onChange={(e) => setReqNote(e.target.value)}
                className="w-full px-4 py-4 rounded-2xl border border-slate-200 bg-white min-h-[120px]"
                placeholder="Optional note (no phone numbers)…"
              />
            </div>
          </div>

          <div className="flex gap-3">
            <button
              onClick={() => setReqOpen(false)}
              className="flex-1 px-5 py-4 rounded-2xl border border-slate-200 bg-white hover:bg-slate-50 font-extrabold"
              type="button"
            >
              Cancel
            </button>
            <button
              disabled={loading}
              onClick={submitRequest}
              className="flex-1 px-5 py-4 rounded-2xl bg-slate-900 text-white hover:bg-slate-800 font-extrabold disabled:opacity-60 inline-flex items-center justify-center gap-2"
              type="button"
            >
              <SendHorizontal size={16} />
              Send request
            </button>
          </div>
        </div>
      </Modal>
    </div>
  );
}
