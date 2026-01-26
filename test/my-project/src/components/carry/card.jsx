// src/components/carry/card.jsx
import React from "react";
import {
  Package,
  Plane,
  Send,
  MapPin,
  Calendar,
  Weight,
  ArrowRight,
  Sparkles,
  LogIn,
  Hourglass,
  Trash2,
  Pencil,
  Heart,
  ShieldCheck,
  CheckCircle2,
  XCircle,
  User,
  BadgeCheck,
  Link as LinkIcon,
  Clock,
  Tag,
  Globe,
} from "lucide-react";

/* =========================
   Helpers
========================= */
const cn = (...a) => a.filter(Boolean).join(" ");
const fmtDT = (v) => (!v ? "—" : String(v).replace("T", " ").replace("Z", ""));

const CATEGORY_LABELS = {
  mobile_accessories: "Mobile accessories",
  phones_tablets: "Phones & tablets",
  laptops: "Laptops",
  computer_accessories: "Computer accessories",
  electronics_other: "Electronics (other)",
  clothing: "Clothing",
  shoes: "Shoes",
  bags: "Bags",
  cosmetics: "Cosmetics / skincare",
  supplements: "Supplements (OTC)",
  medicines_otc: "Medicines (OTC)",
  books: "Books",
  documents: "Documents",
  toys: "Toys",
  gifts: "Gifts",
  other: "Other",
};
const categoryLabel = (v) => CATEGORY_LABELS[String(v || "").trim()] || "Other";

const PROHIBITED_LABELS = {
  weapons: "Weapons / ammo",
  explosives: "Explosives / fireworks",
  alcohol: "Alcohol",
  illegal_drugs: "Illegal drugs",
  vapes: "Vapes / nicotine",
  hazmat: "Hazardous materials (hazmat)",
  liquids_bulk: "Large liquids (bulk)",
  lithium_loose: "Loose lithium batteries",
  aerosols: "Perfume / aerosols",
  perishable_food: "Perishable food",
  cash_giftcards: "Cash / gift cards",
  sensitive_docs: "Sensitive documents",
  prescription_meds: "Prescription meds (restricted)",
};

function isEmpty(v) {
  return v === null || v === undefined || String(v).trim() === "";
}
function pickFirstNonEmpty(it, key, fallback = "") {
  const candidates = [
    it?.[key],
    it?.data?.[key],
    it?.payload?.[key],
    it?.meta?.[key],
  ];
  for (const v of candidates) if (!isEmpty(v)) return v;
  return fallback;
}
function pick(it, key, fallback = "") {
  return pickFirstNonEmpty(it, key, fallback);
}
function pickStr(it, key, fallback = "") {
  const v = pickFirstNonEmpty(it, key, fallback);
  return isEmpty(v) ? "" : String(v).trim();
}
function pickUpper(it, key, fallback = "") {
  const v = pickStr(it, key, fallback);
  return v ? v.toUpperCase() : "";
}

function normStatus(v) {
  return String(v || "open")
    .trim()
    .toLowerCase();
}
function prettyStatus(s) {
  const st = normStatus(s);
  if (st === "open") return "Open";
  if (st === "pending") return "Pending";
  if (st === "accepted") return "Accepted";
  if (st === "rejected") return "Rejected";
  if (st === "cancelled" || st === "canceled") return "Cancelled";
  if (st === "matched") return "Matched";
  if (st === "in_transit") return "In transit";
  if (st === "delivered") return "Delivered";
  if (st === "completed") return "Completed";
  return st;
}

function statusTone(st) {
  const s = normStatus(st);
  if (["accepted", "completed", "delivered"].includes(s)) return "emerald";
  if (["pending", "counter", "counter_offer"].includes(s)) return "amber";
  if (["rejected", "cancelled", "canceled"].includes(s)) return "rose";
  if (["matched", "in_transit"].includes(s)) return "amber";
  return "slate";
}

function OwnerLine({ ownerId, verified }) {
  return (
    <div className="flex items-center gap-2 text-xs text-slate-500 font-semibold">
      <span className="inline-flex items-center gap-1">
        <User size={12} /> Owner #{ownerId ?? "—"}
      </span>
      {verified ? (
        <span className="inline-flex items-center gap-1 text-emerald-700">
          <ShieldCheck size={12} /> Verified
        </span>
      ) : null}
    </div>
  );
}

function clampText(v, max = 120) {
  const s = String(v || "").trim();
  if (!s) return "";
  return s.length > max ? s.slice(0, max - 1) + "…" : s;
}

function safeHref(url) {
  const u = String(url || "").trim();
  if (!u) return "";
  if (u.startsWith("http://") || u.startsWith("https://")) return u;
  // allow user paste without scheme
  if (u.includes(".") && !u.startsWith("javascript:")) return `https://${u}`;
  return "";
}

/* =========================
   UI atoms
========================= */
export function Pill({ tone = "slate", children }) {
  const map = {
    slate: "bg-slate-100 text-slate-700 ring-slate-200",
    indigo: "bg-indigo-50 text-indigo-700 ring-indigo-100",
    emerald: "bg-emerald-50 text-emerald-700 ring-emerald-100",
    amber: "bg-amber-50 text-amber-800 ring-amber-100",
    rose: "bg-rose-50 text-rose-700 ring-rose-100",
    black: "bg-slate-900 text-white ring-slate-900",
  };
  return (
    <span
      className={cn(
        "inline-flex items-center gap-1 px-2.5 py-1 rounded-full text-xs font-extrabold ring-1",
        map[tone] || map.slate
      )}
    >
      {children}
    </span>
  );
}

/* =========================
   CarryCard (wrapper)
========================= */
export function CarryCard({
  kind, // "trip" | "shipment"
  it,
  saved,
  currentUserId,
  loading,
  requestState,
  flags = {
    showSave: true,
    showPrimary: true,
    showDetails: true,
    showOwnerActions: true,
    showImage: true,
  },
  onOpen,
  onRequest,
  onSave,
  onEdit,
  onDelete,
  onLogin,
}) {
  if (kind === "shipment") {
    return (
      <ShipmentCard
        it={it}
        saved={saved}
        currentUserId={currentUserId}
        flags={flags}
        onOpen={onOpen}
        onSave={onSave}
        onEdit={onEdit}
        onDelete={onDelete}
        onLogin={onLogin}
      />
    );
  }

  return (
    <TripCard
      it={it}
      saved={saved}
      currentUserId={currentUserId}
      loading={loading}
      requestState={requestState}
      flags={flags}
      onOpen={onOpen}
      onRequest={onRequest}
      onSave={onSave}
      onEdit={onEdit}
      onDelete={onDelete}
      onLogin={onLogin}
    />
  );
}

/* =========================
   TripCard
========================= */
export function TripCard({
  it,
  saved,
  currentUserId,
  loading,
  requestState,
  flags,
  onOpen,
  onRequest,
  onSave,
  onEdit,
  onDelete,
  onLogin,
}) {
  const ownerId =
    it?._ownerId ?? it?.owner_id ?? it?.user_id ?? it?.created_by ?? null;

  const authed = !!currentUserId;
  const owner = authed && String(ownerId) === String(currentUserId);

  const tripStatus = normStatus(it?.status || "open");

  const fromCode =
    pickUpper(it, "from_airport_code") ||
    pickUpper(it, "from_airport") ||
    pickUpper(it, "from_iata") ||
    pickUpper(it, "from_code");

  const toCode =
    pickUpper(it, "to_airport_code") ||
    pickUpper(it, "to_airport") ||
    pickUpper(it, "to_iata") ||
    pickUpper(it, "to_code");

  const fromCity = pickStr(it, "from_city");
  const fromCountry = pickStr(it, "from_country");
  const toCity = pickStr(it, "to_city");
  const toCountry = pickStr(it, "to_country");

  const fromLabel =
    fromCode || [fromCity, fromCountry].filter(Boolean).join(", ") || "—";
  const toLabel =
    toCode || [toCity, toCountry].filter(Boolean).join(", ") || "—";

  const travelDate = fmtDT(
    pickStr(it, "travel_date") || pickStr(it, "date") || ""
  );
  const arrivalDate = fmtDT(pickStr(it, "arrival_date") || "");

  const airline = pickStr(it, "traveler_airline") || pickStr(it, "airline");
  const flight =
    pickStr(it, "traveler_flight_number") ||
    pickStr(it, "flight_number") ||
    pickStr(it, "flight_no");

  const weightVal = pick(it, "available_weight", null);
  const weightText =
    weightVal === null || weightVal === undefined || weightVal === ""
      ? "—"
      : `${weightVal} kg`;

  const noCarryRaw = pick(it, "no_carry", []);
  const noCarry = Array.isArray(noCarryRaw) ? noCarryRaw : [];

  const savedOn = saved?.has?.(String(it?.id));

  const requestDisabled =
    loading ||
    owner ||
    (authed &&
      (requestState === "pending" ||
        requestState === "accepted" ||
        requestState === "counter_offer"));

  const primary = !authed
    ? {
        key: "login",
        label: "Login",
        icon: LogIn,
        onClick: onLogin,
        tone: "indigo",
      }
    : owner
    ? {
        key: "owner",
        label: "Your trip",
        icon: User,
        onClick: null,
        tone: "slate",
      }
    : requestState === "pending"
    ? {
        key: "pending",
        label: "Pending",
        icon: Hourglass,
        onClick: null,
        tone: "amber",
      }
    : requestState === "accepted"
    ? {
        key: "accepted",
        label: "Accepted",
        icon: CheckCircle2,
        onClick: onOpen,
        tone: "emerald",
      }
    : requestState === "counter_offer"
    ? {
        key: "counter",
        label: "Counter",
        icon: Sparkles,
        onClick: onOpen,
        tone: "amber",
      }
    : requestState === "rejected"
    ? {
        key: "rejected",
        label: "Rejected",
        icon: XCircle,
        onClick: onOpen,
        tone: "rose",
      }
    : {
        key: "request",
        label: "Request",
        icon: Send,
        onClick: onRequest,
        tone: "indigo",
      };

  const PrimaryIcon = primary.icon;

  const showDetails = !!flags?.showDetails;
  const showPrimary = !!flags?.showPrimary;
  const showSave = !!flags?.showSave;
  const showOwnerActions = !!flags?.showOwnerActions;

  const actionsCols =
    showDetails && showPrimary ? "grid-cols-2" : "grid-cols-1";

  const primaryClass =
    primary.tone === "emerald"
      ? "border-emerald-200 bg-emerald-600 text-white hover:bg-emerald-700"
      : primary.tone === "amber"
      ? "border-amber-200 bg-amber-50 text-amber-800 hover:bg-amber-100"
      : primary.tone === "rose"
      ? "border-rose-200 bg-rose-50 text-rose-700 hover:bg-rose-100"
      : primary.tone === "slate"
      ? "border-slate-200 bg-white text-slate-400"
      : "border-indigo-200 bg-indigo-600 text-white hover:bg-indigo-700";

  return (
    <div className="group rounded-3xl border border-slate-200 bg-white shadow-sm overflow-hidden transition-all hover:shadow-md">
      <div className="p-4 pb-3 flex items-start justify-between gap-3">
        <div className="min-w-0">
          <div className="flex flex-wrap items-center gap-2">
            <Pill tone="indigo">
              <Plane size={14} /> Trip
            </Pill>
            <Pill
              tone={tripStatus === "open" ? "emerald" : statusTone(tripStatus)}
            >
              {prettyStatus(tripStatus)}
            </Pill>
          </div>

          <div className="mt-2 text-base font-extrabold text-slate-900">
            Trip #{it?.id}
          </div>

          <div className="mt-2 space-y-1">
            <OwnerLine ownerId={ownerId} verified={!!it?.verified} />
            <div className="text-xs text-slate-500 font-semibold flex items-center gap-2">
              <Calendar size={12} /> {travelDate}
            </div>
          </div>
        </div>

        {showSave ? (
          <button
            onClick={(e) => {
              e.stopPropagation();
              if (!authed) return onLogin?.();
              onSave?.();
            }}
            className={cn(
              "p-2 rounded-2xl border text-xs font-extrabold inline-flex items-center gap-2",
              savedOn
                ? "border-rose-200 bg-rose-50 text-rose-700"
                : "border-slate-200 bg-white hover:bg-slate-50 text-slate-700"
            )}
            title={savedOn ? "Saved" : "Save"}
            type="button"
          >
            <Heart size={16} className={savedOn ? "fill-current" : ""} />
          </button>
        ) : null}
      </div>

      {/* ROUTE */}
      <div className="px-4">
        <div className="rounded-2xl border border-slate-200 bg-slate-50 p-3">
          <div className="text-[11px] font-extrabold text-slate-500 uppercase tracking-wide">
            Route
          </div>
          <div className="mt-2 grid grid-cols-[1fr_auto_1fr] items-center gap-2">
            <div className="min-w-0">
              <div className="text-sm font-extrabold text-slate-900 truncate">
                {fromLabel}
              </div>
              <div className="text-xs text-slate-500">From</div>
            </div>
            <div className="h-9 w-9 rounded-full bg-white border border-slate-200 flex items-center justify-center text-slate-700 font-black">
              →
            </div>
            <div className="min-w-0 text-right">
              <div className="text-sm font-extrabold text-slate-900 truncate">
                {toLabel}
              </div>
              <div className="text-xs text-slate-500">To</div>
            </div>
          </div>

          {noCarry.length ? (
            <div className="mt-3">
              <div className="text-[11px] font-extrabold text-slate-500 uppercase tracking-wide">
                Won&apos;t carry
              </div>
              <div className="mt-2 flex flex-wrap gap-2">
                {noCarry.slice(0, 6).map((x) => (
                  <Pill key={x} tone="amber">
                    <BadgeCheck size={12} />
                    {PROHIBITED_LABELS[x] || String(x)}
                  </Pill>
                ))}
                {noCarry.length > 6 ? (
                  <Pill tone="slate">+{noCarry.length - 6} more</Pill>
                ) : null}
              </div>
            </div>
          ) : null}
        </div>
      </div>

      {/* DETAILS */}
      <div className="px-4 mt-3">
        <div className="grid grid-cols-2 gap-2">
          <div className="rounded-2xl border border-slate-200 bg-white p-3">
            <div className="flex items-center gap-1 text-[11px] font-extrabold text-slate-500 uppercase">
              <Sparkles size={14} /> Airline
            </div>
            <div className="mt-1 text-sm font-extrabold text-slate-900 truncate">
              {airline || "—"}
            </div>
          </div>

          <div className="rounded-2xl border border-slate-200 bg-white p-3">
            <div className="flex items-center gap-1 text-[11px] font-extrabold text-slate-500 uppercase">
              <Plane size={14} /> Flight
            </div>
            <div className="mt-1 text-sm font-extrabold text-slate-900 truncate">
              {flight || "—"}
            </div>
          </div>

          <div className="rounded-2xl border border-slate-200 bg-white p-3">
            <div className="flex items-center gap-1 text-[11px] font-extrabold text-slate-500 uppercase">
              <Weight size={14} /> Weight
            </div>
            <div className="mt-1 text-sm font-extrabold text-slate-900">
              {weightText}
            </div>
          </div>

          <div className="rounded-2xl border border-slate-200 bg-white p-3">
            <div className="flex items-center gap-1 text-[11px] font-extrabold text-slate-500 uppercase">
              <Calendar size={14} /> Arrival
            </div>
            <div className="mt-1 text-sm font-extrabold text-slate-900">
              {arrivalDate !== "—" ? arrivalDate : travelDate}
            </div>
          </div>
        </div>
      </div>

      {/* ACTIONS */}
      <div className="p-4 pt-3 border-t border-slate-100 bg-slate-50/40 space-y-2">
        {showDetails || showPrimary ? (
          <div className={cn("grid gap-2", actionsCols)}>
            {showDetails ? (
              <button
                type="button"
                onClick={onOpen}
                className="inline-flex items-center justify-center gap-2 px-4 py-3 rounded-2xl bg-slate-900 text-white hover:bg-slate-800 text-sm font-extrabold"
              >
                View details <ArrowRight size={16} />
              </button>
            ) : null}

            {showPrimary ? (
              <button
                type="button"
                disabled={
                  loading ||
                  primary.key === "owner" ||
                  primary.key === "pending" ||
                  requestDisabled
                }
                onClick={primary.onClick || undefined}
                className={cn(
                  "inline-flex items-center justify-center gap-2 px-4 py-3 rounded-2xl text-sm font-extrabold border",
                  primaryClass,
                  loading || requestDisabled ? "opacity-80" : ""
                )}
              >
                <PrimaryIcon size={16} />
                {primary.label}
              </button>
            ) : null}
          </div>
        ) : null}

        {showOwnerActions && owner && onEdit && onDelete ? (
          <div className="space-y-2">
            <button
              type="button"
              onClick={(e) => {
                e.stopPropagation();
                onEdit();
              }}
              className="w-full inline-flex items-center justify-center gap-2 px-4 py-2.5 rounded-2xl border border-slate-200 bg-white hover:bg-slate-50 text-sm font-extrabold"
            >
              <Pencil size={16} /> Edit
            </button>

            <button
              type="button"
              onClick={(e) => {
                e.stopPropagation();
                onDelete();
              }}
              className="w-full inline-flex items-center justify-center gap-2 px-4 py-2.5 rounded-2xl border border-red-200 bg-white hover:bg-red-50 text-sm font-extrabold text-red-700"
            >
              <Trash2 size={16} /> Delete
            </button>
          </div>
        ) : null}
      </div>
    </div>
  );
}

/* =========================
   ShipmentCard (more professional + more data)
========================= */
export function ShipmentCard({
  it,
  saved,
  currentUserId,
  flags,
  onOpen,
  onSave,
  onEdit,
  onDelete,
  onLogin,
}) {
  const authed = !!currentUserId;

  const ownerId =
    it?._ownerId ?? it?.owner_id ?? it?.user_id ?? it?.created_by ?? null;
  const owner = authed && String(ownerId ?? "") === String(currentUserId ?? "");

  const savedOn = saved?.has?.(String(it?.id));

  const st = normStatus(pickStr(it, "status") || "open");

  const fromCity = pickStr(it, "from_city");
  const fromCountry = pickStr(it, "from_country");
  const toCity = pickStr(it, "to_city");
  const toCountry = pickStr(it, "to_country");

  const fromLabel = [fromCity, fromCountry].filter(Boolean).join(", ") || "—";
  const toLabel = [toCity, toCountry].filter(Boolean).join(", ") || "—";

  const weightRaw = pick(it, "item_weight", null);
  const weightText =
    weightRaw === null ||
    weightRaw === undefined ||
    String(weightRaw).trim() === ""
      ? "—"
      : `${weightRaw} kg`;

  const budgetAmt = pick(it, "budget_amount", null);
  const budgetCur = pickStr(it, "budget_currency", "USD") || "USD";
  const budgetText =
    budgetAmt === null ||
    budgetAmt === undefined ||
    String(budgetAmt).trim() === ""
      ? "—"
      : `${budgetCur} ${budgetAmt}`;

  const catText = categoryLabel(
    pickStr(it, "category") ||
      pickStr(it, "item_category") ||
      pickStr(it, "cat")
  );

  const title = pickStr(it, "item_title") || pickStr(it, "title") || "Shipment";

  const desc = clampText(
    pickStr(it, "item_desc") || pickStr(it, "description"),
    140
  );

  const deadline = fmtDT(
    pickStr(it, "deadline") || pickStr(it, "deliver_before")
  );

  const productUrl =
    pickStr(it, "product_url") ||
    pickStr(it, "productUrl") ||
    pickStr(it, "url") ||
    "";
  const productHref = safeHref(productUrl);

  const img =
    pickStr(it, "image") || pickStr(it, "item_image") || pickStr(it, "photo");

  const showDetails = !!flags?.showDetails;
  const showSave = !!flags?.showSave;
  const showImage = !!flags?.showImage;
  const showOwnerActions = !!flags?.showOwnerActions;

  return (
    <div className="group rounded-3xl border border-slate-200 bg-white shadow-sm overflow-hidden transition-all hover:shadow-md">
      <div className="p-4 pb-3 flex items-start justify-between gap-3">
        <div className="min-w-0">
          <div className="flex flex-wrap items-center gap-2">
            <Pill tone="amber">
              <Package size={14} /> Shipment
            </Pill>

            <Pill tone="indigo">
              <Tag size={14} /> {catText}
            </Pill>

            <Pill tone={statusTone(st)}>{prettyStatus(st)}</Pill>
          </div>

          <div className="mt-2 text-base font-extrabold text-slate-900 truncate">
            #{it?.id} • {title}
          </div>

          <div className="mt-2 space-y-1">
            <OwnerLine ownerId={ownerId} verified={!!it?.verified} />
          </div>
        </div>

        {showSave ? (
          <button
            onClick={(e) => {
              e.stopPropagation();
              if (!authed) return onLogin?.();
              onSave?.();
            }}
            className={cn(
              "p-2 rounded-2xl border text-xs font-extrabold inline-flex items-center gap-2",
              savedOn
                ? "border-rose-200 bg-rose-50 text-rose-700"
                : "border-slate-200 bg-white hover:bg-slate-50 text-slate-700"
            )}
            title={savedOn ? "Saved" : "Save"}
            type="button"
          >
            <Heart size={16} className={savedOn ? "fill-current" : ""} />
          </button>
        ) : null}
      </div>

      {/* ROUTE */}
      <div className="px-4">
        <div className="rounded-2xl border border-slate-200 bg-slate-50 p-3">
          <div className="text-[11px] font-extrabold text-slate-500 uppercase tracking-wide">
            Route
          </div>
          <div className="mt-2 grid grid-cols-[1fr_auto_1fr] gap-2 items-center">
            <div className="min-w-0">
              <div className="text-sm font-extrabold text-slate-900 truncate">
                {fromLabel}
              </div>
              <div className="text-xs text-slate-500 inline-flex items-center gap-1">
                <Globe size={12} /> From
              </div>
            </div>
            <div className="h-9 w-9 rounded-full bg-white border border-slate-200 flex items-center justify-center text-slate-700 font-black">
              →
            </div>
            <div className="min-w-0 text-right">
              <div className="text-sm font-extrabold text-slate-900 truncate">
                {toLabel}
              </div>
              <div className="text-xs text-slate-500 inline-flex items-center gap-1 justify-end">
                <Globe size={12} /> To
              </div>
            </div>
          </div>

          <div className="mt-3 grid grid-cols-2 gap-2">
            <div className="rounded-2xl border border-slate-200 bg-white p-3">
              <div className="flex items-center gap-1 text-[11px] font-extrabold text-slate-500 uppercase">
                <Clock size={14} /> Deadline
              </div>
              <div className="mt-1 text-sm font-extrabold text-slate-900">
                {deadline}
              </div>
            </div>

            <div className="rounded-2xl border border-slate-200 bg-white p-3">
              <div className="flex items-center gap-1 text-[11px] font-extrabold text-slate-500 uppercase">
                <Weight size={14} /> Weight
              </div>
              <div className="mt-1 text-sm font-extrabold text-slate-900">
                {weightText}
              </div>
            </div>

            <div className="rounded-2xl border border-slate-200 bg-white p-3">
              <div className="flex items-center gap-1 text-[11px] font-extrabold text-slate-500 uppercase">
                <MapPin size={14} /> Budget
              </div>
              <div className="mt-1 text-sm font-extrabold text-slate-900">
                {budgetText}
              </div>
            </div>

            <div className="rounded-2xl border border-slate-200 bg-white p-3">
              <div className="flex items-center gap-1 text-[11px] font-extrabold text-slate-500 uppercase">
                <LinkIcon size={14} /> Product
              </div>
              <div className="mt-1 text-sm font-extrabold text-slate-900 truncate">
                {productHref ? (
                  <a
                    href={productHref}
                    target="_blank"
                    rel="noreferrer"
                    className="text-indigo-700 hover:underline"
                    onClick={(e) => e.stopPropagation()}
                    title={productHref}
                  >
                    Open link
                  </a>
                ) : productUrl ? (
                  <span className="text-slate-500">Invalid link</span>
                ) : (
                  "—"
                )}
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* IMAGE */}
      {showImage ? (
        <div className="px-4 mt-3">
          {img ? (
            <div className="rounded-2xl border border-slate-200 bg-slate-50 overflow-hidden">
              <img src={img} alt="" className="w-full h-44 object-cover" />
            </div>
          ) : (
            <div className="rounded-2xl border border-slate-200 bg-slate-50 p-4 text-sm text-slate-500">
              No image
            </div>
          )}
        </div>
      ) : null}

      {/* DESC */}
      {desc ? (
        <div className="px-4 mt-3">
          <div className="rounded-2xl border border-slate-200 bg-white p-3">
            <div className="text-[11px] font-extrabold text-slate-500 uppercase">
              Description
            </div>
            <div className="mt-1 text-sm text-slate-800 font-semibold whitespace-pre-wrap break-words">
              {desc}
            </div>
          </div>
        </div>
      ) : null}

      {/* ACTIONS */}
      <div className="p-4 pt-3 border-t border-slate-100 bg-slate-50/40 space-y-2">
        {showDetails ? (
          <button
            type="button"
            onClick={onOpen}
            className="w-full inline-flex items-center justify-center gap-2 px-4 py-3 rounded-2xl bg-slate-900 text-white hover:bg-slate-800 text-sm font-extrabold"
          >
            View details <ArrowRight size={16} />
          </button>
        ) : null}

        {showOwnerActions && owner && onEdit && onDelete ? (
          <div className="space-y-2">
            <button
              type="button"
              onClick={(e) => {
                e.stopPropagation();
                onEdit();
              }}
              className="w-full inline-flex items-center justify-center gap-2 px-4 py-2.5 rounded-2xl border border-slate-200 bg-white hover:bg-slate-50 text-sm font-extrabold"
            >
              <Pencil size={16} /> Edit
            </button>

            <button
              type="button"
              onClick={(e) => {
                e.stopPropagation();
                onDelete();
              }}
              className="w-full inline-flex items-center justify-center gap-2 px-4 py-2.5 rounded-2xl border border-red-200 bg-white hover:bg-red-50 text-sm font-extrabold text-red-700"
            >
              <Trash2 size={16} /> Delete
            </button>
          </div>
        ) : null}
      </div>
    </div>
  );
}
