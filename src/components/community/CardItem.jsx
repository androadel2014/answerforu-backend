// src/components/community/CardItem.jsx
import React, { useEffect, useMemo, useState } from "react";
import { useNavigate } from "react-router-dom";

import {
  User,
  MapPin,
  Globe,
  Phone,
  Users,
  Building2,
  Pencil,
  Trash2,
  ExternalLink,
  Navigation,
  MessageCircle,
  Star,
  Briefcase,
  Home,
  Boxes,
  Hammer,
  Wrench,
  MoreVertical,
} from "lucide-react";

const API_BASE =
  import.meta.env.VITE_API_BASE_URL ||
  import.meta.env.VITE_API_BASE ||
  "http://localhost:5000";

/* =========================
   Helpers
========================= */

function classNames(...arr) {
  return arr.filter(Boolean).join(" ");
}

function safeUrl(url) {
  const v = (url || "").trim();
  if (!v) return "";
  if (/^https?:\/\//i.test(v)) return v;
  return `https://${v}`;
}

function mapsLink({ address, city, state }) {
  const parts = [address, city, state].filter(Boolean).join(", ");
  const q = encodeURIComponent(parts || "");
  return q ? `https://www.google.com/maps/search/?api=1&query=${q}` : "";
}

function parseDateLike(v) {
  if (!v) return null;
  const d1 = new Date(v);
  if (!Number.isNaN(d1.getTime())) return d1;

  const n = Number(v);
  if (!Number.isNaN(n) && n > 0) {
    const ms = n < 1e12 ? n * 1000 : n;
    const d2 = new Date(ms);
    if (!Number.isNaN(d2.getTime())) return d2;
  }
  return null;
}

function pickCreatedAt(it) {
  return (
    it.createdAt ??
    it.created_at ??
    it.created ??
    it.created_on ??
    it.insertedAt ??
    it.inserted_at ??
    it.time ??
    it.timestamp ??
    null
  );
}

function timeAgo(date, t) {
  const d = parseDateLike(date);
  if (!d) return "";

  const diffMs = Date.now() - d.getTime();
  if (diffMs < 0) return t.justNow;

  const sec = Math.floor(diffMs / 1000);
  if (sec < 60) return t.justNow;

  const min = Math.floor(sec / 60);
  if (min < 60) return t.mAgo(min);

  const hr = Math.floor(min / 60);
  if (hr < 24) return t.hAgo(hr);

  const day = Math.floor(hr / 24);
  if (day < 7) return t.dAgo(day);

  const wk = Math.floor(day / 7);
  if (wk < 5) return t.wAgo(wk);

  const mo = Math.floor(day / 30);
  if (mo < 12) return t.moAgo(mo);

  const yr = Math.floor(day / 365);
  return t.yAgo(yr);
}

function isNewByDate(date, hours = 48) {
  const d = parseDateLike(date);
  if (!d) return false;
  const diffMs = Date.now() - d.getTime();
  return diffMs >= 0 && diffMs <= hours * 60 * 60 * 1000;
}

/* =========================
   i18n
========================= */

const getDir = (lang) => (lang === "ar" ? "rtl" : "ltr");

const UI = {
  ar: {
    listing: "إعلان",
    curated: "مُختار للمهاجرين الجدد",
    joinConnect: "انضم وتواصل مع الناس",
    marketplace: "ماركت بليس للمهاجرين الجدد",
    locationNotSet: "الموقع غير محدد",
    newTag: "جديد",
    actions: "إجراءات",
    edit: "تعديل",
    del: "حذف",
    openMap: "فتح الخريطة",
    website: "الموقع",
    openLink: "فتح الرابط",
    open: "فتح",
    contact: "تواصل",
    justNow: "دلوقتي",
    by: "بواسطة",
    mAgo: (n) => `منذ ${n} د`,
    hAgo: (n) => `منذ ${n} س`,
    dAgo: (n) => `منذ ${n} يوم`,
    wAgo: (n) => `منذ ${n} أسبوع`,
    moAgo: (n) => `منذ ${n} شهر`,
    yAgo: (n) => `منذ ${n} سنة`,
    ratingNew: "جديد",
  },
  en: {
    listing: "Listing",
    curated: "Curated for newcomers",
    joinConnect: "Join & connect with people",
    marketplace: "Marketplace for newcomers",
    locationNotSet: "Location not set",
    newTag: "NEW",
    actions: "Actions",
    edit: "Edit",
    del: "Delete",
    openMap: "Open Map",
    website: "Website",
    openLink: "Open Link",
    open: "Open",
    contact: "Contact",
    justNow: "just now",
    by: "by",
    mAgo: (n) => `${n}m ago`,
    hAgo: (n) => `${n}h ago`,
    dAgo: (n) => `${n}d ago`,
    wAgo: (n) => `${n}w ago`,
    moAgo: (n) => `${n}mo ago`,
    yAgo: (n) => `${n}y ago`,
    ratingNew: "New",
  },
  es: {
    listing: "Anuncio",
    curated: "Recomendado para recién llegados",
    joinConnect: "Únete y conecta con personas",
    marketplace: "Mercado para recién llegados",
    locationNotSet: "Ubicación no establecida",
    newTag: "NUEVO",
    actions: "Acciones",
    edit: "Editar",
    del: "Eliminar",
    openMap: "Abrir mapa",
    website: "Sitio web",
    openLink: "Abrir enlace",
    open: "Abrir",
    contact: "Contacto",
    justNow: "ahora",
    by: "por",
    mAgo: (n) => `hace ${n} min`,
    hAgo: (n) => `hace ${n} h`,
    dAgo: (n) => `hace ${n} d`,
    wAgo: (n) => `hace ${n} sem`,
    moAgo: (n) => `hace ${n} mes`,
    yAgo: (n) => `hace ${n} año`,
    ratingNew: "Nuevo",
  },
};

function normalizePlaceCategoryKey(raw) {
  const v = String(raw || "")
    .trim()
    .toLowerCase();
  if (!v) return "other";
  if (v.includes("restaurant")) return "restaurant";
  if (v === "cafe" || v.includes("cafe")) return "cafe";
  if (v.includes("bakery")) return "bakery";
  if (
    v.includes("grocery") ||
    v.includes("arab market") ||
    v.includes("market")
  )
    return "grocery";
  if (v.includes("things to do")) return "todo";
  if (v.includes("park") || v.includes("outdoors")) return "park";
  if (v.includes("attraction")) return "attraction";
  if (v.includes("mosque")) return "mosque";
  if (v.includes("church")) return "church";
  if (v.includes("school") || v.includes("daycare")) return "school";
  if (v.includes("clinic") || v.includes("doctor")) return "clinic";
  if (v.includes("lawyer") || v.includes("immigration")) return "lawyer";
  if (v.includes("car")) return "car";
  if (v.includes("handyman") || v.includes("home services")) return "handyman";
  if (v.includes("barber") || v.includes("beauty")) return "beauty";
  if (v.includes("shopping") || v.includes("mall")) return "shopping";
  return "other";
}

function normalizeGroupPlatformKey(raw) {
  const v = String(raw || "")
    .trim()
    .toLowerCase();
  if (!v) return "other";
  if (v.includes("facebook")) return "facebook";
  if (v.includes("whatsapp")) return "whatsapp";
  if (v.includes("telegram")) return "telegram";
  if (v.includes("discord")) return "discord";
  if (v.includes("meetup")) return "meetup";
  return "other";
}

/* =========================
   Banners
========================= */

const PLACE_BANNER = {
  restaurant: {
    label: "Restaurant",
    Icon: Hammer,
    bg: "from-orange-50 to-orange-100 border-orange-200",
    icon: "bg-orange-600",
  },
  cafe: {
    label: "Cafe",
    Icon: Hammer,
    bg: "from-amber-50 to-amber-100 border-amber-200",
    icon: "bg-amber-600",
  },
  bakery: {
    label: "Bakery",
    Icon: Hammer,
    bg: "from-yellow-50 to-yellow-100 border-yellow-200",
    icon: "bg-yellow-600",
  },
  grocery: {
    label: "Grocery / Arab Market",
    Icon: Boxes,
    bg: "from-sky-50 to-sky-100 border-sky-200",
    icon: "bg-sky-600",
  },
  todo: {
    label: "Things to do",
    Icon: Star,
    bg: "from-fuchsia-50 to-fuchsia-100 border-fuchsia-200",
    icon: "bg-fuchsia-600",
  },
  park: {
    label: "Park / Outdoors",
    Icon: Star,
    bg: "from-green-50 to-green-100 border-green-200",
    icon: "bg-green-600",
  },
  attraction: {
    label: "Attraction",
    Icon: Star,
    bg: "from-violet-50 to-violet-100 border-violet-200",
    icon: "bg-violet-600",
  },
  mosque: {
    label: "Mosque",
    Icon: Building2,
    bg: "from-emerald-50 to-emerald-100 border-emerald-200",
    icon: "bg-emerald-600",
  },
  church: {
    label: "Church",
    Icon: Building2,
    bg: "from-indigo-50 to-indigo-100 border-indigo-200",
    icon: "bg-indigo-600",
  },
  school: {
    label: "School / Daycare",
    Icon: Building2,
    bg: "from-blue-50 to-blue-100 border-blue-200",
    icon: "bg-blue-600",
  },
  clinic: {
    label: "Clinic / Doctor",
    Icon: Building2,
    bg: "from-red-50 to-red-100 border-red-200",
    icon: "bg-red-600",
  },
  lawyer: {
    label: "Lawyer / Immigration",
    Icon: Building2,
    bg: "from-slate-50 to-slate-100 border-slate-200",
    icon: "bg-slate-700",
  },
  car: {
    label: "Car Services",
    Icon: Wrench,
    bg: "from-gray-50 to-gray-100 border-gray-200",
    icon: "bg-gray-800",
  },
  handyman: {
    label: "Handyman / Home Services",
    Icon: Wrench,
    bg: "from-teal-50 to-teal-100 border-teal-200",
    icon: "bg-teal-700",
  },
  beauty: {
    label: "Barber / Beauty",
    Icon: Wrench,
    bg: "from-pink-50 to-pink-100 border-pink-200",
    icon: "bg-pink-600",
  },
  shopping: {
    label: "Shopping / Mall",
    Icon: Boxes,
    bg: "from-zinc-50 to-zinc-100 border-zinc-200",
    icon: "bg-zinc-800",
  },
  other: {
    label: "Place",
    Icon: Building2,
    bg: "from-gray-50 to-gray-100 border-gray-200",
    icon: "bg-gray-700",
  },
};

const GROUP_BANNER = {
  facebook: {
    label: "Facebook Group",
    Icon: Users,
    bg: "from-blue-50 to-blue-100 border-blue-200",
    icon: "bg-blue-600",
  },
  whatsapp: {
    label: "WhatsApp Group",
    Icon: MessageCircle,
    bg: "from-emerald-50 to-emerald-100 border-emerald-200",
    icon: "bg-emerald-600",
  },
  telegram: {
    label: "Telegram Group",
    Icon: MessageCircle,
    bg: "from-sky-50 to-sky-100 border-sky-200",
    icon: "bg-sky-600",
  },
  discord: {
    label: "Discord",
    Icon: Users,
    bg: "from-indigo-50 to-indigo-100 border-indigo-200",
    icon: "bg-indigo-600",
  },
  meetup: {
    label: "Meetup",
    Icon: Users,
    bg: "from-orange-50 to-orange-100 border-orange-200",
    icon: "bg-orange-600",
  },
  other: {
    label: "Group",
    Icon: Users,
    bg: "from-gray-50 to-gray-100 border-gray-200",
    icon: "bg-gray-700",
  },
};

const TYPE_BANNER = {
  services: {
    label: "Service",
    Icon: Hammer,
    bg: "from-teal-50 to-teal-100 border-teal-200",
    icon: "bg-teal-700",
  },
  jobs: {
    label: "Job",
    Icon: Briefcase,
    bg: "from-slate-50 to-slate-100 border-slate-200",
    icon: "bg-slate-800",
  },
  housing: {
    label: "Housing",
    Icon: Home,
    bg: "from-amber-50 to-amber-100 border-amber-200",
    icon: "bg-amber-700",
  },
  products: {
    label: "Product",
    Icon: Boxes,
    bg: "from-zinc-50 to-zinc-100 border-zinc-200",
    icon: "bg-zinc-900",
  },
};

function CardBanner({
  tab,
  placeCategory,
  groupPlatform,
  subtitleRight,
  lang,
}) {
  const t = UI[lang] || UI.en;

  let ui = {
    label: t.listing,
    Icon: Building2,
    bg: "from-gray-50 to-gray-100 border-gray-200",
    icon: "bg-gray-700",
    sub: t.marketplace,
  };

  if (tab === "places") {
    const key = normalizePlaceCategoryKey(placeCategory);
    ui = { ...(PLACE_BANNER[key] || PLACE_BANNER.other), sub: t.curated };
  } else if (tab === "groups") {
    const key = normalizeGroupPlatformKey(groupPlatform);
    ui = { ...(GROUP_BANNER[key] || GROUP_BANNER.other), sub: t.joinConnect };
  } else {
    ui = { ...(TYPE_BANNER[tab] || ui), sub: t.marketplace };
  }

  const Icon = ui.Icon;

  return (
    <div
      className={classNames(
        "w-full rounded-2xl border bg-gradient-to-r px-4 py-3 flex items-center justify-between",
        ui.bg,
      )}
    >
      <div className="flex items-center gap-3 min-w-0">
        <div
          className={classNames(
            "w-9 h-9 rounded-xl flex items-center justify-center shrink-0",
            ui.icon,
          )}
        >
          <Icon className="w-5 h-5 text-white" />
        </div>
        <div className="min-w-0">
          <div className="font-extrabold text-gray-900 leading-5 truncate">
            {ui.label}
          </div>
          <div className="text-xs text-gray-600 mt-0.5 truncate">{ui.sub}</div>
        </div>
      </div>

      {subtitleRight ? (
        <div className="hidden sm:inline-flex text-xs font-semibold text-gray-700 rounded-full bg-white/70 border border-gray-200 px-2.5 py-1">
          {subtitleRight}
        </div>
      ) : null}
    </div>
  );
}

/* =========================
   ✅ CardItem
========================= */
const OWNER_NAME_CACHE = new Map(); // ownerId -> name
const OWNER_KEY_CACHE = new Map(); // ownerId -> (public_id or fallback numeric string)

export function CardItem({
  tab,
  it,
  isLoggedIn,
  onEdit,
  onDelete,
  onOpen,
  lang = "en",
}) {
  const [open, setOpen] = useState(false);
  const navigate = useNavigate();

  const t = UI[lang] || UI.en;
  const dir = getDir(lang);

  const cardClickable = typeof onOpen === "function";

  const typeKey = tab === "all" ? it.type || "places" : tab;
  const isPlace = typeKey === "places";
  const isGroup = typeKey === "groups";

  const placeMapUrl = isPlace
    ? mapsLink({ address: it.address, city: it.city, state: it.state })
    : "";

  const locText =
    [it.city, it.state].filter(Boolean).join(", ") || t.locationNotSet;

  const typeChip = typeKey;

  const badgeChip = isGroup
    ? it.topic || it.platform || ""
    : it.category || it.platform || "";

  const ratingRaw =
    it.avg_rating ??
    it.rating_avg ??
    it.avgRating ??
    it.avg_rating_value ??
    it.rating ??
    it.stars ??
    it.score ??
    0;

  const reviewsRaw =
    it.reviews_count ??
    it.review_count ??
    it.reviewsCount ??
    it.reviewCount ??
    it.ratings_count ??
    it.rating_count ??
    it.count ??
    0;

  const rating = Number(ratingRaw) || 0;
  const reviewsCount = Number(reviewsRaw) || 0;

  const ratingText = rating > 0 ? rating.toFixed(1) : t.ratingNew;

  const titleText = it.name || it.title || "Untitled";
  const priceValue = it.price_value;
  const createdAtVal = pickCreatedAt(it);
  const createdAgo = timeAgo(createdAtVal, t);
  const isNew = isNewByDate(createdAtVal, 48);

  // ✅ Ownership (show actions ONLY for owner)
  const me = useMemo(() => {
    try {
      return JSON.parse(localStorage.getItem("user") || "null");
    } catch {
      return null;
    }
  }, []);

  const meId = Number(
    me?.id ?? me?.userId ?? me?.user_id ?? me?.uid ?? me?.sub ?? 0,
  );

  const ownerId = Number(
    it?.created_by ??
      it?.createdBy ??
      it?.created_by_id ??
      it?.user_id ??
      it?.userId ??
      it?.owner_id ??
      it?.ownerId ??
      it?.createdById ??
      it?.createdByUserId ??
      0,
  );

  const initialOwnerName =
    it?.owner_name ||
    it?.ownerName ||
    it?.created_by_name ||
    it?.createdByName ||
    it?.created_by_username ||
    it?.createdByUsername ||
    it?.user?.username ||
    it?.user?.name ||
    it?.owner?.username ||
    it?.owner?.name ||
    "";

  const [ownerName, setOwnerName] = useState(() => {
    const cached = OWNER_NAME_CACHE.get(ownerId);
    return String(cached || initialOwnerName || "").trim();
  });

  const [ownerKey, setOwnerKey] = useState(() => {
    const cached = OWNER_KEY_CACHE.get(ownerId);
    return String(cached || (ownerId ? String(ownerId) : "") || "").trim();
  });

  useEffect(() => {
    if (!ownerId) return;

    const cachedKey = OWNER_KEY_CACHE.get(ownerId);
    if (cachedKey) {
      const v = String(cachedKey || "").trim();
      if (v && v !== ownerKey) setOwnerKey(v);
    } else {
      const fallback = ownerId ? String(ownerId) : "";
      if (fallback && fallback !== ownerKey) setOwnerKey(fallback);
    }

    const cachedName = OWNER_NAME_CACHE.get(ownerId);
    if (cachedName) {
      const v = String(cachedName || "").trim();
      if (v && v !== ownerName) setOwnerName(v);
      return;
    }

    const seed = String(initialOwnerName || "").trim();
    if (seed) {
      OWNER_NAME_CACHE.set(ownerId, seed);
      if (seed !== ownerName) setOwnerName(seed);
    }

    let cancelled = false;

    (async () => {
      try {
        let res = null;
        let data = null;

        try {
          res = await fetch(`${API_BASE}/api/profile/${ownerId}`, {
            headers: { "Content-Type": "application/json" },
          });
          data = await res.json().catch(() => ({}));
        } catch {}

        if (!res || !res.ok) {
          try {
            res = await fetch(`${API_BASE}/api/users/${ownerId}`, {
              headers: { "Content-Type": "application/json" },
            });
            data = await res.json().catch(() => ({}));
          } catch {
            return;
          }
        }

        if (!res || !res.ok) return;

        const src = data?.profile || data?.user_profile || data?.user || data;

        const publicId =
          src?.public_id ?? src?.publicId ?? data?.public_id ?? data?.publicId;

        const finalKey = String(publicId || "").trim() || String(ownerId);

        OWNER_KEY_CACHE.set(ownerId, finalKey);
        if (!cancelled) setOwnerKey(finalKey);

        const name =
          src?.display_name ||
          src?.displayName ||
          src?.username ||
          src?.name ||
          src?.full_name ||
          src?.email ||
          "";

        const finalName = String(name || "").trim();
        if (finalName) {
          OWNER_NAME_CACHE.set(ownerId, finalName);
          if (!cancelled) setOwnerName(finalName);
        }
      } catch {}
    })();

    return () => {
      cancelled = true;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [ownerId]);

  const isOwner = !!meId && !!ownerId && meId === ownerId;

  const rightSubtitle =
    typeKey !== "places" && typeKey !== "groups" && (it.price || it.budget)
      ? String(it.price || it.budget)
      : typeKey === "groups"
        ? it.platform || ""
        : it.category || "";

  return (
    <div
      role={cardClickable ? "button" : undefined}
      tabIndex={cardClickable ? 0 : undefined}
      onClick={() => cardClickable && onOpen()}
      onKeyDown={(e) => {
        if (!cardClickable) return;
        if (e.key === "Enter" || e.key === " ") {
          e.preventDefault();
          onOpen();
        }
      }}
      className={classNames(
        "rounded-2xl relative border border-gray-200 bg-white p-4 shadow-sm hover:shadow-md transition hover:ring-2 hover:ring-black/5",
        cardClickable ? "cursor-pointer" : "",
      )}
    >
      <CardBanner
        tab={tab === "all" ? it.type || "places" : tab}
        placeCategory={it.category}
        groupPlatform={it.platform}
        subtitleRight={rightSubtitle}
        lang={lang}
      />

      <div className="mt-4 flex items-start justify-between gap-3">
        <div className="min-w-0 flex-1">
          <div className="flex items-center gap-2 flex-wrap">
            <div className="text-base md:text-lg font-extrabold text-gray-900 truncate">
              {titleText}
            </div>

            <span className="inline-flex items-center px-2.5 py-1 rounded-full text-xs font-semibold bg-white border border-gray-200 text-gray-700">
              {typeChip}
            </span>

            {badgeChip ? (
              <span className="inline-flex items-center px-2.5 py-1 rounded-full text-xs font-semibold bg-white border border-gray-200 text-gray-700">
                {badgeChip}
              </span>
            ) : null}

            <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-extrabold border border-gray-200 bg-gray-50 text-gray-900">
              <Star size={14} className="text-amber-500" />
              {ratingText}
              {reviewsCount > 0 ? (
                <span className="text-xs font-semibold text-gray-600">
                  ({reviewsCount})
                </span>
              ) : null}
            </span>
            {ownerId ? (
              <button
                type="button"
                onClick={(e) => {
                  e.preventDefault();
                  e.stopPropagation();
                  const key = String(ownerKey || ownerId || "").trim();
                  if (!key) return;
                  navigate(`/u/${encodeURIComponent(key)}`);
                }}
                className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-semibold bg-white border border-gray-200 text-gray-700 hover:bg-gray-50"
                title={ownerName || "—"}
              >
                <User size={14} className="text-gray-500" />
                <span className="text-gray-500">{t.by}</span>
                <span className="font-extrabold text-gray-900">
                  {ownerName || "—"}
                </span>
              </button>
            ) : null}

            {createdAgo ? (
              <span className="inline-flex items-center px-2.5 py-1 rounded-full text-xs font-semibold bg-gray-50 border border-gray-200 text-gray-700">
                {createdAgo}
              </span>
            ) : null}

            {priceValue ? (
              <span className="inline-flex items-center px-2.5 py-1 rounded-full text-xs font-semibold bg-gray-50 border border-gray-200 text-gray-700">
                ${priceValue}
              </span>
            ) : null}

            {isNew ? (
              <span className="inline-flex items-center px-2.5 py-1 rounded-full text-xs font-extrabold bg-emerald-50 border border-emerald-200 text-emerald-700">
                {t.newTag}
              </span>
            ) : null}
          </div>

          <div className="mt-2 text-sm text-gray-700 space-y-1">
            <div className="flex items-center gap-2">
              <MapPin size={16} className="text-gray-500" />
              <span className="truncate">{locText}</span>
            </div>

            {isPlace && it.address && placeMapUrl ? (
              <a
                href={placeMapUrl}
                target="_blank"
                rel="noreferrer"
                className="text-gray-600 truncate hover:underline"
                onClick={(e) => e.stopPropagation()}
              >
                {it.address}
              </a>
            ) : null}

            {isGroup ? (
              <div className="text-gray-600">{it.topic || "General"}</div>
            ) : null}

            {!isPlace && !isGroup && it.description ? (
              <div className="text-gray-600 line-clamp-2">{it.description}</div>
            ) : null}
          </div>

          <div className="mt-3 flex flex-wrap gap-2">
            {isPlace && placeMapUrl ? (
              <a
                href={placeMapUrl}
                target="_blank"
                rel="noreferrer"
                onClick={(e) => e.stopPropagation()}
                className="inline-flex items-center gap-2 text-sm font-semibold px-3 py-1.5 rounded-xl border border-gray-200 hover:bg-gray-50"
              >
                <Navigation size={16} />
                {t.openMap}
                <ExternalLink size={14} className="text-gray-500" />
              </a>
            ) : null}

            {isPlace && it.website ? (
              <a
                href={safeUrl(it.website)}
                target="_blank"
                rel="noreferrer"
                onClick={(e) => e.stopPropagation()}
                className="inline-flex items-center gap-2 text-sm font-semibold px-3 py-1.5 rounded-xl border border-gray-200 hover:bg-gray-50"
              >
                <Globe size={16} />
                {t.website}
                <ExternalLink size={14} className="text-gray-500" />
              </a>
            ) : null}

            {isPlace && it.phone ? (
              <a
                href={`tel:${String(it.phone).replace(/\s+/g, "")}`}
                onClick={(e) => e.stopPropagation()}
                className="inline-flex items-center gap-2 text-sm font-semibold px-3 py-1.5 rounded-xl border border-gray-200 hover:bg-gray-50"
              >
                <Phone size={16} />
                {it.phone}
              </a>
            ) : null}

            {isGroup && it.link ? (
              <a
                href={safeUrl(it.link)}
                target="_blank"
                rel="noreferrer"
                onClick={(e) => e.stopPropagation()}
                className="inline-flex items-center gap-2 text-sm font-semibold px-3 py-1.5 rounded-xl border border-gray-200 hover:bg-gray-50"
              >
                <Globe size={16} />
                {t.openLink}
                <ExternalLink size={14} className="text-gray-500" />
              </a>
            ) : null}

            {!isPlace && !isGroup && it.link ? (
              <a
                href={safeUrl(it.link)}
                target="_blank"
                rel="noreferrer"
                onClick={(e) => e.stopPropagation()}
                className="inline-flex items-center gap-2 text-sm font-semibold px-3 py-1.5 rounded-xl border border-gray-200 hover:bg-gray-50"
              >
                <Globe size={16} />
                {t.open}
                <ExternalLink size={14} className="text-gray-500" />
              </a>
            ) : null}

            {!isPlace && !isGroup && it.contact ? (
              <span className="inline-flex items-center gap-2 text-sm font-semibold px-3 py-1.5 rounded-xl border border-gray-200 bg-gray-50 text-gray-800">
                <Phone size={16} />
                {it.contact}
              </span>
            ) : null}
          </div>

          {it.notes ? (
            <div className="mt-3 text-sm text-gray-600 whitespace-pre-wrap">
              {it.notes}
            </div>
          ) : null}
        </div>

        {isLoggedIn && isOwner ? (
          <div className="shrink-0 relative">
            <button
              onClick={(e) => {
                e.preventDefault();
                e.stopPropagation();
                setOpen((v) => !v);
              }}
              className="p-2 rounded-xl border border-gray-200 hover:bg-gray-50"
              title={t.actions}
              type="button"
            >
              <MoreVertical size={18} />
            </button>

            {open ? (
              <div
                className={classNames(
                  "absolute mt-2 w-44 rounded-2xl border border-gray-200 bg-white shadow-xl overflow-hidden z-50",
                  dir === "rtl" ? "left-0" : "right-0",
                )}
              >
                <button
                  onClick={(e) => {
                    e.preventDefault();
                    e.stopPropagation();
                    setOpen(false);
                    onEdit?.();
                  }}
                  className="w-full px-3 py-2.5 text-left hover:bg-gray-50 flex items-center gap-2 text-sm font-semibold"
                  type="button"
                >
                  <Pencil size={16} />
                  {t.edit}
                </button>

                <button
                  onClick={(e) => {
                    e.preventDefault();
                    e.stopPropagation();
                    setOpen(false);
                    onDelete?.();
                  }}
                  className="w-full px-3 py-2.5 text-left hover:bg-red-50 flex items-center gap-2 text-sm font-semibold text-red-600"
                  type="button"
                >
                  <Trash2 size={16} />
                  {t.del}
                </button>
              </div>
            ) : null}
          </div>
        ) : null}
      </div>
    </div>
  );
}
