// src/components/ItemDetailsView.jsx
import React, { useEffect, useMemo, useState } from "react";
import { useParams, Link, useNavigate, useLocation } from "react-router-dom";
import {
  Modal,
  ListingFormBody,
  getSchema,
  buildEditValues,
  applyEditField,
} from "./community/MarketplaceShared";

import { toastConfirm, notify } from "../lib/notify";
import {
  ArrowLeft,
  ArrowRight,
  MapPin,
  Phone,
  Star,
  ExternalLink,
  MessageSquarePlus,
  Sparkles,
  Lock,
  Pencil,
  Trash2,
  X,
} from "lucide-react";

const API_BASE =
  import.meta.env.VITE_API_BASE_URL ||
  import.meta.env.VITE_API_BASE ||
  "http://localhost:5000";

const cn = (...a) => a.filter(Boolean).join(" ");

/* =========================
   i18n (AR/EN/ES) + dir
========================= */
const normLang = (v) => {
  const s = String(v || "en").toLowerCase();
  if (s.startsWith("ar")) return "ar";
  if (s.startsWith("es")) return "es";
  return "en";
};
const isRTL = (lang) => normLang(lang) === "ar";
const dirForLang = (lang) => (isRTL(lang) ? "rtl" : "ltr");

const I18N = {
  en: {
    back: "Back",
    backToCommunity: "Back to Community",
    openMap: "Open Map",
    openLink: "Open link",
    price: "Price",
    rating: "Rating",
    verified: "Verified",
    directions: "Directions",
    call: "Call",
    reviews: "Reviews",
    refresh: "Refresh",
    loadingReviews: "Loading reviews…",
    noReviews: "No reviews yet.",
    yourReview: "Your review",
    addReview: "Add a review",
    loginRequired: "Login required",
    oneReview: "One review per user",
    goToLogin: "Go to Login",
    writeReview: "Write a review",
    youReviewAs: "You are reviewing as",
    yourRating: "Your rating",
    selected: "Selected:",
    submitReview: "Submit review",
    editYourReview: "Edit your review",
    updateReview: "Update review",
    delete: "Delete",
    edit: "Edit",
    cancel: "Cancel",
    reviewUpdated: "Review updated",
    reviewAdded: "Review added",
    reviewDeleted: "Review deleted",
    itemUpdated: "Item updated",
    itemDeleted: "Item deleted",
    saving: "Saving...",
    save: "Save",
    deleting: "Deleting…",
    confirmDeleteTitle: "Confirm delete",
    confirmDeleteReviewMsg: "Are you sure you want to delete your review?",
    confirmDeleteItemMsg: "Are you sure you want to delete this item?",
    placeNotFound: "Item not found.",
    writeOpinion: "Write your review…",
    tip: "Tip: be clear about quality, price, and service.",
    editItemTitle: "Edit item",
    editItemSub: "Update info then save",
    loginFirst: "Please login first",
    starsRange: "Stars must be 1..5",
    writeText: "Write something",
    youTag: "Your review",
    owner: "Owner",
    viewProfile: "View profile",
  },
  ar: {
    back: "رجوع",
    backToCommunity: "رجوع للمجتمع",
    openMap: "فتح الخريطة",
    openLink: "فتح الرابط",
    price: "السعر",
    rating: "التقييم",
    verified: "موثّق",
    directions: "الاتجاهات",
    call: "اتصال",
    reviews: "التقييمات",
    refresh: "تحديث",
    loadingReviews: "جارٍ تحميل التقييمات…",
    noReviews: "لا توجد تقييمات بعد.",
    yourReview: "تقييمك",
    addReview: "إضافة تقييم",
    loginRequired: "تسجيل الدخول مطلوب",
    oneReview: "تقييم واحد لكل مستخدم",
    goToLogin: "اذهب لتسجيل الدخول",
    writeReview: "اكتب تقييمك",
    youReviewAs: "أنت تقيّم باسم",
    yourRating: "تقييمك",
    selected: "المختار:",
    submitReview: "إرسال التقييم",
    editYourReview: "تعديل تقييمك",
    updateReview: "تحديث التقييم",
    delete: "حذف",
    edit: "تعديل",
    cancel: "إلغاء",
    reviewUpdated: "تم تحديث التقييم",
    reviewAdded: "تم إضافة التقييم",
    reviewDeleted: "تم حذف التقييم",
    itemUpdated: "تم تحديث العنصر",
    itemDeleted: "تم حذف العنصر",
    saving: "جارٍ الحفظ...",
    save: "حفظ",
    deleting: "جارٍ الحذف…",
    confirmDeleteTitle: "تأكيد الحذف",
    confirmDeleteReviewMsg: "متأكد عايز تمسح الريفيو بتاعك؟",
    confirmDeleteItemMsg: "متأكد عايز تمسح الـ Item ده؟",
    placeNotFound: "العنصر غير موجود.",
    writeOpinion: "اكتب رأيك…",
    tip: "نصيحة: قول رأيك بوضوح: جودة الخدمة، الأسعار، المكان، التعامل.",
    editItemTitle: "تعديل العنصر",
    editItemSub: "عدّل البيانات وبعدين احفظ",
    loginFirst: "لازم تسجّل دخول الأول",
    starsRange: "النجوم لازم تكون من 1 إلى 5",
    writeText: "اكتب رأيك",
    youTag: "تقييمك",
    owner: "صاحب الإعلان",
    viewProfile: "عرض البروفايل",
  },
  es: {
    back: "Volver",
    backToCommunity: "Volver a Comunidad",
    openMap: "Abrir mapa",
    openLink: "Abrir enlace",
    price: "Precio",
    rating: "Calificación",
    verified: "Verificado",
    directions: "Cómo llegar",
    call: "Llamar",
    reviews: "Reseñas",
    refresh: "Actualizar",
    loadingReviews: "Cargando reseñas…",
    noReviews: "Aún no hay reseñas.",
    yourReview: "Tu reseña",
    addReview: "Agregar reseña",
    loginRequired: "Se requiere inicio de sesión",
    oneReview: "Una reseña por usuario",
    goToLogin: "Ir a iniciar sesión",
    writeReview: "Escribe una reseña",
    youReviewAs: "Estás reseñando como",
    yourRating: "Tu calificación",
    selected: "Seleccionado:",
    submitReview: "Enviar reseña",
    editYourReview: "Editar tu reseña",
    updateReview: "Actualizar reseña",
    delete: "Eliminar",
    edit: "Editar",
    cancel: "Cancelar",
    reviewUpdated: "Reseña actualizada",
    reviewAdded: "Reseña agregada",
    reviewDeleted: "Reseña eliminada",
    itemUpdated: "Elemento actualizado",
    itemDeleted: "Elemento eliminado",
    saving: "Guardando...",
    save: "Guardar",
    deleting: "Eliminando…",
    confirmDeleteTitle: "Confirmar eliminación",
    confirmDeleteReviewMsg: "¿Seguro que quieres eliminar tu reseña?",
    confirmDeleteItemMsg: "¿Seguro que quieres eliminar este elemento?",
    placeNotFound: "Elemento no encontrado.",
    writeOpinion: "Escribe tu reseña…",
    tip: "Consejo: sé claro sobre calidad, precio y servicio.",
    editItemTitle: "Editar elemento",
    editItemSub: "Actualiza la info y guarda",
    loginFirst: "Inicia sesión primero",
    starsRange: "Las estrellas deben ser 1..5",
    writeText: "Escribe algo",
    youTag: "Tu reseña",
    owner: "Propietario",
    viewProfile: "Ver perfil",
  },
};

const t = (lang, key) => {
  const L = normLang(lang);
  return (I18N[L] && I18N[L][key]) || I18N.en[key] || key;
};

function SelectField({
  label,
  value,
  onChange,
  options = [],
  placeholder,
  dir,
}) {
  const [open, setOpen] = useState(false);
  const wrapRef = React.useRef(null);

  useEffect(() => {
    if (!open) return;
    const onDown = (e) => {
      if (!wrapRef.current) return;
      if (!wrapRef.current.contains(e.target)) setOpen(false);
    };
    document.addEventListener("mousedown", onDown);
    return () => document.removeEventListener("mousedown", onDown);
  }, [open]);

  const current = String(value || "").trim();

  return (
    <div ref={wrapRef} className="relative">
      {label ? (
        <div className="mb-1 text-xs font-semibold text-gray-700">{label}</div>
      ) : null}

      <button
        type="button"
        onClick={() => setOpen((s) => !s)}
        className={cn(
          "w-full rounded-xl border px-3 py-2 text-sm bg-white hover:bg-gray-50",
          dir === "rtl" ? "text-right" : "text-left"
        )}
      >
        {current || placeholder || "Select"}
      </button>

      {open ? (
        <div
          className={cn(
            "absolute left-0 right-0 mt-2 z-[99999] rounded-xl border bg-white shadow-lg overflow-hidden",
            dir === "rtl" ? "text-right" : "text-left"
          )}
        >
          <div className="max-h-64 overflow-auto">
            {options.map((opt) => {
              const v = typeof opt === "string" ? opt : opt?.value;
              const labelTxt = typeof opt === "string" ? opt : opt?.label || v;
              const active = String(v) === String(current);

              return (
                <button
                  key={String(v)}
                  type="button"
                  onClick={() => {
                    onChange(String(v));
                    setOpen(false);
                  }}
                  className={cn(
                    "w-full px-3 py-2 text-sm hover:bg-gray-50",
                    dir === "rtl" ? "text-right" : "text-left",
                    active ? "bg-gray-100 font-semibold" : ""
                  )}
                >
                  {labelTxt}
                </button>
              );
            })}
          </div>
        </div>
      ) : null}
    </div>
  );
}

function getToken() {
  return localStorage.getItem("token") || "";
}
function authHeaders() {
  const token = getToken();
  return token ? { Authorization: `Bearer ${token}` } : {};
}
function isLoggedIn() {
  return !!getToken();
}

/* ============================
   ✅ JWT fallback (strong)
============================ */
function base64UrlToUtf8(b64url) {
  try {
    const b64 = b64url.replace(/-/g, "+").replace(/_/g, "/");
    const pad = b64.length % 4 ? "=".repeat(4 - (b64.length % 4)) : "";
    const str = atob(b64 + pad);
    const bytes = Uint8Array.from(str, (c) => c.charCodeAt(0));
    return new TextDecoder("utf-8").decode(bytes);
  } catch {
    return null;
  }
}
function decodeJwtPayload(token) {
  try {
    const part = token.split(".")[1];
    if (!part) return null;
    const json = base64UrlToUtf8(part);
    if (!json) return null;
    return JSON.parse(json);
  } catch {
    return null;
  }
}
function getMeFromTokenFallback() {
  const token = getToken();
  const p = decodeJwtPayload(token);
  if (!p) return null;

  const id = p.id || p.userId || p.user_id || p.sub || p.uid;
  const username =
    p.username ||
    p.name ||
    p.full_name ||
    p.displayName ||
    p.email ||
    (p.firstName && p.lastName ? `${p.firstName} ${p.lastName}` : null) ||
    "You";

  return { id: id ?? null, username };
}

function getMeFromStorageFallback() {
  try {
    const keys = [
      "user",
      "me",
      "profile",
      "currentUser",
      "current_user",
      "authUser",
      "auth_user",
      "userData",
      "user_data",
      "account",
      "sessionUser",
      "session_user",
    ];

    for (const k of keys) {
      const raw = localStorage.getItem(k);
      if (!raw) continue;

      let obj = null;
      try {
        obj = JSON.parse(raw);
      } catch {
        obj = { username: raw };
      }

      const id =
        obj?.id ||
        obj?.userId ||
        obj?.user_id ||
        obj?._id ||
        obj?.sub ||
        obj?.uid ||
        obj?.user?.id ||
        obj?.profile?.id;

      const username =
        obj?.username ||
        obj?.name ||
        obj?.full_name ||
        obj?.displayName ||
        obj?.email ||
        obj?.user?.username ||
        obj?.user?.name ||
        obj?.profile?.username ||
        obj?.profile?.name;

      if (id || username)
        return { id: id ?? null, username: username || "You" };
    }
  } catch {}
  return null;
}

function normName(v) {
  return String(v || "")
    .trim()
    .toLowerCase();
}

function safeDateToLocaleString(v) {
  if (!v) return "";
  const s = String(v).trim().replace(" ", "T");
  const d = new Date(s);
  if (Number.isNaN(d.getTime())) return String(v);
  return d.toLocaleString();
}

/** ===== Stars (read-only) ===== */
function StarsReadOnly({ value = 0, size = 16, showValue = true }) {
  const v = Math.max(0, Math.min(5, Number(value || 0)));
  const rounded = Math.round(v);
  const cls = size >= 20 ? "h-5 w-5" : "h-4 w-4";

  return (
    <div className="inline-flex items-center gap-1">
      {Array.from({ length: 5 }).map((_, i) => {
        const on = i + 1 <= rounded;
        return (
          <Star
            key={i}
            className={cn(
              cls,
              "transition",
              on ? "fill-yellow-400 text-yellow-400" : "text-gray-300"
            )}
          />
        );
      })}
      {showValue ? (
        <span className="ml-1 text-sm font-semibold text-gray-900">
          {v.toFixed(1)}
        </span>
      ) : null}
    </div>
  );
}

/** ===== Stars Picker (interactive) ===== */
function StarsPicker({ lang, value, onChange, size = 24 }) {
  const [hover, setHover] = useState(0);
  const v = Math.max(1, Math.min(5, Number(value || 5)));
  const active = hover || v;

  const cls = size >= 24 ? "h-6 w-6" : "h-5 w-5";
  const labelsEn = {
    1: "Bad",
    2: "Okay",
    3: "Good",
    4: "Very good",
    5: "Excellent",
  };
  const labelsAr = {
    1: "سيئ",
    2: "مقبول",
    3: "جيد",
    4: "جيد جدًا",
    5: "ممتاز",
  };
  const labelsEs = {
    1: "Malo",
    2: "Regular",
    3: "Bueno",
    4: "Muy bueno",
    5: "Excelente",
  };
  const labels =
    normLang(lang) === "ar"
      ? labelsAr
      : normLang(lang) === "es"
      ? labelsEs
      : labelsEn;

  return (
    <div className="flex items-center justify-between gap-3">
      <div className="inline-flex items-center gap-1">
        {Array.from({ length: 5 }).map((_, i) => {
          const n = i + 1;
          const on = n <= active;
          return (
            <button
              key={n}
              type="button"
              onMouseEnter={() => setHover(n)}
              onMouseLeave={() => setHover(0)}
              onClick={() => onChange(n)}
              className="rounded-lg p-1 transition hover:bg-yellow-50 focus:outline-none focus:ring-2 focus:ring-yellow-200"
              aria-label={`Rate ${n} star`}
              title={`Rate ${n} star`}
            >
              <Star
                className={cn(
                  cls,
                  "transition-transform",
                  on ? "fill-yellow-400 text-yellow-400" : "text-gray-300"
                )}
              />
            </button>
          );
        })}
      </div>
      <div className="text-sm font-semibold text-gray-800">
        {labels[active] || ""}
      </div>
    </div>
  );
}

/** ===== Breakdown bars ===== */
function RatingBreakdown({ reviews = [] }) {
  const counts = useMemo(() => {
    const c = { 1: 0, 2: 0, 3: 0, 4: 0, 5: 0 };
    for (const r of reviews) {
      const s = Math.max(1, Math.min(5, Number(r.stars) || 0));
      if (s) c[s] = (c[s] || 0) + 1;
    }
    return c;
  }, [reviews]);

  const total = reviews.length || 0;

  return (
    <div className="mt-3 space-y-2">
      {[5, 4, 3, 2, 1].map((n) => {
        const cnt = counts[n] || 0;
        const pct = total ? Math.round((cnt / total) * 100) : 0;
        return (
          <div key={n} className="flex items-center gap-2">
            <div className="w-10 text-xs font-semibold text-gray-700">{n}★</div>
            <div className="flex-1 h-2 rounded-full bg-gray-100 overflow-hidden border">
              <div
                className="h-full bg-yellow-400"
                style={{ width: `${pct}%` }}
              />
            </div>
            <div className="w-10 text-right text-xs text-gray-600">{cnt}</div>
          </div>
        );
      })}
    </div>
  );
}

function pickId(obj) {
  return (
    obj?.id ??
    obj?.userId ??
    obj?.user_id ??
    obj?._id ??
    obj?.uid ??
    obj?.sub ??
    null
  );
}
function pickUsername(obj) {
  return (
    obj?.username ||
    obj?.name ||
    obj?.full_name ||
    obj?.displayName ||
    obj?.email ||
    null
  );
}

function normalizeMe(any) {
  if (!any) return null;
  const p = any.profile || any.user || any.me || any;
  const id = pickId(p);
  const username = pickUsername(p);
  if (!id && !username) return null;
  return { id: id ?? null, username: username || "You" };
}

function normalizeToPlaceShape(raw, kind) {
  if (!raw) return null;
  const obj = raw?.item || raw?.data?.item || raw?.data || raw;
  if (!obj) return null;

  const name =
    obj.title ||
    obj.name ||
    obj.business_name ||
    obj.company ||
    obj.service_name ||
    obj.product_name ||
    "Listing";

  const category =
    obj.category ||
    obj.kind ||
    obj.type ||
    obj.listing_type ||
    (kind ? String(kind) : "") ||
    "";

  const address =
    obj.address ||
    obj.location ||
    obj.addr ||
    obj.full_address ||
    obj.formatted_address ||
    "";

  const city = obj.city || obj.town || obj.locality || "";
  const state = obj.state || obj.st || obj.region || "";
  const zip = obj.zip || obj.postal || obj.postal_code || "";

  const phone =
    obj.phone || obj.contact || obj.tel || obj.mobile || obj.whatsapp || "";

  const website = obj.website || obj.link || obj.url || obj.site || "";

  const description =
    obj.description || obj.desc || obj.notes || obj.about || "";

  const notes = obj.notes || (obj.description ? obj.description : "") || "";

  const price =
    obj.price ||
    obj.price_value ||
    obj.amount ||
    obj.budget ||
    obj.cost ||
    obj.rate ||
    "";

  return {
    ...obj,
    name,
    category,
    address,
    city,
    state,
    zip,
    phone,
    website,
    description,
    notes,
    price,
  };
}

export default function ItemDetailsView({ lang = "en" }) {
  const L = normLang(lang);
  const dir = dirForLang(L);
  const BackIcon = dir === "rtl" ? ArrowRight : ArrowLeft;

  const params = useParams();
  const navigate = useNavigate();
  const location = useLocation();

  const placeId =
    params.id ||
    params.placeId ||
    params.groupId ||
    params.serviceId ||
    params.jobId ||
    params.housingId ||
    params.productId;

  const navType = location.state?.type ? String(location.state.type) : "";
  const storedType =
    sessionStorage.getItem(`mp:type:${String(placeId || "").trim()}`) || "";

  useEffect(() => {
    const pid = String(placeId || "").trim();
    const ttype = String(navType || "").trim();
    if (!pid) return;
    if (ttype) sessionStorage.setItem(`mp:type:${pid}`, ttype);
  }, [placeId, navType]);

  const [loading, setLoading] = useState(true);
  const [place, setPlace] = useState(null);
  const [detailsBaseUrl, setDetailsBaseUrl] = useState("");

  const [showEditItem, setShowEditItem] = useState(false);
  const [editItemLoading, setEditItemLoading] = useState(false);

  const [eTitle, setETitle] = useState("");
  const [eCategory, setECategory] = useState("");
  const [eAddress, setEAddress] = useState("");
  const [eCity, setECity] = useState("");
  const [eState, setEState] = useState("");
  const [eZip, setEZip] = useState("");
  const [ePhone, setEPhone] = useState("");
  const [eWebsite, setEWebsite] = useState("");
  const [eDesc, setEDesc] = useState("");
  const [ePrice, setEPrice] = useState("");

  const [reviewsLoading, setReviewsLoading] = useState(true);
  const [reviews, setReviews] = useState([]);

  const [meLoading, setMeLoading] = useState(false);
  const [me, setMe] = useState(null);
  const [ownerProfile, setOwnerProfile] = useState(null);

  const [myReview, setMyReview] = useState(null);
  const [rStars, setRStars] = useState(5);
  const [rText, setRText] = useState("");
  const [showReviewForm, setShowReviewForm] = useState(false);

  const arr = (v) => (Array.isArray(v) ? v : []);

  const effectiveMe = useMemo(() => {
    return (
      normalizeMe(me) ||
      normalizeMe(getMeFromStorageFallback()) ||
      normalizeMe(getMeFromTokenFallback()) ||
      null
    );
  }, [me]);

  const requireLogin = () => {
    notify.error(t(L, "loginFirst"));
    navigate("/auth");
  };

  const ALLOWED_KINDS = new Set([
    "places",
    "groups",
    "services",
    "jobs",
    "housing",
    "products",
  ]);

  const rawKind = String(navType || storedType || "")
    .trim()
    .toLowerCase();
  const idStr = String(placeId || "").trim();
  const idPrefix = idStr.includes("_") ? idStr.split("_")[0].toLowerCase() : "";

  const kind = ALLOWED_KINDS.has(rawKind)
    ? rawKind
    : ALLOWED_KINDS.has(idPrefix)
    ? idPrefix
    : "places";
  const shortId = idStr.includes("_")
    ? idStr.split("_").slice(1).join("_")
    : idStr;

  const isPlaceType = kind === "places";

  const canEditItem = useMemo(() => {
    const myId = effectiveMe?.id ? String(effectiveMe.id) : "";
    const createdBy =
      place?.created_by ??
      place?.createdBy ??
      place?.user_id ??
      place?.userId ??
      place?.owner_id ??
      place?.ownerId ??
      null;

    if (!myId || !createdBy) return false;
    return String(createdBy) === myId;
  }, [effectiveMe, place]);

  const detailsCandidates = useMemo(() => {
    if (kind === "places") {
      return [
        `${API_BASE}/api/community/places/${placeId}`,
        `${API_BASE}/api/community/places/${shortId}`,
        `${API_BASE}/api/marketplace/places/${placeId}`,
        `${API_BASE}/api/marketplace/places/${shortId}`,
        `${API_BASE}/api/listings/${placeId}`,
        `${API_BASE}/api/listings/${shortId}`,
        `${API_BASE}/api/marketplace/listings/${placeId}`,
        `${API_BASE}/api/marketplace/listings/${shortId}`,
      ];
    }

    if (kind === "groups") {
      return [
        `${API_BASE}/api/community/groups/${placeId}`,
        `${API_BASE}/api/community/groups/${shortId}`,
        `${API_BASE}/api/marketplace/groups/${placeId}`,
        `${API_BASE}/api/marketplace/groups/${shortId}`,
        `${API_BASE}/api/listings/${placeId}`,
        `${API_BASE}/api/listings/${shortId}`,
        `${API_BASE}/api/marketplace/listings/${placeId}`,
        `${API_BASE}/api/marketplace/listings/${shortId}`,
      ];
    }

    return [
      `${API_BASE}/api/listings/${placeId}`,
      `${API_BASE}/api/listings/${shortId}`,
      `${API_BASE}/api/marketplace/listings/${placeId}`,
      `${API_BASE}/api/marketplace/listings/${shortId}`,
      `${API_BASE}/api/listings?type=${encodeURIComponent(kind)}&id=${placeId}`,
      `${API_BASE}/api/listings?type=${encodeURIComponent(kind)}&id=${shortId}`,
    ];
  }, [kind, placeId, shortId]);

  const reviewsCandidates = useMemo(() => {
    const k = encodeURIComponent(kind);
    return [
      `${API_BASE}/api/community/${k}/${placeId}/reviews`,
      `${API_BASE}/api/community/${k}/${shortId}/reviews`,
      `${API_BASE}/api/marketplace/${k}/${placeId}/reviews`,
      `${API_BASE}/api/marketplace/${k}/${shortId}/reviews`,
      `${API_BASE}/api/listings/${placeId}/reviews`,
      `${API_BASE}/api/listings/${shortId}/reviews`,
      `${API_BASE}/api/marketplace/listings/${placeId}/reviews`,
      `${API_BASE}/api/marketplace/listings/${shortId}/reviews`,
    ];
  }, [kind, placeId, shortId]);

  const reviewMeCandidates = useMemo(() => {
    const k = encodeURIComponent(kind);
    return [
      `${API_BASE}/api/community/${k}/${placeId}/reviews/me`,
      `${API_BASE}/api/community/${k}/${shortId}/reviews/me`,
      `${API_BASE}/api/marketplace/${k}/${placeId}/reviews/me`,
      `${API_BASE}/api/marketplace/${k}/${shortId}/reviews/me`,
      `${API_BASE}/api/listings/${placeId}/reviews/me`,
      `${API_BASE}/api/listings/${shortId}/reviews/me`,
      `${API_BASE}/api/marketplace/listings/${placeId}/reviews/me`,
      `${API_BASE}/api/marketplace/listings/${shortId}/reviews/me`,
    ];
  }, [kind, placeId, shortId]);

  const [reviewsBaseUrl, setReviewsBaseUrl] = useState("");
  const [reviewMeBaseUrl, setReviewMeBaseUrl] = useState("");

  const openMapUrl = useMemo(() => {
    if (!place) return "";
    const q = encodeURIComponent(
      [place.name, place.address, place.city, place.state, place.zip]
        .filter(Boolean)
        .join(", ")
    );
    return `https://www.google.com/maps/search/?api=1&query=${q}`;
  }, [place]);

  const displayPrice = useMemo(() => {
    const p =
      place?.price ??
      place?.budget ??
      place?.avg_price ??
      place?.avgPrice ??
      "";
    return String(p || "").trim();
  }, [place]);

  const displayContact = useMemo(() => {
    const c = place?.contact ?? place?.phone ?? "";
    return String(c || "").trim();
  }, [place]);

  const displayLink = useMemo(() => {
    const w = place?.website ?? place?.link ?? place?.url ?? "";
    return String(w || "").trim();
  }, [place]);

  const displayUserName = (u) =>
    u?.username ||
    u?.name ||
    u?.full_name ||
    u?.displayName ||
    u?.email ||
    "You";
  // ✅ OWNER (avoid falling back to listing title/name)
  const pickOwnerId = (p) => {
    const raw = p?.raw || p?.data?.raw || null;

    return (
      p?.created_by ??
      p?.createdBy ??
      p?.user_id ??
      p?.userId ??
      p?.owner_id ??
      p?.ownerId ??
      raw?.created_by ??
      raw?.createdBy ??
      raw?.user_id ??
      raw?.userId ??
      p?.owner?.id ??
      p?.owner?.user_id ??
      p?.owner?.userId ??
      p?.user?.id ??
      p?.user?.user_id ??
      p?.user?.userId ??
      p?.user?.uid ??
      null
    );
  };

  const pickOwnerName = (p) => {
    // ❌ ممنوع تستخدم p.name / p.title هنا (ده اسم الإعلان)
    return (
      p?.created_by_name ??
      p?.createdByName ??
      p?.created_by_username ??
      p?.createdByUsername ??
      p?.owner_name ??
      p?.ownerName ??
      p?.user?.username ??
      p?.user?.name ??
      p?.user?.full_name ??
      p?.owner?.username ??
      p?.owner?.name ??
      p?.owner?.full_name ??
      p?.createdByUser?.username ??
      p?.createdByUser?.name ??
      p?.createdByUser?.full_name ??
      p?.created_by_email ??
      p?.createdByEmail ??
      null
    );
  };

  const rawOwnerId = useMemo(() => {
    const v = place ? pickOwnerId(place) : null;
    return v == null ? "" : String(v);
  }, [place]);

  const rawOwnerName = useMemo(
    () => (place ? pickOwnerName(place) : "—"),
    [place]
  );
  const ownerId = String(rawOwnerId || ownerProfile?.id || "").trim();
  const ownerName = String(ownerProfile?.name || rawOwnerName || "—").trim();

  const avgRating = useMemo(() => {
    if (!reviews?.length) return 0;
    const sum = reviews.reduce((a, r) => a + (Number(r.stars) || 0), 0);
    return sum / reviews.length;
  }, [reviews]);

  const sortReviewsDesc = (arrv) => {
    const list = Array.isArray(arrv) ? [...arrv] : [];
    const toTime = (v) => {
      if (!v) return 0;
      const s = String(v).trim().replace(" ", "T");
      const tt = new Date(s).getTime();
      return Number.isNaN(tt) ? 0 : tt;
    };
    list.sort((a, b) => {
      const tb = toTime(b?.created_at || b?.createdAt);
      const ta = toTime(a?.created_at || a?.createdAt);
      if (tb !== ta) return tb - ta;
      const ib = Number(b?.id || b?.review_id || 0);
      const ia = Number(a?.id || a?.review_id || 0);
      return ib - ia;
    });
    return list;
  };

  const fetchPlace = async () => {
    try {
      setLoading(true);
      const out = await fetchFirstOk(detailsCandidates, {
        headers: { ...authHeaders() },
      });
      if (!out.ok) throw new Error(out.error || "Failed to load details");
      setDetailsBaseUrl(out.url || "");
      const normalized = normalizeToPlaceShape(out.data, kind);
      setPlace(normalized);
    } catch (e) {
      notify.error(e?.message || "Failed to load details");
      setPlace(null);
    } finally {
      setLoading(false);
    }
  };

  const fetchReviews = async () => {
    try {
      setReviewsLoading(true);

      const out = await fetchFirstOk(reviewsCandidates, {
        headers: { ...authHeaders() },
      });

      if (!out.ok) {
        setReviews([]);
        setReviewsBaseUrl("");
        setReviewMeBaseUrl("");
        return;
      }

      const list = Array.isArray(out.data) ? out.data : out.data?.reviews || [];
      setReviews(sortReviewsDesc(list));
      setReviewsBaseUrl(out.url || "");

      const guessMe = (out.url || "").replace(/\/reviews\/?$/, "/reviews/me");
      setReviewMeBaseUrl(guessMe || "");
    } catch (e) {
      notify.error(e?.message || "Failed to load reviews");
      setReviews([]);
      setReviewsBaseUrl("");
      setReviewMeBaseUrl("");
    } finally {
      setReviewsLoading(false);
    }
  };

  const fetchMe = async () => {
    if (!isLoggedIn()) {
      setMe(null);
      return;
    }

    const boot =
      normalizeMe(getMeFromStorageFallback()) ||
      normalizeMe(getMeFromTokenFallback());
    if (!me && boot) setMe(boot);

    try {
      setMeLoading(true);
      const res = await fetch(`${API_BASE}/api/profile/me`, {
        headers: { ...authHeaders() },
      });
      const data = await res.json().catch(() => ({}));
      if (!res.ok) {
        if (boot) setMe(boot);
        return;
      }
      setMe(normalizeMe(data) || boot);
    } catch {
      setMe(boot || null);
    } finally {
      setMeLoading(false);
    }
  };

  const fetchOwnerProfile = async (oidParam) => {
    const oid = String(oidParam || "").trim();

    if (!oid) {
      setOwnerProfile(null);
      return;
    }

    const urls = [
      `${API_BASE}/api/profile/${oid}`, // ✅ مؤكد عندك
      `${API_BASE}/api/users/${oid}`,
      `${API_BASE}/api/user/${oid}`,
      `${API_BASE}/api/profiles/${oid}`,
      `${API_BASE}/api/users/${oid}/profile`,
      `${API_BASE}/api/user/${oid}/profile`,
    ];

    const out = await fetchFirstOk(urls, { headers: { ...authHeaders() } });
    if (!out.ok) {
      setOwnerProfile(null);
      return;
    }

    const u = out.data?.user || out.data?.profile || out.data?.me || out.data;
    const pu = out.data?.profile || out.data?.user_profile || null;
    const src = pu || u;

    const name =
      src?.display_name ||
      src?.displayName ||
      src?.username ||
      src?.name ||
      src?.full_name ||
      src?.email ||
      "";

    const id =
      src?.id ||
      src?.userId ||
      src?.user_id ||
      src?._id ||
      src?.uid ||
      src?.sub ||
      oid;

    setOwnerProfile({
      id: String(id || oid),
      name: String(name || "").trim() || "—",
    });
  };

  useEffect(() => {
    fetchPlace();
    fetchReviews();
    fetchMe();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [placeId, navType]);
  useEffect(() => {
    if (!rawOwnerId) {
      setOwnerProfile(null);
      return;
    }
    fetchOwnerProfile(rawOwnerId);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [rawOwnerId]);

  // Detect myReview by id OR name
  useEffect(() => {
    if (!effectiveMe) {
      setMyReview(null);
      setShowReviewForm(false);
      return;
    }

    const myId = effectiveMe?.id ? String(effectiveMe.id) : null;
    const myName = normName(effectiveMe?.username);

    const mineById =
      myId &&
      (reviews.find((r) => String(r.user_id) === myId) ||
        reviews.find((r) => String(r.userId) === myId) ||
        reviews.find((r) => String(r.user?.id) === myId) ||
        reviews.find((r) => String(r.user?.user_id) === myId));

    const mineByName =
      !mineById &&
      myName &&
      reviews.find((r) => {
        const rn = normName(
          r.username ||
            r.name ||
            r.user_name ||
            r.user?.username ||
            r.user?.name
        );
        return rn && rn === myName;
      });

    const mine = mineById || mineByName || null;

    setMyReview(mine);

    if (mine) {
      setRStars(Number(mine.stars) || 5);
      setRText(mine.text || "");
      setShowReviewForm(false);
    } else {
      setRStars(5);
      setRText("");
      setShowReviewForm(true);
    }
  }, [reviews, effectiveMe]);

  const submitOrUpdateReview = async () => {
    if (!isLoggedIn()) return requireLogin();

    const text = rText.trim();
    const stars = Number(rStars);

    if (!text) return notify.error(t(L, "writeText"));
    if (!(stars >= 1 && stars <= 5)) return notify.error(t(L, "starsRange"));

    const pid = String(placeId || "").trim();
    const sid = String(shortId || "").trim();
    const hasPrefix = pid.includes("_");

    const singular =
      kind === "groups"
        ? "group"
        : kind === "places"
        ? "place"
        : kind === "services"
        ? "service"
        : kind;

    const prefId = hasPrefix ? pid : `${singular}_${sid || pid}`;
    const kEnc = encodeURIComponent(kind);

    const postUrls =
      kind === "places" || kind === "groups"
        ? [
            `${API_BASE}/api/community/${kEnc}/${sid}/reviews`,
            `${API_BASE}/api/community/${kEnc}/${pid}/reviews`,
            `${API_BASE}/api/listings/${sid}/reviews`,
            `${API_BASE}/api/marketplace/listings/${sid}/reviews`,
            `${API_BASE}/api/listings/${prefId}/reviews`,
            `${API_BASE}/api/marketplace/listings/${prefId}/reviews`,
          ]
        : [
            `${API_BASE}/api/listings/${prefId}/reviews`,
            `${API_BASE}/api/listings/${pid}/reviews`,
            `${API_BASE}/api/listings/${sid}/reviews`,
            `${API_BASE}/api/marketplace/listings/${prefId}/reviews`,
            `${API_BASE}/api/marketplace/listings/${pid}/reviews`,
            `${API_BASE}/api/marketplace/listings/${sid}/reviews`,
          ];

    const methods = ["POST"];

    const reviewId =
      myReview?.id ||
      myReview?.review_id ||
      myReview?._id ||
      myReview?.rid ||
      null;

    const payload = {
      stars,
      rating: stars,
      score: stars,
      text,
      comment: text,
      body: text,
      message: text,
      id: sid || pid,
      itemId: sid || pid,
      placeId: sid || pid,
      listingId: sid || pid,
      targetId: sid || pid,
      prefixedId: prefId,
      listing_id: sid || pid,
      kind,
      type: kind,
      listing_type: kind,
    };

    let lastErr = null;

    try {
      for (const u of postUrls) {
        for (const m of methods) {
          try {
            const res = await fetch(u, {
              method: m,
              headers: { "Content-Type": "application/json", ...authHeaders() },
              body: JSON.stringify(payload),
            });
            const data = await res.json().catch(() => ({}));

            if (res.status === 401) return requireLogin();

            if (res.ok) {
              notify.success(
                myReview ? t(L, "reviewUpdated") : t(L, "reviewAdded")
              );
              await fetchReviews();
              setShowReviewForm(false);
              return;
            }

            lastErr =
              data?.error ||
              data?.message ||
              data?.details ||
              (typeof data === "string" ? data : JSON.stringify(data)) ||
              `HTTP ${res.status}`;
          } catch (e) {
            lastErr = e?.message || "Network error";
          }
        }
      }

      if (reviewId) {
        for (const u of postUrls) {
          try {
            const res = await fetch(`${u}/${reviewId}`, {
              method: "POST",
              headers: { "Content-Type": "application/json", ...authHeaders() },
              body: JSON.stringify(payload),
            });
            const data = await res.json().catch(() => ({}));
            if (res.status === 401) return requireLogin();

            if (res.ok) {
              notify.success(t(L, "reviewUpdated"));
              await fetchReviews();
              setShowReviewForm(false);
              return;
            }

            lastErr =
              data?.error ||
              data?.message ||
              data?.details ||
              (typeof data === "string" ? data : JSON.stringify(data)) ||
              `HTTP ${res.status}`;
          } catch (e) {
            lastErr = e?.message || "Network error";
          }
        }
      }

      throw new Error(lastErr || "Failed to save review");
    } catch (e) {
      notify.error(e?.message || "Failed to save review");
    }
  };

  const startEditMyReview = () => {
    if (!myReview) return;
    setRStars(Number(myReview.stars) || 5);
    setRText(myReview.text || "");
    setShowReviewForm(true);
  };

  const cancelEdit = () => {
    if (myReview) {
      setRStars(Number(myReview.stars) || 5);
      setRText(myReview.text || "");
      setShowReviewForm(false);
    } else {
      setRStars(5);
      setRText("");
      setShowReviewForm(true);
    }
  };

  const deleteMyReview = async () => {
    if (!isLoggedIn()) return requireLogin();

    const pid = String(placeId || "").trim();
    const sid = String(shortId || "").trim();
    const hasPrefix = pid.includes("_");

    const singular =
      kind === "groups"
        ? "group"
        : kind === "places"
        ? "place"
        : kind === "services"
        ? "service"
        : kind;

    const prefId = hasPrefix ? pid : `${singular}_${sid || pid}`;
    const reviewId =
      myReview?.id ||
      myReview?.review_id ||
      myReview?._id ||
      myReview?.rid ||
      null;

    const deleteUrls = [
      ...reviewMeCandidates,
      ...(reviewsBaseUrl
        ? [reviewsBaseUrl.replace(/\/reviews\/?$/, "/reviews/me")]
        : []),
      ...(reviewId
        ? [
            `${API_BASE}/api/listings/${prefId}/reviews/${reviewId}`,
            `${API_BASE}/api/listings/${pid}/reviews/${reviewId}`,
            `${API_BASE}/api/listings/${sid}/reviews/${reviewId}`,
            `${API_BASE}/api/marketplace/listings/${prefId}/reviews/${reviewId}`,
            `${API_BASE}/api/marketplace/listings/${pid}/reviews/${reviewId}`,
            `${API_BASE}/api/marketplace/listings/${sid}/reviews/${reviewId}`,
          ]
        : []),
    ].filter(Boolean);

    const ok = await toastConfirm({
      title: t(L, "confirmDeleteTitle"),
      message: t(L, "confirmDeleteReviewMsg"),
      confirmText: t(L, "delete"),
      cancelText: t(L, "cancel"),
    });
    if (!ok) return;

    const loadingId = notify.loading(t(L, "deleting"));

    try {
      let lastErr = null;

      for (const u of deleteUrls) {
        try {
          const res = await fetch(u, {
            method: "DELETE",
            headers: { ...authHeaders() },
          });
          const data = await res.json().catch(() => ({}));

          if (res.status === 401) return requireLogin();

          if (res.ok) {
            notify.dismiss(loadingId);
            notify.success(t(L, "reviewDeleted"));
            setMyReview(null);
            setShowReviewForm(true);
            setRStars(5);
            setRText("");
            await fetchReviews();
            return;
          }

          lastErr =
            data?.error ||
            data?.message ||
            (typeof data === "string" ? data : JSON.stringify(data)) ||
            `HTTP ${res.status}`;
        } catch (e) {
          lastErr = e?.message || "Network error";
        }
      }

      throw new Error(lastErr || "Failed to delete review");
    } catch (e) {
      notify.dismiss(loadingId);
      notify.error(e?.message || "Failed to delete review");
    }
  };

  const isReviewMine = (r) => {
    if (!effectiveMe) return false;

    const myId = effectiveMe?.id ? String(effectiveMe.id) : null;
    const myName = normName(effectiveMe?.username);

    const rid =
      r?.user_id || r?.userId || r?.user?.id || r?.user?.user_id || null;
    if (myId && rid && String(rid) === myId) return true;

    const rn = normName(
      r?.username ||
        r?.name ||
        r?.user_name ||
        r?.user?.username ||
        r?.user?.name
    );
    if (myName && rn && rn === myName) return true;

    return false;
  };

  const writeFirstOk = async (urls, opts) => {
    let lastErr = null;
    for (const u of urls) {
      try {
        const res = await fetch(u, opts);
        const data = await res.json().catch(() => ({}));
        if (res.status === 401)
          return { ok: false, unauthorized: true, url: u, data };
        if (res.ok) return { ok: true, url: u, data };
        lastErr = data?.error || data?.message || `HTTP ${res.status}`;
      } catch (e) {
        lastErr = e?.message || "Network error";
      }
    }
    return { ok: false, error: lastErr || "Failed" };
  };

  const buildItemWriteCandidates = () => {
    const pid = String(placeId || "").trim();
    const sid = String(shortId || "").trim();
    const base = detailsBaseUrl ? [detailsBaseUrl] : [];
    const kEnc = encodeURIComponent(kind);

    const more =
      kind === "places" || kind === "groups"
        ? [
            `${API_BASE}/api/community/${kEnc}/${sid}`,
            `${API_BASE}/api/community/${kEnc}/${pid}`,
            `${API_BASE}/api/listings/${sid}`,
            `${API_BASE}/api/listings/${pid}`,
            `${API_BASE}/api/marketplace/listings/${sid}`,
            `${API_BASE}/api/marketplace/listings/${pid}`,
          ]
        : [
            `${API_BASE}/api/listings/${pid}`,
            `${API_BASE}/api/listings/${sid}`,
            `${API_BASE}/api/marketplace/listings/${pid}`,
            `${API_BASE}/api/marketplace/listings/${sid}`,
          ];

    return [...base, ...more].filter(Boolean);
  };

  const openEditItem = () => {
    if (!place) return;
    setETitle(place.name || "");
    setECategory(place.category || "");
    setEAddress(place.address || "");
    setECity(place.city || "");
    setEState(place.state || "");
    setEZip(place.zip || "");
    setEPhone(place.phone || place.contact || "");
    setEWebsite(place.website || place.link || place.url || "");
    setEDesc(place.description || place.notes || "");
    setEPrice(String(place.price || place.price_value || place.amount || ""));
    setShowEditItem(true);
  };

  const updateItem = async () => {
    if (!isLoggedIn()) return requireLogin();
    if (!place) return;

    const payload = {
      name: eTitle,
      title: eTitle,
      category: eCategory,
      address: eAddress,
      city: eCity,
      state: eState,
      zip: eZip,
      phone: ePhone,
      contact: ePhone,
      website: eWebsite,
      link: eWebsite,
      url: eWebsite,
      description: eDesc,
      notes: eDesc,
      price: ePrice,
      price_value: ePrice,
      amount: ePrice,
    };

    const urls = buildItemWriteCandidates();
    setEditItemLoading(true);
    const loadingId = notify.loading(t(L, "saving"));

    try {
      const out = await writeFirstOk(urls, {
        method: "PUT",
        headers: { "Content-Type": "application/json", ...authHeaders() },
        body: JSON.stringify(payload),
      });

      if (!out.ok && out.unauthorized) return requireLogin();

      if (!out.ok) {
        const out2 = await writeFirstOk(urls, {
          method: "PATCH",
          headers: { "Content-Type": "application/json", ...authHeaders() },
          body: JSON.stringify(payload),
        });
        if (!out2.ok && out2.unauthorized) return requireLogin();
        if (!out2.ok) throw new Error(out2.error || "Failed to update item");
      }

      notify.dismiss(loadingId);
      notify.success(t(L, "itemUpdated"));
      setShowEditItem(false);
      await fetchPlace();
    } catch (e) {
      notify.dismiss(loadingId);
      notify.error(e?.message || "Failed to update item");
    } finally {
      setEditItemLoading(false);
    }
  };

  const deleteItem = async () => {
    if (!isLoggedIn()) return requireLogin();
    if (!place) return;

    const ok = await toastConfirm({
      title: t(L, "confirmDeleteTitle"),
      message: t(L, "confirmDeleteItemMsg"),
      confirmText: t(L, "delete"),
      cancelText: t(L, "cancel"),
    });
    if (!ok) return;

    const urls = buildItemWriteCandidates();
    const loadingId = notify.loading(t(L, "deleting"));

    try {
      const out = await writeFirstOk(urls, {
        method: "DELETE",
        headers: { ...authHeaders() },
      });

      if (!out.ok && out.unauthorized) return requireLogin();
      if (!out.ok) throw new Error(out.error || "Failed to delete item");

      notify.dismiss(loadingId);
      notify.success(t(L, "itemDeleted"));
      navigate("/community");
    } catch (e) {
      notify.dismiss(loadingId);
      notify.error(e?.message || "Failed to delete item");
    }
  };

  if (!place) {
    return (
      <div dir={dir} className="mx-auto max-w-5xl p-4 md:p-6">
        <div className="rounded-2xl border bg-white p-6 shadow-sm">
          <div className="mb-4 flex items-center gap-2">
            <Link
              to="/community"
              className="inline-flex items-center gap-2 text-sm text-gray-700 hover:underline"
            >
              <BackIcon className="h-4 w-4" /> {t(L, "back")}
            </Link>
          </div>
          {t(L, "placeNotFound")}
        </div>
      </div>
    );
  }

  return (
    <div dir={dir} className="mx-auto max-w-5xl p-4 md:p-6">
      {/* Top bar */}
      <div className="mb-4 flex flex-col sm:flex-row sm:items-center sm:justify-between gap-3">
        <Link
          to="/community"
          className="inline-flex items-center gap-2 text-sm text-gray-700 hover:underline"
        >
          <BackIcon className="h-4 w-4" /> {t(L, "backToCommunity")}
        </Link>

        <a
          href={openMapUrl}
          target="_blank"
          rel="noreferrer"
          className="inline-flex items-center justify-center gap-2 rounded-xl bg-black px-4 py-2 text-sm text-white hover:opacity-90 w-full sm:w-auto"
        >
          <ExternalLink className="h-4 w-4" /> {t(L, "openMap")}
        </a>
      </div>

      {/* Place card */}
      <div className="rounded-2xl border bg-white p-5 md:p-6 shadow-sm">
        {canEditItem ? (
          <div
            className={cn(
              "flex items-center gap-2 mb-5",
              dir === "rtl" ? "mr-auto" : "ml-auto"
            )}
          >
            <button
              onClick={openEditItem}
              className="inline-flex items-center gap-2 rounded-xl border bg-white px-3 py-2 text-sm text-gray-700 hover:bg-gray-50"
              title={t(L, "edit")}
              type="button"
            >
              <Pencil className="h-4 w-4" />
              {t(L, "edit")}
            </button>

            <button
              onClick={deleteItem}
              className="inline-flex items-center gap-2 rounded-xl border bg-white px-3 py-2 text-sm text-red-600 hover:bg-red-50"
              title={t(L, "delete")}
              type="button"
            >
              <Trash2 className="h-4 w-4" />
              {t(L, "delete")}
            </button>
          </div>
        ) : null}

        <div className="flex flex-col gap-5 md:flex-row md:items-start md:justify-between">
          {/* Left info */}
          <div className="min-w-0 flex-1">
            <div className="mb-2 inline-flex items-center gap-2 rounded-full border bg-gray-50 px-3 py-1 text-xs text-gray-700 max-w-full">
              <span className="h-2 w-2 rounded-full bg-orange-500" />
              <span className="truncate">{place.category || "Place"}</span>
              <span className="text-gray-300">•</span>
              <span className="truncate">
                {place.city || "—"}, {place.state || "—"}
              </span>
            </div>

            <h1 className="text-xl md:text-2xl font-semibold text-gray-900 break-words">
              {place.name}
            </h1>
            <div className="mt-3 flex flex-wrap items-center gap-2 text-sm">
              {ownerId ? (
                <span className="inline-flex items-center gap-2 rounded-xl bg-gray-50 border px-3 py-2 text-gray-700">
                  <span className="text-xs font-semibold text-gray-600">
                    {t(L, "owner")}
                    {/* {console.log(place)} */}
                  </span>

                  {ownerId && ownerName && ownerName !== "—" ? (
                    <button
                      type="button"
                      onClick={() => navigate(`/u/${ownerId}`)}
                      className="font-semibold text-gray-900 hover:underline"
                      title={ownerName}
                    >
                      {ownerName}
                    </button>
                  ) : (
                    <span className="font-semibold text-gray-900">
                      {ownerName || "—"}
                    </span>
                  )}
                </span>
              ) : null}
            </div>

            <div className="mt-2 flex flex-wrap items-center gap-2 text-sm text-gray-700">
              <span className="inline-flex items-center gap-2 rounded-xl bg-gray-50 border px-3 py-2">
                <MapPin className="h-4 w-4 text-gray-500" />
                {[place.address, place.city, place.state, place.zip]
                  .filter(Boolean)
                  .join(", ")}
              </span>

              {!isPlaceType && displayPrice ? (
                <span className="inline-flex items-center gap-2 rounded-xl bg-gray-50 border px-3 py-2">
                  <span className="text-xs font-semibold text-gray-600">
                    {t(L, "price")}
                  </span>
                  <span className="text-sm font-semibold text-gray-900">
                    ${displayPrice}
                  </span>
                </span>
              ) : null}

              {!isPlaceType && displayLink ? (
                <a
                  className="inline-flex items-center gap-2 rounded-xl bg-gray-50 border px-3 py-2 hover:bg-gray-100"
                  href={displayLink}
                  target="_blank"
                  rel="noreferrer"
                >
                  <ExternalLink className="h-4 w-4 text-gray-500" />
                  {t(L, "openLink")}
                </a>
              ) : null}

              {!isPlaceType && displayContact ? (
                <a
                  className="inline-flex items-center gap-2 rounded-xl bg-gray-50 border px-3 py-2 hover:bg-gray-100"
                  href={`tel:${String(displayContact).replace(/\s+/g, "")}`}
                >
                  <Phone className="h-4 w-4 text-gray-500" />
                  {displayContact}
                </a>
              ) : null}

              {place.phone ? (
                <a
                  className="inline-flex items-center gap-2 rounded-xl bg-gray-50 border px-3 py-2 hover:bg-gray-100"
                  href={`tel:${String(place.phone).replace(/\s+/g, "")}`}
                >
                  <Phone className="h-4 w-4 text-gray-500" />
                  {place.phone}
                </a>
              ) : null}
            </div>

            {place.description ? (
              <p className="mt-4 text-sm leading-6 text-gray-700">
                {place.description}
              </p>
            ) : null}
            {!place.description && place.notes ? (
              <p className="mt-4 text-sm leading-6 text-gray-700">
                {place.notes}
              </p>
            ) : null}
          </div>

          {/* Right rating card */}
          <div className="w-full md:w-[340px] shrink-0">
            <div className="rounded-2xl border bg-gradient-to-b from-gray-50 to-white p-4 shadow-sm">
              <div className="flex items-start justify-between gap-3">
                <div>
                  <div className="text-sm font-semibold text-gray-900">
                    {t(L, "rating")}
                  </div>
                  <div className="mt-1 flex items-center gap-2">
                    <div className="inline-flex items-center justify-center rounded-2xl bg-black text-white px-3 py-2">
                      <div className="text-2xl font-extrabold leading-none">
                        {avgRating ? avgRating.toFixed(1) : "0.0"}
                      </div>
                    </div>
                    <div className="mt-1">
                      <StarsReadOnly
                        value={avgRating}
                        size={20}
                        showValue={false}
                      />
                      <div className="text-xs text-gray-600 mt-1">
                        {reviews.length} {t(L, "reviews")}
                      </div>
                    </div>
                  </div>
                </div>

                <div className="inline-flex items-center gap-1 rounded-xl border bg-white px-3 py-2 text-xs text-gray-700">
                  <Sparkles className="h-4 w-4" />
                  {t(L, "verified")}
                </div>
              </div>

              <RatingBreakdown reviews={reviews} />

              <div className="mt-4 grid grid-cols-2 gap-2">
                <a
                  href={openMapUrl}
                  target="_blank"
                  rel="noreferrer"
                  className="inline-flex items-center justify-center gap-2 rounded-xl bg-black px-3 py-2 text-sm text-white hover:opacity-90"
                >
                  <MapPin className="h-4 w-4" /> {t(L, "directions")}
                </a>

                {place.phone ? (
                  <a
                    href={`tel:${String(place.phone).replace(/\s+/g, "")}`}
                    className="inline-flex items-center justify-center gap-2 rounded-xl border bg-white px-3 py-2 text-sm hover:bg-gray-50"
                  >
                    <Phone className="h-4 w-4" /> {t(L, "call")}
                  </a>
                ) : (
                  <button
                    disabled
                    className="inline-flex items-center justify-center gap-2 rounded-xl border bg-white px-3 py-2 text-sm opacity-50"
                    type="button"
                  >
                    <Phone className="h-4 w-4" /> {t(L, "call")}
                  </button>
                )}
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* Reviews + Add/Update */}
      <div className="mt-6 grid grid-cols-1 gap-6 md:grid-cols-5">
        {/* List */}
        <div className="md:col-span-3">
          <div className="rounded-2xl border bg-white p-5 shadow-sm">
            <div className="flex items-center justify-between">
              <h2 className="text-base font-semibold text-gray-900">
                {t(L, "reviews")}
              </h2>
              <button
                onClick={fetchReviews}
                className="text-sm text-gray-700 hover:underline"
                type="button"
              >
                {t(L, "refresh")}
              </button>
            </div>

            {reviewsLoading ? (
              <div className="mt-4 text-sm text-gray-600">
                {t(L, "loadingReviews")}
              </div>
            ) : reviews.length === 0 ? (
              <div className="mt-4 text-sm text-gray-600">
                {t(L, "noReviews")}
              </div>
            ) : (
              <div className="mt-4 space-y-3">
                {reviews.map((r) => {
                  const isMine = isReviewMine(r);
                  const safeKey =
                    r.id ||
                    `${r.user_id || r.userId || r.user?.id || "u"}-${
                      r.created_at || "t"
                    }-${r.stars || "s"}`;

                  return (
                    <div
                      key={safeKey}
                      className={cn(
                        "rounded-2xl border p-4 transition",
                        isMine
                          ? "border-black/20 bg-gray-50"
                          : "hover:bg-gray-50"
                      )}
                    >
                      <div className="flex items-start justify-between gap-3">
                        <div className="min-w-0">
                          <div className="flex flex-wrap items-center gap-2">
                            <div className="font-semibold text-gray-900 text-sm break-words">
                              {r.username ||
                                r.name ||
                                r.user_name ||
                                r.user?.username ||
                                r.user?.name ||
                                "User"}
                            </div>

                            {isMine ? (
                              <span className="inline-flex items-center rounded-full bg-black text-white px-2 py-0.5 text-[11px] font-semibold">
                                {t(L, "youTag")}
                              </span>
                            ) : null}

                            <div className="inline-flex items-center gap-2 rounded-full bg-gray-100 border px-2 py-0.5">
                              <StarsReadOnly
                                value={Number(r.stars || 0)}
                                size={14}
                                showValue={false}
                              />
                              <span className="text-xs font-semibold text-gray-700">
                                {Number(r.stars || 0)}/5
                              </span>
                            </div>
                          </div>
                        </div>

                        <div className="flex items-center gap-2">
                          <div className="text-xs text-gray-500 whitespace-nowrap">
                            {safeDateToLocaleString(r.created_at)}
                          </div>

                          {isMine && isLoggedIn() ? (
                            <button
                              onClick={startEditMyReview}
                              className="inline-flex items-center gap-1 rounded-lg border bg-white px-2 py-1 text-xs text-gray-700 hover:bg-gray-50"
                              title={t(L, "edit")}
                              type="button"
                            >
                              <Pencil className="h-3.5 w-3.5" />
                              {t(L, "edit")}
                            </button>
                          ) : null}
                        </div>
                      </div>

                      <p className="mt-3 text-sm leading-6 text-gray-700">
                        {r.text}
                      </p>
                    </div>
                  );
                })}
              </div>
            )}
          </div>

          {/* If has review and clicked Edit => show form under list */}
          {isLoggedIn() && myReview && showReviewForm ? (
            <div className="mt-6 rounded-2xl border bg-white p-5 shadow-sm">
              <div className="flex items-center justify-between gap-3">
                <div className="flex items-center gap-2">
                  <MessageSquarePlus className="h-4 w-4 text-gray-700" />
                  <h2 className="text-base font-semibold text-gray-900">
                    {t(L, "editYourReview")}
                  </h2>
                </div>

                <button
                  onClick={cancelEdit}
                  className="inline-flex items-center gap-1 rounded-lg border bg-white px-2 py-1 text-xs text-gray-700 hover:bg-gray-50"
                  title={t(L, "cancel")}
                  type="button"
                >
                  <X className="h-4 w-4" /> {t(L, "cancel")}
                </button>
              </div>

              <div className="mt-4 space-y-3">
                <div className="rounded-2xl border bg-gray-50 p-3">
                  <div className="text-xs font-semibold text-gray-700">
                    {t(L, "youReviewAs")}
                  </div>
                  <div className="mt-1 text-sm font-semibold text-gray-900">
                    {meLoading
                      ? t(L, "loadingReviews")
                      : displayUserName(effectiveMe)}
                  </div>
                </div>

                <div className="rounded-2xl border bg-gray-50 p-3">
                  <div className="text-xs font-semibold text-gray-700 mb-2">
                    {t(L, "yourRating")}
                  </div>
                  <StarsPicker
                    lang={L}
                    value={rStars}
                    onChange={setRStars}
                    size={24}
                  />
                  <div className="mt-2 text-xs text-gray-600">
                    {t(L, "selected")}{" "}
                    <span className="font-semibold">{rStars}</span>/5
                  </div>
                </div>

                <textarea
                  value={rText}
                  onChange={(e) => setRText(e.target.value)}
                  placeholder={t(L, "writeOpinion")}
                  rows={5}
                  className="w-full rounded-xl border px-3 py-2 text-sm outline-none focus:ring-2 focus:ring-black/10"
                />

                <div className="grid grid-cols-2 gap-2">
                  <button
                    onClick={submitOrUpdateReview}
                    className="w-full rounded-xl bg-black px-4 py-2 text-sm text-white hover:opacity-90"
                    type="button"
                  >
                    {t(L, "updateReview")}
                  </button>
                  <button
                    onClick={deleteMyReview}
                    className="w-full rounded-xl border bg-white px-4 py-2 text-sm text-red-600 hover:bg-red-50"
                    type="button"
                  >
                    {t(L, "delete")}
                  </button>
                </div>

                <p className="text-xs text-gray-500">{t(L, "tip")}</p>
              </div>
            </div>
          ) : null}
        </div>

        {/* Right column: Add review OR Your review */}
        <div className="md:col-span-2">
          {!myReview ? (
            <div className="rounded-2xl border bg-white p-5 shadow-sm">
              <div className="flex items-center justify-between gap-3">
                <div className="flex items-center gap-2">
                  <MessageSquarePlus className="h-4 w-4 text-gray-700" />
                  <h2 className="text-base font-semibold text-gray-900">
                    {t(L, "addReview")}
                  </h2>
                </div>

                {!isLoggedIn() ? (
                  <div className="inline-flex items-center gap-1 text-xs text-gray-600">
                    <Lock className="h-4 w-4" /> {t(L, "loginRequired")}
                  </div>
                ) : (
                  <div className="inline-flex items-center gap-1 text-xs text-gray-600">
                    <Pencil className="h-4 w-4" /> {t(L, "oneReview")}
                  </div>
                )}
              </div>

              {!isLoggedIn() ? (
                <div className="mt-4 rounded-2xl border bg-gray-50 p-4">
                  <div className="text-sm text-gray-700">
                    {t(L, "loginRequired")}
                  </div>
                  <button
                    onClick={() => navigate("/auth")}
                    className="mt-3 w-full rounded-xl bg-black px-4 py-2 text-sm text-white hover:opacity-90"
                    type="button"
                  >
                    {t(L, "goToLogin")}
                  </button>
                </div>
              ) : (
                <div className="mt-4 space-y-3">
                  <div className="text-sm font-semibold text-gray-900">
                    {t(L, "writeReview")}
                  </div>

                  <div className="rounded-2xl border bg-gray-50 p-3">
                    <div className="text-xs font-semibold text-gray-700">
                      {t(L, "youReviewAs")}
                    </div>
                    <div className="mt-1 text-sm font-semibold text-gray-900">
                      {meLoading
                        ? t(L, "loadingReviews")
                        : displayUserName(effectiveMe)}
                    </div>
                  </div>

                  <div className="rounded-2xl border bg-gray-50 p-3">
                    <div className="text-xs font-semibold text-gray-700 mb-2">
                      {t(L, "yourRating")}
                    </div>
                    <StarsPicker
                      lang={L}
                      value={rStars}
                      onChange={setRStars}
                      size={24}
                    />
                    <div className="mt-2 text-xs text-gray-600">
                      {t(L, "selected")}{" "}
                      <span className="font-semibold">{rStars}</span>/5
                    </div>
                  </div>

                  <textarea
                    value={rText}
                    onChange={(e) => setRText(e.target.value)}
                    placeholder={t(L, "writeOpinion")}
                    rows={5}
                    className="w-full rounded-xl border px-3 py-2 text-sm outline-none focus:ring-2 focus:ring-black/10"
                  />

                  <button
                    onClick={submitOrUpdateReview}
                    className="w-full rounded-xl bg-black px-4 py-2 text-sm text-white hover:opacity-90"
                    type="button"
                  >
                    {t(L, "submitReview")}
                  </button>

                  <p className="text-xs text-gray-500">{t(L, "tip")}</p>
                </div>
              )}
            </div>
          ) : (
            <div className="rounded-2xl border bg-white p-5 shadow-sm">
              <div className="flex items-center justify-between">
                <h3 className="text-base font-semibold text-gray-900">
                  {t(L, "yourReview")}
                </h3>
                <button
                  onClick={startEditMyReview}
                  className="inline-flex items-center gap-1 rounded-lg border bg-white px-2 py-1 text-xs text-gray-700 hover:bg-gray-50"
                  type="button"
                >
                  <Pencil className="h-3.5 w-3.5" /> {t(L, "edit")}
                </button>
              </div>

              <div className="mt-3 rounded-2xl border bg-gray-50 p-3">
                <div className="text-xs text-gray-600">{t(L, "rating")}</div>
                <div className="mt-1">
                  <StarsReadOnly
                    value={Number(myReview?.stars || 0)}
                    size={18}
                  />
                </div>
                <div className="mt-3 text-xs text-gray-600">
                  {t(L, "comment")}
                </div>
                <div className="mt-1 text-sm text-gray-800 break-words">
                  {myReview?.text || "—"}
                </div>
              </div>

              <button
                onClick={deleteMyReview}
                className="mt-4 w-full rounded-xl border bg-white px-4 py-2 text-sm text-red-600 hover:bg-red-50"
                type="button"
              >
                {t(L, "delete")}
              </button>

              <p className="mt-3 text-xs text-gray-500">{t(L, "tip")}</p>
            </div>
          )}
        </div>
      </div>

      <Modal
        open={showEditItem}
        onClose={() => setShowEditItem(false)}
        title={t(L, "editItemTitle")}
        subtitle={t(L, "editItemSub")}
      >
        <ListingFormBody
          formType={kind}
          values={buildEditValues(kind, {
            title: eTitle,
            category: eCategory,
            address: eAddress,
            city: eCity,
            state: eState,
            zip: eZip,
            phone: ePhone,
            website: eWebsite,
            desc: eDesc,
            price: ePrice,
          })}
          setField={(k, v) =>
            applyEditField(
              {
                setTitle: setETitle,
                setCategory: setECategory,
                setAddress: setEAddress,
                setCity: setECity,
                setState: setEState,
                setZip: setEZip,
                setPhone: setEPhone,
                setWebsite: setEWebsite,
                setDesc: setEDesc,
                setPrice: setEPrice,
              },
              k,
              v
            )
          }
          getSchema={getSchema}
          formCitySuggestions={[]}
          onCancel={() => setShowEditItem(false)}
          onSubmit={updateItem}
          submitLabel={editItemLoading ? t(L, "saving") : t(L, "save")}
        />
      </Modal>
    </div>
  );
}

async function fetchFirstOk(urls, opts) {
  let lastErr = null;
  for (const u of urls) {
    try {
      const res = await fetch(u, opts);
      const data = await res.json().catch(() => ({}));
      if (res.ok) return { ok: true, url: u, data };
      lastErr = data?.error || `HTTP ${res.status}`;
    } catch (e) {
      lastErr = e?.message || "Network error";
    }
  }
  return { ok: false, error: lastErr || "Failed" };
}

export {
  StarsReadOnly,
  StarsPicker,
  RatingBreakdown,
  safeDateToLocaleString,
  normalizeToPlaceShape,
  fetchFirstOk,
  normalizeMe,
  getMeFromTokenFallback,
  getMeFromStorageFallback,
  getToken,
  authHeaders,
  isLoggedIn,
};
