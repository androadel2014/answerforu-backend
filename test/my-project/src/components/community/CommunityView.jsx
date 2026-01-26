// src/components/community/CommunityView.jsx
// ✅ Marketplace / Explore unified view
// ✅ No window.confirm (uses toastConfirm)
// ✅ Backward compatible with legacy community endpoints (places/groups)
// ✅ Forward compatible with new listing types (services/jobs/housing/products)
// ✅ Unified filters + unified "Add Listing" menu
// ✅ i18n: AR/EN/ES (no react-i18next usage) + RTL support
// ✅ Mobile-first layout improvements (clean header + scrollable tabs + collapsible filters + FAB)

import React, { useEffect, useMemo, useRef, useState } from "react";
import toast from "react-hot-toast";
import { useNavigate } from "react-router-dom";
import { toastConfirm } from "../../lib/notify";
import {
  classNames,
  getToken,
  authHeaders,
  tryFetchJSON,
} from "../../lib/apiHelpers";
import { CardItem } from "../community/CardItem";
import {
  Modal,
  ListingFormBody,
  US_STATES,
  PLACE_CATEGORIES,
  GROUP_PLATFORMS,
  GROUP_TOPICS,
  SERVICE_CATEGORIES,
  JOB_CATEGORIES,
  HOUSING_CATEGORIES,
  PRODUCT_CATEGORIES,
  EMPTY_VALUES,
  normalizeId,
  getCategoryOptionsForTab,
  getSchema,
} from "../community/MarketplaceShared";

import {
  Plus,
  Search,
  Globe,
  X,
  Users,
  Building2,
  RefreshCw,
  LocateFixed,
  Wrench,
  Briefcase,
  Home,
  ShoppingCart,
  SlidersHorizontal,
  Filter,
} from "lucide-react";

const API_BASE =
  import.meta.env.VITE_API_BASE_URL ||
  import.meta.env.VITE_API_BASE ||
  "http://localhost:5000";

/* =========================
   Constants
========================= */
const PAGE_SIZE = 12;

const DIR = (l) => (l === "ar" ? "rtl" : "ltr");

function pick(v, l) {
  if (!v) return "";
  if (typeof v === "string") return v;
  return v?.[l] || v?.en || v?.ar || v?.es || Object.values(v || {})[0] || "";
}

const TXT = {
  ar: {
    explore: "استكشف",
    exploreDesc:
      "سوق للمهاجرين الجدد: أماكن، جروبات، خدمات، وظائف، سكن، منتجات.",
    tip: "نصيحة:",
    tipText: "استخدم",
    state: "الولاية",
    city: "المدينة",
    search: "بحث",
    refresh: "تحديث",
    nearMe: "قريب مني",
    nearMeTitle: "قريب مني (سيضع الإحداثيات في المدينة)",
    addListing: "إضافة إعلان",
    addListingTitle: "إضافة إعلان",
    addListingSub: "اختر النوع ثم اختر تصنيف (اختياري).",
    presets: "تصنيفات جاهزة",
    presetsSub: "اختر واحد أو اضغط “مخصص”.",
    custom: "مخصص",
    listingType: "نوع الإعلان",
    apply: "تطبيق",
    reset: "مسح",
    any: "الكل",
    all: "الكل",
    platform: "المنصة",
    topic: "الموضوع",
    category: "التصنيف",
    loading: "جاري التحميل…",
    noItems: "لا يوجد عناصر بعد",
    noItemsDesc: "كن أول من يضيف إعلان مفيد.",
    pageOf: "صفحة",
    of: "من",
    items: "عنصر",
    first: "الأولى",
    prev: "السابق",
    next: "التالي",
    last: "الأخيرة",
    editListing: "تعديل الإعلان",
    fillFields: "املأ البيانات ثم احفظ.",
    save: "حفظ",
    update: "تحديث",
    loginRequired: "لازم تسجل دخول",
    titleRequired: "العنوان مطلوب",
    linkRequired: "الرابط مطلوب",
    added: "تمت الإضافة",
    updated: "تم التحديث",
    addFailed: "فشل الإضافة",
    updateFailed: "فشل التعديل",
    somethingWrong: "حصل خطأ",
    failedLoad: "فشل تحميل الإعلانات",
    deleteTitle: "حذف الإعلان؟",
    deleteDesc: "لا يمكن التراجع عن هذا الإجراء.",
    deleteFailed: "فشل الحذف",
    deleted: "تم الحذف",
    geoNotSupported: "الموقع غير مدعوم على جهازك",
    geoLoading: "جاري تحديد موقعك…",
    geoSuccess: "تم تحديد الموقع. (اختر الولاية ثم اضغط تطبيق)",
    geoDenied: "تم رفض صلاحية الموقع",
    geoFailed: "فشل تحديد الموقع",
    searchPH_places: "مثال: كباب، مسجد، كنيسة…",
    searchPH_groups: "مثال: عرب، وظائف، هجرة…",
    searchPH_other: "مثال: سباك، غرفة، آيفون…",
    cityPH: "مثال: Fairfax",

    // ✅ mobile ui
    filters: "فلاتر",
    showFilters: "إظهار الفلاتر",
    hideFilters: "إخفاء الفلاتر",
    clear: "مسح",
    confirm: "تأكيد",
    cancel: "إلغاء",
  },
  en: {
    explore: "Explore",
    exploreDesc:
      "Marketplace for newcomers: places, groups, services, jobs, housing, products.",
    tip: "Tip:",
    tipText: "Use",
    state: "State",
    city: "City",
    search: "Search",
    refresh: "Refresh",
    nearMe: "Near me",
    nearMeTitle: "Near me (fills city with coordinates)",
    addListing: "Add Listing",
    addListingTitle: "Add Listing",
    addListingSub: "Choose a type, then pick a preset (optional).",
    presets: "Presets",
    presetsSub: "Pick one, or choose “Custom”.",
    custom: "Custom",
    listingType: "Listing Type",
    apply: "Apply",
    reset: "Reset",
    any: "Any",
    all: "All",
    platform: "Platform",
    topic: "Topic",
    category: "Category",
    loading: "Loading…",
    noItems: "No items yet",
    noItemsDesc: "Be the first to add a helpful listing.",
    pageOf: "Page",
    of: "of",
    items: "items",
    first: "First",
    prev: "Prev",
    next: "Next",
    last: "Last",
    editListing: "Edit Listing",
    fillFields: "Fill the fields and save.",
    save: "Save",
    update: "Update",
    loginRequired: "Login required",
    titleRequired: "Title is required",
    linkRequired: "Link is required",
    added: "Added",
    updated: "Updated",
    addFailed: "Add failed",
    updateFailed: "Update failed",
    somethingWrong: "Something went wrong",
    failedLoad: "Failed to load listings",
    deleteTitle: "Delete this listing?",
    deleteDesc: "This action cannot be undone.",
    deleteFailed: "Delete failed",
    deleted: "Deleted",
    geoNotSupported: "Geolocation not supported",
    geoLoading: "Getting your location…",
    geoSuccess: "Location detected. (Tip: pick your State, then Apply)",
    geoDenied: "Location permission denied",
    geoFailed: "Failed to get location",
    searchPH_places: "e.g. kebab, mosque, church...",
    searchPH_groups: "e.g. arab, jobs, immigration...",
    searchPH_other: "e.g. handyman, room, iPhone...",
    cityPH: "e.g. Fairfax",

    // ✅ mobile ui
    filters: "Filters",
    showFilters: "Show filters",
    hideFilters: "Hide filters",
    clear: "Clear",
    confirm: "Confirm",
    cancel: "Cancel",
  },
  es: {
    explore: "Explorar",
    exploreDesc:
      "Marketplace para recién llegados: lugares, grupos, servicios, trabajos, vivienda, productos.",
    tip: "Tip:",
    tipText: "Usa",
    state: "Estado",
    city: "Ciudad",
    search: "Buscar",
    refresh: "Actualizar",
    nearMe: "Cerca de mí",
    nearMeTitle: "Cerca de mí (pone coordenadas en ciudad)",
    addListing: "Agregar anuncio",
    addListingTitle: "Agregar anuncio",
    addListingSub: "Elige un tipo y luego un preset (opcional).",
    presets: "Presets",
    presetsSub: "Elige uno o usa “Personalizado”.",
    custom: "Personalizado",
    listingType: "Tipo",
    apply: "Aplicar",
    reset: "Reiniciar",
    any: "Cualquiera",
    all: "Todos",
    platform: "Plataforma",
    topic: "Tema",
    category: "Categoría",
    loading: "Cargando…",
    noItems: "Aún no hay elementos",
    noItemsDesc: "Sé el primero en agregar un anuncio útil.",
    pageOf: "Página",
    of: "de",
    items: "elementos",
    first: "Primera",
    prev: "Anterior",
    next: "Siguiente",
    last: "Última",
    editListing: "Editar anuncio",
    fillFields: "Completa los campos y guarda.",
    save: "Guardar",
    update: "Actualizar",
    loginRequired: "Se requiere iniciar sesión",
    titleRequired: "El título es obligatorio",
    linkRequired: "El enlace es obligatorio",
    added: "Agregado",
    updated: "Actualizado",
    addFailed: "Falló agregar",
    updateFailed: "Falló actualizar",
    somethingWrong: "Algo salió mal",
    failedLoad: "No se pudieron cargar los anuncios",
    deleteTitle: "¿Eliminar este anuncio?",
    deleteDesc: "No se puede deshacer.",
    deleteFailed: "Falló eliminar",
    deleted: "Eliminado",
    geoNotSupported: "Geolocalización no soportada",
    geoLoading: "Obteniendo tu ubicación…",
    geoSuccess: "Ubicación detectada. (Elige Estado y luego Aplicar)",
    geoDenied: "Permiso de ubicación denegado",
    geoFailed: "No se pudo obtener la ubicación",
    searchPH_places: "Ej: kebab, mezquita, iglesia…",
    searchPH_groups: "Ej: árabes, trabajos, inmigración…",
    searchPH_other: "Ej: plomero, cuarto, iPhone…",
    cityPH: "Ej: Fairfax",

    // ✅ mobile ui
    filters: "Filtros",
    showFilters: "Mostrar filtros",
    hideFilters: "Ocultar filtros",
    clear: "Limpiar",
    confirm: "Confirmar",
    cancel: "Cancelar",
  },
};

/* =========================
   Utilities
========================= */
function safeTrim(v) {
  return String(v ?? "").trim();
}

function safeUrl(v) {
  const s = safeTrim(v);
  return s || "";
}

function useOutsideClick(ref, handler) {
  useEffect(() => {
    function onDown(e) {
      if (!ref?.current) return;
      if (ref.current.contains(e.target)) return;
      handler?.(e);
    }
    document.addEventListener("mousedown", onDown);
    document.addEventListener("touchstart", onDown, { passive: true });
    return () => {
      document.removeEventListener("mousedown", onDown);
      document.removeEventListener("touchstart", onDown);
    };
  }, [ref, handler]);
}

function parseDateLike(v) {
  if (!v) return null;
  const s = String(v).trim();
  if (!s) return null;

  if (/^\d+$/.test(s)) {
    const n = Number(s);
    if (!Number.isFinite(n)) return null;
    return new Date(n < 1e12 ? n * 1000 : n);
  }

  const d = new Date(s);
  return Number.isNaN(d.getTime()) ? null : d;
}

function pickCreatedAt(x) {
  const candidates = [
    x?.createdAt,
    x?.created_at,
    x?.created,
    x?.createdDate,
    x?.created_date,
    x?.timestamp,
    x?.time,
    x?.date,
  ];

  for (const c of candidates) {
    const d = parseDateLike(c);
    if (d) return d.toISOString();
  }

  return null;
}

/* =========================
   Listing Types (Unified) - localized
========================= */
const LISTING_TYPES = [
  {
    key: "all",
    label: { en: "All", ar: "الكل", es: "Todos" },
    Icon: Globe,
    hint: { en: "View everything", ar: "عرض كل شيء", es: "Ver todo" },
    hideFromAdd: true,
  },
  {
    key: "places",
    label: { en: "Places", ar: "أماكن", es: "Lugares" },
    Icon: Building2,
    hint: { en: "Restaurants...", ar: "مطاعم...", es: "Restaurantes..." },
    legacy: true,
  },
  {
    key: "groups",
    label: { en: "Groups", ar: "جروبات", es: "Grupos" },
    Icon: Users,
    hint: { en: "Social groups...", ar: "مجموعات...", es: "Grupos..." },
    legacy: true,
  },
  {
    key: "services",
    label: { en: "Services", ar: "خدمات", es: "Servicios" },
    Icon: Wrench,
    hint: { en: "Handyman...", ar: "صنايعي...", es: "Manitas..." },
  },
  {
    key: "jobs",
    label: { en: "Jobs", ar: "وظائف", es: "Trabajos" },
    Icon: Briefcase,
    hint: { en: "Work opportunities", ar: "فرص عمل", es: "Oportunidades" },
  },
  {
    key: "housing",
    label: { en: "Housing", ar: "سكن", es: "Vivienda" },
    Icon: Home,
    hint: {
      en: "Rooms, rent...",
      ar: "غرف، إيجار...",
      es: "Cuartos, renta...",
    },
  },
  {
    key: "products",
    label: { en: "Products", ar: "منتجات", es: "Productos" },
    Icon: ShoppingCart,
    hint: { en: "Buy & sell", ar: "بيع وشراء", es: "Comprar y vender" },
  },
];

/* =========================
   UI Components
========================= */
function EmptyState({ icon, title, desc }) {
  const Icon = icon;
  return (
    <div className="text-center py-14">
      <div className="mx-auto w-14 h-14 rounded-2xl bg-gray-100 flex items-center justify-center text-gray-700">
        <Icon size={24} />
      </div>
      <div className="mt-4 text-lg font-semibold text-gray-900">{title}</div>
      <div className="mt-1 text-gray-500">{desc}</div>
    </div>
  );
}

/* =========================
   Page (Unified Marketplace)
========================= */
export default function CommunityView({ lang = "en" }) {
  const navigate = useNavigate();
  const L = TXT[lang] || TXT.en;

  const [tab, setTab] = useState("all");
  const [loading, setLoading] = useState(false);
  const [items, setItems] = useState([]);
  const [page, setPage] = useState(1);

  const [isLoggedIn, setIsLoggedIn] = useState(!!getToken());
  useEffect(() => {
    const onStorage = () => setIsLoggedIn(!!getToken());
    window.addEventListener("storage", onStorage);
    const t = setInterval(() => setIsLoggedIn(!!getToken()), 900);
    return () => {
      window.removeEventListener("storage", onStorage);
      clearInterval(t);
    };
  }, []);

  // filters
  const [q, setQ] = useState("");
  const [state, setState] = useState("");
  const [city, setCity] = useState("");

  const [category, setCategory] = useState("");
  const [platform, setPlatform] = useState("");
  const [topic, setTopic] = useState("");

  // ✅ Mobile: collapsible filters
  const [filtersOpen, setFiltersOpen] = useState(false);

  // Add Picker
  const [addPickerOpen, setAddPickerOpen] = useState(false);
  const [addPickerType, setAddPickerType] = useState("places");

  const endpoints = useMemo(() => {
    const mpList = (type) => [
      `${API_BASE}/api/listings?type=${type}`,
      `${API_BASE}/api/marketplace/listings?type=${type}`,
    ];

    if (tab === "all") {
      return {
        places: [`${API_BASE}/api/community/places`],
        groups: [`${API_BASE}/api/community/groups`],
        services: mpList("services"),
        jobs: mpList("jobs"),
        housing: mpList("housing"),
        products: mpList("products"),
      };
    }

    if (tab === "places") return [`${API_BASE}/api/community/places`];
    if (tab === "groups") return [`${API_BASE}/api/community/groups`];
    return mpList(tab);
  }, [tab]);

  /* =========================
     Unified Form
  ========================= */
  const [formOpen, setFormOpen] = useState(false);
  const [formMode, setFormMode] = useState("add");
  const [formType, setFormType] = useState("places");
  const [formId, setFormId] = useState(null);
  const [values, setValues] = useState({ ...EMPTY_VALUES });

  function setField(k, v) {
    setValues((p) => ({ ...p, [k]: v }));
  }

  function resetFilters() {
    setQ("");
    setState("");
    setCity("");
    setCategory("");
    setPlatform("");
    setTopic("");
  }

  function requireLoginOrToast() {
    const ok = !!getToken();
    if (!ok) toast.error(L.loginRequired);
    return ok;
  }

  function openFormAdd(typeKey, preset) {
    if (!requireLoginOrToast()) return;
    setFormMode("add");
    setFormType(typeKey);
    setFormId(null);

    const next = { ...EMPTY_VALUES };
    if (typeKey === "places") next.category = preset || "Restaurant";
    if (typeKey === "groups") next.platform = preset || "Facebook";
    if (["services", "jobs", "housing", "products"].includes(typeKey))
      next.category = preset || "";

    setValues(next);
    setFormOpen(true);
  }

  function openFormEdit(item, typeKey) {
    if (!requireLoginOrToast()) return;

    const t = typeKey || tab;
    const idNum = normalizeId(item.id, t);
    const prefix =
      t === "places"
        ? "place"
        : t === "groups"
        ? "group"
        : t === "services"
        ? "service"
        : t === "products"
        ? "product"
        : t === "jobs"
        ? "jobs"
        : t === "housing"
        ? "housing"
        : "";

    const idPrefixed = prefix ? `${prefix}_${idNum}` : idNum;

    setFormMode("edit");
    setFormType(t);
    setFormId(idPrefixed);

    const next = { ...EMPTY_VALUES };

    if (t === "places") {
      next.title = item.name || "";
      next.category = item.category || "Restaurant";
      next.state = item.state || "";
      next.city = item.city || "";
      next.address = item.address || "";
      next.phone = item.phone || "";
      next.website = item.website || "";
      next.notes = item.notes || "";
    } else if (t === "groups") {
      next.title = item.name || "";
      next.platform = item.platform || "Facebook";
      next.topic = item.topic || "Immigration";
      next.state = item.state || "";
      next.city = item.city || "";
      next.link = item.link || "";
      next.notes = item.notes || "";
    } else {
      next.title = item.title || item.name || "";
      next.category = item.category || "";
      next.state = item.state || "";
      next.city = item.city || "";

      const pv =
        item?.price_value ??
        item?.priceValue ??
        item?.price ??
        item?.budget ??
        item?.amount ??
        "";
      next.price = pv === null || pv === undefined ? "" : String(pv);

      next.link = item.link || item.url || item.website || "";
      next.contact = item.contact || item.phone || "";
      next.description = item.description || item.notes || "";
    }

    setValues(next);
    setFormOpen(true);
  }

  async function submitForm() {
    try {
      if (!requireLoginOrToast()) return;

      if (!values.title.trim()) return toast.error(L.titleRequired);
      const type = formType;

      if (type === "groups" && !String(values.link || "").trim())
        return toast.error(L.linkRequired);

      const isEdit = formMode === "edit" && !!formId;

      const title = safeTrim(values.title);
      const cityV = safeTrim(values.city);
      const stateV = safeTrim(values.state);

      let url = "";
      let method = isEdit ? "PATCH" : "POST";
      let body = null;

      if (type === "places") {
        const placeId = isEdit ? normalizeId(formId, type) : null;

        url = isEdit
          ? `${API_BASE}/api/community/places/${placeId}`
          : `${API_BASE}/api/community/places`;

        body = {
          name: title,
          category: safeTrim(values.category) || "Restaurant",
          state: stateV,
          city: cityV,
          address: safeTrim(values.address),
          phone: safeTrim(values.phone),
          website: safeUrl(values.website),
          notes: safeTrim(values.notes),
        };
      } else if (type === "groups") {
        const groupId = isEdit ? normalizeId(formId, type) : null;

        url = isEdit
          ? `${API_BASE}/api/community/groups/${groupId}`
          : `${API_BASE}/api/community/groups`;

        body = {
          name: title,
          platform: safeTrim(values.platform) || "Facebook",
          topic: safeTrim(values.topic) || "Immigration",
          state: stateV,
          city: cityV,
          link: safeUrl(values.link),
          notes: safeTrim(values.notes),
        };
      } else {
        url = isEdit
          ? `${API_BASE}/api/listings/${formId}`
          : `${API_BASE}/api/listings`;

        body = {
          type,
          title,
          category: safeTrim(values.category),
          state: stateV,
          city: cityV,
          price_value: safeTrim(values.price),
          price: safeTrim(values.price),
          contact: safeTrim(values.contact),
          website: safeUrl(values.link),
          notes: safeTrim(values.description) || safeTrim(values.notes),
        };
      }

      const out = await tryFetchJSON(url, {
        method,
        headers: { "Content-Type": "application/json", ...authHeaders() },
        body: JSON.stringify(body),
      });

      if (!out.ok) return toast.error(isEdit ? L.updateFailed : L.addFailed);

      toast.success(isEdit ? L.updated : L.added);
      setFormOpen(false);

      if (tab !== type) setTab(type);
      setTimeout(load, 150);
    } catch (e) {
      console.error(e);
      toast.error(L.somethingWrong);
    }
  }

  /* =========================
     Loading
  ========================= */
  async function load() {
    setPage(1);
    setLoading(true);

    try {
      const params = new URLSearchParams();
      if (q.trim()) params.set("q", q.trim());
      if (state && state !== "any" && state !== "Any")
        params.set("state", state);
      if (city.trim()) params.set("city", city.trim());

      if (tab === "groups") {
        if (platform && platform !== "all" && platform !== "All")
          params.set("platform", platform);
        if (topic && topic !== "all" && topic !== "All")
          params.set("topic", topic);
      } else {
        if (tab !== "all") {
          if (category && category !== "all" && category !== "All")
            params.set("category", category);
        }
      }

      if (tab === "all") {
        const bundles = endpoints;
        const types = Object.keys(bundles);

        const results = await Promise.all(
          types.map(async (typeKey) => {
            const bases = bundles[typeKey] || [];
            const qs = params.toString();
            const urls = bases.map((base) => {
              if (!qs) return base;
              const joiner = base.includes("?") ? "&" : "?";
              return `${base}${joiner}${qs}`;
            });

            const out = await tryFetchJSON(urls, {
              method: "GET",
              headers: { ...authHeaders() },
            });

            const arr = out.ok ? extractArray(out.data) : [];

            const legacyPrefix =
              typeKey === "places"
                ? "place"
                : typeKey === "groups"
                ? "group"
                : typeKey;

            return arr.map((x) => {
              const createdAt = pickCreatedAt(x);

              const rawId =
                x?.id ??
                x?.place_id ??
                x?.group_id ??
                x?.listing_id ??
                x?.item_id;

              const baseId = normalizeId(rawId, typeKey);

              const mergedId =
                typeKey === "places" || typeKey === "groups"
                  ? `${legacyPrefix}_${baseId}`
                  : String(x?.id || "").includes("_")
                  ? x.id
                  : `${typeKey}_${baseId}`;

              return {
                ...x,
                id: mergedId,
                type: typeKey,
                createdAt: x.createdAt ?? createdAt,
                created_at: x.created_at ?? createdAt,
              };
            });
          })
        );

        const merged = results.flat();

        const filtered =
          category && category !== "all" && category !== "All"
            ? merged.filter((x) => {
                const c = String(x?.category || "").trim();
                return c && c.toLowerCase() === String(category).toLowerCase();
              })
            : merged;

        setItems(filtered);
        return;
      }

      const qs = params.toString();
      const urls = endpoints.map((base) => {
        if (!qs) return base;
        const joiner = base.includes("?") ? "&" : "?";
        return `${base}${joiner}${qs}`;
      });

      const out = await tryFetchJSON(urls, {
        method: "GET",
        headers: { ...authHeaders() },
      });
      if (!out.ok) throw out.error;

      const arr = extractArray(out.data);
      setItems(
        arr.map((x) => {
          const createdAt = pickCreatedAt(x);
          return {
            ...x,
            createdAt: x.createdAt ?? createdAt,
            created_at: x.created_at ?? createdAt,
          };
        })
      );
    } catch (e) {
      console.error(e);
      toast.error(L.failedLoad);
      setItems([]);
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    setItems([]);
    load();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tab]);

  useEffect(() => {
    if (tab !== "all") return;
    const t = setTimeout(() => load(), 250);
    return () => clearTimeout(t);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tab, q, state, city, category, platform, topic]);

  const citySuggestions = useMemo(() => {
    const s = new Set();
    for (const it of items) {
      if (!it) continue;
      const st = String(it.state || "")
        .trim()
        .toUpperCase();
      const c = String(it.city || "").trim();
      if (!c) continue;

      const selectedState = state || "";
      if (selectedState) {
        if (st === String(selectedState).toUpperCase()) s.add(c);
      } else {
        s.add(c);
      }
    }
    return Array.from(s).sort((a, b) => a.localeCompare(b));
  }, [items, state]);

  const formCitySuggestions = useMemo(() => {
    const s = new Set();
    const selectedState = values.state || "";

    for (const it of items) {
      if (!it) continue;
      const st = String(it.state || "")
        .trim()
        .toUpperCase();
      const c = String(it.city || "").trim();
      if (!c) continue;

      if (!selectedState) s.add(c);
      else if (st === String(selectedState).toUpperCase()) s.add(c);
    }

    return Array.from(s).sort((a, b) => a.localeCompare(b));
  }, [items, values.state]);

  function extractArray(data) {
    if (Array.isArray(data)) return data;
    if (Array.isArray(data?.items)) return data.items;
    if (Array.isArray(data?.rows)) return data.rows;
    if (Array.isArray(data?.results)) return data.results;
    if (Array.isArray(data?.data)) return data.data;
    return [];
  }

  async function removeItem(id, typeKey) {
    try {
      if (!requireLoginOrToast()) return;

      // ✅ FIX: correct toastConfirm usage (object)
      const ok = await toastConfirm({
        title: L.deleteTitle,
        message: L.deleteDesc,
        confirmText:
          lang === "ar" ? "مسح" : lang === "es" ? "Eliminar" : "Delete",
        cancelText:
          lang === "ar" ? "إلغاء" : lang === "es" ? "Cancelar" : "Cancel",
        variant: "danger",
      });
      if (!ok) return;

      const t = typeKey || tab;
      let url = "";

      if (t === "places") url = `${API_BASE}/api/community/places/${id}`;
      else if (t === "groups") url = `${API_BASE}/api/community/groups/${id}`;
      else {
        const prefixed = String(id).includes("_")
          ? id
          : `${t}_${normalizeId(id, t)}`;
        url = `${API_BASE}/api/listings/${prefixed}`;
      }

      const out = await tryFetchJSON(url, {
        method: "DELETE",
        headers: { ...authHeaders() },
      });

      if (!out.ok) return toast.error(L.deleteFailed);

      toast.success(L.deleted);
      load();
    } catch (e) {
      console.error(e);
      toast.error(L.deleteFailed);
    }
  }

  async function nearMe() {
    try {
      if (!navigator.geolocation) return toast.error(L.geoNotSupported);
      toast.loading(L.geoLoading, { id: "geo" });

      navigator.geolocation.getCurrentPosition(
        (pos) => {
          const { latitude, longitude } = pos.coords || {};
          toast.success(L.geoSuccess, { id: "geo" });
          setQ((prev) => prev || "nearby");
          setCity(`${latitude.toFixed(4)}, ${longitude.toFixed(4)}`);
          setFiltersOpen(true);
        },
        (err) => {
          console.error(err);
          toast.error(L.geoDenied, { id: "geo" });
        },
        { enableHighAccuracy: false, timeout: 9000 }
      );
    } catch (e) {
      console.error(e);
      toast.error(L.geoFailed, { id: "geo" });
    }
  }

  const totalPages = Math.max(1, Math.ceil(items.length / PAGE_SIZE));
  const pageSafe = Math.min(Math.max(1, page), totalPages);

  const pagedItems = useMemo(() => {
    const start = (pageSafe - 1) * PAGE_SIZE;
    return items.slice(start, start + PAGE_SIZE);
  }, [items, pageSafe]);

  useEffect(() => {
    if (page !== pageSafe) setPage(pageSafe);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [pageSafe]);

  const activeTypeMeta =
    LISTING_TYPES.find((t) => t.key === tab) || LISTING_TYPES[0];

  // ✅ count active filters (for mobile label)
  const activeFiltersCount = useMemo(() => {
    let n = 0;
    if (q.trim()) n++;
    if (state) n++;
    if (city.trim()) n++;
    if (tab === "groups") {
      if (platform) n++;
      if (topic) n++;
    } else {
      if (category) n++;
    }
    return n;
  }, [q, state, city, category, platform, topic, tab]);

  return (
    <div className="max-w-6xl mx-auto px-4 py-4 sm:py-6" dir={DIR(lang)}>
      {/* ✅ Mobile-first Header (clean) */}
      <div className="sticky top-0 z-40 bg-white/90 backdrop-blur border-b border-gray-100 -mx-4 px-4 py-3">
        <div className="flex flex-col sm:flex-row sm:items-start sm:justify-between gap-3">
          <div className="min-w-0">
            <h1 className="text-2xl sm:text-3xl font-extrabold text-gray-900 leading-tight">
              {L.explore}
            </h1>
            <p className="mt-1 text-sm sm:text-base text-gray-600">
              {L.exploreDesc}
            </p>

            <div className="mt-3 hidden sm:inline-flex flex-wrap items-center gap-2 rounded-2xl border border-gray-200 bg-white px-3 py-2 shadow-sm">
              <span className="text-xs font-semibold text-gray-600">
                {L.tip}
              </span>
              <span className="text-xs text-gray-600">
                {L.tipText} <span className="font-semibold">{L.state}</span> +{" "}
                <span className="font-semibold">{L.city}</span>
              </span>
            </div>
          </div>

          {/* ✅ Actions (icon-only on mobile) */}
          <div className="flex items-center gap-2 shrink-0">
            <button
              onClick={load}
              className={classNames(
                "inline-flex items-center gap-2 rounded-xl border border-gray-200 hover:bg-gray-50 text-gray-800",
                "px-3 py-2 sm:px-4 sm:py-2"
              )}
              type="button"
              title={L.refresh}
              aria-label={L.refresh}
            >
              <RefreshCw size={16} />
              <span className="hidden sm:inline">{L.refresh}</span>
            </button>

            <button
              onClick={nearMe}
              className={classNames(
                "inline-flex items-center gap-2 rounded-xl border border-gray-200 hover:bg-gray-50 text-gray-800",
                "px-3 py-2 sm:px-4 sm:py-2"
              )}
              title={L.nearMeTitle}
              type="button"
              aria-label={L.nearMe}
            >
              <LocateFixed size={16} />
              <span className="hidden sm:inline">{L.nearMe}</span>
            </button>

            {isLoggedIn ? (
              <button
                onClick={() => {
                  setAddPickerType("places");
                  setAddPickerOpen(true);
                }}
                className={classNames(
                  "inline-flex items-center gap-2 rounded-xl bg-black text-white hover:bg-black/90",
                  "px-3 py-2 sm:px-4 sm:py-2"
                )}
                type="button"
                aria-label={L.addListing}
                title={L.addListing}
              >
                <Plus size={18} />
                <span className="hidden sm:inline">{L.addListing}</span>
              </button>
            ) : null}
          </div>
        </div>

        {/* ✅ Mobile Tip */}
        <div className="mt-3 sm:hidden">
          <div className="inline-flex flex-wrap items-center gap-2 rounded-2xl border border-gray-200 bg-white px-3 py-2 shadow-sm w-full">
            <span className="text-xs font-semibold text-gray-600">{L.tip}</span>
            <span className="text-xs text-gray-600">
              {L.tipText} <span className="font-semibold">{L.state}</span> +{" "}
              <span className="font-semibold">{L.city}</span>
            </span>
          </div>
        </div>
      </div>

      {/* ✅ Add Picker Modal */}
      <Modal
        open={addPickerOpen}
        onClose={() => setAddPickerOpen(false)}
        title={L.addListingTitle}
        subtitle={L.addListingSub}
      >
        <div className="grid grid-cols-2 md:grid-cols-3 gap-3">
          {LISTING_TYPES.filter((t) => !t.hideFromAdd).map((t) => {
            const Active = addPickerType === t.key;
            const Icon = t.Icon;
            return (
              <button
                key={t.key}
                onClick={() => setAddPickerType(t.key)}
                className={classNames(
                  "rounded-2xl border p-4 text-left hover:bg-gray-50 transition",
                  Active
                    ? "border-black ring-2 ring-black/10"
                    : "border-gray-200"
                )}
                type="button"
              >
                <div className="flex items-center gap-3">
                  <div
                    className={classNames(
                      "w-10 h-10 rounded-xl flex items-center justify-center border",
                      Active
                        ? "bg-black text-white border-black"
                        : "bg-gray-50 text-gray-800 border-gray-200"
                    )}
                  >
                    <Icon size={18} />
                  </div>
                  <div className="min-w-0">
                    <div className="font-extrabold text-gray-900">
                      {pick(t.label, lang)}
                    </div>
                    <div className="text-xs text-gray-500 truncate">
                      {pick(t.hint, lang)}
                    </div>
                  </div>
                </div>
              </button>
            );
          })}
        </div>

        <div className="mt-5 rounded-2xl border border-gray-200 bg-white">
          <div className="px-4 py-3 border-b border-gray-100">
            <div className="text-sm font-extrabold text-gray-900">
              {L.presets}
            </div>
            <div className="text-xs text-gray-500 mt-0.5">{L.presetsSub}</div>
          </div>

          <div className="p-3 grid grid-cols-1 sm:grid-cols-2 gap-2">
            {addPickerType === "places" ? (
              <>
                {[
                  "Restaurant",
                  "Mosque",
                  "Church",
                  "Clinic / Doctor",
                  "School / Daycare",
                  "Grocery / Arab Market",
                  "Things to do",
                  "Park / Outdoors",
                  "Car Services",
                  "Handyman / Home Services",
                ].map((preset) => (
                  <button
                    key={preset}
                    onClick={() => {
                      setAddPickerOpen(false);
                      openFormAdd("places", preset);
                    }}
                    className="px-3 py-2.5 rounded-xl border border-gray-200 hover:bg-gray-50 text-left font-semibold"
                    type="button"
                  >
                    {preset}
                  </button>
                ))}
              </>
            ) : addPickerType === "groups" ? (
              <>
                {["Facebook", "WhatsApp", "Telegram", "Discord", "Meetup"].map(
                  (preset) => (
                    <button
                      key={preset}
                      onClick={() => {
                        setAddPickerOpen(false);
                        openFormAdd("groups", preset);
                      }}
                      className="px-3 py-2.5 rounded-xl border border-gray-200 hover:bg-gray-50 text-left font-semibold"
                      type="button"
                    >
                      {preset}
                    </button>
                  )
                )}
              </>
            ) : (
              <>
                {getCategoryOptionsForTab(addPickerType)
                  .slice(0, 10)
                  .map((preset) => (
                    <button
                      key={preset}
                      onClick={() => {
                        setAddPickerOpen(false);
                        openFormAdd(addPickerType, preset);
                      }}
                      className="px-3 py-2.5 rounded-xl border border-gray-200 hover:bg-gray-50 text-left font-semibold"
                      type="button"
                    >
                      {preset}
                    </button>
                  ))}
              </>
            )}
          </div>

          <div className="px-3 pb-3">
            <button
              onClick={() => {
                setAddPickerOpen(false);
                openFormAdd(addPickerType);
              }}
              className="w-full px-4 py-3 rounded-xl bg-black text-white hover:bg-black/90 font-extrabold"
              type="button"
            >
              {L.custom}{" "}
              {pick(
                LISTING_TYPES.find((t) => t.key === addPickerType)?.label,
                lang
              ) || ""}
            </button>
          </div>
        </div>
      </Modal>

      {/* ✅ Listing type (scrollable pills on mobile) */}
      <div className="mt-5 flex flex-col sm:flex-row sm:items-end sm:justify-between gap-3">
        <div>
          <div className="text-sm font-extrabold text-gray-900">
            {L.listingType}
          </div>
          <div className="text-xs text-gray-500 mt-0.5">
            {pick(activeTypeMeta.hint, lang)}
          </div>
        </div>

        <div className="w-full sm:w-auto overflow-x-auto">
          <div className="flex gap-2 whitespace-nowrap pb-1">
            {LISTING_TYPES.map((t) => (
              <button
                key={t.key}
                onClick={() => {
                  setTab(t.key);
                  resetFilters();
                  setPage(1);
                }}
                className={classNames(
                  "px-4 py-2 rounded-full border text-sm font-semibold shrink-0",
                  tab === t.key
                    ? "bg-black text-white border-black"
                    : "bg-white text-gray-700 border-gray-200 hover:bg-gray-50"
                )}
                type="button"
              >
                {pick(t.label, lang)}
              </button>
            ))}
          </div>
        </div>
      </div>

      {/* ✅ Filters (mobile collapsible) */}
      <div className="mt-4 rounded-2xl border border-gray-200 bg-white p-4 shadow-sm">
        {/* Top row: Search + filters toggle + apply/reset */}
        <div className="grid grid-cols-1 gap-3">
          <div>
            <label className="text-sm font-medium text-gray-700">
              {L.search}
            </label>
            <div className="mt-1 relative">
              <Search
                size={18}
                className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-500"
              />
              <input
                value={q}
                onChange={(e) => setQ(e.target.value)}
                placeholder={
                  tab === "places"
                    ? L.searchPH_places
                    : tab === "groups"
                    ? L.searchPH_groups
                    : L.searchPH_other
                }
                className="w-full pl-10 pr-3 py-2.5 rounded-xl border border-gray-200 focus:outline-none focus:ring-2 focus:ring-black/20"
              />
            </div>
          </div>

          {/* Mobile controls */}
          <div className="sm:hidden flex items-center gap-2">
            <button
              type="button"
              onClick={() => setFiltersOpen((v) => !v)}
              className="flex-1 px-4 py-2.5 rounded-xl border border-gray-200 hover:bg-gray-50 text-gray-800 inline-flex items-center justify-center gap-2 font-semibold"
            >
              <SlidersHorizontal size={16} />
              {filtersOpen ? L.hideFilters : L.showFilters}
              {activeFiltersCount ? (
                <span className="ml-1 inline-flex items-center justify-center min-w-[22px] h-[22px] rounded-full bg-black text-white text-xs font-black px-2">
                  {activeFiltersCount}
                </span>
              ) : null}
            </button>

            <button
              onClick={load}
              className="px-4 py-2.5 rounded-xl bg-black text-white font-semibold hover:bg-black/90"
              type="button"
            >
              {L.apply}
            </button>

            <button
              onClick={() => {
                resetFilters();
                setTimeout(load, 0);
              }}
              className="px-3 py-2.5 rounded-xl border border-gray-200 hover:bg-gray-50 text-gray-800"
              title={L.reset}
              aria-label={L.reset}
              type="button"
            >
              <X size={18} />
            </button>
          </div>
        </div>

        {/* Desktop grid (always open on sm+) */}
        <div className="hidden sm:block mt-3">
          <div className="grid grid-cols-1 md:grid-cols-12 gap-3 items-end">
            <div className="md:col-span-2">
              <label className="text-sm font-medium text-gray-700">
                {L.state}
              </label>
              <select
                value={state}
                onChange={(e) => setState(e.target.value)}
                className="mt-1 w-full py-2.5 px-3 rounded-xl border border-gray-200 focus:outline-none focus:ring-2 focus:ring-black/20"
              >
                <option value="">{L.any}</option>
                {US_STATES.map((s) => (
                  <option key={s} value={s}>
                    {s}
                  </option>
                ))}
              </select>
            </div>

            <div className="md:col-span-2">
              <label className="text-sm font-medium text-gray-700">
                {L.city}
              </label>
              <input
                value={city}
                onChange={(e) => setCity(e.target.value)}
                placeholder={L.cityPH}
                list="city-suggestions"
                className="mt-1 w-full py-2.5 px-3 rounded-xl border border-gray-200 focus:outline-none focus:ring-2 focus:ring-black/20"
              />
            </div>

            {tab === "groups" ? (
              <>
                <div className="md:col-span-3">
                  <label className="text-sm font-medium text-gray-700">
                    {L.platform}
                  </label>
                  <select
                    value={platform}
                    onChange={(e) => setPlatform(e.target.value)}
                    className="mt-1 w-full py-2.5 px-3 rounded-xl border border-gray-200 focus:outline-none focus:ring-2 focus:ring-black/20"
                  >
                    <option value="">{L.all}</option>
                    {GROUP_PLATFORMS.map((p) => (
                      <option key={p} value={p}>
                        {p}
                      </option>
                    ))}
                  </select>
                </div>

                <div className="md:col-span-3">
                  <label className="text-sm font-medium text-gray-700">
                    {L.topic}
                  </label>
                  <select
                    value={topic}
                    onChange={(e) => setTopic(e.target.value)}
                    className="mt-1 w-full py-2.5 px-3 rounded-xl border border-gray-200 focus:outline-none focus:ring-2 focus:ring-black/20"
                  >
                    <option value="">{L.all}</option>
                    {GROUP_TOPICS.map((t) => (
                      <option key={t} value={t}>
                        {t}
                      </option>
                    ))}
                  </select>
                </div>
              </>
            ) : (
              <div className="md:col-span-4">
                <label className="text-sm font-medium text-gray-700">
                  {L.category}
                </label>
                <select
                  value={category}
                  onChange={(e) => setCategory(e.target.value)}
                  className="mt-1 w-full py-2.5 px-3 rounded-xl border border-gray-200 focus:outline-none focus:ring-2 focus:ring-black/20"
                >
                  <option value="">{L.all}</option>

                  {tab === "all"
                    ? Array.from(
                        new Set([
                          ...PLACE_CATEGORIES,
                          ...SERVICE_CATEGORIES,
                          ...JOB_CATEGORIES,
                          ...HOUSING_CATEGORIES,
                          ...PRODUCT_CATEGORIES,
                        ])
                      ).map((c) => (
                        <option key={c} value={c}>
                          {c}
                        </option>
                      ))
                    : getCategoryOptionsForTab(tab).map((c) => (
                        <option key={c} value={c}>
                          {c}
                        </option>
                      ))}
                </select>
              </div>
            )}

            <div className="md:col-span-2 flex gap-2">
              <button
                onClick={load}
                className="w-full px-4 py-2.5 rounded-xl bg-black text-white font-semibold hover:bg-black/90"
                type="button"
              >
                {L.apply}
              </button>
              <button
                onClick={() => {
                  resetFilters();
                  setTimeout(load, 0);
                }}
                className="px-3 py-2.5 rounded-xl border border-gray-200 hover:bg-gray-50 text-gray-800"
                title={L.reset}
                aria-label={L.reset}
                type="button"
              >
                <X size={18} />
              </button>
            </div>
          </div>
        </div>

        {/* Mobile expanded filters */}
        {filtersOpen ? (
          <div className="sm:hidden mt-3 space-y-3">
            <div className="grid grid-cols-1 gap-3">
              <div>
                <label className="text-sm font-medium text-gray-700">
                  {L.state}
                </label>
                <select
                  value={state}
                  onChange={(e) => setState(e.target.value)}
                  className="mt-1 w-full py-2.5 px-3 rounded-xl border border-gray-200 focus:outline-none focus:ring-2 focus:ring-black/20"
                >
                  <option value="">{L.any}</option>
                  {US_STATES.map((s) => (
                    <option key={s} value={s}>
                      {s}
                    </option>
                  ))}
                </select>
              </div>

              <div>
                <label className="text-sm font-medium text-gray-700">
                  {L.city}
                </label>
                <input
                  value={city}
                  onChange={(e) => setCity(e.target.value)}
                  placeholder={L.cityPH}
                  list="city-suggestions"
                  className="mt-1 w-full py-2.5 px-3 rounded-xl border border-gray-200 focus:outline-none focus:ring-2 focus:ring-black/20"
                />
              </div>

              {tab === "groups" ? (
                <>
                  <div>
                    <label className="text-sm font-medium text-gray-700">
                      {L.platform}
                    </label>
                    <select
                      value={platform}
                      onChange={(e) => setPlatform(e.target.value)}
                      className="mt-1 w-full py-2.5 px-3 rounded-xl border border-gray-200 focus:outline-none focus:ring-2 focus:ring-black/20"
                    >
                      <option value="">{L.all}</option>
                      {GROUP_PLATFORMS.map((p) => (
                        <option key={p} value={p}>
                          {p}
                        </option>
                      ))}
                    </select>
                  </div>

                  <div>
                    <label className="text-sm font-medium text-gray-700">
                      {L.topic}
                    </label>
                    <select
                      value={topic}
                      onChange={(e) => setTopic(e.target.value)}
                      className="mt-1 w-full py-2.5 px-3 rounded-xl border border-gray-200 focus:outline-none focus:ring-2 focus:ring-black/20"
                    >
                      <option value="">{L.all}</option>
                      {GROUP_TOPICS.map((t) => (
                        <option key={t} value={t}>
                          {t}
                        </option>
                      ))}
                    </select>
                  </div>
                </>
              ) : (
                <div>
                  <label className="text-sm font-medium text-gray-700">
                    {L.category}
                  </label>
                  <select
                    value={category}
                    onChange={(e) => setCategory(e.target.value)}
                    className="mt-1 w-full py-2.5 px-3 rounded-xl border border-gray-200 focus:outline-none focus:ring-2 focus:ring-black/20"
                  >
                    <option value="">{L.all}</option>

                    {tab === "all"
                      ? Array.from(
                          new Set([
                            ...PLACE_CATEGORIES,
                            ...SERVICE_CATEGORIES,
                            ...JOB_CATEGORIES,
                            ...HOUSING_CATEGORIES,
                            ...PRODUCT_CATEGORIES,
                          ])
                        ).map((c) => (
                          <option key={c} value={c}>
                            {c}
                          </option>
                        ))
                      : getCategoryOptionsForTab(tab).map((c) => (
                          <option key={c} value={c}>
                            {c}
                          </option>
                        ))}
                  </select>
                </div>
              )}
            </div>
          </div>
        ) : null}

        <datalist id="city-suggestions">
          {citySuggestions.map((c) => (
            <option key={c} value={c} />
          ))}
        </datalist>
      </div>

      {/* ✅ List */}
      <div className="mt-4">
        {loading ? (
          <div className="py-10 text-center text-gray-500">{L.loading}</div>
        ) : items.length === 0 ? (
          <EmptyState
            icon={activeTypeMeta.Icon}
            title={L.noItems}
            desc={L.noItemsDesc}
          />
        ) : (
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
            {pagedItems.map((it) => (
              <CardItem
                key={`${it.type || tab}-${it.id}`}
                lang={lang}
                tab={tab}
                it={it}
                isLoggedIn={isLoggedIn}
                onEdit={() => {
                  const t = tab === "all" ? it.type || "places" : tab;
                  openFormEdit({ ...it, id: normalizeId(it.id, t) }, t);
                }}
                onDelete={() => {
                  const t = tab === "all" ? it.type || "places" : tab;
                  removeItem(normalizeId(it.id, t), t);
                }}
                onOpen={() => {
                  const t = tab === "all" ? it.type || "places" : tab;
                  const id = normalizeId(it.id, t);
                  sessionStorage.setItem(`mp:type:${t}:${id}`, t);
                  return navigate(`/marketplace/item/${id}`, {
                    state: { type: t },
                  });
                }}
              />
            ))}
          </div>
        )}
      </div>

      {/* ✅ Pagination */}
      {items.length > 0 ? (
        <div className="mt-6 flex flex-col sm:flex-row sm:items-center sm:justify-between gap-3">
          <div className="text-sm text-gray-600">
            {L.pageOf} <span className="font-semibold">{pageSafe}</span> {L.of}{" "}
            <span className="font-semibold">{totalPages}</span> —{" "}
            <span className="font-semibold">{items.length}</span> {L.items}
          </div>

          <div className="flex items-center gap-2 flex-wrap">
            <button
              onClick={() => setPage(1)}
              disabled={pageSafe === 1}
              className={classNames(
                "px-3 py-2 rounded-xl border text-sm font-semibold",
                pageSafe === 1
                  ? "border-gray-200 text-gray-400 bg-gray-50 cursor-not-allowed"
                  : "border-gray-200 text-gray-800 hover:bg-gray-50"
              )}
              type="button"
            >
              {L.first}
            </button>

            <button
              onClick={() => setPage((p) => Math.max(1, p - 1))}
              disabled={pageSafe === 1}
              className={classNames(
                "px-3 py-2 rounded-xl border text-sm font-semibold",
                pageSafe === 1
                  ? "border-gray-200 text-gray-400 bg-gray-50 cursor-not-allowed"
                  : "border-gray-200 text-gray-800 hover:bg-gray-50"
              )}
              type="button"
            >
              {L.prev}
            </button>

            <button
              onClick={() => setPage((p) => Math.min(totalPages, p + 1))}
              disabled={pageSafe === totalPages}
              className={classNames(
                "px-3 py-2 rounded-xl border text-sm font-semibold",
                pageSafe === totalPages
                  ? "border-gray-200 text-gray-400 bg-gray-50 cursor-not-allowed"
                  : "border-gray-200 text-gray-800 hover:bg-gray-50"
              )}
              type="button"
            >
              {L.next}
            </button>

            <button
              onClick={() => setPage(totalPages)}
              disabled={pageSafe === totalPages}
              className={classNames(
                "px-3 py-2 rounded-xl border text-sm font-semibold",
                pageSafe === totalPages
                  ? "border-gray-200 text-gray-400 bg-gray-50 cursor-not-allowed"
                  : "border-gray-200 text-gray-800 hover:bg-gray-50"
              )}
              type="button"
            >
              {L.last}
            </button>
          </div>
        </div>
      ) : null}

      {/* ✅ Mobile FAB (Add Listing) */}
      {isLoggedIn ? (
        <button
          type="button"
          onClick={() => {
            setAddPickerType("places");
            setAddPickerOpen(true);
          }}
          className={classNames(
            "fixed sm:hidden z-50 right-5 bottom-24",
            "w-14 h-14 rounded-2xl bg-black text-white shadow-2xl",
            "flex items-center justify-center active:scale-95 transition"
          )}
          aria-label={L.addListing}
          title={L.addListing}
        >
          <Plus size={22} />
        </button>
      ) : null}

      {/* ✅ Unified Add/Edit Modal */}
      <Modal
        open={formOpen}
        onClose={() => setFormOpen(false)}
        title={
          formMode === "edit"
            ? L.editListing
            : `${L.addListingTitle} ${
                pick(
                  LISTING_TYPES.find((t) => t.key === formType)?.label,
                  lang
                ) || ""
              }`
        }
        subtitle={L.fillFields}
      >
        <ListingFormBody
          formType={formType}
          values={values}
          setField={setField}
          getSchema={getSchema}
          formCitySuggestions={formCitySuggestions}
          onCancel={() => setFormOpen(false)}
          onSubmit={submitForm}
          submitLabel={formMode === "edit" ? L.update : L.save}
        />
      </Modal>
    </div>
  );
}
