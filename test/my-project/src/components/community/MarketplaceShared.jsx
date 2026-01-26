import React, { useEffect, useMemo } from "react";
import { createPortal } from "react-dom";
import { X } from "lucide-react";

const US_STATES = [
  "AL",
  "AK",
  "AZ",
  "AR",
  "CA",
  "CO",
  "CT",
  "DE",
  "FL",
  "GA",
  "HI",
  "ID",
  "IL",
  "IN",
  "IA",
  "KS",
  "KY",
  "LA",
  "ME",
  "MD",
  "MA",
  "MI",
  "MN",
  "MS",
  "MO",
  "MT",
  "NE",
  "NV",
  "NH",
  "NJ",
  "NM",
  "NY",
  "NC",
  "ND",
  "OH",
  "OK",
  "OR",
  "PA",
  "RI",
  "SC",
  "SD",
  "TN",
  "TX",
  "UT",
  "VT",
  "VA",
  "WA",
  "WV",
  "WI",
  "WY",
  "DC",
];

// legacy place categories (stored values)
const PLACE_CATEGORIES = [
  "Restaurant",
  "Cafe",
  "Bakery",
  "Grocery / Arab Market",
  "Things to do",
  "Park / Outdoors",
  "Attraction",
  "Mosque",
  "Church",
  "School / Daycare",
  "Clinic / Doctor",
  "Lawyer / Immigration",
  "Car Services",
  "Handyman / Home Services",
  "Barber / Beauty",
  "Shopping / Mall",
  "Other",
];

// groups (stored values)
const GROUP_PLATFORMS = [
  "Facebook",
  "WhatsApp",
  "Telegram",
  "Discord",
  "Meetup",
  "Other",
];
const GROUP_TOPICS = [
  "Immigration",
  "Jobs",
  "Housing",
  "Education",
  "Health",
  "Friends",
  "Business",
  "Other",
];

const SERVICE_CATEGORIES = [
  "Cleaning",
  "Moving",
  "Handyman",
  "Plumbing",
  "Electrical",
  "HVAC",
  "TV Mounting",
  "Car Repair",
  "Beauty",
  "Tutoring",
  "Legal",
  "Other",
];

const JOB_CATEGORIES = [
  "Restaurant",
  "Delivery",
  "Warehouse",
  "Construction",
  "Office",
  "Tech",
  "Healthcare",
  "Driver",
  "Other",
];

const HOUSING_CATEGORIES = [
  "Rent",
  "Room",
  "Sublease",
  "For Sale",
  "Looking For",
  "Other",
];

const PRODUCT_CATEGORIES = [
  "Electronics",
  "Furniture",
  "Appliances",
  "Clothing",
  "Cars/Parts",
  "Baby",
  "Kitchen",
  "Other",
];

const cn = (...a) => a.filter(Boolean).join(" ");

function normLang(lang) {
  const v = String(lang || "en").toLowerCase();
  if (v.startsWith("ar")) return "ar";
  if (v.startsWith("es")) return "es";
  return "en";
}
function isRTL(lang) {
  return normLang(lang) === "ar";
}
function dirForLang(lang) {
  return isRTL(lang) ? "rtl" : "ltr";
}

const I18N = {
  en: {
    close: "Close",
    select: "Select",
    cancel: "Cancel",
    nameRequired: "Name *",
    groupNameRequired: "Group Name *",
    titleRequired: "Title *",
    category: "Category",
    state: "State",
    city: "City",
    phone: "Phone",
    address: "Address",
    website: "Website",
    notes: "Notes",
    platform: "Platform",
    topic: "Topic",
    linkRequired: "Link *",
    link: "Link",
    contact: "Contact",
    priceBudget: "Price / Budget",
    description: "Description",
    ph_kabul: "e.g. Kabul Kabob",
    ph_fairfax: "e.g. Fairfax",
    ph_phone: "+1 703...",
    ph_fullAddress: "Full address",
    ph_https: "https://",
    ph_placeNotes: "What’s special about this place?",
    ph_groupName: "e.g. Arabs in Virginia",
    ph_alexandria: "e.g. Alexandria",
    ph_invite: "Facebook/WhatsApp invite link",
    ph_rules: "Rules, who it’s for, etc.",
    ph_title: "Title...",
    ph_arlington: "e.g. Arlington",
    ph_link_optional: "Optional: website / post / form",
    ph_contact_optional: "Optional: phone / email",
    ph_desc: "Explain details for newcomers…",
    ph_jobs_price: "$18/hr",
    ph_other_price: "$500",
  },
  ar: {
    close: "إغلاق",
    select: "اختيار",
    cancel: "إلغاء",
    nameRequired: "الاسم *",
    groupNameRequired: "اسم الجروب *",
    titleRequired: "العنوان *",
    category: "التصنيف",
    state: "الولاية",
    city: "المدينة",
    phone: "الهاتف",
    address: "العنوان",
    website: "الموقع",
    notes: "ملاحظات",
    platform: "المنصة",
    topic: "الموضوع",
    linkRequired: "الرابط *",
    link: "رابط",
    contact: "تواصل",
    priceBudget: "السعر / الميزانية",
    description: "الوصف",
    ph_kabul: "مثال: مطعم ...",
    ph_fairfax: "مثال: Fairfax",
    ph_phone: "+1 703...",
    ph_fullAddress: "العنوان بالكامل",
    ph_https: "https://",
    ph_placeNotes: "إيه المميز في المكان ده؟",
    ph_groupName: "مثال: Arabs in Virginia",
    ph_alexandria: "مثال: Alexandria",
    ph_invite: "رابط دعوة فيسبوك/واتساب",
    ph_rules: "قواعد الجروب، مناسب لمين…",
    ph_title: "اكتب عنوان…",
    ph_arlington: "مثال: Arlington",
    ph_link_optional: "اختياري: موقع / بوست / فورم",
    ph_contact_optional: "اختياري: هاتف / إيميل",
    ph_desc: "اشرح التفاصيل للمهاجرين الجدد…",
    ph_jobs_price: "$18/ساعة",
    ph_other_price: "$500",
  },
  es: {
    close: "Cerrar",
    select: "Seleccionar",
    cancel: "Cancelar",
    nameRequired: "Nombre *",
    groupNameRequired: "Nombre del grupo *",
    titleRequired: "Título *",
    category: "Categoría",
    state: "Estado",
    city: "Ciudad",
    phone: "Teléfono",
    address: "Dirección",
    website: "Sitio web",
    notes: "Notas",
    platform: "Plataforma",
    topic: "Tema",
    linkRequired: "Enlace *",
    link: "Enlace",
    contact: "Contacto",
    priceBudget: "Precio / Presupuesto",
    description: "Descripción",
    ph_kabul: "p. ej. Kabul Kabob",
    ph_fairfax: "p. ej. Fairfax",
    ph_phone: "+1 703...",
    ph_fullAddress: "Dirección completa",
    ph_https: "https://",
    ph_placeNotes: "¿Qué tiene de especial este lugar?",
    ph_groupName: "p. ej. Arabs in Virginia",
    ph_alexandria: "p. ej. Alexandria",
    ph_invite: "Enlace de invitación Facebook/WhatsApp",
    ph_rules: "Reglas, para quién es, etc.",
    ph_title: "Título...",
    ph_arlington: "p. ej. Arlington",
    ph_link_optional: "Opcional: web / post / formulario",
    ph_contact_optional: "Opcional: teléfono / email",
    ph_desc: "Explica detalles para recién llegados…",
    ph_jobs_price: "$18/h",
    ph_other_price: "$500",
  },
};

// Category label translations (value stays English for DB)
const LABELS = {
  places: {
    Restaurant: { ar: "مطعم", es: "Restaurante" },
    Cafe: { ar: "كافيه", es: "Café" },
    Bakery: { ar: "مخبز", es: "Panadería" },
    "Grocery / Arab Market": {
      ar: "سوبرماركت / ماركت عربي",
      es: "Tienda / Mercado árabe",
    },
    "Things to do": { ar: "أماكن خروج", es: "Cosas para hacer" },
    "Park / Outdoors": {
      ar: "حديقة / أماكن مفتوحة",
      es: "Parque / Aire libre",
    },
    Attraction: { ar: "معلم/مزار", es: "Atracción" },
    Mosque: { ar: "مسجد", es: "Mezquita" },
    Church: { ar: "كنيسة", es: "Iglesia" },
    "School / Daycare": { ar: "مدرسة / حضانة", es: "Escuela / Guardería" },
    "Clinic / Doctor": { ar: "عيادة / دكتور", es: "Clínica / Doctor" },
    "Lawyer / Immigration": { ar: "محامي / هجرة", es: "Abogado / Inmigración" },
    "Car Services": { ar: "خدمات سيارات", es: "Servicios de auto" },
    "Handyman / Home Services": {
      ar: "صيانة منزلية",
      es: "Manitas / Servicios del hogar",
    },
    "Barber / Beauty": { ar: "حلاق / تجميل", es: "Barbero / Belleza" },
    "Shopping / Mall": { ar: "تسوق / مول", es: "Compras / Centro comercial" },
    Other: { ar: "أخرى", es: "Otro" },
  },
  groups: {
    Facebook: { ar: "فيسبوك", es: "Facebook" },
    WhatsApp: { ar: "واتساب", es: "WhatsApp" },
    Telegram: { ar: "تيليجرام", es: "Telegram" },
    Discord: { ar: "ديسكورد", es: "Discord" },
    Meetup: { ar: "Meetup", es: "Meetup" },
    Immigration: { ar: "هجرة", es: "Inmigración" },
    Jobs: { ar: "وظائف", es: "Empleos" },
    Housing: { ar: "سكن", es: "Vivienda" },
    Education: { ar: "تعليم", es: "Educación" },
    Health: { ar: "صحة", es: "Salud" },
    Friends: { ar: "أصدقاء", es: "Amistades" },
    Business: { ar: "بيزنس", es: "Negocios" },
    Other: { ar: "أخرى", es: "Otro" },
  },
  services: {
    Cleaning: { ar: "تنظيف", es: "Limpieza" },
    Moving: { ar: "نقل/موفينج", es: "Mudanza" },
    Handyman: { ar: "صنايعي/هاندي مان", es: "Manitas" },
    Plumbing: { ar: "سباكة", es: "Plomería" },
    Electrical: { ar: "كهرباء", es: "Electricidad" },
    HVAC: { ar: "تكييف/تدفئة", es: "HVAC" },
    "TV Mounting": { ar: "تركيب تلفزيون", es: "Instalación de TV" },
    "Car Repair": { ar: "تصليح سيارات", es: "Reparación de autos" },
    Beauty: { ar: "تجميل", es: "Belleza" },
    Tutoring: { ar: "دروس", es: "Tutoría" },
    Legal: { ar: "قانوني", es: "Legal" },
    Other: { ar: "أخرى", es: "Otro" },
  },
  jobs: {
    Restaurant: { ar: "مطاعم", es: "Restaurante" },
    Delivery: { ar: "توصيل", es: "Reparto" },
    Warehouse: { ar: "مستودعات", es: "Almacén" },
    Construction: { ar: "بناء", es: "Construcción" },
    Office: { ar: "مكتب", es: "Oficina" },
    Tech: { ar: "تقنية", es: "Tecnología" },
    Healthcare: { ar: "رعاية صحية", es: "Salud" },
    Driver: { ar: "سائق", es: "Conductor" },
    Other: { ar: "أخرى", es: "Otro" },
  },
  housing: {
    Rent: { ar: "إيجار", es: "Alquiler" },
    Room: { ar: "غرفة", es: "Habitación" },
    Sublease: { ar: "تنازل/سابليس", es: "Subarrendo" },
    "For Sale": { ar: "للبيع", es: "En venta" },
    "Looking For": { ar: "مطلوب", es: "Busco" },
    Other: { ar: "أخرى", es: "Otro" },
  },
  products: {
    Electronics: { ar: "إلكترونيات", es: "Electrónica" },
    Furniture: { ar: "أثاث", es: "Muebles" },
    Appliances: { ar: "أجهزة", es: "Electrodomésticos" },
    Clothing: { ar: "ملابس", es: "Ropa" },
    "Cars/Parts": { ar: "سيارات/قطع", es: "Autos/Piezas" },
    Baby: { ar: "أطفال", es: "Bebé" },
    Kitchen: { ar: "مطبخ", es: "Cocina" },
    Other: { ar: "أخرى", es: "Otro" },
  },
};

function t(lang, key) {
  const L = normLang(lang);
  return (I18N[L] && I18N[L][key]) || I18N.en[key] || key;
}

function optionLabel(kind, value, lang) {
  const L = normLang(lang);
  if (!value) return "";
  if (L === "en") return value;
  const map = LABELS[kind] || {};
  const rec = map[value];
  if (!rec) return value;
  return rec[L] || value;
}

export function Modal({
  open,
  onClose,
  title,
  subtitle,
  children,
  lang = "en",
}) {
  useEffect(() => {
    const html = document.documentElement;
    const body = document.body;

    const KEY = "__modalScrollLock__";
    if (!window[KEY]) {
      window[KEY] = {
        count: 0,
        prevHtmlOverflow: "",
        prevBodyOverflow: "",
        prevBodyPaddingRight: "",
      };
    }

    if (!open) return;

    if (window[KEY].count === 0) {
      window[KEY].prevHtmlOverflow = html.style.overflow || "";
      window[KEY].prevBodyOverflow = body.style.overflow || "";
      window[KEY].prevBodyPaddingRight = body.style.paddingRight || "";

      const scrollBarWidth =
        window.innerWidth - document.documentElement.clientWidth;

      html.style.overflow = "hidden";
      body.style.overflow = "hidden";
      if (scrollBarWidth > 0) body.style.paddingRight = `${scrollBarWidth}px`;
    }

    window[KEY].count += 1;

    return () => {
      window[KEY].count = Math.max(0, window[KEY].count - 1);
      if (window[KEY].count === 0) {
        html.style.overflow = window[KEY].prevHtmlOverflow || "";
        body.style.overflow = window[KEY].prevBodyOverflow || "";
        body.style.paddingRight = window[KEY].prevBodyPaddingRight || "";
      }
    };
  }, [open]);

  if (!open) return null;

  const dir = dirForLang(lang);

  return createPortal(
    <div
      dir={dir}
      className="fixed inset-0 z-[9999] bg-black/60 backdrop-blur-sm flex items-end sm:items-center justify-center p-3 sm:p-4 overscroll-contain"
      role="dialog"
      aria-modal="true"
    >
      <div className="w-full max-w-3xl rounded-2xl bg-white shadow-2xl border border-gray-200 overflow-hidden">
        <div className="flex items-start justify-between gap-3 px-4 sm:px-6 py-4 sm:py-5 border-b border-gray-100">
          <div className="min-w-0">
            <div className="text-lg sm:text-xl font-extrabold text-gray-900 truncate">
              {title}
            </div>
            {subtitle ? (
              <div className="mt-1 text-sm text-gray-500 leading-snug">
                {subtitle}
              </div>
            ) : null}
          </div>

          <button
            onClick={onClose}
            className="shrink-0 p-2 rounded-xl hover:bg-gray-100 text-gray-700 active:scale-[0.98]"
            aria-label={t(lang, "close")}
            type="button"
          >
            <X size={18} />
          </button>
        </div>

        <div
          className="px-4 sm:px-6 py-4 sm:py-5 max-h-[78vh] overflow-y-auto overscroll-contain"
          style={{ WebkitOverflowScrolling: "touch" }}
        >
          {children}
        </div>
      </div>
    </div>,
    document.body
  );
}

export function ListingFormBody({
  formType,
  values,
  setField,
  getSchema,
  formCitySuggestions,
  onCancel,
  onSubmit,
  submitLabel,
  lang = "en",
}) {
  const dir = dirForLang(lang);
  const schema = useMemo(
    () => getSchema(formType, { lang }),
    [formType, getSchema, lang]
  );

  return (
    <div dir={dir}>
      <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
        {schema.map((f) => (
          <div
            key={f.k}
            className={cn(f.span2 ? "sm:col-span-2" : "", "min-w-0")}
          >
            <label className="text-sm font-medium text-gray-700">
              {f.label}
            </label>

            {f.type === "select" ? (
              <select
                value={values[f.k] || ""}
                onChange={(e) => setField(f.k, e.target.value)}
                className="mt-1 w-full py-2.5 px-3 rounded-xl border border-gray-200 focus:outline-none focus:ring-2 focus:ring-black/20 bg-white"
              >
                {(f.options || []).map((op) => (
                  <option key={op || "empty"} value={op}>
                    {op
                      ? f.optionKind
                        ? optionLabel(f.optionKind, op, lang)
                        : op
                      : t(lang, "select")}
                  </option>
                ))}
              </select>
            ) : f.type === "textarea" ? (
              <textarea
                value={values[f.k] || ""}
                onChange={(e) => setField(f.k, e.target.value)}
                className="mt-1 w-full min-h-[110px] py-2.5 px-3 rounded-xl border border-gray-200 focus:outline-none focus:ring-2 focus:ring-black/20"
                placeholder={f.ph || ""}
              />
            ) : (
              <input
                value={values[f.k] || ""}
                onChange={(e) => setField(f.k, e.target.value)}
                list={f.list || undefined}
                className="mt-1 w-full py-2.5 px-3 rounded-xl border border-gray-200 focus:outline-none focus:ring-2 focus:ring-black/20"
                placeholder={f.ph || ""}
                inputMode={f.inputMode || undefined}
              />
            )}
          </div>
        ))}

        <datalist id="form-city-suggestions">
          {formCitySuggestions.map((c) => (
            <option key={c} value={c} />
          ))}
        </datalist>
      </div>

      <div
        className={cn("mt-6 flex flex-col sm:flex-row gap-2 sm:justify-end")}
      >
        <button
          onClick={onCancel}
          className="w-full sm:w-auto px-4 py-2.5 rounded-xl border border-gray-200 hover:bg-gray-50 font-semibold active:scale-[0.99]"
          type="button"
        >
          {t(lang, "cancel")}
        </button>
        <button
          onClick={onSubmit}
          className="w-full sm:w-auto px-4 py-2.5 rounded-xl bg-black text-white hover:bg-black/90 font-semibold active:scale-[0.99]"
          type="button"
        >
          {submitLabel}
        </button>
      </div>
    </div>
  );
}

// =========================
// Shared form helpers (USED BY CommunityView + ItemDetailsView)
// =========================

export const EMPTY_VALUES = {
  // shared
  title: "",
  category: "",
  state: "",
  city: "",
  notes: "",
  // places
  address: "",
  phone: "",
  website: "",
  // groups
  platform: "Facebook",
  topic: "Immigration",
  link: "",
  // marketplace
  price: "",
  contact: "",
  description: "",
};

export function normalizeId(raw, typeKey) {
  const v = String(raw || "").trim();
  if (!v) return v;

  // ✅ لو جاي بصيغة: product_7 / housing_10 / services_3 ... → خليه "7"
  if (v.includes("_")) {
    const last = v.split("_").pop();
    if (last && /^\d+$/.test(last)) return last;
    return last || v;
  }

  // ✅ لو رقم أصلاً
  if (/^\d+$/.test(v)) return v;

  // ✅ لو فيه رقم جوّه أي سترنج: "id:10" → "10"
  const m = v.match(/(\d+)/);
  return m ? m[1] : v;
}

export function getCategoryOptionsForTab(t) {
  if (t === "places") return PLACE_CATEGORIES;
  if (t === "services") return SERVICE_CATEGORIES;
  if (t === "jobs") return JOB_CATEGORIES;
  if (t === "housing") return HOUSING_CATEGORIES;
  if (t === "products") return PRODUCT_CATEGORIES;
  return [];
}

export function getSchema(typeKey, opts = {}) {
  const lang = opts.lang || "en";

  // fields to show per type
  if (typeKey === "places") {
    return [
      {
        k: "title",
        label: t(lang, "nameRequired"),
        type: "text",
        ph: t(lang, "ph_kabul"),
        span2: true,
      },
      {
        k: "category",
        label: t(lang, "category"),
        type: "select",
        options: PLACE_CATEGORIES,
        optionKind: "places",
      },
      {
        k: "state",
        label: t(lang, "state"),
        type: "select",
        options: ["", ...US_STATES],
      },
      {
        k: "city",
        label: t(lang, "city"),
        type: "text",
        ph: t(lang, "ph_fairfax"),
        list: "form-city-suggestions",
      },
      {
        k: "phone",
        label: t(lang, "phone"),
        type: "text",
        ph: t(lang, "ph_phone"),
        inputMode: "tel",
      },
      {
        k: "address",
        label: t(lang, "address"),
        type: "text",
        ph: t(lang, "ph_fullAddress"),
        span2: true,
      },
      {
        k: "website",
        label: t(lang, "website"),
        type: "text",
        ph: t(lang, "ph_https"),
        span2: true,
      },
      {
        k: "notes",
        label: t(lang, "notes"),
        type: "textarea",
        ph: t(lang, "ph_placeNotes"),
        span2: true,
      },
    ];
  }

  if (typeKey === "groups") {
    return [
      {
        k: "title",
        label: t(lang, "groupNameRequired"),
        type: "text",
        ph: t(lang, "ph_groupName"),
        span2: true,
      },
      {
        k: "platform",
        label: t(lang, "platform"),
        type: "select",
        options: GROUP_PLATFORMS,
        optionKind: "groups",
      },
      {
        k: "topic",
        label: t(lang, "topic"),
        type: "select",
        options: GROUP_TOPICS,
        optionKind: "groups",
      },
      {
        k: "state",
        label: t(lang, "state"),
        type: "select",
        options: ["", ...US_STATES],
      },
      {
        k: "city",
        label: t(lang, "city"),
        type: "text",
        ph: t(lang, "ph_alexandria"),
        list: "form-city-suggestions",
      },
      {
        k: "link",
        label: t(lang, "linkRequired"),
        type: "text",
        ph: t(lang, "ph_invite"),
        span2: true,
      },
      {
        k: "notes",
        label: t(lang, "notes"),
        type: "textarea",
        ph: t(lang, "ph_rules"),
        span2: true,
      },
    ];
  }

  // services/jobs/housing/products
  return [
    {
      k: "title",
      label: t(lang, "titleRequired"),
      type: "text",
      ph: t(lang, "ph_title"),
      span2: true,
    },
    {
      k: "category",
      label: t(lang, "category"),
      type: "select",
      options: getCategoryOptionsForTab(typeKey),
      optionKind: typeKey,
    },
    {
      k: "price",
      label: t(lang, "priceBudget"),
      type: "text",
      ph:
        typeKey === "jobs"
          ? t(lang, "ph_jobs_price")
          : t(lang, "ph_other_price"),
    },
    {
      k: "state",
      label: t(lang, "state"),
      type: "select",
      options: ["", ...US_STATES],
    },
    {
      k: "city",
      label: t(lang, "city"),
      type: "text",
      ph: t(lang, "ph_arlington"),
      list: "form-city-suggestions",
    },
    {
      k: "link",
      label: t(lang, "link"),
      type: "text",
      ph: t(lang, "ph_link_optional"),
      span2: true,
    },
    {
      k: "contact",
      label: t(lang, "contact"),
      type: "text",
      ph: t(lang, "ph_contact_optional"),
      span2: true,
    },
    {
      k: "description",
      label: t(lang, "description"),
      type: "textarea",
      ph: t(lang, "ph_desc"),
      span2: true,
    },
  ];
}

// =========================
// ✅ Shared helpers for Edit forms (single source of truth)
// =========================
export function buildEditValues(kind, s) {
  return {
    // keep both name/title for compatibility across schemas
    name: s.title,
    title: s.title,

    category: s.category,
    city: s.city,
    state: s.state,
    phone: s.phone,
    address: s.address,
    zip: s.zip,
    website: s.website,

    // keep both notes/description
    notes: s.desc,
    description: s.desc,

    // price only used when schema includes it
    price: s.price,

    // groups extras (safe keep if present)
    platform: s.platform || "Facebook",
    topic: s.topic || "Immigration",
    link: s.link || s.website || "",
    contact: s.contact || s.phone || "",
  };
}

export function applyEditField(setters, k, v) {
  const {
    setTitle,
    setCategory,
    setAddress,
    setCity,
    setState,
    setZip,
    setPhone,
    setWebsite,
    setDesc,
    setPrice,
    setPlatform,
    setTopic,
    setLink,
    setContact,
  } = setters;

  if (k === "title" || k === "name") return setTitle?.(v);
  if (k === "category") return setCategory?.(v);
  if (k === "address") return setAddress?.(v);
  if (k === "city") return setCity?.(v);
  if (k === "state") return setState?.(v);
  if (k === "zip") return setZip?.(v);
  if (k === "phone") return setPhone?.(v);
  if (k === "website" || k === "url") return setWebsite?.(v);
  if (k === "description" || k === "notes") return setDesc?.(v);
  if (k === "price" || k === "price_value" || k === "amount")
    return setPrice?.(v);

  // groups / marketplace extras
  if (k === "platform") return setPlatform?.(v);
  if (k === "topic") return setTopic?.(v);
  if (k === "link") return setLink?.(v);
  if (k === "contact") return setContact?.(v);

  // compatibility bridges
  if (k === "contact" || k === "phone") return setPhone?.(v);
  if (k === "link") return setWebsite?.(v);
}

export {
  US_STATES,
  PLACE_CATEGORIES,
  GROUP_PLATFORMS,
  GROUP_TOPICS,
  SERVICE_CATEGORIES,
  JOB_CATEGORIES,
  HOUSING_CATEGORIES,
  PRODUCT_CATEGORIES,
};
