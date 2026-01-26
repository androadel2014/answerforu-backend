// src/components/CVEditView.jsx (FULL FILE - copy/paste)
// ✅ Pretty UI (modern card + hints)
// ✅ Loads CV from /api/get-cv/:cvId
// ✅ Converts finalCV -> builder draft with auto start/end from dates
// ✅ Opens /cv_builder?cvId=...&mode=edit and preloads localStorage cv_data_full + cv_edit_meta

import React, { useEffect, useMemo, useState } from "react";
import toast from "react-hot-toast";
import { useNavigate } from "react-router-dom";
import {
  ArrowLeft,
  ArrowRight,
  Sparkles,
  FileText,
  ShieldCheck,
  Wand2,
} from "lucide-react";

export const CVEditView = ({ lang = "en" }) => {
  const navigate = useNavigate();

  const normLang = (v) => {
    const s = String(v || "en").toLowerCase();
    if (s.startsWith("ar")) return "ar";
    if (s.startsWith("es")) return "es";
    return "en";
  };

  const LKEY = normLang(lang);
  const dir = LKEY === "ar" ? "rtl" : "ltr";
  const BackIcon = LKEY === "ar" ? ArrowRight : ArrowLeft;

  const T = useMemo(() => {
    const base = {
      ar: {
        missingCvId: "cvId غير موجود",
        loginAgain: "لازم تسجّل دخول تاني",
        failedLoad: "فشل تحميل البيانات",
        loading: "جارٍ التحميل...",
        noData: "لا توجد بيانات لهذه السيرة",
        title: "تعديل السيرة الذاتية",
        back: "رجوع",
        openBuilder: "افتح منشئ السيرة",
        step1Hint:
          "هتفتح منشئ السيرة على الخطوة الأولى بالبيانات اللي هنا، تعدّل براحتك، وبعدها تعمل Generate وتعدّي على الـ AI تاني علشان تحافظ على النسخة الإنجليزية.",
        saveDisabledHint:
          "ملحوظة: الحفظ من صفحة التعديل اتلغى. التعديل لازم يتم عبر CV Builder علشان مايرجعش عربي.",
        badge1: "English-safe",
        badge2: "AI flow",
        badge3: "No data loss",
      },
      en: {
        missingCvId: "Missing cvId",
        loginAgain: "Please login again",
        failedLoad: "Failed to load",
        loading: "Loading...",
        noData: "No CV data found",
        title: "Edit Resume",
        back: "Back",
        openBuilder: "Open CV Builder",
        step1Hint:
          "We will open CV Builder on Step 1 with your data. Edit, then Generate and pass through AI again to keep the English version.",
        saveDisabledHint:
          "Note: Saving from this page is disabled. Editing must go through CV Builder to preserve the English CV.",
        badge1: "English-safe",
        badge2: "AI flow",
        badge3: "No data loss",
      },
      es: {
        missingCvId: "Falta cvId",
        loginAgain: "Inicia sesión de nuevo",
        failedLoad: "No se pudo cargar",
        loading: "Cargando...",
        noData: "No se encontraron datos del CV",
        title: "Editar CV",
        back: "Volver",
        openBuilder: "Abrir CV Builder",
        step1Hint:
          "Abriremos CV Builder en el Paso 1 con tus datos. Edita, luego genera y pasa por IA de nuevo para mantener el CV en inglés.",
        saveDisabledHint:
          "Nota: Guardar desde esta página está deshabilitado. La edición debe hacerse en CV Builder para conservar el CV en inglés.",
        badge1: "English-safe",
        badge2: "AI flow",
        badge3: "No data loss",
      },
    };
    return base[LKEY] || base.en;
  }, [LKEY]);

  const params = useMemo(() => new URLSearchParams(window.location.search), []);
  const cvId = params.get("cvId");

  const API_BASE =
    import.meta.env.VITE_API_BASE_URL ||
    import.meta.env.VITE_API_BASE ||
    "http://localhost:5000";

  const [loading, setLoading] = useState(true);
  const [data, setData] = useState(null);

  const arr = (v) => (Array.isArray(v) ? v : []);

  const getToken = () =>
    localStorage.getItem("token") ||
    localStorage.getItem("authToken") ||
    localStorage.getItem("jwt") ||
    "";

  const authHeaders = () => {
    const token = getToken();
    return { ...(token ? { Authorization: `Bearer ${token}` } : {}) };
  };

  // ✅ Parse combined dates into start/end
  const pickDates = (datesStr) => {
    const s = String(datesStr || "").trim();
    if (!s) return { start: "", end: "" };

    // "10/2016 to 10/2022" OR "Oct 2016 - Oct 2022"
    const parts = s.split(/\s+to\s+|\s+-\s+/i).map((x) => x.trim());
    if (parts.length >= 2) return { start: parts[0], end: parts[1] };
    return { start: "", end: "" };
  };

  // ✅ IMPORTANT: map DB CV (final or draft) into builder-friendly draft data
  const normalizeToBuilderDraft = (dbData) => {
    // Case 1: already in builder draft format (personalInfo/experiences/etc)
    if (dbData?.personalInfo) {
      return {
        personalInfo: {
          fullName:
            dbData.personalInfo?.fullName || dbData.personal?.name || "",
          email: dbData.personalInfo?.email || dbData.personal?.email || "",
          phone: dbData.personalInfo?.phone || dbData.personal?.phone || "",
          address:
            dbData.personalInfo?.address || dbData.personal?.address || "",
        },
        summary: dbData.summary || "",
        experiences: arr(dbData.experiences || dbData.experience).map((j) => ({
          id: j.id || Date.now() + Math.random(),
          title: j.title || "",
          company: j.company || "",
          location: j.location || "",
          start: j.start || "",
          end: j.end || "",
          descriptionRaw:
            j.descriptionRaw ||
            (arr(j.bullets).length ? arr(j.bullets).join("\n") : ""),
        })),
        education: arr(dbData.education).map((e) => ({
          id: e.id || Date.now() + Math.random(),
          degree: e.degree || "",
          major: e.major || "",
          school: e.school || "",
          location: e.location || "",
          year: e.year || e.date || "",
        })),
        courses: arr(dbData.courses).map((c) => ({
          id: c.id || Date.now() + Math.random(),
          name: c.name || "",
          provider: c.provider || "",
          date: c.date || "",
        })),
        skills: arr(dbData.skills),
        languages: dbData.languages || "",
        targetJob: dbData.targetJob || {
          title: "Resume",
          company: "",
          state: "",
        },
      };
    }

    // Case 2: finalCV format (name/contact/experience[] with dates)
    if (dbData?.name && (dbData?.experience || dbData?.experiences)) {
      const contactRaw = String(dbData.contact || "");
      const parts = contactRaw
        .replace(/\[([^\]]+)\]\(([^)]+)\)/g, "$1")
        .replace(/mailto:/gi, "")
        .split("|")
        .map((x) => x.trim())
        .filter(Boolean);

      const email = parts.find((x) => /@/.test(x)) || "";
      const phone = parts.find((x) => /(\+?\d[\d\s().-]{7,})/.test(x)) || "";
      const address = parts.find((x) => x && x !== email && x !== phone) || "";

      const personalInfo = {
        fullName: dbData.name || "",
        address: address || "",
        phone: phone || "",
        email: email || "",
      };

      const experiences = arr(dbData.experience || dbData.experiences).map(
        (j) => {
          const dd = pickDates(j.dates || j.date || "");
          return {
            id: Date.now() + Math.random(),
            title: j.title || "",
            company: j.company || "",
            location: j.location || "",
            start: j.start || dd.start || "",
            end: j.end || dd.end || "",
            descriptionRaw: arr(j.bullets).length
              ? arr(j.bullets).join("\n")
              : "",
          };
        }
      );

      const education = arr(dbData.education).map((e) => ({
        id: Date.now() + Math.random(),
        degree: e.degree || "",
        major: "",
        school: e.school || "",
        location: e.location || "",
        year: e.date || "",
      }));

      const courses = arr(dbData.courses).map((c) => ({
        id: Date.now() + Math.random(),
        name: c.name || "",
        provider: c.provider || "",
        date: c.date || "",
      }));

      return {
        personalInfo,
        summary: dbData.summary || "",
        experiences,
        education,
        courses,
        skills: arr(dbData.skills),
        languages: dbData.languages || "",
        targetJob: { title: "Resume", company: "", state: "" },
      };
    }

    // fallback
    return {
      targetJob: { title: "Resume", company: "", state: "" },
      personalInfo: { fullName: "", phone: "", email: "", address: "" },
      education: [],
      courses: [],
      experiences: [],
      languages: "",
      summary: "",
      skills: [],
    };
  };

  useEffect(() => {
    if (!cvId) {
      setLoading(false);
      toast.error(T.missingCvId);
      navigate("/profile", { replace: true });
      return;
    }

    (async () => {
      try {
        setLoading(true);

        const token = getToken();
        if (!token) {
          toast.error(T.loginAgain);
          navigate("/auth", { replace: true });
          return;
        }

        const res = await fetch(`${API_BASE}/api/get-cv/${cvId}`, {
          method: "GET",
          headers: { ...authHeaders() },
        });

        if (res.status === 404) {
          setData(null);
          return;
        }

        if (!res.ok) {
          let msg = "";
          try {
            const err = await res.json();
            msg = err?.message || "";
          } catch {}
          throw new Error(msg || `Request failed (${res.status})`);
        }

        const payload = await res.json();

        let raw = payload?.cv_data ?? payload;
        if (typeof raw === "string") {
          try {
            raw = JSON.parse(raw);
          } catch {}
        }
        

        setData(normalizeToBuilderDraft(raw));
      } catch (e) {
        toast.error(T.failedLoad + (e?.message ? `: ${e.message}` : ""));
        setData(null);
      } finally {
        setLoading(false);
      }
    })();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [cvId, LKEY]);

  // ✅ send user to /cv_builder with draft preloaded + edit marker
  const continueInBuilder = () => {
    try {
      localStorage.setItem("cv_data_full", JSON.stringify(data || {}));
      localStorage.setItem(
        "cv_edit_meta",
        JSON.stringify({ cvId: String(cvId || ""), ts: Date.now() })
      );
    } catch {}

    navigate(`/cv_builder?cvId=${encodeURIComponent(String(cvId))}&mode=edit`, {
      replace: true,
    });
  };

  if (loading) {
    return (
      <div className="max-w-3xl mx-auto p-4 sm:p-6" dir={dir}>
        <div className="bg-white border border-slate-100 rounded-3xl shadow-sm p-8 sm:p-10">
          <div className="animate-pulse space-y-4">
            <div className="h-6 w-1/2 bg-slate-100 rounded-xl" />
            <div className="h-10 w-full bg-slate-100 rounded-2xl" />
            <div className="h-24 w-full bg-slate-100 rounded-2xl" />
            <div className="h-16 w-full bg-slate-100 rounded-2xl" />
          </div>
          <div className="mt-6 text-center text-slate-400 font-black">
            {T.loading}
          </div>
        </div>
      </div>
    );
  }

  if (!data) {
    return (
      <div className="max-w-3xl mx-auto p-4 sm:p-6" dir={dir}>
        <div className="bg-white border border-slate-100 rounded-3xl shadow-sm p-10 text-center">
          <div className="mx-auto w-16 h-16 rounded-2xl bg-slate-100 flex items-center justify-center text-slate-500">
            <FileText size={30} />
          </div>
          <div className="mt-4 font-black text-slate-800 text-xl">
            {T.noData}
          </div>
          <div className="mt-6">
            <button
              onClick={() => navigate("/profile", { replace: true })}
              className="px-5 py-3 rounded-2xl bg-slate-900 text-white font-black inline-flex items-center gap-2"
              type="button"
            >
              <BackIcon size={18} /> {T.back}
            </button>
          </div>
        </div>
      </div>
    );
  }

  const name =
    data?.personalInfo?.fullName || data?.targetJob?.title || "Resume";

  const chip = (Icon, text, tone = "slate") => {
    const toneMap = {
      slate: "bg-slate-50 text-slate-700 border-slate-200",
      blue: "bg-blue-50 text-blue-700 border-blue-200",
      emerald: "bg-emerald-50 text-emerald-700 border-emerald-200",
      purple: "bg-purple-50 text-purple-700 border-purple-200",
    };
    const cls = toneMap[tone] || toneMap.slate;
    return (
      <span
        className={`inline-flex items-center gap-2 px-3 py-1.5 rounded-full border text-[12px] font-black ${cls}`}
      >
        <Icon size={16} />
        {text}
      </span>
    );
  };

  return (
    <div className="max-w-4xl mx-auto p-4 sm:p-8" dir={dir}>
      <div className="bg-white border border-slate-100 rounded-[2.5rem] shadow-xl overflow-hidden">
        {/* Header */}
        <div className="p-6 sm:p-8 bg-gradient-to-r from-blue-600 to-indigo-700 text-white">
          <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
            <div className="min-w-0">
              <div className="text-xs font-black opacity-80 tracking-widest uppercase">
                {T.title}
              </div>
              <div className="mt-1 text-2xl sm:text-3xl font-black truncate">
                {name}
              </div>
            </div>

            <div className="flex flex-col sm:flex-row gap-2">
              <button
                onClick={() => navigate("/profile", { replace: true })}
                className="px-5 py-3 rounded-2xl bg-white/15 hover:bg-white/20 border border-white/20 font-black inline-flex items-center justify-center gap-2 active:scale-[0.99]"
                type="button"
              >
                <BackIcon size={18} /> {T.back}
              </button>

              <button
                onClick={continueInBuilder}
                className="px-5 py-3 rounded-2xl bg-white text-blue-700 hover:bg-slate-50 font-black inline-flex items-center justify-center gap-2 shadow-lg active:scale-[0.99]"
                type="button"
              >
                <Sparkles size={18} /> {T.openBuilder}
              </button>
            </div>
          </div>

          <div className="mt-5 flex flex-wrap gap-2">
            {chip(ShieldCheck, T.badge1, "emerald")}
            {chip(Wand2, T.badge2, "purple")}
            {chip(FileText, T.badge3, "blue")}
          </div>
        </div>

        {/* Body */}
        <div className="p-6 sm:p-8 space-y-4">
          <div className="p-5 rounded-3xl bg-blue-50 border border-blue-100 text-blue-900">
            <div className="font-black mb-2">
              {LKEY === "ar"
                ? "هتعمل إيه دلوقتي؟"
                : LKEY === "es"
                ? "¿Qué harás ahora?"
                : "What happens next?"}
            </div>
            <div className="text-sm font-bold leading-relaxed">
              {T.step1Hint}
            </div>
          </div>

          <div className="p-5 rounded-3xl bg-slate-50 border border-slate-200 text-slate-700">
            <div className="font-black mb-2">
              {LKEY === "ar"
                ? "مهم"
                : LKEY === "es"
                ? "Importante"
                : "Important"}
            </div>
            <div className="text-sm font-bold leading-relaxed">
              {T.saveDisabledHint}
            </div>
          </div>

          {/* Quick preview cards */}
          <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
            <div className="p-4 rounded-3xl border border-slate-200 bg-white">
              <div className="text-xs font-black text-slate-400 uppercase tracking-widest">
                {LKEY === "ar"
                  ? "الخبرات"
                  : LKEY === "es"
                  ? "Experiencia"
                  : "Experience"}
              </div>
              <div className="mt-1 text-2xl font-black text-slate-900">
                {Array.isArray(data.experiences) ? data.experiences.length : 0}
              </div>
            </div>

            <div className="p-4 rounded-3xl border border-slate-200 bg-white">
              <div className="text-xs font-black text-slate-400 uppercase tracking-widest">
                {LKEY === "ar"
                  ? "التعليم"
                  : LKEY === "es"
                  ? "Educación"
                  : "Education"}
              </div>
              <div className="mt-1 text-2xl font-black text-slate-900">
                {Array.isArray(data.education) ? data.education.length : 0}
              </div>
            </div>

            <div className="p-4 rounded-3xl border border-slate-200 bg-white">
              <div className="text-xs font-black text-slate-400 uppercase tracking-widest">
                {LKEY === "ar"
                  ? "المهارات"
                  : LKEY === "es"
                  ? "Habilidades"
                  : "Skills"}
              </div>
              <div className="mt-1 text-2xl font-black text-slate-900">
                {Array.isArray(data.skills) ? data.skills.length : 0}
              </div>
            </div>
          </div>

          {/* Tiny info row */}
          <div className="text-xs text-slate-400 font-black">
            {LKEY === "ar" ? `cvId: ${cvId}` : `cvId: ${cvId}`}
          </div>
        </div>
      </div>
    </div>
  );
};

export default CVEditView;
