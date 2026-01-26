// src/components/cvbuilder/CVBuilderView.jsx
import React, { useEffect, useMemo, useState } from "react";
import * as docx from "docx";
import { saveAs } from "file-saver";
import toast from "react-hot-toast";
import html2pdf from "html2pdf.js";

import { Copy } from "lucide-react";

import StepInput from "./StepInput";
import StepProcess from "./StepProcess";
import StepPreview from "./StepPreview";
import PromptModal from "./PromptModal";

import {
  FALLBACK,
  normalizeCVData,
  resolveSection,
  getExperienceArrayFrom,
  safeWriteClipboard,
} from "./cvHelpers";

import { toastConfirm } from "../../lib/notify";

const normLang = (lang) => {
  const v = String(lang || "en").toLowerCase();
  if (v.startsWith("ar")) return "ar";
  if (v.startsWith("es")) return "es";
  return "en";
};
const isRTL = (lang) => normLang(lang) === "ar";
const dirForLang = (lang) => (isRTL(lang) ? "rtl" : "ltr");

const I18N = {
  en: {
    needEmailOrPhone: "Add at least Email or Phone before continuing",
    tipEmailPhone: "Tip: Add both Email and Phone for ATS",
    expDatesMissing: (i, title) =>
      `Please fill Start/End dates in experience #${i} (${title})`,
    eduYearMissing: (i, school) =>
      `Please fill graduation date in education #${i} (${school})`,
    courseDateMissing: (i, name) =>
      `Please fill course date for course #${i} (${name})`,
    optimizeOnNeedJD:
      "Optimize for JD is ON — please paste the Job Description",
    loginFirst: "Please login first",
    cvAdded: "CV Added Successfully",
    serverError: "Server Connection Error",
    copyManual: "Copy manually",
    copiedAuto: "Copied automatically ✅",
    copyAutoFail: "Auto copy failed",
    copiedManual: "Copied manually ✅",
    copyManualFail: "Copy failed",
    invalidJson: "Invalid JSON. Make sure you pasted ONLY the JSON code block.",
    noDataWord: "No data to create the file",
    noCVYet: "No CV yet",
    goStep2Desc: "Go to Step 2, paste the JSON, then Create.",
    goStep2: "Go to Step 2",
    excellent: "Excellent",
    good: "Good",
    needsWork: "Needs Work",
    confirmDeleteAll: "Delete all data?",
    deleted: "Deleted",
  },
  ar: {
    needEmailOrPhone: "لازم تضيف Email أو Phone على الأقل قبل المتابعة",
    tipEmailPhone: "تنبيه: الأفضل تضيف Email و Phone معًا عشان ATS",
    expDatesMissing: (i, title) =>
      `لازم تكمّل التواريخ (من/إلى) في الخبرة رقم ${i} (${title})`,
    eduYearMissing: (i, school) =>
      `لازم تكتب تاريخ التخرج في التعليم رقم ${i} (${school})`,
    courseDateMissing: (i, name) => `لازم تكتب تاريخ الكورس رقم ${i} (${name})`,
    optimizeOnNeedJD:
      "إنت مُفعّل Optimize for JD — لازم تلزق الـ Job Description",
    loginFirst: "سجل دخول الأول",
    cvAdded: "تمت إضافة السيرة الذاتية للقائمة بنجاح",
    serverError: "خطأ في الاتصال بالسيرفر",
    copyManual: "نسخ يدوي",
    copiedAuto: "تم النسخ تلقائياً ✅",
    copyAutoFail: "فشل النسخ التلقائي",
    copiedManual: "تم النسخ يدوياً ✅",
    copyManualFail: "فشل النسخ",
    invalidJson: "الكود مش مظبوط. اتأكد إنك نسخت الـ JSON Code Block بس.",
    noDataWord: "لا توجد بيانات لإنشاء الملف",
    noCVYet: "مفيش CV لسه",
    goStep2Desc: "روح Step 2 واعمل Paste للـ JSON وبعدين Create.",
    goStep2: "روح Step 2",
    excellent: "ممتاز",
    good: "جيد",
    needsWork: "محتاج تحسين",
    confirmDeleteAll: "هل أنت متأكد من حذف جميع البيانات؟",
    deleted: "تم الحذف",
  },
  es: {
    needEmailOrPhone: "Agrega al menos Email o Teléfono antes de continuar",
    tipEmailPhone: "Tip: Agrega Email y Teléfono para ATS",
    expDatesMissing: (i, title) =>
      `Completa fechas de inicio/fin en experiencia #${i} (${title})`,
    eduYearMissing: (i, school) =>
      `Completa la fecha de graduación en educación #${i} (${school})`,
    courseDateMissing: (i, name) =>
      `Completa la fecha del curso #${i} (${name})`,
    optimizeOnNeedJD: "Optimize for JD está activado — pega la Job Description",
    loginFirst: "Inicia sesión primero",
    cvAdded: "CV agregado correctamente",
    serverError: "Error de conexión con el servidor",
    copyManual: "Copiar manualmente",
    copiedAuto: "Copiado automáticamente ✅",
    copyAutoFail: "Falló el copiado automático",
    copiedManual: "Copiado manualmente ✅",
    copyManualFail: "Falló la copia",
    invalidJson:
      "JSON inválido. Asegúrate de pegar SOLO el bloque de código JSON.",
    noDataWord: "No hay datos para crear el archivo",
    noCVYet: "Aún no hay CV",
    goStep2Desc: "Ve al Paso 2, pega el JSON y luego Create.",
    goStep2: "Ir al Paso 2",
    excellent: "Excelente",
    good: "Bueno",
    needsWork: "Necesita mejorar",
    confirmDeleteAll: "¿Eliminar todos los datos?",
    deleted: "Eliminado",
  },
};

const t = (lang, key, ...args) => {
  const L = normLang(lang);
  const v = (I18N[L] && I18N[L][key]) || I18N.en[key] || key;
  return typeof v === "function" ? v(...args) : v;
};

export const CVBuilderView = ({ lang }) => {
  const API_BASE =
    import.meta.env.VITE_API_BASE_URL ||
    import.meta.env.VITE_API_BASE ||
    "http://localhost:5000";

  // ✅ Edit mode support (from /cv_edit -> /cv_builder?mode=edit&cvId=123)
  const qs = useMemo(() => new URLSearchParams(window.location.search), []);
  const isEditMode = (qs.get("mode") || "").toLowerCase() === "edit";
  const editingCvId = (qs.get("cvId") || "").trim();

  const token = useMemo(() => localStorage.getItem("token") || "", []);
  const authHeaders = useMemo(() => {
    const h = { "Content-Type": "application/json" };
    if (token) h.Authorization = `Bearer ${token}`;
    return h;
  }, [token]);

  const [activeTab, setActiveTab] = useState("input");
  // ✅ DEBUG: show incoming data shapes
  const [debugOpen, setDebugOpen] = useState(false);
  const [debugInfo, setDebugInfo] = useState({
    localRaw: null,
    apiPayload: null,
    apiRaw: null,
    apiDraft: null,
    lastError: "",
  });

  const [data, setData] = useState(() => {
    const savedData = localStorage.getItem("cv_data_full");
    if (!savedData) return FALLBACK;
    try {
      return normalizeCVData(JSON.parse(savedData));
    } catch {
      return FALLBACK;
    }
  });
  // ✅ If opened in edit mode, reload draft data saved by CVEditView
  useEffect(() => {
    if (!isEditMode || !editingCvId) return;

    // 1) show localStorage draft shape
    try {
      const saved = localStorage.getItem("cv_data_full");
      const localRaw = saved ? JSON.parse(saved) : null;
      setDebugInfo((p) => ({ ...p, localRaw }));
      if (localRaw) {
        const parsed = normalizeCVData(localRaw);
        setData(parsed);
        setActiveTab("input");
      }
    } catch (e) {
      setDebugInfo((p) => ({
        ...p,
        lastError: "localStorage parse failed",
      }));
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [isEditMode, editingCvId]);

  useEffect(() => {
    try {
      localStorage.setItem("cv_data_full", JSON.stringify(data));
    } catch {}
  }, [data]);

  // History
  const [history, setHistory] = useState(() => {
    const savedHistory = localStorage.getItem("cv_history_db");
    const fallback = {
      names: [],
      emails: [],
      phones: [],
      jobTitles: [],
      companies: [],
      schools: [],
      locations: [],
      degrees: [],
      majors: [],
      providers: [],
      courseNames: [],
    };
    if (!savedHistory) return fallback;
    try {
      const parsed = JSON.parse(savedHistory);
      return {
        ...fallback,
        ...parsed,
        names: Array.isArray(parsed.names) ? parsed.names : [],
        emails: Array.isArray(parsed.emails) ? parsed.emails : [],
        phones: Array.isArray(parsed.phones) ? parsed.phones : [],
        jobTitles: Array.isArray(parsed.jobTitles) ? parsed.jobTitles : [],
        companies: Array.isArray(parsed.companies) ? parsed.companies : [],
        schools: Array.isArray(parsed.schools) ? parsed.schools : [],
        locations: Array.isArray(parsed.locations) ? parsed.locations : [],
        degrees: Array.isArray(parsed.degrees) ? parsed.degrees : [],
        majors: Array.isArray(parsed.majors) ? parsed.majors : [],
        providers: Array.isArray(parsed.providers) ? parsed.providers : [],
        courseNames: Array.isArray(parsed.courseNames)
          ? parsed.courseNames
          : [],
      };
    } catch {
      return fallback;
    }
  });

  useEffect(() => {
    try {
      localStorage.setItem("cv_history_db", JSON.stringify(history));
    } catch {}
  }, [history]);

  const addToHistory = (category, value) => {
    const v = (value || "").toString().trim();
    if (!v) return;
    setHistory((prev) => {
      const arr = Array.isArray(prev[category]) ? prev[category] : [];
      if (arr.includes(v)) return prev;
      return { ...prev, [category]: [...arr, v] };
    });
  };

  // Current user once
  const [currentUser] = useState(() => {
    const saved = localStorage.getItem("user");
    try {
      return saved ? JSON.parse(saved) : null;
    } catch {
      return null;
    }
  });

  const getExperienceArray = () => getExperienceArrayFrom(data);

  // Tooltip (mobile-safe)
  const Tooltip = ({ text, children }) => {
    if (!text) return children;
    return (
      <span className="relative inline-flex group">
        {children}
        <span className="pointer-events-none absolute -top-9 left-1/2 -translate-x-1/2 opacity-0 group-hover:opacity-100 transition bg-black text-white text-[11px] px-2 py-1 rounded-md whitespace-nowrap shadow-lg">
          {text}
        </span>
      </span>
    );
  };

  const validateContact = () => {
    const email = (data.personalInfo?.email || "").trim();
    const phone = (data.personalInfo?.phone || "").trim();

    if (!email && !phone) {
      toast.error(t(lang, "needEmailOrPhone"));
      return false;
    }

    if (!email || !phone) {
      toast(t(lang, "tipEmailPhone"));
    }

    return true;
  };

  const validateDates = () => {
    const exps = getExperienceArray();
    const eduList = Array.isArray(data.education) ? data.education : [];
    const courseList = Array.isArray(data.courses) ? data.courses : [];

    for (let i = 0; i < exps.length; i++) {
      const j = exps[i] || {};
      const title = (j.title || `Job ${i + 1}`).toString().trim();
      const start = (j.start || "").toString().trim();
      const end = (j.end || "").toString().trim();
      if (!start || !end) {
        toast.error(t(lang, "expDatesMissing", i + 1, title));
        return false;
      }
    }

    for (let i = 0; i < eduList.length; i++) {
      const e = eduList[i] || {};
      const school = (e.school || `Education ${i + 1}`).toString().trim();
      const year = (e.year || "").toString().trim();
      if (!year) {
        toast.error(t(lang, "eduYearMissing", i + 1, school));
        return false;
      }
    }

    for (let i = 0; i < courseList.length; i++) {
      const c = courseList[i] || {};
      const name = (c.name || `Course ${i + 1}`).toString().trim();
      const date = (c.date || "").toString().trim();
      if (!date) {
        toast.error(t(lang, "courseDateMissing", i + 1, name));
        return false;
      }
    }

    return true;
  };

  // Score
  const resumeScore = useMemo(() => {
    let score = 0;
    const name = (data.personalInfo?.fullName || "").trim();
    const email = (data.personalInfo?.email || "").trim();
    const phone = (data.personalInfo?.phone || "").trim();
    const title = (data.targetJob?.title || "").trim();
    const hasEdu = Array.isArray(data.education) && data.education.length > 0;
    const exps = getExperienceArray();
    const hasExp = Array.isArray(exps) && exps.length > 0;

    if (name) score += 10;
    if (title) score += 10;
    if (email) score += 10;
    if (phone) score += 10;
    if (email && phone) score += 5;
    if (hasEdu) score += 10;

    if (hasExp) {
      score += 25;
      const withDesc = exps.filter((x) =>
        (x.descriptionRaw || "").trim()
      ).length;
      if (withDesc >= 1) score += 10;
      if (withDesc >= 2) score += 5;
    }

    if (Array.isArray(data.courses) && data.courses.length > 0) score += 5;
    if ((data.languages || "").trim()) score += 5;

    if (score > 100) score = 100;
    return score;
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [data]);

  const scoreLabel =
    resumeScore >= 85
      ? t(lang, "excellent")
      : resumeScore >= 70
      ? t(lang, "good")
      : t(lang, "needsWork");

  // Optimize JD
  const [optimizeForJD, setOptimizeForJD] = useState(false);
  const [jobDescription, setJobDescription] = useState("");

  const validateOptimizeJD = () => {
    if (!optimizeForJD) return true;
    const jd = (jobDescription || "").trim();
    if (!jd) {
      toast.error(t(lang, "optimizeOnNeedJD"));
      return false;
    }
    return true;
  };

  const splitDegreeMajor = (deg) => {
    const s = String(deg || "").trim();
    if (!s) return { degree: "", major: "" };

    // "Bachelor of Commerce in Accounting"
    const m = s.match(/^(.*?)(?:\s+in\s+)(.+)$/i);
    if (m) return { degree: m[1].trim(), major: m[2].trim() };

    return { degree: s, major: "" };
  };

  const splitDatesToStartEnd = (dates) => {
    const s = String(dates || "").trim();
    if (!s) return { start: "", end: "" };

    // supports: "10/2016 to 10/2022" OR "Oct 2016 - Oct 2022" OR "Oct 2016 – Oct 2022"
    const parts = s
      .split(/\s+to\s+|\s*-\s*|\s*–\s*/i)
      .map((x) => x.trim())
      .filter(Boolean);

    if (parts.length >= 2) return { start: parts[0], end: parts[1] };
    return { start: "", end: "" };
  };

  const normalizeToBuilderDraft = (raw) => {
    // already builder-draft
    if (raw?.personalInfo) {
      return normalizeCVData(raw);
    }

    // finalCV => builder draft
    const fullName = raw?.name || "";
    const contactRaw = String(raw?.contact || "");

    const parts = contactRaw
      .replace(/\[([^\]]+)\]\(([^)]+)\)/g, "$1")
      .replace(/mailto:/gi, "")
      .split("|")
      .map((x) => x.trim())
      .filter(Boolean);

    const email = parts.find((x) => /@/.test(x)) || "";
    const phone = parts.find((x) => /(\+?\d[\d\s().-]{7,})/.test(x)) || "";
    const address = parts.find((x) => x && x !== email && x !== phone) || "";

    const experiences = Array.isArray(raw?.experience)
      ? raw.experience.map((j) => {
          const dd = splitDatesToStartEnd(j?.dates || "");
          return {
            id: Date.now() + Math.random(),
            title: j?.title || "",
            company: j?.company || "",
            location: j?.location || "",
            start: dd.start || "",
            end: dd.end || "",
            descriptionRaw: Array.isArray(j?.bullets)
              ? j.bullets.join("\n")
              : "",
          };
        })
      : [];

    const education = Array.isArray(raw?.education)
      ? raw.education.map((e) => {
          const dm = splitDegreeMajor(e?.degree || "");
          return {
            id: Date.now() + Math.random(),
            school: e?.school || "",
            location: e?.location || "",
            degree: dm.degree || e?.degree || "",
            major: dm.major || e?.major || "",
            year: e?.date || e?.year || "",
          };
        })
      : [];

    const courses = Array.isArray(raw?.courses)
      ? raw.courses.map((c) => ({
          id: Date.now() + Math.random(),
          name: c?.name || "",
          provider: c?.provider || "",
          date: c?.date || "",
        }))
      : [];

    const draft = {
      personalInfo: { fullName, email, phone, address },
      targetJob: raw?.targetJob || { title: "Resume", company: "", state: "" },
      summary: raw?.summary || "",
      languages: raw?.languages || "",
      skills: Array.isArray(raw?.skills) ? raw.skills : [],
      experiences,
      education,
      courses,
    };

    return normalizeCVData(draft);
  };

  // Load CV on mount
  useEffect(() => {
    if (!currentUser?.id) return;

    // ✅ IMPORTANT: do NOT override localStorage draft in edit mode
    const __params = new URLSearchParams(window.location.search);
    const __mode = (__params.get("mode") || "").toLowerCase();
    if (__mode === "edit") return;

    const params = new URLSearchParams(window.location.search);
    const cvId = params.get("cvId");
    const action = params.get("action");
    const format = params.get("format");
    const mode = (params.get("mode") || "").toLowerCase();

    // ✅ EDIT MODE: load CV -> convert to builder draft (major + start/end) -> DO NOT overwrite with old /api/cv logic
    if (mode === "edit" && cvId) {
      fetch(`${API_BASE}/api/get-cv/${cvId}`, { headers: authHeaders })
        .then((res) => {
          if (!res.ok) throw new Error(`api/get-cv failed (${res.status})`);
          return res.json();
        })
        .then((payload) => {
          console.log("🟢 API PAYLOAD (FULL):", payload);
          let raw = payload?.cv_data ?? payload;
          if (typeof raw === "string") {
            try {
              raw = JSON.parse(raw);
            } catch {}
          }

          console.log("🟡 API RAW (cv_data):", raw);
          const draft = normalizeToBuilderDraft(raw);
          console.log("🔵 DRAFT AFTER normalizeToBuilderDraft:", draft);
          console.log("🔵 DRAFT.education:", draft.education);
          console.log("🔵 DRAFT.experiences:", draft.experiences);

          // ✅ DEBUG snapshots
          setDebugInfo((p) => ({
            ...p,
            apiPayload: payload,
            apiRaw: raw,
            apiDraft: draft,
            lastError: "",
          }));

          setData(draft);
          setActiveTab("input");
        })
        .catch((e) => {
          setDebugInfo((p) => ({
            ...p,
            lastError: e?.message || "api fetch failed",
          }));

          // fallback: keep whatever is in localStorage already
          try {
            const saved = localStorage.getItem("cv_data_full");
            if (saved) setData(normalizeCVData(JSON.parse(saved)));
          } catch {}
        });

      return;
    }

    const endpoint = cvId ? `${API_BASE}/api/cv/${cvId}` : `${API_BASE}/api/cv`;

    fetch(endpoint, { headers: authHeaders })
      .then((res) => {
        if (!res.ok) {
          return res.text().then((text) => {
            throw new Error(text || "Server error");
          });
        }
        return res.json();
      })
      .then((savedData) => {
        if (
          savedData &&
          typeof savedData === "object" &&
          savedData.message !== "no_data_found" &&
          !savedData.error
        ) {
          const normalized = normalizeCVData(savedData);
          setData(normalized);

          if (action === "download") {
            setTimeout(() => {
              if (format === "pdf") downloadPDF();
              if (format === "word") downloadWord();
              setTimeout(() => window.close(), 3000);
            }, 1500);
          }
        } else {
          setData((prev) => ({
            ...prev,
            personalInfo: {
              fullName: currentUser.username || "",
              email: currentUser.email || "",
              phone: currentUser.phone || "",
              address: currentUser.address || "",
            },
          }));
        }
      })
      .catch(() => {
        setData((prev) => ({
          ...prev,
          personalInfo: {
            fullName: currentUser.username || "",
            email: currentUser.email || "",
            phone: currentUser.phone || "",
            address: currentUser.address || "",
          },
        }));
      });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [currentUser?.id]);

  // Save AI result
  const onAISuccess = (aiResponse) => {
    if (!currentUser?.id) {
      toast.error(t(lang, "loginFirst"));
      return;
    }

    const splitDegreeMajorLocal = (deg) => {
      const s = String(deg || "").trim();
      if (!s) return { degree: "", major: "" };
      const m = s.match(/^(.*?)(?:\s+in\s+)(.+)$/i);
      if (m) return { degree: m[1].trim(), major: m[2].trim() };
      return { degree: s, major: "" };
    };

    const aiExpArr = Array.isArray(aiResponse?.experiences)
      ? aiResponse.experiences
      : Array.isArray(aiResponse?.experience)
      ? aiResponse.experience
      : [];

    const aiEduArr = Array.isArray(aiResponse?.education)
      ? aiResponse.education
      : [];
    const aiCoursesArr = Array.isArray(aiResponse?.courses)
      ? aiResponse.courses
      : [];
    const aiSkillsArr = Array.isArray(aiResponse?.skills)
      ? aiResponse.skills
      : [];

    const draftExps = Array.isArray(data?.experiences)
      ? data.experiences
      : Array.isArray(data?.experience)
      ? data.experience
      : [];

    const draftEdu = Array.isArray(data?.education) ? data.education : [];
    const draftCourses = Array.isArray(data?.courses) ? data.courses : [];

    // ✅ preserve start/end + major from draft, but take English content from AI
    const mergedExperiences = draftExps.map((d, idx) => {
      const a = aiExpArr[idx] || {};
      const bullets = Array.isArray(a?.bullets) ? a.bullets : [];
      return {
        ...d,
        title: a.title || d.title || "",
        company: a.company || d.company || "",
        location: a.location || d.location || "",
        // keep start/end from draft (IMPORTANT)
        start: d.start || "",
        end: d.end || "",
        // store AI bullets into descriptionRaw so ProfileView can build bullets later
        descriptionRaw: bullets.length
          ? bullets.join("\n")
          : d.descriptionRaw || "",
      };
    });

    const mergedEducation = draftEdu.map((d, idx) => {
      const a = aiEduArr[idx] || {};
      const dm = splitDegreeMajorLocal(a.degree || "");
      return {
        ...d,
        school: a.school || d.school || "",
        location: a.location || d.location || "",
        year: a.date || a.year || d.year || "",
        // keep major from draft, if empty try parse from AI degree
        major: (d.major || "").trim() ? d.major : dm.major || "",
        // prefer AI degree, fallback to draft degree
        degree: (a.degree || "").trim()
          ? a.degree
          : d.degree || dm.degree || "",
      };
    });

    const mergedCourses =
      draftCourses.length > 0
        ? draftCourses.map((d, idx) => {
            const a = aiCoursesArr[idx] || {};
            return {
              ...d,
              name: a.name || d.name || "",
              provider: a.provider || d.provider || "",
              date: a.date || d.date || "",
            };
          })
        : aiCoursesArr.map((c, idx) => ({
            id: Date.now() + Math.random() + idx,
            name: c?.name || "",
            provider: c?.provider || "",
            date: c?.date || "",
          }));

    const cleanedData = {
      // ✅ keep builder draft shape in DB (so major + start/end survive)
      personalInfo: {
        fullName:
          aiResponse.name ||
          aiResponse.personalInfo?.fullName ||
          aiResponse.personal?.name ||
          data.personalInfo?.fullName ||
          currentUser.username ||
          "",
        email: data.personalInfo?.email || currentUser.email || "",
        phone: data.personalInfo?.phone || currentUser.phone || "",
        address: data.personalInfo?.address || currentUser.address || "",
      },
      summary: aiResponse.summary || data.summary || "",
      experiences: mergedExperiences,
      education: mergedEducation,
      courses: mergedCourses,
      skills: aiSkillsArr,
      // ✅ languages ONLY user input
      languages: (data.languages || "").toString().trim(),
      // ✅ keep targetJob draft (it has company/state)
      targetJob: data.targetJob || { title: "Resume", company: "", state: "" },
    };

    setData((prev) => ({
      ...prev,
      ...cleanedData,
      experiences: Array.isArray(cleanedData.experiences)
        ? cleanedData.experiences
        : [],
      education: Array.isArray(cleanedData.education)
        ? cleanedData.education
        : [],
      courses: Array.isArray(cleanedData.courses) ? cleanedData.courses : [],
      skills: Array.isArray(cleanedData.skills) ? cleanedData.skills : [],
    }));

    const autoName =
      (cleanedData.targetJob?.title || data.targetJob?.title || "New Resume")
        .toString()
        .trim() || "New Resume";

    const saveUrl =
      isEditMode && editingCvId
        ? `${API_BASE}/api/update-cv/${encodeURIComponent(editingCvId)}`
        : `${API_BASE}/api/cv`;

    const saveMethod = isEditMode && editingCvId ? "PUT" : "POST";

    const saveBody =
      isEditMode && editingCvId
        ? { cv_data: cleanedData, cv_name: autoName }
        : {
            user_id: Number(currentUser.id),
            cv_data: cleanedData,
            cv_name: autoName,
          };

    fetch(saveUrl, {
      method: saveMethod,
      headers: authHeaders,
      body: JSON.stringify(saveBody),
    })
      .then((res) => res.json())
      .then(() => {
        toast.success(t(lang, "cvAdded"));
      })
      .catch(() => {
        toast.error(t(lang, "serverError"));
      });
  };

  // Update handlers
  const updateData = (section, field, value) => {
    setData((prev) => {
      const target = prev[section];
      if (typeof target === "string") return { ...prev, [section]: value };
      return { ...prev, [section]: { ...(target || {}), [field]: value } };
    });
  };

  const updateTarget = (field, value) =>
    setData((prev) => ({
      ...prev,
      targetJob: { ...(prev.targetJob || {}), [field]: value },
    }));

  const addItem = (section, template) =>
    setData((prev) => {
      const actual = resolveSection(prev, section);
      const baseArr = Array.isArray(prev[actual]) ? prev[actual] : [];
      return {
        ...prev,
        [actual]: [...baseArr, { id: Date.now(), ...template }],
      };
    });

  const removeItem = (section, id) => {
    setData((prev) => {
      const actual = resolveSection(prev, section);
      if (!Array.isArray(prev[actual])) return prev;
      return { ...prev, [actual]: prev[actual].filter((i) => i.id !== id) };
    });
  };

  const updateItem = (section, id, field, value) => {
    setData((prev) => {
      const actual = resolveSection(prev, section);
      if (!Array.isArray(prev[actual])) return prev;
      return {
        ...prev,
        [actual]: prev[actual].map((i) =>
          i.id === id ? { ...i, [field]: value } : i
        ),
      };
    });
  };

  const clearData = async () => {
    const ok = await toastConfirm(t(lang, "confirmDeleteAll"), {
      confirmText:
        normLang(lang) === "ar"
          ? "حذف"
          : normLang(lang) === "es"
          ? "Eliminar"
          : "Delete",
      cancelText:
        normLang(lang) === "ar"
          ? "إلغاء"
          : normLang(lang) === "es"
          ? "Cancelar"
          : "Cancel",
      tone: "danger",
    });
    if (!ok) return;
    try {
      localStorage.removeItem("cv_data_full");
    } catch {}
    window.location.reload();
  };

  const addHomeCountryExp = () => {
    const targetRole = data.targetJob?.title || "Specialist";
    const mockExp = {
      id: Date.now(),
      title: targetRole,
      company: "AI_AUTO_DETECT_TOP_COMPANY_IN_EGYPT",
      location: "Cairo, Egypt",
      start: "Jan 2015",
      end: "Jan 2019",
      descriptionRaw: `AI_GENERATE_DUTIES_FOR_${targetRole.replace(
        /\s+/g,
        "_"
      )}`,
    };

    setData((prev) => {
      const actual = resolveSection(prev, "experiences");
      const baseArr = Array.isArray(prev[actual]) ? prev[actual] : [];
      return { ...prev, [actual]: [...baseArr, mockExp] };
    });
  };

  // Datalists (unchanged)
  const HistoryDatalists = () => (
    <>
      <datalist id="list-names">
        {history.names?.map((item, i) => (
          <option key={i} value={item} />
        ))}
      </datalist>
      <datalist id="list-emails">
        {history.emails?.map((item, i) => (
          <option key={i} value={item} />
        ))}
      </datalist>
      <datalist id="list-phones">
        {history.phones?.map((item, i) => (
          <option key={i} value={item} />
        ))}
      </datalist>
      <datalist id="list-jobs">
        {history.jobTitles?.map((item, i) => (
          <option key={i} value={item} />
        ))}
      </datalist>
      <datalist id="list-companies">
        {history.companies?.map((item, i) => (
          <option key={i} value={item} />
        ))}
      </datalist>
      <datalist id="list-schools">
        {history.schools?.map((item, i) => (
          <option key={i} value={item} />
        ))}
      </datalist>
      <datalist id="list-locations">
        {history.locations?.map((item, i) => (
          <option key={i} value={item} />
        ))}
      </datalist>
      <datalist id="list-degrees">
        {history.degrees?.map((item, i) => (
          <option key={i} value={item} />
        ))}
      </datalist>
      <datalist id="list-majors">
        {history.majors?.map((item, i) => (
          <option key={i} value={item} />
        ))}
      </datalist>
      <datalist id="list-providers">
        {history.providers?.map((item, i) => (
          <option key={i} value={item} />
        ))}
      </datalist>
      <datalist id="list-courseNames">
        {history.courseNames?.map((item, i) => (
          <option key={i} value={item} />
        ))}
      </datalist>
    </>
  );

  // Prompt flow
  const [finalCV, setFinalCV] = useState(null);
  const [pastedJson, setPastedJson] = useState("");
  const [showModal, setShowModal] = useState(false);
  const [generatedPrompt, setGeneratedPrompt] = useState("");
  const [copyStatus, setCopyStatus] = useState(t(lang, "copyManual"));

  useEffect(() => {
    setCopyStatus(t(lang, "copyManual"));
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [lang]);

  const preparePrompt = async () => {
    if (!validateContact()) return;
    if (!validateDates()) return;
    if (!validateOptimizeJD()) return;

    const exps = getExperienceArray();
    const eduList = Array.isArray(data.education) ? data.education : [];
    const courseList = Array.isArray(data.courses) ? data.courses : [];

    let workContext = "";
    exps.forEach((job, idx) => {
      workContext += `JOB_${idx}: Title: "${job.title || ""}", Company: "${
        job.company || ""
      }", Location: "${job.location || ""}", Dates: "${job.start || ""} to ${
        job.end || ""
      }". Draft Duties: "${job.descriptionRaw || ""}"\n`;
    });

    let eduContext = "";
    eduList.forEach((edu) => {
      eduContext += `${edu.degree || ""} in ${edu.major || ""}, ${
        edu.school || ""
      }, ${edu.location || ""}, ${edu.year || ""}\n`;
    });

    let coursesContext = "";
    courseList.forEach((c) => {
      coursesContext += `${c.name || ""} at ${c.provider || ""} (${
        c.date || ""
      })\n`;
    });

    const cleanName = (data.personalInfo?.fullName || "")
      .replace(/[\r\n]+/g, " ")
      .trim();
    const cleanAddress = (data.personalInfo?.address || "")
      .replace(/[\r\n]+/g, " ")
      .trim();

    const jdBlock = optimizeForJD
      ? `
--------------------------------------------------
JOB DESCRIPTION (JD):
"""
${(jobDescription || "").trim()}
"""
--------------------------------------------------
`
      : "";

    const jdInstructions = optimizeForJD
      ? `
### 🎯 ATS OPTIMIZATION MODE (JD IS PROVIDED)
- Tailor SUMMARY, SKILLS, and EXPERIENCE BULLETS to match the JD (keywords, tools, responsibilities).
- Keep it truthful. Do NOT invent degrees, employers, or certifications.
- Prefer measurable impact when possible (%, $, time, volume) but do NOT invent numbers; only quantify if implied.
`
      : "";

    const prompt = `
ACT AS A PROFESSIONAL RESUME WRITER. I will provide you with data. You must process it and return a SINGLE JSON OBJECT.
--------------------------------------------------
INPUT DATA:
Target Job: ${data.targetJob?.title || ""} at ${
      data.targetJob?.company || ""
    }, ${data.targetJob?.state || ""}
Name: ${cleanName}
Contact: ${cleanAddress} | ${data.personalInfo?.phone || ""} | ${
      data.personalInfo?.email || ""
    }
Languages: ${data.languages || ""}
EDUCATION:
${eduContext}
COURSES:
${coursesContext}
WORK EXPERIENCE:
${workContext}
${jdBlock}
--------------------------------------------------
INSTRUCTIONS:
# LANGUAGE MODE: STRICT ENGLISH ONLY
### 🛑 CRITICAL INSTRUCTIONS (ZERO TOLERANCE FOR ARABIC)
1. TRANSLATE EVERYTHING: If the user input contains Arabic, translate to professional US English.
2. NO ARABIC OUTPUT: The final JSON must contain ONLY English text.
3. LOGIC FIX: If Date is "2065", change to "2015". If "Ain Shams Academy", change to "Ain Shams University".
4. DATES RULE: NEVER leave dates empty. Use the provided dates exactly. If something is missing, infer "Present" ONLY for current job end-date; do not remove dates.
${jdInstructions}
5. CONTACT FORMAT: Output contact as plain text only (NO markdown links, NO mailto:).
6. LANGUAGES RULE: If Languages input is empty, output "languages": "" exactly. Do NOT invent languages.

--------------------------------------------------
### ✍️ GENERATION TASKS:
1. SUMMARY: Write a 2-3 sentence professional summary in English.
2. EXPERIENCE: Translate titles/companies. Write 3-5 high-impact bullet points for each job in English.
3. SKILLS: Extract 15-18 ATS keywords in English.
--------------------------------------------------
REQUIRED JSON OUTPUT FORMAT:
{
  "name": "...",
  "contact": "...",
  "summary": "...",
  "languages": "...",
  "education": [ { "school": "...", "location": "...", "date": "...", "degree": "..." } ],
  "courses": [ { "name": "...", "provider": "...", "date": "..." } ],
  "experience": [ { "company": "...", "location": "...", "dates": "...", "title": "...", "bullets": ["...", "..."] } ],
  "skills": ["...", "..."]
}
`.trim();

    setGeneratedPrompt(prompt);

    const ok = await safeWriteClipboard(prompt);
    setCopyStatus(ok ? t(lang, "copiedAuto") : t(lang, "copyAutoFail"));
    setShowModal(true);
  };

  const copyToClipboard = async () => {
    const ok = await safeWriteClipboard(generatedPrompt);
    setCopyStatus(ok ? t(lang, "copiedManual") : t(lang, "copyManualFail"));
  };

  const handleJsonImport = () => {
    try {
      const jsonMatch = pastedJson.match(/\{[\s\S]*\}/);
      if (!jsonMatch) throw new Error("No JSON found");
      const parsed = JSON.parse(jsonMatch[0]);
      // ✅ sanitize AI output (no markdown links / no invented languages)
      const stripMarkdownLinks = (s) =>
        String(s || "").replace(/\[([^\]]+)\]\(([^)]+)\)/g, "$1");

      // contact: remove markdown mailto and any raw mailto:
      if (parsed && typeof parsed === "object") {
        if (parsed.contact) {
          parsed.contact = stripMarkdownLinks(parsed.contact).replace(
            /mailto:/gi,
            ""
          );
        }

        // ✅ Languages: ONLY from user input (never from AI)
        parsed.languages = (data.languages || "").toString().trim();
      }

      if (Array.isArray(parsed.education) && parsed.education.length === 0)
        delete parsed.education;
      if (Array.isArray(parsed.courses) && parsed.courses.length === 0)
        delete parsed.courses;
      if (Array.isArray(parsed.experience) && parsed.experience.length === 0)
        delete parsed.experience;

      setFinalCV(parsed);
      setActiveTab("preview");
      onAISuccess(parsed);
    } catch {
      toast.error(t(lang, "invalidJson"));
    }
  };

  // PDF
  const downloadPDF = () => {
    if (!validateContact()) return;
    const element = document.getElementById("cv-document");
    if (!element) return;

    const tempContainer = document.createElement("div");
    tempContainer.style.position = "absolute";
    tempContainer.style.left = "-9999px";
    tempContainer.style.top = "0";
    tempContainer.style.background = "white";

    const clone = element.cloneNode(true);
    clone.style.width = "816px";
    clone.style.background = "white";

    tempContainer.appendChild(clone);
    document.body.appendChild(tempContainer);

    const safeName =
      (data.personalInfo?.fullName || "Resume").replace(/\s+/g, "_") ||
      "Resume";

    const opt = {
      margin: 0,
      filename: `${safeName}_Resume.pdf`,
      image: { type: "jpeg", quality: 0.98 },
      html2canvas: {
        scale: 2,
        useCORS: true,
        logging: false,
        scrollY: 0,
        y: 0,
        backgroundColor: "#ffffff",
      },
      jsPDF: { unit: "in", format: "letter", orientation: "portrait" },
    };

    html2pdf()
      .set(opt)
      .from(clone)
      .save()
      .then(() => {
        document.body.removeChild(tempContainer);
      })
      .catch(() => {
        try {
          document.body.removeChild(tempContainer);
        } catch {}
      });
  };

  // Word
  const downloadWord = () => {
    if (!validateContact()) return;
    if (!finalCV) return toast.error(t(lang, "noDataWord"));

    const {
      Document,
      Paragraph,
      TextRun,
      Tab,
      TabStopType,
      ShadingType,
      AlignmentType,
    } = docx;

    const font = "Times New Roman";
    const RIGHT_TAB_POS = 10800;
    const MID_TAB_POS = 5200;

    const createHeader = (text) =>
      new Paragraph({
        children: [
          new TextRun({
            text: (text || "").toUpperCase(),
            bold: true,
            font,
            size: 24,
          }),
        ],
        spacing: { before: 300, after: 240, line: 420 },
        border: {
          bottom: { color: "auto", space: 1, value: "single", size: 6 },
        },
        shading: { type: ShadingType.CLEAR, fill: "F1F5F9" },
      });

    const sections = [];

    sections.push(
      new Paragraph({
        children: [
          new TextRun({
            text: finalCV.name || finalCV.personal?.name || "RESUME",
            bold: true,
            size: 56,
            font,
          }),
        ],
        alignment: AlignmentType.CENTER,
        spacing: { after: 100 },
      }),
      new Paragraph({
        children: [
          new TextRun({
            text:
              finalCV.contact ||
              `${finalCV.personal?.address || ""} | ${
                finalCV.personal?.phone || ""
              }`,
            size: 35,
            font,
          }),
        ],
        alignment: AlignmentType.CENTER,
        shading: { type: ShadingType.CLEAR, fill: "E2E8F0" },
        spacing: { after: 300 },
      })
    );

    if (finalCV.summary) {
      sections.push(createHeader("Professional Summary"));
      sections.push(
        new Paragraph({
          children: [new TextRun({ text: finalCV.summary, font, size: 24 })],
          alignment: AlignmentType.JUSTIFIED,
        })
      );
    }

    if (finalCV.education?.length) {
      sections.push(createHeader("Education"));
      finalCV.education.forEach((edu) => {
        sections.push(
          new Paragraph({
            children: [
              new TextRun({
                text: `${edu.school || ""}, ${edu.location || ""}`,
                bold: true,
                font,
                size: 24,
              }),
              new TextRun({
                children: [new Tab(), edu.date || ""],
                bold: true,
                font,
                size: 24,
              }),
            ],
            tabStops: [{ type: TabStopType.RIGHT, position: RIGHT_TAB_POS }],
          })
        );
        sections.push(
          new Paragraph({
            children: [new TextRun({ text: edu.degree || "", font, size: 24 })],
            spacing: { after: 120 },
          })
        );
      });
    }

    if (finalCV.courses?.length) {
      sections.push(createHeader("Relevant Courses"));
      finalCV.courses.forEach((course) => {
        sections.push(
          new Paragraph({
            children: [
              new TextRun({
                text: `${course.name || ""} – ${course.provider || ""}`,
                bold: true,
                font,
                size: 24,
              }),
              new TextRun({
                children: [new Tab(), course.date || ""],
                bold: true,
                font,
                size: 24,
              }),
            ],
            tabStops: [{ type: TabStopType.RIGHT, position: RIGHT_TAB_POS }],
            spacing: { after: 60 },
          })
        );
      });
    }

    if (finalCV.experience?.length) {
      sections.push(createHeader("Work Experience"));
      finalCV.experience.forEach((job) => {
        sections.push(
          new Paragraph({
            children: [
              new TextRun({
                text: `${job.company || ""}, ${job.location || ""}`,
                bold: true,
                font,
                size: 24,
              }),
              new TextRun({
                children: [new Tab(), job.dates || ""],
                bold: true,
                font,
                size: 24,
              }),
            ],
            tabStops: [{ type: TabStopType.RIGHT, position: RIGHT_TAB_POS }],
          })
        );

        sections.push(
          new Paragraph({
            children: [
              new TextRun({
                text: job.title || "",
                italics: true,
                font,
                size: 24,
              }),
            ],
            spacing: { after: 60 },
          })
        );

        (job.bullets || []).forEach((b) =>
          sections.push(
            new Paragraph({
              children: [
                new TextRun({
                  text: `• ${String(b || "")
                    .replace(/^-|\*/, "")
                    .trim()}`,
                  font,
                  size: 24,
                }),
              ],
              spacing: { after: 40 },
            })
          )
        );

        sections.push(new Paragraph({ text: "", spacing: { after: 120 } }));
      });
    }

    if (finalCV.skills?.length) {
      sections.push(createHeader("Skills"));

      if (finalCV.languages) {
        sections.push(
          new Paragraph({
            children: [
              new TextRun({ text: "Languages: ", bold: true, font, size: 24 }),
              new TextRun({ text: finalCV.languages, font, size: 24 }),
            ],
            spacing: { after: 80 },
          })
        );
      }

      const skills = finalCV.skills;

      for (let i = 0; i < skills.length; i += 2) {
        const left = skills[i] ? `• ${skills[i]}` : "";
        const right = skills[i + 1] ? `• ${skills[i + 1]}` : "";

        sections.push(
          new Paragraph({
            children: [
              new TextRun({ text: left, font, size: 24 }),
              new TextRun({
                children: [new docx.Tab(), right],
                font,
                size: 24,
              }),
            ],
            tabStops: [{ type: docx.TabStopType.LEFT, position: MID_TAB_POS }],
            spacing: { after: 40 },
          })
        );
      }
    }

    const doc = new Document({
      sections: [
        {
          properties: {
            page: { margin: { top: 720, right: 720, bottom: 720, left: 720 } },
          },
          children: sections,
        },
      ],
    });

    docx.Packer.toBlob(doc).then((blob) => {
      saveAs(blob, `${(finalCV.name || "Resume").replace(/\s+/g, "_")}.docx`);
    });
  };

  const dir = dirForLang(lang);

  // UI (mobile-first + 3 langs dir)
  return (
    <div className="max-w-6xl mx-auto p-3 sm:p-4 font-sans" dir={dir}>
      {/* ✅ DEBUG PANEL */}
      <div className="mb-3">
        <button
          type="button"
          onClick={() => setDebugOpen((s) => !s)}
          className="px-4 py-2 rounded-xl bg-slate-900 text-white font-bold"
        >
          {debugOpen ? "Hide Debug" : "Show Debug"}
        </button>

        {debugOpen && (
          <pre className="mt-3 p-3 rounded-xl bg-slate-50 border border-slate-200 text-xs overflow-auto max-h-[420px]">
            {JSON.stringify(
              {
                isEditMode,
                editingCvId,
                activeTab,
                debugInfo,
                stateData: data,
              },
              null,
              2
            )}
          </pre>
        )}
      </div>

      <HistoryDatalists />
      {activeTab === "input" && (
        <StepInput
          lang={lang}
          data={data}
          history={history}
          resumeScore={resumeScore}
          scoreLabel={scoreLabel}
          Tooltip={Tooltip}
          addToHistory={addToHistory}
          updateData={updateData}
          updateTarget={updateTarget}
          addItem={addItem}
          removeItem={removeItem}
          updateItem={updateItem}
          clearData={clearData}
          getExperienceArray={getExperienceArray}
          addHomeCountryExp={addHomeCountryExp}
          optimizeForJD={optimizeForJD}
          setOptimizeForJD={setOptimizeForJD}
          jobDescription={jobDescription}
          setJobDescription={setJobDescription}
          onPreparePrompt={preparePrompt}
          activeTab={activeTab}
          setActiveTab={setActiveTab}
        />
      )}

      <PromptModal
        show={showModal}
        copyStatus={copyStatus}
        generatedPrompt={generatedPrompt}
        onClose={() => setShowModal(false)}
        onCopyManual={copyToClipboard}
        onOpenChatGPT={() => {
          window.open("https://chatgpt.com", "_blank");
          setActiveTab("process");
          setShowModal(false);
        }}
        lang={lang}
      />

      {activeTab === "process" && (
        <StepProcess
          lang={lang}
          pastedJson={pastedJson}
          setPastedJson={setPastedJson}
          onImportJson={handleJsonImport}
          onBackToInput={() => setActiveTab("input")}
        />
      )}

      {activeTab === "preview" && finalCV && (
        <StepPreview
          lang={lang}
          finalCV={finalCV}
          onBack={() => setActiveTab("process")}
          onDownloadWord={downloadWord}
          onDownloadPDF={downloadPDF}
        />
      )}

      {activeTab === "preview" && !finalCV && (
        <div className="bg-white p-5 sm:p-8 rounded-2xl shadow-lg text-center max-w-2xl mx-auto">
          <div className="text-slate-700 font-black text-xl sm:text-2xl mb-2">
            {t(lang, "noCVYet")}
          </div>
          <div className="text-slate-500 mb-6 text-sm sm:text-base">
            {t(lang, "goStep2Desc")}
          </div>
          <button
            onClick={() => setActiveTab("process")}
            className="w-full sm:w-auto px-6 py-3 bg-slate-900 text-white rounded-xl font-bold flex items-center justify-center gap-2 mx-auto active:scale-[0.99]"
          >
            <Copy size={18} /> {t(lang, "goStep2")}
          </button>
        </div>
      )}
    </div>
  );
};

export default CVBuilderView;
