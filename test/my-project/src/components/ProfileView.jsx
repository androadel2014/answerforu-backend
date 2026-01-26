// src/components/ProfileView.jsx (FULL FILE - copy/paste)
// ✅ 3 languages (AR/EN/ES) + RTL/LTR
// ✅ Keeps your CV preview/export EXACT (same builder markup + same PDF/Word logic)
// ✅ No browser confirm dialogs (uses your modals)
// ✅ Mobile-safe modals + buttons wrap + max widths

import React, { useEffect, useMemo, useRef, useState } from "react";
import {
  User,
  Edit2,
  Trash2,
  Download,
  FileText,
  Plus,
  Briefcase,
  MapPin,
  Info,
  Eye,
  X,
} from "lucide-react";
import toast from "react-hot-toast";
import { useNavigate } from "react-router-dom";

import html2pdf from "html2pdf.js";
import * as docx from "docx";
import { saveAs } from "file-saver";

/* =========================
   i18n
========================= */
const STR = {
  ar: {
    loading: "جارٍ التحميل...",
    saveFailed: "فشل الحفظ",
    updated: "تم تحديث البيانات",
    connErr: "خطأ في الاتصال",
    deleteFailed: "فشل الحذف",
    deleted: "تم الحذف بنجاح",
    previewFailed: "فشل فتح المعاينة",
    downloadFailed: "فشل التحميل",
    downloaded: "تم التحميل ✅",
    scoreTitle: "تفاصيل التقييم",
    scoreSub: "دي الحاجات اللي تزود بيها التقييم",
    close: "إغلاق",
    score: "التقييم",
    doFollowing: "اعمل التالي:",
    stats: "إحصائيات:",
    exp: "الخبرة",
    edu: "التعليم",
    skills: "المهارات",
    bullets: "نقاط الخبرة",
    excellentNoIssues: "ممتاز ✅ مش محتاج تعديلات كبيرة",
    previewTitle: "معاينة السيرة الذاتية",
    escHint: "اضغط Esc للخروج",
    confirmDeleteTitle: "متأكد من الحذف؟",
    confirmDeleteDesc: "سيتم مسح هذا السي في نهائياً.",
    cancel: "إلغاء",
    del: "حذف",
    profile: "تعديل البروفايل",
    saveChanges: "حفظ التغييرات",
    editProfile: "تعديل البروفايل",
    fullName: "الاسم الكامل",
    phone: "رقم الهاتف",
    address: "العنوان",
    bio: "نبذة شخصية",
    cvMgmt: "إدارة السير الذاتية",
    createNew: "إنشاء جديد",
    noResumes: "لا تمتلك أي سيرة ذاتية بعد",
    startNow: "ابدأ الآن",
    tooltipEdit: "تعديل بيانات البروفايل",
    tooltipSave: "حفظ بيانات البروفايل",
    tooltipCreate: "إنشاء سيرة ذاتية جديدة",
    atsSafe: "ATS Safe: مناسب للسيستم اللي بيقرأ السي في",
    onePage: "1 Page: سي في مختصر صفحة واحدة",
    usFormat: "US Format: تنسيق مناسب لأمريكا",
    previewTip: "Preview: معاينة بنفس شكل التحميل",
    editTip: "Edit: تعديل السي في",
    pdfTip: "تحميل PDF",
    wordTip: "تحميل Word",
    deleteTip: "حذف",
    calc: "جارٍ الحساب...",
  },
  en: {
    loading: "Loading...",
    saveFailed: "Save failed",
    updated: "Profile Updated",
    connErr: "Connection error",
    deleteFailed: "Delete failed",
    deleted: "Deleted successfully",
    previewFailed: "Preview failed",
    downloadFailed: "Download failed",
    downloaded: "Downloaded ✅",
    scoreTitle: "Score Details",
    scoreSub: "Exact improvements to raise the score",
    close: "Close",
    score: "Score",
    doFollowing: "Do the following:",
    stats: "Stats:",
    exp: "Exp",
    edu: "Edu",
    skills: "Skills",
    bullets: "Bullets",
    excellentNoIssues: "Excellent ✅ no major issues",
    previewTitle: "Preview CV",
    escHint: "Press Esc to close",
    confirmDeleteTitle: "Confirm Delete",
    confirmDeleteDesc: "This will be permanently removed.",
    cancel: "Cancel",
    del: "Delete",
    profile: "Edit Profile",
    saveChanges: "Save Changes",
    editProfile: "Edit Profile",
    fullName: "Full Name",
    phone: "Phone Number",
    address: "Address",
    bio: "Bio",
    cvMgmt: "Resume Management",
    createNew: "Create New",
    noResumes: "No resumes yet",
    startNow: "Start Now",
    tooltipEdit: "Edit profile data",
    tooltipSave: "Save profile changes",
    tooltipCreate: "Create a new CV",
    atsSafe: "ATS Safe: readable by ATS systems",
    onePage: "1 Page: compact one-page resume",
    usFormat: "US Format: U.S. resume style",
    previewTip: "Preview: same export design",
    editTip: "Edit CV",
    pdfTip: "Download PDF",
    wordTip: "Download Word",
    deleteTip: "Delete",
    calc: "Calculating...",
  },
  es: {
    loading: "Cargando...",
    saveFailed: "Falló guardar",
    updated: "Perfil actualizado",
    connErr: "Error de conexión",
    deleteFailed: "Falló eliminar",
    deleted: "Eliminado correctamente",
    previewFailed: "Falló la vista previa",
    downloadFailed: "Falló la descarga",
    downloaded: "Descargado ✅",
    scoreTitle: "Detalles de puntuación",
    scoreSub: "Mejoras exactas para subir la puntuación",
    close: "Cerrar",
    score: "Puntuación",
    doFollowing: "Haz lo siguiente:",
    stats: "Estadísticas:",
    exp: "Exp",
    edu: "Edu",
    skills: "Habilidades",
    bullets: "Puntos",
    excellentNoIssues: "Excelente ✅ sin problemas mayores",
    previewTitle: "Vista previa CV",
    escHint: "Pulsa Esc para cerrar",
    confirmDeleteTitle: "Confirmar eliminación",
    confirmDeleteDesc: "Se eliminará permanentemente.",
    cancel: "Cancelar",
    del: "Eliminar",
    profile: "Editar perfil",
    saveChanges: "Guardar cambios",
    editProfile: "Editar perfil",
    fullName: "Nombre completo",
    phone: "Teléfono",
    address: "Dirección",
    bio: "Bio",
    cvMgmt: "Gestión de CV",
    createNew: "Crear nuevo",
    noResumes: "Aún no hay CV",
    startNow: "Empezar",
    tooltipEdit: "Editar datos del perfil",
    tooltipSave: "Guardar cambios del perfil",
    tooltipCreate: "Crear un CV nuevo",
    atsSafe: "ATS Safe: legible por ATS",
    onePage: "1 Página: CV compacto",
    usFormat: "Formato USA",
    previewTip: "Vista previa: mismo diseño",
    editTip: "Editar CV",
    pdfTip: "Descargar PDF",
    wordTip: "Descargar Word",
    deleteTip: "Eliminar",
    calc: "Calculando...",
  },
};

const t = (lang, key) => (STR[lang] || STR.en)[key] || STR.en[key] || key;
const getDir = (lang) => (lang === "ar" ? "rtl" : "ltr");

export const ProfileView = ({ lang = "en" }) => {
  const navigate = useNavigate();
  const dir = getDir(lang);

  // ✅ API base from .env (portable)
  const API_BASE =
    import.meta.env.VITE_API_BASE_URL ||
    import.meta.env.VITE_API_BASE ||
    "http://localhost:5000";

  // =========================
  // ✅ Auth helpers (Bearer)
  // =========================
  const getToken = () =>
    localStorage.getItem("token") ||
    localStorage.getItem("authToken") ||
    localStorage.getItem("jwt") ||
    "";

  const authHeaders = () => {
    const token = getToken();
    return {
      "Content-Type": "application/json",
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
    };
  };

  const hardLogout = () => {
    localStorage.removeItem("token");
    localStorage.removeItem("authToken");
    localStorage.removeItem("jwt");
    localStorage.removeItem("user");
    window.dispatchEvent(new Event("auth_changed"));
    navigate("/auth", { replace: true });
  };

  // =========================
  // ✅ State
  // =========================
  const [loading, setLoading] = useState(true);

  const [user, setUser] = useState(null);
  const [cvList, setCvList] = useState([]);
  const [isEditing, setIsEditing] = useState(false);
  const [deleteModal, setDeleteModal] = useState({ isOpen: false, cvId: null });

  const [previewModal, setPreviewModal] = useState({
    isOpen: false,
    cvId: null,
    finalCV: null,
    title: "",
  });

  const previewContainerRef = useRef(null);

  const [downloading, setDownloading] = useState({
    cvId: null,
    format: null, // "pdf" | "word"
  });

  // ✅ CV Scores map
  const [cvScores, setCvScores] = useState({});

  // ✅ Score modal
  const [scoreModal, setScoreModal] = useState({
    isOpen: false,
    cvId: null,
    title: "",
    scoreObj: null,
  });

  const [editData, setEditData] = useState({
    username: "",
    phone: "",
    address: "",
    bio: "",
  });

  // =========================
  // ✅ Tooltip
  // =========================
  const Tooltip = ({ text, children }) => (
    <span className="relative inline-flex items-center group/tt">
      {children}
      <span className="pointer-events-none absolute -top-11 left-1/2 -translate-x-1/2 opacity-0 group-hover/tt:opacity-100 transition-opacity duration-150 z-[9999]">
        <span className="text-[11px] font-black px-3 py-2 rounded-xl bg-slate-900 text-white shadow-xl whitespace-nowrap">
          {text}
        </span>
      </span>
    </span>
  );

  // =========================
  // ✅ Date helpers (SQLite UTC)
  // =========================
  const parseSqliteUTC = (sqliteDateString) => {
    if (!sqliteDateString) return null;
    const iso = String(sqliteDateString).replace(" ", "T") + "Z";
    const d = new Date(iso);
    return Number.isNaN(d.getTime()) ? null : d;
  };

  const formatDateTime = (sqliteDateString) => {
    const d = parseSqliteUTC(sqliteDateString);
    if (!d) return sqliteDateString || "";
    const datePart = d.toLocaleDateString(lang === "ar" ? "ar-EG" : "en-US", {
      year: "numeric",
      month: "long",
      day: "numeric",
    });
    const timePart = d.toLocaleTimeString(lang === "ar" ? "ar-EG" : "en-US", {
      hour: "2-digit",
      minute: "2-digit",
    });
    return `${datePart} • ${timePart}`;
  };

  // =========================
  // ✅ Normalize CV data from DB (handles cv_data string)
  // =========================
  const normalizeCvData = (dbPayload) => {
    let raw = dbPayload?.cv_data ?? dbPayload;

    if (typeof raw === "string") {
      try {
        raw = JSON.parse(raw);
      } catch {}
    }

    const personalInfo =
      raw?.personalInfo ||
      (raw?.personal
        ? {
            fullName: raw.personal.name || "",
            phone: raw.personal.phone || "",
            email: raw.personal.email || "",
            address: raw.personal.address || "",
          }
        : { fullName: "", phone: "", email: "", address: "" });

    const education = Array.isArray(raw?.education) ? raw.education : [];
    const courses = Array.isArray(raw?.courses) ? raw.courses : [];
    const experiences = Array.isArray(raw?.experiences)
      ? raw.experiences
      : Array.isArray(raw?.experience)
      ? raw.experience
      : [];

    return {
      name: raw?.name || personalInfo.fullName || "RESUME",
      contact:
        raw?.contact ||
        `${personalInfo.address || ""} | ${personalInfo.phone || ""} | ${
          personalInfo.email || ""
        }`,
      summary: raw?.summary || "",
      languages: raw?.languages || "",
      education: education.map((e) => ({
        school: e.school || "",
        location: e.location || "",
        date: e.date || e.year || "",
        degree:
          e.degree && e.major ? `${e.degree} in ${e.major}` : e.degree || "",
      })),
      courses: courses.map((c) => ({
        name: c.name || "",
        provider: c.provider || "",
        date: c.date || "",
      })),
      experience: experiences.map((ex) => ({
        company: ex.company || "",
        location: ex.location || "",
        dates:
          ex.dates || `${ex.start || ""}${ex.end ? " - " + ex.end : ""}`.trim(),
        title: ex.title || "",
        bullets: Array.isArray(ex.bullets)
          ? ex.bullets
          : Array.isArray(ex.description)
          ? ex.description
          : ex.descriptionRaw
          ? String(ex.descriptionRaw)
              .split("\n")
              .map((x) => x.trim())
              .filter(Boolean)
          : [],
      })),
      skills: Array.isArray(raw?.skills) ? raw.skills : [],
    };
  };

  // =========================
  // ✅ BUILDER-EXACT HELPERS
  // =========================
  const sanitizeContact = (raw) => {
    if (!raw) return "";
    let s = String(raw);
    s = s.replace(/\[([^\]]+)\]\(mailto:[^)]+\)/gi, "$1");
    s = s.replace(/\(mailto:[^)]+\)/gi, "");
    s = s.replace(/mailto:/gi, "");
    s = s
      .split("|")
      .map((x) => x.trim())
      .filter(Boolean)
      .join(" | ");
    s = s.replace(/\s{2,}/g, " ").trim();
    return s;
  };

  // =========================
  // ✅ SCORE
  // =========================
  const clamp = (n, min, max) => Math.max(min, Math.min(max, n));

  const computeCvScoreBreakdown = (finalCV) => {
    const safe = finalCV || {};
    const actions = [];
    let score = 0;

    const name = String(safe?.name || "").trim();
    const contactClean = sanitizeContact(safe?.contact || "");
    const summary = String(safe?.summary || "").trim();

    const eduCount = Array.isArray(safe?.education) ? safe.education.length : 0;
    const expCount = Array.isArray(safe?.experience)
      ? safe.experience.length
      : 0;
    const skillsCount = Array.isArray(safe?.skills) ? safe.skills.length : 0;

    const bulletsCount = Array.isArray(safe?.experience)
      ? safe.experience.reduce(
          (acc, ex) =>
            acc + (Array.isArray(ex?.bullets) ? ex.bullets.length : 0),
          0
        )
      : 0;

    const languagesLen = String(safe?.languages || "").trim().length;

    if (name.length >= 3) score += 12;
    else
      actions.push(
        lang === "ar"
          ? "✦ اكتب اسمك في أعلى السيرة (Full Name)."
          : lang === "es"
          ? "✦ Añade tu nombre completo arriba."
          : "✦ Add your full name at the top."
      );

    if (contactClean.length >= 10) score += 12;
    else
      actions.push(
        lang === "ar"
          ? "✦ ضيف بيانات تواصل: Email + Phone + City/State."
          : lang === "es"
          ? "✦ Agrega contacto: Email + Teléfono + Ciudad/Estado."
          : "✦ Add contact: Email + Phone + City/State."
      );

    if (summary.length >= 40) score += 14;
    else {
      const need = Math.max(0, 40 - summary.length);
      actions.push(
        lang === "ar"
          ? `✦ زوّد الـ Summary: ناقص تقريبًا ${need} حرف (خليه 2–3 سطور).`
          : lang === "es"
          ? `✦ Amplía el resumen: añade ~${need} caracteres (2–3 líneas).`
          : `✦ Expand summary: add ~${need} more characters (2–3 lines).`
      );
    }

    if (eduCount > 0) score += 12;
    else
      actions.push(
        lang === "ar"
          ? "✦ ضيف Education (مدرسة/جامعة/شهادة/كورسات)."
          : lang === "es"
          ? "✦ Añade Educación (escuela/título/certificados)."
          : "✦ Add an Education section (school/degree/certificates)."
      );

    if (expCount > 0) score += 18;
    else
      actions.push(
        lang === "ar"
          ? "✦ ضيف Work Experience واحدة على الأقل."
          : lang === "es"
          ? "✦ Añade al menos una experiencia laboral."
          : "✦ Add at least one Work Experience."
      );

    if (bulletsCount >= 3) score += 8;
    else {
      const need = Math.max(0, 3 - bulletsCount);
      actions.push(
        lang === "ar"
          ? `✦ زوّد نقاط الخبرة: ناقص ${need} نقطة (minimum 3).`
          : lang === "es"
          ? `✦ Añade puntos de experiencia: faltan ${need} (mínimo 3).`
          : `✦ Add experience bullets: need ${need} more (minimum 3).`
      );
    }

    if (bulletsCount >= 6) score += 6;
    else {
      const need = Math.max(0, 6 - bulletsCount);
      actions.push(
        lang === "ar"
          ? `✦ علشان يبقى قوي: حاول توصل لـ 6 نقاط خبرة (ناقص ${need}).`
          : lang === "es"
          ? `✦ Para un CV fuerte: llega a 6 puntos (faltan ${need}).`
          : `✦ For a strong CV: reach 6 bullets (need ${need} more).`
      );
    }

    if (skillsCount >= 4) score += 10;
    else {
      const need = Math.max(0, 4 - skillsCount);
      actions.push(
        lang === "ar"
          ? `✦ زوّد Skills: ناقص ${need} مهارة (minimum 4).`
          : lang === "es"
          ? `✦ Añade habilidades: faltan ${need} (mínimo 4).`
          : `✦ Add skills: need ${need} more (minimum 4).`
      );
    }

    if (languagesLen > 0) score += 8;
    else
      actions.push(
        lang === "ar"
          ? "✦ ضيف Languages (مثلاً: English, Arabic)."
          : lang === "es"
          ? "✦ Añade Idiomas (p.ej.: English, Arabic)."
          : "✦ Add Languages (e.g., English, Arabic)."
      );

    score = clamp(score, 0, 100);

    const level =
      score >= 85
        ? lang === "ar"
          ? "ممتاز"
          : lang === "es"
          ? "Excelente"
          : "Excellent"
        : score >= 70
        ? lang === "ar"
          ? "جيد جدًا"
          : lang === "es"
          ? "Fuerte"
          : "Strong"
        : score >= 55
        ? lang === "ar"
          ? "متوسط"
          : lang === "es"
          ? "Promedio"
          : "Average"
        : lang === "ar"
        ? "ضعيف"
        : lang === "es"
        ? "Débil"
        : "Weak";

    const colorClass =
      score >= 80
        ? "text-emerald-600"
        : score >= 60
        ? "text-blue-600"
        : score >= 40
        ? "text-amber-600"
        : "text-red-600";

    const topActions = actions.slice(0, 3);

    const tooltip =
      lang === "ar"
        ? `Score: ${score}% (${level})
To improve:
${topActions.length ? topActions.join(" | ") : "✓ ممتاز، مفيش نقص واضح"}
Stats: Exp=${expCount}, Edu=${eduCount}, Skills=${skillsCount}, Bullets=${bulletsCount}`
        : lang === "es"
        ? `Puntuación: ${score}% (${level})
Mejoras:
${topActions.length ? topActions.join(" | ") : "✓ Está muy bien"}
Stats: Exp=${expCount}, Edu=${eduCount}, Skills=${skillsCount}, Bullets=${bulletsCount}`
        : `Score: ${score}% (${level})
To improve:
${topActions.length ? topActions.join(" | ") : "✓ Looks great, nothing major"}
Stats: Exp=${expCount}, Edu=${eduCount}, Skills=${skillsCount}, Bullets=${bulletsCount}`;

    return {
      score,
      level,
      colorClass,
      bulletsCount,
      skillsCount,
      expCount,
      eduCount,
      tooltip,
      actions,
    };
  };

  // =========================
  // ✅ Build CV DOM (same design as export)
  // =========================
  const buildCvDocumentNode = (finalCV) => {
    const container = document.createElement("div");
    container.id = "cv-document";

    const header = document.createElement("div");
    header.className = "cv-header";

    const nameDiv = document.createElement("div");
    nameDiv.className = "header-name";
    nameDiv.textContent = finalCV?.name || "RESUME";

    const contactDiv = document.createElement("div");
    contactDiv.className = "contact-info";
    contactDiv.textContent = sanitizeContact(finalCV?.contact || "");

    header.appendChild(nameDiv);
    header.appendChild(contactDiv);

    const body = document.createElement("div");
    body.className = "cv-body";

    if (finalCV?.summary) {
      const title = document.createElement("div");
      title.className = "section-title";
      title.textContent = "Professional Summary";

      const p = document.createElement("div");
      p.textContent = finalCV.summary;

      body.appendChild(title);
      body.appendChild(p);
    }

    if (Array.isArray(finalCV?.education) && finalCV.education.length > 0) {
      const title = document.createElement("div");
      title.className = "section-title";
      title.textContent = "Education";
      body.appendChild(title);

      finalCV.education.forEach((edu) => {
        const block = document.createElement("div");
        block.style.marginBottom = "10px";

        const row = document.createElement("div");
        row.className = "row-header";

        const left = document.createElement("div");
        left.textContent = `${edu.school || ""}${
          edu.location ? ", " + edu.location : ""
        }`;

        const right = document.createElement("div");
        right.textContent = edu.date || "";

        row.appendChild(left);
        row.appendChild(right);

        const deg = document.createElement("div");
        deg.textContent = edu.degree || "";

        block.appendChild(row);
        block.appendChild(deg);
        body.appendChild(block);
      });
    }

    if (Array.isArray(finalCV?.courses) && finalCV.courses.length > 0) {
      const title = document.createElement("div");
      title.className = "section-title";
      title.textContent = "Relevant Courses";
      body.appendChild(title);

      finalCV.courses.forEach((course) => {
        const row = document.createElement("div");
        row.className = "course-row";
        row.style.marginBottom = "6px";

        const left = document.createElement("div");
        left.innerHTML = `<b>${String(course.name || "")
          .replace(/</g, "&lt;")
          .replace(/>/g, "&gt;")}</b>${
          course.provider
            ? " – " +
              String(course.provider || "")
                .replace(/</g, "&lt;")
                .replace(/>/g, "&gt;")
            : ""
        }`;

        const right = document.createElement("div");
        right.textContent = course.date || "";

        row.appendChild(left);
        row.appendChild(right);
        body.appendChild(row);
      });
    }

    if (Array.isArray(finalCV?.experience) && finalCV.experience.length > 0) {
      const title = document.createElement("div");
      title.className = "section-title";
      title.textContent = "Work Experience";
      body.appendChild(title);

      finalCV.experience.forEach((exp) => {
        const block = document.createElement("div");
        block.style.marginBottom = "14px";

        const row = document.createElement("div");
        row.className = "row-header";

        const left = document.createElement("div");
        left.textContent = `${exp.company || ""}${
          exp.location ? ", " + exp.location : ""
        }`;

        const right = document.createElement("div");
        right.textContent = exp.dates || "";

        row.appendChild(left);
        row.appendChild(right);

        const sub = document.createElement("div");
        sub.className = "row-subheader";
        sub.textContent = exp.title || "";

        block.appendChild(row);
        block.appendChild(sub);

        const bullets = Array.isArray(exp.bullets) ? exp.bullets : [];
        if (bullets.length > 0) {
          const ul = document.createElement("ul");
          ul.className = "standard-list";
          bullets.forEach((b) => {
            const li = document.createElement("li");
            li.textContent = String(b || "");
            ul.appendChild(li);
          });
          block.appendChild(ul);
        }

        body.appendChild(block);
      });
    }

    if (
      (Array.isArray(finalCV?.skills) && finalCV.skills.length > 0) ||
      finalCV?.languages
    ) {
      const title = document.createElement("div");
      title.className = "section-title";
      title.textContent = "Skills";
      body.appendChild(title);

      if (finalCV.languages) {
        const p = document.createElement("div");
        p.style.marginBottom = "8px";
        p.innerHTML = `<b>Languages:</b> ${String(finalCV.languages || "")
          .replace(/</g, "&lt;")
          .replace(/>/g, "&gt;")}`;
        body.appendChild(p);
      }

      if (Array.isArray(finalCV.skills) && finalCV.skills.length > 0) {
        const grid = document.createElement("div");
        grid.className = "skills-grid";

        finalCV.skills.forEach((skill) => {
          const item = document.createElement("div");
          item.className = "skill-item";

          const dot = document.createElement("span");
          dot.className = "skill-dot";

          const text = document.createElement("span");
          text.textContent = skill;

          item.appendChild(dot);
          item.appendChild(text);
          grid.appendChild(item);
        });

        body.appendChild(grid);
      }
    }

    container.appendChild(header);
    container.appendChild(body);
    return container;
  };

  const buildPdfCloneWithStyles = (element) => {
    const clone = element.cloneNode(true);
    clone.classList.add("pdf-export");

    const style = document.createElement("style");
    style.innerHTML = `
      .pdf-export { width: 816px; background:#fff; padding:0; margin:0; font-family:"Times New Roman", Times, serif; color:#0f172a; }
      .pdf-export .cv-header { padding: 26px 32px 10px 32px; text-align:center; }
      .pdf-export .header-name { font-size: 46px; font-weight: 700; line-height: 1.05; margin: 0 0 12px 0; }
      .pdf-export .contact-info { display:block; text-align:center; font-size:16px; padding:10px 14px; margin-top:25px; background:#e2e8f0; line-height:1.25; word-break:break-word; }
      .pdf-export .cv-body { padding:0 32px 28px 32px; font-size:14px; line-height:1.45; }
      .pdf-export .section-title { margin-bottom:10px; padding:8px 12px; background:#f1f5f9; font-weight:700; text-transform:uppercase; letter-spacing:.02em; border-bottom:2px solid #cbd5e1; }
      .pdf-export .row-header, .pdf-export .course-row { display:flex; justify-content:space-between; gap:16px; font-weight:700; }
      .pdf-export .row-subheader { font-style: italic; margin-top:4px; margin-bottom:6px; }
      .pdf-export .standard-list { margin:6px 0 0 18px; padding:0; }
      .pdf-export .standard-list li { margin:0 0 6px 0; }
      .pdf-export .skills-grid { display:grid; grid-template-columns:1fr 1fr; gap:8px 18px; }
      .pdf-export .skill-item { display:flex; align-items:center; gap:8px; }
      .pdf-export .skill-dot { width:6px; height:6px; border-radius:999px; background:#0f172a; display:inline-block; }
    `;
    clone.prepend(style);
    return clone;
  };

  const downloadPDFSamePage = async (finalCV) => {
    const tempContainer = document.createElement("div");
    tempContainer.style.position = "absolute";
    tempContainer.style.left = "-9999px";
    tempContainer.style.top = "0";
    document.body.appendChild(tempContainer);

    const cvNode = buildCvDocumentNode(finalCV);
    const clone = buildPdfCloneWithStyles(cvNode);
    tempContainer.appendChild(clone);

    const safeName =
      (finalCV?.name || "Resume").replace(/\s+/g, "_") || "Resume";

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
      },
      jsPDF: { unit: "in", format: "letter", orientation: "portrait" },
    };

    try {
      await html2pdf().set(opt).from(clone).save();
    } finally {
      try {
        document.body.removeChild(tempContainer);
      } catch {}
    }
  };

  const downloadWordSamePage = async (finalCV) => {
    const {
      Document,
      Packer,
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
            text: finalCV.name || "RESUME",
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
            text: sanitizeContact(finalCV.contact || ""),
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
                text: `${edu.school || ""}${
                  edu.location ? ", " + edu.location : ""
                }`,
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
                text: `${course.name || ""}${
                  course.provider ? " – " + course.provider : ""
                }`,
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
                text: `${job.company || ""}${
                  job.location ? ", " + job.location : ""
                }`,
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

    if (finalCV.skills?.length || finalCV.languages) {
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

      const skills = Array.isArray(finalCV.skills) ? finalCV.skills : [];
      for (let i = 0; i < skills.length; i += 2) {
        const left = skills[i] ? `• ${skills[i]}` : "";
        const right = skills[i + 1] ? `• ${skills[i + 1]}` : "";

        sections.push(
          new Paragraph({
            children: [
              new TextRun({ text: left, font, size: 24 }),
              new TextRun({ children: [new Tab(), right], font, size: 24 }),
            ],
            tabStops: [{ type: TabStopType.LEFT, position: MID_TAB_POS }],
            spacing: { after: 40 },
          })
        );
      }
    }

    const docFile = new Document({
      sections: [
        {
          properties: {
            page: { margin: { top: 720, right: 720, bottom: 720, left: 720 } },
          },
          children: sections,
        },
      ],
    });

    const blob = await Packer.toBlob(docFile);
    const safeName =
      (finalCV.name || "Resume").replace(/\s+/g, "_") || "Resume";
    saveAs(blob, `${safeName}.docx`);
  };

  // =========================
  // ✅ Modal helpers
  // =========================
  const closePreview = () => {
    setPreviewModal({ isOpen: false, cvId: null, finalCV: null, title: "" });
  };

  const closeScoreModal = () => {
    setScoreModal({ isOpen: false, cvId: null, title: "", scoreObj: null });
  };

  useEffect(() => {
    if (!previewModal.isOpen && !scoreModal.isOpen) return;

    const prevOverflow = document.body.style.overflow;
    document.body.style.overflow = "hidden";

    const onKeyDown = (e) => {
      if (e.key === "Escape") {
        closePreview();
        closeScoreModal();
      }
    };

    window.addEventListener("keydown", onKeyDown);

    return () => {
      document.body.style.overflow = prevOverflow || "";
      window.removeEventListener("keydown", onKeyDown);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [previewModal.isOpen, scoreModal.isOpen]);

  // =========================
  // ✅ API: Fetch CVs list
  // =========================
  const fetchUserCVs = async () => {
    try {
      const res = await fetch(`${API_BASE}/api/cv`, {
        headers: authHeaders(),
      });

      if (res.status === 401) return hardLogout();

      const payload = await res.json().catch(() => ({}));
      if (!res.ok) {
        console.error("fetchUserCVs failed:", payload);
        setCvList([]);
        return;
      }

      const list = Array.isArray(payload)
        ? payload
        : Array.isArray(payload.cvs)
        ? payload.cvs
        : Array.isArray(payload.data)
        ? payload.data
        : [];

      const normalized = list.map((x) => ({
        ...x,
        last_updated: x.updated_at || x.last_updated || x.created_at || "",
      }));

      setCvList(normalized);
    } catch (err) {
      console.error("Error fetching CVs:", err);
      setCvList([]);
    }
  };

  // =========================
  // ✅ Load user + list + HARD GUARD
  // =========================
  useEffect(() => {
    const token = getToken();
    if (!token) {
      setLoading(false);
      return hardLogout();
    }

    const init = async () => {
      setLoading(true);

      const savedUser = localStorage.getItem("user");
      if (savedUser) {
        try {
          const userData = JSON.parse(savedUser);
          setUser(userData);
          setEditData({
            username: userData.username || "",
            phone: userData.phone || "",
            address: userData.address || "",
            bio: userData.bio || "",
          });
        } catch {}
      }

      try {
        const res = await fetch(`${API_BASE}/api/users/me`, {
          headers: authHeaders(),
        });

        if (res.status === 401) return hardLogout();

        const me = await res.json().catch(() => null);

        if (res.ok && me) {
          setUser(me);
          setEditData({
            username: me.username || "",
            phone: me.phone || "",
            address: me.address || "",
            bio: me.bio || "",
          });
          localStorage.setItem("user", JSON.stringify(me));
        }
      } catch (e) {
        console.error("Failed to load /api/users/me", e);
      }

      await fetchUserCVs();

      setLoading(false);
    };

    init();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // =========================
  // ✅ Save profile
  // =========================
  const handleSave = async () => {
    try {
      const response = await fetch(`${API_BASE}/api/users/me`, {
        method: "PUT",
        headers: authHeaders(),
        body: JSON.stringify({
          username: editData.username,
          phone: editData.phone,
          address: editData.address,
          bio: editData.bio,
        }),
      });

      if (response.status === 401) return hardLogout();

      const data = await response.json().catch(() => ({}));

      if (!response.ok) {
        toast.error(data?.message || t(lang, "saveFailed"));
        return;
      }

      toast.success(t(lang, "updated"));

      const updatedUser = { ...(user || {}), ...editData };
      localStorage.setItem("user", JSON.stringify(updatedUser));
      setUser(updatedUser);

      setIsEditing(false);
    } catch (error) {
      toast.error(t(lang, "connErr"));
    }
  };

  // =========================
  // ✅ Delete CV
  // =========================
  const confirmDelete = async () => {
    try {
      const response = await fetch(`${API_BASE}/api/cv/${deleteModal.cvId}`, {
        method: "DELETE",
        headers: authHeaders(),
      });

      if (response.status === 401) return hardLogout();

      const data = await response.json().catch(() => ({}));

      if (!response.ok) {
        toast.error(data?.message || t(lang, "deleteFailed"));
        return;
      }

      toast.success(t(lang, "deleted"));
      setCvList((prev) => prev.filter((cv) => cv.id !== deleteModal.cvId));
      setDeleteModal({ isOpen: false, cvId: null });
    } catch (error) {
      toast.error(t(lang, "connErr"));
    }
  };

  // =========================
  // ✅ Sorted CV list
  // =========================
  const sortedCvList = useMemo(() => {
    return [...(cvList || [])].sort((a, b) => {
      const da = parseSqliteUTC(a.last_updated)?.getTime() || 0;
      const dbb = parseSqliteUTC(b.last_updated)?.getTime() || 0;
      return dbb - da;
    });
  }, [cvList]);

  // =========================
  // ✅ Compute scores (uses GET /api/get-cv/:id ✅)
  // =========================
  useEffect(() => {
    if (!sortedCvList?.length) return;
    if (!getToken()) return;

    let cancelled = false;

    (async () => {
      try {
        const entries = await Promise.all(
          sortedCvList.map(async (cv) => {
            try {
              const res = await fetch(`${API_BASE}/api/get-cv/${cv.id}`, {
                headers: authHeaders(),
              });

              if (res.status === 401)
                return [cv.id, computeCvScoreBreakdown(null)];
              if (!res.ok) return [cv.id, computeCvScoreBreakdown(null)];

              const payload = await res.json();
              const finalCV = normalizeCvData(payload);
              const breakdown = computeCvScoreBreakdown(finalCV);
              return [cv.id, breakdown];
            } catch {
              return [cv.id, computeCvScoreBreakdown(null)];
            }
          })
        );

        if (!cancelled) {
          const map = {};
          entries.forEach(([id, b]) => (map[id] = b));
          setCvScores(map);
        }
      } catch {}
    })();

    return () => {
      cancelled = true;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [sortedCvList, lang]);

  // =========================
  // ✅ Preview / Download (uses GET /api/get-cv/:id ✅)
  // =========================
  const handlePreviewCV = async (cvId) => {
    try {
      const res = await fetch(`${API_BASE}/api/get-cv/${cvId}`, {
        headers: authHeaders(),
      });

      if (res.status === 401) return hardLogout();
      if (!res.ok) throw new Error("Failed to load CV");

      const payload = await res.json();
      const finalCV = normalizeCvData(payload);

      setPreviewModal({
        isOpen: true,
        cvId,
        finalCV,
        title: finalCV?.name || "RESUME",
      });
    } catch (e) {
      console.error(e);
      toast.error(t(lang, "previewFailed"));
    }
  };

  useEffect(() => {
    if (!previewModal.isOpen) return;
    if (!previewModal.finalCV) return;
    if (!previewContainerRef.current) return;

    previewContainerRef.current.innerHTML = "";
    const node = buildCvDocumentNode(previewModal.finalCV);
    previewContainerRef.current.appendChild(node);
  }, [previewModal.isOpen, previewModal.finalCV]);

  const handleDownloadSamePage = async (cvId, format) => {
    try {
      setDownloading({ cvId, format });

      const res = await fetch(`${API_BASE}/api/get-cv/${cvId}`, {
        headers: authHeaders(),
      });

      if (res.status === 401) return hardLogout();
      if (!res.ok) throw new Error("Failed to load CV");

      const payload = await res.json();
      const finalCV = normalizeCvData(payload);

      if (format === "pdf") await downloadPDFSamePage(finalCV);
      else await downloadWordSamePage(finalCV);

      toast.success(t(lang, "downloaded"));
    } catch (e) {
      console.error(e);
      toast.error(t(lang, "downloadFailed"));
    } finally {
      setDownloading({ cvId: null, format: null });
    }
  };

  // =========================
  // ✅ Loading + Guard
  // =========================
  if (loading) {
    return (
      <div className="p-20 text-center font-black text-slate-400" dir={dir}>
        {t(lang, "loading")}
      </div>
    );
  }

  if (!getToken()) return null;

  // =========================
  // ✅ Render
  // =========================
  return (
    <div className="max-w-4xl mx-auto p-4 sm:p-6 relative" dir={dir}>
      {/* ✅ Score Modal */}
      {scoreModal.isOpen && (
        <div className="fixed inset-0 z-50">
          <div
            className="absolute inset-0 bg-black/55 backdrop-blur-sm"
            onClick={closeScoreModal}
          />
          <div className="absolute inset-0 flex items-center justify-center p-4">
            <div className="bg-white rounded-[2rem] w-full max-w-xl shadow-2xl overflow-hidden flex flex-col max-h-[85vh]">
              <div className="flex items-center justify-between p-5 border-b">
                <div className="min-w-0">
                  <h3 className="font-black text-slate-800 truncate">
                    {t(lang, "scoreTitle")} — {scoreModal.title}
                  </h3>
                  <p className="text-xs text-slate-400 font-bold">
                    {t(lang, "scoreSub")}
                  </p>
                </div>

                <Tooltip text={t(lang, "close")}>
                  <button
                    onClick={closeScoreModal}
                    className="p-3 rounded-xl bg-slate-100 text-slate-700 hover:bg-slate-200 transition-all"
                    aria-label="Close"
                  >
                    <X size={20} />
                  </button>
                </Tooltip>
              </div>

              <div className="p-6 overflow-auto">
                <div className="flex items-center justify-between bg-slate-50 border border-slate-200 rounded-2xl p-4">
                  <div className="text-xs font-black text-slate-500 uppercase tracking-widest">
                    {t(lang, "score")}
                  </div>
                  <div className="font-black text-lg">
                    <span
                      className={
                        scoreModal.scoreObj?.colorClass || "text-slate-700"
                      }
                    >
                      {scoreModal.scoreObj?.score ?? 0}%
                    </span>{" "}
                    <span className="text-slate-400 text-sm">
                      ({scoreModal.scoreObj?.level || ""})
                    </span>
                  </div>
                </div>

                <div className="mt-4">
                  <div className="text-sm font-black text-slate-800 mb-2">
                    {t(lang, "doFollowing")}
                  </div>

                  {scoreModal.scoreObj?.actions?.length ? (
                    <ul className="space-y-2">
                      {scoreModal.scoreObj.actions.map((a, idx) => (
                        <li
                          key={idx}
                          className="p-3 rounded-2xl bg-white border border-slate-200 text-slate-700 font-bold"
                        >
                          {a}
                        </li>
                      ))}
                    </ul>
                  ) : (
                    <div className="p-4 rounded-2xl bg-emerald-50 border border-emerald-200 text-emerald-700 font-black">
                      {t(lang, "excellentNoIssues")}
                    </div>
                  )}
                </div>

                <div className="mt-5 text-xs text-slate-500 font-bold">
                  {t(lang, "stats")} {t(lang, "exp")}=
                  {scoreModal.scoreObj?.expCount ?? 0} | {t(lang, "edu")}=
                  {scoreModal.scoreObj?.eduCount ?? 0} | {t(lang, "skills")}=
                  {scoreModal.scoreObj?.skillsCount ?? 0} | {t(lang, "bullets")}
                  ={scoreModal.scoreObj?.bulletsCount ?? 0}
                </div>
              </div>

              <div className="p-4 border-t bg-white flex justify-end gap-2">
                <button
                  onClick={closeScoreModal}
                  className="px-5 py-3 rounded-xl bg-slate-100 text-slate-700 font-black hover:bg-slate-200 transition-all"
                >
                  {t(lang, "close")}
                </button>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* ✅ Preview Modal */}
      {previewModal.isOpen && (
        <div className="fixed inset-0 z-50">
          <div
            className="absolute inset-0 bg-black/55 backdrop-blur-sm"
            onClick={closePreview}
          />
          <div className="absolute inset-0 flex items-center justify-center p-4">
            <div className="bg-white rounded-[2rem] w-full max-w-5xl shadow-2xl overflow-hidden flex flex-col max-h-[90vh]">
              <div className="flex items-center justify-between p-5 border-b">
                <div className="min-w-0">
                  <h3 className="font-black text-slate-800 truncate">
                    {t(lang, "previewTitle")} — {previewModal.title}
                  </h3>
                  <p className="text-xs text-slate-400 font-bold">
                    {t(lang, "escHint")}
                  </p>
                </div>

                <Tooltip text={t(lang, "close")}>
                  <button
                    onClick={closePreview}
                    className="p-3 rounded-xl bg-slate-100 text-slate-700 hover:bg-slate-200 transition-all"
                    aria-label="Close"
                  >
                    <X size={20} />
                  </button>
                </Tooltip>
              </div>

              <div className="flex-1 bg-slate-50 overflow-auto p-4 sm:p-6">
                <div className="mx-auto w-fit bg-white shadow-lg">
                  <style>{`
                    #cv-document { width: 816px; background:#fff; font-family:"Times New Roman", Times, serif; color:#0f172a; }
                    #cv-document .cv-header { padding: 26px 32px 10px 32px; text-align:center; }
                    #cv-document .header-name { font-size:46px; font-weight:700; line-height:1.05; margin:0 0 12px 0; }
                    #cv-document .contact-info { display:block; text-align:center; font-size:16px; padding:10px 14px; background:#e2e8f0; line-height:1.25; word-break:break-word; }
                    #cv-document .cv-body { padding:0 32px 28px 32px; font-size:14px; line-height:1.45; }
                    #cv-document .section-title { margin-bottom:10px; padding:8px 12px; background:#f1f5f9; font-weight:700; text-transform:uppercase; letter-spacing:.02em; border-bottom:2px solid #cbd5e1; }
                    #cv-document .row-header, #cv-document .course-row { display:flex; justify-content:space-between; gap:16px; font-weight:700; }
                    #cv-document .row-subheader { font-style: italic; margin-top:4px; margin-bottom:6px; }
                    #cv-document .standard-list { margin:6px 0 0 18px; padding:0; }
                    #cv-document .standard-list li { margin:0 0 6px 0; }
                    #cv-document .skills-grid { display:grid; grid-template-columns:1fr 1fr; gap:8px 18px; }
                    #cv-document .skill-item { display:flex; align-items:center; gap:8px; }
                    #cv-document .skill-dot { width:6px; height:6px; border-radius:999px; background:#0f172a; display:inline-block; }
                  `}</style>
                  <div ref={previewContainerRef} />
                </div>
              </div>

              <div className="p-4 border-t bg-white flex justify-end gap-2">
                <button
                  onClick={closePreview}
                  className="px-5 py-3 rounded-xl bg-slate-100 text-slate-700 font-black hover:bg-slate-200 transition-all"
                >
                  {t(lang, "close")}
                </button>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* ✅ Delete Modal */}
      {deleteModal.isOpen && (
        <div className="fixed inset-0 bg-black/50 backdrop-blur-sm z-50 flex items-center justify-center p-4">
          <div className="bg-white rounded-[2rem] p-8 max-w-sm w-full shadow-2xl">
            <div className="w-16 h-16 bg-red-100 text-red-600 rounded-full flex items-center justify-center mx-auto mb-4">
              <Trash2 size={32} />
            </div>
            <h3 className="text-xl font-black text-center text-slate-800 mb-2">
              {t(lang, "confirmDeleteTitle")}
            </h3>
            <p className="text-slate-500 text-center mb-6">
              {t(lang, "confirmDeleteDesc")}
            </p>
            <div className="flex gap-3">
              <button
                onClick={() => setDeleteModal({ isOpen: false, cvId: null })}
                className="flex-1 py-3 bg-slate-100 text-slate-600 rounded-xl font-bold"
              >
                {t(lang, "cancel")}
              </button>
              <button
                onClick={confirmDelete}
                className="flex-1 py-3 bg-red-500 text-white rounded-xl font-bold shadow-lg shadow-red-200"
              >
                {t(lang, "del")}
              </button>
            </div>
          </div>
        </div>
      )}

      {/* ===== Main Card ===== */}
      <div className="bg-white rounded-[2.5rem] shadow-2xl overflow-hidden border border-slate-100">
        <div className="bg-gradient-to-r from-blue-600 to-indigo-700 p-8 sm:p-10 text-white flex flex-col md:flex-row justify-between items-center gap-6">
          <div className="flex items-center gap-5">
            <div className="w-20 h-20 bg-white/20 rounded-2xl flex items-center justify-center border border-white/30">
              <User size={40} />
            </div>
            <div>
              <h2 className="text-3xl font-black">{user?.username || ""}</h2>
              <p className="opacity-80 font-medium">{user?.email || ""}</p>
            </div>
          </div>

          <Tooltip
            text={isEditing ? t(lang, "tooltipSave") : t(lang, "tooltipEdit")}
          >
            <button
              onClick={() => (isEditing ? handleSave() : setIsEditing(true))}
              className="bg-white text-blue-600 px-8 py-3 rounded-2xl font-black shadow-xl active:scale-95 transition-all"
            >
              {isEditing ? t(lang, "saveChanges") : t(lang, "editProfile")}
            </button>
          </Tooltip>
        </div>

        <div className="p-6 sm:p-10 space-y-6">
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            <div className="space-y-2">
              <label className="text-xs font-black text-slate-400 uppercase tracking-widest flex items-center gap-2">
                <User size={14} /> {t(lang, "fullName")}
              </label>
              <input
                disabled={!isEditing}
                className="w-full p-4 bg-slate-50 rounded-2xl border-2 border-transparent focus:border-blue-500 outline-none transition-all font-bold text-slate-700 disabled:opacity-60"
                value={editData.username}
                onChange={(e) =>
                  setEditData({ ...editData, username: e.target.value })
                }
              />
            </div>

            <div className="space-y-2">
              <label className="text-xs font-black text-slate-400 uppercase tracking-widest flex items-center gap-2">
                <Edit2 size={14} /> {t(lang, "phone")}
              </label>
              <input
                disabled={!isEditing}
                className="w-full p-4 bg-slate-50 rounded-2xl border-2 border-transparent focus:border-blue-500 outline-none transition-all font-bold text-slate-700 disabled:opacity-60"
                value={editData.phone}
                onChange={(e) =>
                  setEditData({ ...editData, phone: e.target.value })
                }
              />
            </div>

            <div className="space-y-2 md:col-span-2">
              <label className="text-xs font-black text-slate-400 uppercase tracking-widest flex items-center gap-2">
                <MapPin size={14} /> {t(lang, "address")}
              </label>
              <input
                disabled={!isEditing}
                className="w-full p-4 bg-slate-50 rounded-2xl border-2 border-transparent focus:border-blue-500 outline-none transition-all font-bold text-slate-700 disabled:opacity-60"
                value={editData.address}
                onChange={(e) =>
                  setEditData({ ...editData, address: e.target.value })
                }
              />
            </div>

            <div className="space-y-2 md:col-span-2">
              <label className="text-xs font-black text-slate-400 uppercase tracking-widest flex items-center gap-2">
                <Info size={14} /> {t(lang, "bio")}
              </label>
              <textarea
                disabled={!isEditing}
                rows="3"
                className="w-full p-4 bg-slate-50 rounded-2xl border-2 border-transparent focus:border-blue-500 outline-none transition-all font-bold text-slate-700 disabled:opacity-60 resize-none"
                value={editData.bio}
                onChange={(e) =>
                  setEditData({ ...editData, bio: e.target.value })
                }
              />
            </div>
          </div>

          <div className="mt-10 pt-8 border-t-2 border-slate-50">
            <div className="flex flex-col sm:flex-row justify-between items-start sm:items-center gap-4 mb-8">
              <h3 className="text-2xl font-black text-slate-800 flex items-center gap-3">
                <Briefcase className="text-blue-600" />
                {t(lang, "cvMgmt")}
              </h3>

              <Tooltip text={t(lang, "tooltipCreate")}>
                <button
                  onClick={() => navigate("/cv_builder")}
                  className="flex items-center gap-2 text-blue-600 font-black hover:bg-blue-50 px-4 py-2 rounded-xl transition-all"
                >
                  <Plus size={20} /> {t(lang, "createNew")}
                </button>
              </Tooltip>
            </div>

            <div className="grid gap-4">
              {sortedCvList.length > 0 ? (
                sortedCvList.map((cv) => {
                  const safeDateTime = formatDateTime(cv.last_updated);

                  const isPdfLoading =
                    downloading.cvId === cv.id && downloading.format === "pdf";
                  const isWordLoading =
                    downloading.cvId === cv.id && downloading.format === "word";

                  const scoreObj = cvScores[cv.id];
                  const scoreVal = scoreObj?.score ?? 0;

                  const scoreTooltip = scoreObj?.tooltip || t(lang, "calc");

                  return (
                    <div
                      key={cv.id}
                      className="group flex flex-col lg:flex-row items-start lg:items-center justify-between bg-white border-2 border-slate-100 p-6 rounded-[2rem] hover:border-blue-200 hover:shadow-xl hover:shadow-blue-50 transition-all gap-4"
                    >
                      <div className="flex items-center gap-5 w-full lg:w-auto">
                        <div className="w-14 h-14 bg-blue-50 text-blue-600 rounded-2xl flex items-center justify-center group-hover:bg-blue-600 group-hover:text-white transition-all shrink-0">
                          <FileText size={28} />
                        </div>

                        <div className="min-w-0">
                          <h4 className="font-black text-slate-800 text-lg uppercase leading-tight truncate">
                            {cv.cv_name || "Resume #" + cv.id}
                          </h4>
                          <p className="text-xs text-slate-400 font-bold tracking-widest mt-1">
                            {safeDateTime}
                          </p>

                          <div className="mt-2 flex flex-wrap gap-2">
                            <Tooltip text={t(lang, "atsSafe")}>
                              <span className="text-[10px] px-2 py-1 rounded-full bg-emerald-50 text-emerald-700 border border-emerald-200 font-black tracking-wide">
                                ATS Safe
                              </span>
                            </Tooltip>

                            <Tooltip text={t(lang, "onePage")}>
                              <span className="text-[10px] px-2 py-1 rounded-full bg-blue-50 text-blue-700 border border-blue-200 font-black tracking-wide">
                                Page 1
                              </span>
                            </Tooltip>

                            <Tooltip text={t(lang, "usFormat")}>
                              <span className="text-[10px] px-2 py-1 rounded-full bg-purple-50 text-purple-700 border border-purple-200 font-black tracking-wide">
                                US Format
                              </span>
                            </Tooltip>

                            <Tooltip text={scoreTooltip}>
                              <button
                                type="button"
                                onClick={() =>
                                  setScoreModal({
                                    isOpen: true,
                                    cvId: cv.id,
                                    title: cv.cv_name || "Resume #" + cv.id,
                                    scoreObj: scoreObj || null,
                                  })
                                }
                                className="inline-flex items-center gap-1.5 text-[10px] px-2 py-1 rounded-full bg-slate-50 border border-slate-200 font-black tracking-wide cursor-pointer hover:bg-slate-100 transition-all"
                              >
                                <span className="text-slate-500 uppercase tracking-widest">
                                  {t(lang, "score")}
                                </span>
                                <span
                                  className={
                                    scoreObj?.colorClass || "text-slate-700"
                                  }
                                >
                                  {scoreVal}%
                                </span>
                              </button>
                            </Tooltip>
                          </div>
                        </div>
                      </div>

                      <div className="flex items-center gap-2 flex-wrap">
                        <Tooltip text={t(lang, "previewTip")}>
                          <button
                            onClick={() => handlePreviewCV(cv.id)}
                            className="p-3 bg-slate-50 text-slate-600 rounded-xl hover:bg-slate-700 hover:text-white transition-all"
                          >
                            <Eye size={20} />
                          </button>
                        </Tooltip>

                        <Tooltip text={t(lang, "editTip")}>
                          <button
                            onClick={() => navigate(`/cv_edit?cvId=${cv.id}`)}
                            className="p-3 bg-blue-50 text-blue-600 rounded-xl hover:bg-blue-600 hover:text-white transition-all"
                          >
                            <Edit2 size={20} />
                          </button>
                        </Tooltip>

                        <Tooltip text={t(lang, "pdfTip")}>
                          <button
                            disabled={isPdfLoading || isWordLoading}
                            onClick={() => handleDownloadSamePage(cv.id, "pdf")}
                            className={`p-3 rounded-xl transition-all ${
                              isPdfLoading
                                ? "bg-red-100 text-red-400 cursor-not-allowed"
                                : "bg-red-50 text-red-500 hover:bg-red-500 hover:text-white"
                            }`}
                          >
                            <Download size={20} />
                          </button>
                        </Tooltip>

                        <Tooltip text={t(lang, "wordTip")}>
                          <button
                            disabled={isPdfLoading || isWordLoading}
                            onClick={() =>
                              handleDownloadSamePage(cv.id, "word")
                            }
                            className={`p-3 rounded-xl transition-all ${
                              isWordLoading
                                ? "bg-emerald-100 text-emerald-400 cursor-not-allowed"
                                : "bg-emerald-50 text-emerald-600 hover:bg-emerald-600 hover:text-white"
                            }`}
                          >
                            <FileText size={20} />
                          </button>
                        </Tooltip>

                        <Tooltip text={t(lang, "deleteTip")}>
                          <button
                            onClick={() =>
                              setDeleteModal({ isOpen: true, cvId: cv.id })
                            }
                            className="p-3 bg-slate-50 text-slate-400 rounded-xl hover:bg-red-600 hover:text-white transition-all"
                          >
                            <Trash2 size={20} />
                          </button>
                        </Tooltip>
                      </div>
                    </div>
                  );
                })
              ) : (
                <div className="text-center py-16 bg-slate-50/50 border-4 border-dashed border-slate-100 rounded-[3rem]">
                  <FileText className="mx-auto text-slate-200 mb-4" size={64} />
                  <p className="text-slate-400 font-bold text-lg mb-6">
                    {t(lang, "noResumes")}
                  </p>
                  <button
                    onClick={() => navigate("/cv_builder")}
                    className="px-8 py-3 bg-blue-600 text-white rounded-2xl font-black shadow-lg shadow-blue-200 hover:scale-105 transition-all"
                  >
                    {t(lang, "startNow")}
                  </button>
                </div>
              )}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};
