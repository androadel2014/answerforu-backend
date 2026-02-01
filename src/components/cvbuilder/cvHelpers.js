// src/components/cvbuilder/cvHelpers.js

export const FALLBACK = {
  targetJob: { title: "", company: "", state: "" },
  personalInfo: { fullName: "", phone: "", email: "", address: "" },
  education: [],
  courses: [],
  experiences: [], // ✅ primary key
  languages: "",
  summary: "",
  skills: [],
};

export const normalizeCVData = (raw) => {
  const parsed = raw && typeof raw === "object" ? raw : {};

  const personalInfo =
    parsed.personalInfo ||
    (parsed.personal
      ? {
          fullName: parsed.personal.name || "",
          phone: parsed.personal.phone || "",
          email: parsed.personal.email || "",
          address: parsed.personal.address || "",
        }
      : FALLBACK.personalInfo);

  const experiences = Array.isArray(parsed.experiences)
    ? parsed.experiences
    : Array.isArray(parsed.experience)
    ? parsed.experience
    : [];

  return {
    ...FALLBACK,
    targetJob: parsed.targetJob || FALLBACK.targetJob,
    personalInfo,
    education: Array.isArray(parsed.education) ? parsed.education : [],
    courses: Array.isArray(parsed.courses) ? parsed.courses : [],
    experiences,
    languages: typeof parsed.languages === "string" ? parsed.languages : "",
    summary: typeof parsed.summary === "string" ? parsed.summary : "",
    skills: Array.isArray(parsed.skills) ? parsed.skills : [],
  };
};

// ✅ resolves experiences vs experience (legacy)
export const resolveSection = (prev, section) => {
  if (section === "experiences" || section === "experience") {
    if (Array.isArray(prev.experiences)) return "experiences";
    if (Array.isArray(prev.experience)) return "experience";
    return "experiences";
  }
  return section;
};

export const getExperienceArrayFrom = (data) =>
  Array.isArray(data?.experiences)
    ? data.experiences
    : Array.isArray(data?.experience)
    ? data.experience
    : [];

// ✅ mobile-safe clipboard with fallback
export const safeWriteClipboard = async (text) => {
  const t = String(text ?? "");

  // 1) Modern API
  try {
    if (navigator?.clipboard?.writeText) {
      await navigator.clipboard.writeText(t);
      return true;
    }
  } catch {}

  // 2) Fallback for some mobile browsers / non-secure contexts
  try {
    const ta = document.createElement("textarea");
    ta.value = t;
    ta.setAttribute("readonly", "");
    ta.style.position = "fixed";
    ta.style.top = "-9999px";
    ta.style.left = "-9999px";
    ta.style.opacity = "0";
    document.body.appendChild(ta);

    ta.focus();
    ta.select();
    ta.setSelectionRange(0, ta.value.length);

    const ok = document.execCommand("copy");
    document.body.removeChild(ta);
    return !!ok;
  } catch {
    return false;
  }
};
