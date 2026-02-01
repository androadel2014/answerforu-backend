// CVBuilderView.jsx (FULL FILE - copy/paste)
// ✅ Fixes applied (without breaking your flow):
// 1) Prevent crashes بسبب history keys (emails/phones كانت ناقصة)
// 2) توحيد البيانات: personalInfo + experiences / experience
// 3) preparePrompt كان بيستخدم data.experience فقط → اتصلح ليقرأ experiences/experience بأمان
// 4) downloadPDF كان بيستخدم data.personal.name → اتصلح لـ personalInfo.fullName
// 5) ضمان Arrays موجودة دايمًا (education/courses/experience)
// 6) handleJsonImport بيعمل delete للـ empty arrays بس بأمان
//
// ✅ ADDED FEATURES (بدون تعديل على فلو شغلك):
// A) Optimize for Job Description (ONE STEP via checkbox in Step 1)
// B) Auto CV Name من Job Title عند الحفظ
// C) تحذير لو مفيش Email / Phone قبل AI & قبل التحميل
// D) Resume Score (rule-based) + Badge في الهيدر
// E) Tooltip Component جاهز لاستخدامه على أي Badge/زر
//
// ✅ NEW REQUIRED CHANGE:
// - التواريخ لازم تتحط ومينفعش تسيبها فاضية (Education year + Experience start/end + Courses date)
//
// ✅ CRITICAL FIX (المشكلة اللي عندك):
// - كل عمليات add/update/remove للـ Experience لازم تشتغل سواء الداتا اسمها experiences أو experience (Legacy)
// - اتعمل Resolver موحد واستخدمناه في addItem/updateItem/removeItem
//
// ✅ EXTRA HARDENING (مهم جدًا):
// - منع وجود experience + experiences معًا في state (الـ spread كان بيرجع legacy key ويبوّظ بعض الحالات)
// - addHomeCountryExp بقى يستخدم نفس الـ resolver

import React, { useState, useEffect, useMemo } from "react";
import * as docx from "docx";
import { saveAs } from "file-saver";
import toast from "react-hot-toast";
import html2pdf from "html2pdf.js";

import {
  X,
  Copy,
  CheckCircle,
  ArrowLeft,
  FileText,
  Save,
  Plus,
  Trash2,
  Download,
  Globe,
  Bot,
} from "lucide-react";

export const CVBuilderView = ({ lang }) => {
  const API_BASE = "http://localhost:5000"; // ✅ NEW: base واحد ثابت
  const token = useMemo(() => localStorage.getItem("token") || "", []);
  const authHeaders = useMemo(() => {
    const h = { "Content-Type": "application/json" };
    if (token) h.Authorization = `Bearer ${token}`;
    return h;
  }, [token]);

  const [activeTab, setActiveTab] = useState("input");

  // ============================================================
  // ✅ Helpers: normalize + prevent legacy keys from leaking
  // ============================================================
  const FALLBACK = {
    targetJob: { title: "", company: "", state: "" },
    personalInfo: { fullName: "", phone: "", email: "", address: "" },
    education: [],
    courses: [],
    experiences: [], // ✅ primary key
    languages: "",
    summary: "",
    skills: [],
  };

  const normalizeCVData = (raw) => {
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

    // ✅ IMPORTANT: we DO NOT spread `parsed` blindly (it can reintroduce legacy `experience`)
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

  // --- 1) DATA STATE MANAGEMENT ---
  const [data, setData] = useState(() => {
    const savedData = localStorage.getItem("cv_data_full");
    if (!savedData) return FALLBACK;

    try {
      const parsed = JSON.parse(savedData);
      return normalizeCVData(parsed);
    } catch {
      return FALLBACK;
    }
  });

  // ✅ Auto-save
  useEffect(() => {
    try {
      localStorage.setItem("cv_data_full", JSON.stringify(data));
    } catch {}
  }, [data]);

  // History for auto-complete
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

  // 1) Current user from localStorage (once)
  const [currentUser] = useState(() => {
    const saved = localStorage.getItem("user");
    try {
      return saved ? JSON.parse(saved) : null;
    } catch {
      return null;
    }
  });

  // ✅ ============ IMPORTANT: Section Resolver ============
  // يحل مشكلة experiences vs experience (legacy)
  const resolveSection = (prev, section) => {
    if (section === "experiences" || section === "experience") {
      if (Array.isArray(prev.experiences)) return "experiences";
      if (Array.isArray(prev.experience)) return "experience";
      return "experiences";
    }
    return section;
  };

  const getExperienceArray = () =>
    Array.isArray(data.experiences)
      ? data.experiences
      : Array.isArray(data.experience)
      ? data.experience
      : [];

  // ✅ Tooltip
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

  // ✅ validation (Email/Phone)
  const validateContact = () => {
    const email = (data.personalInfo?.email || "").trim();
    const phone = (data.personalInfo?.phone || "").trim();

    if (!email && !phone) {
      toast.error(
        lang === "ar"
          ? "لازم تضيف Email أو Phone على الأقل قبل المتابعة"
          : "Add at least Email or Phone before continuing"
      );
      return false;
    }

    if (!email || !phone) {
      toast(
        lang === "ar"
          ? "تنبيه: الأفضل تضيف Email و Phone معًا عشان ATS"
          : "Tip: Add both Email and Phone for ATS"
      );
    }

    return true;
  };

  // ✅ NEW: validation (Dates required)
  const validateDates = () => {
    const exps = getExperienceArray();
    const eduList = Array.isArray(data.education) ? data.education : [];
    const courseList = Array.isArray(data.courses) ? data.courses : [];

    // Experience: start/end required
    for (let i = 0; i < exps.length; i++) {
      const j = exps[i] || {};
      const title = (j.title || `Job ${i + 1}`).toString().trim();
      const start = (j.start || "").toString().trim();
      const end = (j.end || "").toString().trim();

      if (!start || !end) {
        toast.error(
          lang === "ar"
            ? `لازم تكمّل التواريخ (من/إلى) في الخبرة رقم ${i + 1} (${title})`
            : `Please fill Start/End dates in experience #${i + 1} (${title})`
        );
        return false;
      }
    }

    // Education: graduation year/date required
    for (let i = 0; i < eduList.length; i++) {
      const e = eduList[i] || {};
      const school = (e.school || `Education ${i + 1}`).toString().trim();
      const year = (e.year || "").toString().trim();

      if (!year) {
        toast.error(
          lang === "ar"
            ? `لازم تكتب تاريخ التخرج في التعليم رقم ${i + 1} (${school})`
            : `Please fill graduation date in education #${i + 1} (${school})`
        );
        return false;
      }
    }

    // Courses: date required
    for (let i = 0; i < courseList.length; i++) {
      const c = courseList[i] || {};
      const name = (c.name || `Course ${i + 1}`).toString().trim();
      const date = (c.date || "").toString().trim();

      if (!date) {
        toast.error(
          lang === "ar"
            ? `لازم تكتب تاريخ الكورس رقم ${i + 1} (${name})`
            : `Please fill course date for course #${i + 1} (${name})`
        );
        return false;
      }
    }

    return true;
  };

  // ✅ Resume Score
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
    resumeScore >= 85 ? "Excellent" : resumeScore >= 70 ? "Good" : "Needs Work";

  // ✅ Optimize for JD (ONE STEP)
  const [optimizeForJD, setOptimizeForJD] = useState(false);
  const [jobDescription, setJobDescription] = useState("");

  const validateOptimizeJD = () => {
    if (!optimizeForJD) return true;
    const jd = (jobDescription || "").trim();
    if (!jd) {
      toast.error(
        lang === "ar"
          ? "إنت مُفعّل Optimize for JD — لازم تلزق الـ Job Description"
          : "Optimize for JD is ON — please paste the Job Description"
      );
      return false;
    }
    return true;
  };

  // 1) Load CV on mount
  useEffect(() => {
    if (!currentUser?.id) return;

    const params = new URLSearchParams(window.location.search);
    const cvId = params.get("cvId");
    const action = params.get("action");
    const format = params.get("format");

    // ✅ NEW ENDPOINTS
    const endpoint = cvId
      ? `${API_BASE}/api/cv/${cvId}`
      : `${API_BASE}/api/cv`;

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
              if (format === "pdf" && typeof downloadPDF === "function")
                downloadPDF();
              if (format === "word" && typeof downloadWord === "function")
                downloadWord();
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

  // 2) Save AI result
  const onAISuccess = (aiResponse) => {
    if (!currentUser?.id) {
      toast.error(lang === "ar" ? "سجل دخول الأول" : "Please login first");
      return;
    }

    const cleanedData = {
      personalInfo: {
        fullName:
          aiResponse.personalInfo?.fullName ||
          aiResponse.personal?.name ||
          data.personalInfo?.fullName ||
          currentUser.username ||
          "",
        email:
          aiResponse.personalInfo?.email ||
          aiResponse.personal?.email ||
          data.personalInfo?.email ||
          currentUser.email ||
          "",
        phone:
          aiResponse.personalInfo?.phone ||
          aiResponse.personal?.phone ||
          data.personalInfo?.phone ||
          currentUser.phone ||
          "",
        address:
          aiResponse.personalInfo?.address ||
          aiResponse.personal?.address ||
          data.personalInfo?.address ||
          currentUser.address ||
          "",
      },
      summary: aiResponse.summary || "",
      experiences: aiResponse.experiences || aiResponse.experience || [],
      education: aiResponse.education || [],
      skills: aiResponse.skills || [],
      courses: aiResponse.courses || [],
      languages: aiResponse.languages || data.languages || "",
      targetJob: aiResponse.targetJob || data.targetJob || { title: "Resume" },
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

    // ✅ NEW SAVE ENDPOINT
    fetch(`${API_BASE}/api/cv`, {
      method: "POST",
      headers: authHeaders,
      body: JSON.stringify({
        user_id: Number(currentUser.id),
        cv_data: cleanedData,
        cv_name: autoName,
      }),
    })
      .then((res) => res.json())
      .then(() => {
        toast.success(
          lang === "ar"
            ? "تمت إضافة السيرة الذاتية للقائمة بنجاح"
            : "CV Added Successfully"
        );
      })
      .catch(() => {
        toast.error(
          lang === "ar" ? "خطأ في الاتصال بالسيرفر" : "Server Connection Error"
        );
      });
  };

  const addToHistory = (category, value) => {
    const v = (value || "").toString().trim();
    if (!v) return;

    setHistory((prev) => {
      const arr = Array.isArray(prev[category]) ? prev[category] : [];
      if (arr.includes(v)) return prev;
      return { ...prev, [category]: [...arr, v] };
    });
  };

  const [finalCV, setFinalCV] = useState(null);
  const [pastedJson, setPastedJson] = useState("");
  const [showModal, setShowModal] = useState(false);
  const [generatedPrompt, setGeneratedPrompt] = useState("");
  const [copyStatus, setCopyStatus] = useState("نسخ يدوي");

  // --- Update Handlers ---
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

  // ✅ FIXED: addItem uses resolver for experience/experiences
  const addItem = (section, template) =>
    setData((prev) => {
      const actual = resolveSection(prev, section);
      const baseArr = Array.isArray(prev[actual]) ? prev[actual] : [];
      return {
        ...prev,
        [actual]: [...baseArr, { id: Date.now(), ...template }],
      };
    });

  // ✅ FIXED: removeItem uses resolver for experience/experiences
  const removeItem = (section, id) => {
    setData((prev) => {
      const actual = resolveSection(prev, section);
      if (!Array.isArray(prev[actual])) return prev;

      return {
        ...prev,
        [actual]: prev[actual].filter((i) => i.id !== id),
      };
    });
  };

  // ✅ FIXED: updateItem uses resolver for experience/experiences
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

  const clearData = () => {
    if (
      confirm(
        lang === "ar"
          ? "هل أنت متأكد من حذف جميع البيانات؟"
          : "Delete all data?"
      )
    ) {
      localStorage.removeItem("cv_data_full");
      window.location.reload();
    }
  };

  // ✅ FIXED: uses resolver (no more mixing experiences/experience)
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

  // -----------------------------------------------------------
  // 3) PROMPT GENERATION (ONE STEP, optional JD optimize)
  // -----------------------------------------------------------
  const preparePrompt = () => {
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
    navigator.clipboard
      .writeText(prompt)
      .then(() => setCopyStatus("تم النسخ تلقائياً ✅"))
      .catch(() => setCopyStatus("فشل النسخ التلقائي"));
    setShowModal(true);
  };

  const copyToClipboard = () => {
    navigator.clipboard.writeText(generatedPrompt);
    setCopyStatus("تم النسخ يدوياً ✅");
  };

  const handleJsonImport = () => {
    try {
      const jsonMatch = pastedJson.match(/\{[\s\S]*\}/);
      if (!jsonMatch) throw new Error("No JSON found");
      const parsed = JSON.parse(jsonMatch[0]);

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
      alert("الكود مش مظبوط. اتأكد إنك نسخت الـ JSON Code Block بس.");
    }
  };

  // -----------------------------------------------------------
  // 4) PDF DOWNLOADER
  // -----------------------------------------------------------
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

  // ============================================================
  // 5) WORD DOWNLOADER (ATS Safe)
  // ============================================================
  const downloadWord = () => {
    if (!validateContact()) return;
    if (!finalCV) return alert("لا توجد بيانات لإنشاء الملف");

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
              new TextRun({ children: [new Tab(), right], font, size: 24 }),
            ],
            tabStops: [{ type: TabStopType.LEFT, position: MID_TAB_POS }],
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

    Packer.toBlob(doc).then((blob) => {
      saveAs(blob, `${(finalCV.name || "Resume").replace(/\s+/g, "_")}.docx`);
    });
  };

  // -----------------------------------------------------------
  // UI
  // -----------------------------------------------------------
  return (
    <div className="max-w-6xl mx-auto p-4 font-sans" dir="rtl">
      <HistoryDatalists />

      {/* Header Tabs */}
      <div className="flex flex-col md:flex-row justify-between items-center mb-6 gap-4 bg-white p-4 rounded-xl shadow-sm border border-slate-200">
        <div className="flex gap-2 text-sm text-green-700 items-center font-bold">
          <Save size={18} /> حفظ تلقائي + تنسيق مضمون
        </div>

        <div className="flex items-center gap-2">
          <span className="text-xs font-bold text-slate-500">
            Resume Score:
          </span>
          <Tooltip text="Rule-based score: contact + title + education + experience + details">
            <span
              className={`text-xs font-black px-3 py-1 rounded-full border ${
                resumeScore >= 85
                  ? "bg-green-50 text-green-700 border-green-200"
                  : resumeScore >= 70
                  ? "bg-blue-50 text-blue-700 border-blue-200"
                  : "bg-yellow-50 text-yellow-800 border-yellow-200"
              }`}
            >
              {resumeScore}/100 • {scoreLabel}
            </span>
          </Tooltip>
        </div>

        <div className="flex gap-2 bg-slate-100 p-1 rounded-full">
          <button
            onClick={() => setActiveTab("input")}
            className={`px-6 py-2 rounded-full font-bold text-sm transition ${
              activeTab === "input"
                ? "bg-white shadow text-blue-600"
                : "text-slate-500"
            }`}
          >
            1. البيانات
          </button>
          <button
            onClick={() => setActiveTab("process")}
            className={`px-6 py-2 rounded-full font-bold text-sm transition ${
              activeTab === "process"
                ? "bg-white shadow text-purple-600"
                : "text-slate-500"
            }`}
          >
            2. المعالجة
          </button>
          <button
            onClick={() => setActiveTab("preview")}
            className={`px-6 py-2 rounded-full font-bold text-sm transition ${
              activeTab === "preview"
                ? "bg-white shadow text-green-600"
                : "text-slate-500"
            }`}
          >
            3. التحميل
          </button>
        </div>

        <button
          onClick={clearData}
          className="text-xs text-red-500 hover:text-red-700 font-bold border border-red-200 px-3 py-1 rounded-lg"
        >
          حذف البيانات
        </button>
      </div>

      {/* STEP 1 */}
      {activeTab === "input" && (
        <div className="bg-white p-6 md:p-8 rounded-2xl shadow-lg border border-slate-100 animate-[fadeIn_0.3s]">
          <div className="bg-blue-50/50 p-6 rounded-xl border border-blue-100 mb-8">
            <h3 className="font-bold text-blue-900 mb-4 flex items-center gap-2">
              🎯 الوظيفة المستهدفة
            </h3>
            <div className="grid md:grid-cols-3 gap-4">
              <div className="space-y-1">
                <label className="text-xs font-bold text-gray-500">
                  المسمى الوظيفي
                </label>
                <input
                  className="p-3 border rounded-lg bg-white w-full"
                  list="list-jobs"
                  value={data.targetJob?.title || ""}
                  onChange={(e) => updateTarget("title", e.target.value)}
                  onBlur={(e) => addToHistory("jobTitles", e.target.value)}
                />
              </div>

              <div className="space-y-1">
                <label className="text-xs font-bold text-gray-500">
                  الشركة (اختياري)
                </label>
                <input
                  className="p-3 border rounded-lg bg-white w-full"
                  list="list-companies"
                  value={data.targetJob?.company || ""}
                  onChange={(e) => updateTarget("company", e.target.value)}
                  onBlur={(e) => addToHistory("companies", e.target.value)}
                />
              </div>

              <div className="space-y-1">
                <label className="text-xs font-bold text-gray-500">
                  الولاية / المكان
                </label>
                <input
                  className="p-3 border rounded-lg bg-white w-full"
                  list="list-locations"
                  value={data.targetJob?.state || ""}
                  onChange={(e) => updateTarget("state", e.target.value)}
                  onBlur={(e) => addToHistory("locations", e.target.value)}
                />
              </div>
            </div>
          </div>

          {/* Personal Info */}
          <div className="mb-8">
            <h3 className="font-bold text-lg mb-4 text-slate-700 border-b pb-2 flex items-center gap-2">
              <span className="text-red-500">*</span> البيانات الشخصية
            </h3>

            <div className="grid md:grid-cols-2 gap-6">
              <div className="space-y-2">
                <label className="text-sm font-bold text-slate-600">
                  الاسم الكامل
                </label>
                <input
                  type="text"
                  className="w-full p-3 bg-slate-50 rounded-xl border border-slate-200 focus:ring-2 focus:ring-blue-500 outline-none"
                  list="list-names"
                  value={data.personalInfo?.fullName || ""}
                  onChange={(e) =>
                    updateData("personalInfo", "fullName", e.target.value)
                  }
                  onBlur={(e) => addToHistory("names", e.target.value)}
                />
              </div>

              <div className="space-y-2">
                <label className="text-sm font-bold text-slate-600">
                  البريد الإلكتروني
                </label>
                <input
                  type="email"
                  className="w-full p-3 bg-slate-50 rounded-xl border border-slate-200 focus:ring-2 focus:ring-blue-500 outline-none"
                  list="list-emails"
                  value={data.personalInfo?.email || ""}
                  onChange={(e) =>
                    updateData("personalInfo", "email", e.target.value)
                  }
                  onBlur={(e) => addToHistory("emails", e.target.value)}
                />
              </div>

              <div className="space-y-2">
                <label className="text-sm font-bold text-slate-600">
                  رقم الهاتف
                </label>
                <input
                  type="tel"
                  className="w-full p-3 bg-slate-50 rounded-xl border border-slate-200 focus:ring-2 focus:ring-blue-500 outline-none"
                  list="list-phones"
                  placeholder="01xxxxxxxxx"
                  value={data.personalInfo?.phone || ""}
                  onChange={(e) =>
                    updateData("personalInfo", "phone", e.target.value)
                  }
                  onBlur={(e) => addToHistory("phones", e.target.value)}
                />
              </div>

              <div className="space-y-2">
                <label className="text-sm font-bold text-slate-600">
                  العنوان
                </label>
                <input
                  type="text"
                  className="w-full p-3 bg-slate-50 rounded-xl border border-slate-200 focus:ring-2 focus:ring-blue-500 outline-none"
                  list="list-locations"
                  value={data.personalInfo?.address || ""}
                  onChange={(e) =>
                    updateData("personalInfo", "address", e.target.value)
                  }
                  onBlur={(e) => addToHistory("locations", e.target.value)}
                />
              </div>
            </div>
          </div>

          {/* Languages */}
          <div className="mb-8">
            <h3 className="font-bold text-lg mb-4 text-slate-700 border-b pb-2">
              اللغات (اختياري)
            </h3>
            <input
              placeholder="مثلاً: Arabic: Native, English: Fluent"
              className="p-3 border rounded-lg w-full"
              value={data.languages || ""}
              onChange={(e) => updateData("languages", null, e.target.value)}
            />
          </div>

          {/* Education */}
          <div className="mb-8">
            <h3 className="font-bold text-lg mb-4 text-slate-700 border-b pb-2 flex items-center gap-2">
              <span className="text-red-500">*</span> التعليم (Education)
            </h3>

            {(data.education || []).map((edu, i) => (
              <div
                key={edu.id || i}
                className="grid md:grid-cols-3 gap-3 mb-4 bg-slate-50 p-4 rounded-xl relative border border-slate-200"
              >
                <button
                  onClick={() => removeItem("education", edu.id)}
                  className="absolute top-2 left-2 text-red-400 hover:text-red-600"
                >
                  <Trash2 size={18} />
                </button>

                <input
                  placeholder="الدرجة (Bachelor...)"
                  className="p-2 border rounded"
                  list="list-degrees"
                  value={edu.degree || ""}
                  onChange={(e) =>
                    updateItem("education", edu.id, "degree", e.target.value)
                  }
                  onBlur={(e) => addToHistory("degrees", e.target.value)}
                />

                <input
                  placeholder="التخصص (Major)"
                  className="p-2 border rounded"
                  list="list-majors"
                  value={edu.major || ""}
                  onChange={(e) =>
                    updateItem("education", edu.id, "major", e.target.value)
                  }
                  onBlur={(e) => addToHistory("majors", e.target.value)}
                />

                <input
                  placeholder="الجامعة"
                  className="p-2 border rounded"
                  list="list-schools"
                  value={edu.school || ""}
                  onChange={(e) =>
                    updateItem("education", edu.id, "school", e.target.value)
                  }
                  onBlur={(e) => addToHistory("schools", e.target.value)}
                />

                <input
                  placeholder="المكان"
                  className="p-2 border rounded"
                  list="list-locations"
                  value={edu.location || ""}
                  onChange={(e) =>
                    updateItem("education", edu.id, "location", e.target.value)
                  }
                  onBlur={(e) => addToHistory("locations", e.target.value)}
                />

                <input
                  placeholder="تاريخ التخرج (Month Year)"
                  className={`p-2 border rounded ${
                    !edu.year ? "border-red-400 bg-red-50/40" : ""
                  }`}
                  value={edu.year || ""}
                  onChange={(e) =>
                    updateItem("education", edu.id, "year", e.target.value)
                  }
                />
              </div>
            ))}

            <button
              onClick={() =>
                addItem("education", {
                  degree: "",
                  major: "",
                  school: "",
                  location: "",
                  year: "",
                })
              }
              className="flex items-center gap-2 text-blue-600 font-bold text-sm hover:bg-blue-50 px-4 py-2 rounded-lg transition"
            >
              <Plus size={16} /> إضافة مؤهل
            </button>
          </div>

          {/* Experience */}
          <div className="mb-8">
            <div className="flex justify-between items-center mb-4 border-b pb-2">
              <h3 className="font-bold text-lg text-slate-700 flex items-center gap-2">
                <span className="text-red-500">*</span> الخبرات (Experience)
              </h3>
              <button
                onClick={addHomeCountryExp}
                className="text-xs bg-orange-100 text-orange-700 px-3 py-1 rounded-full font-bold hover:bg-orange-200 transition flex items-center gap-1"
              >
                <Globe size={14} /> إضافة خبرة البلد الأم (أوتوماتيك)
              </button>
            </div>

            {getExperienceArray().map((job, i) => (
              <div
                key={job.id || `exp-${i}`}
                className="mb-6 bg-slate-50 p-4 rounded-xl relative border border-slate-200"
              >
                <button
                  onClick={() => removeItem("experiences", job.id)}
                  className="absolute top-2 left-2 text-red-400 hover:text-red-600"
                >
                  <Trash2 size={18} />
                </button>

                <div className="grid md:grid-cols-2 gap-3 mb-3">
                  <input
                    placeholder="المسمى الوظيفي"
                    className="p-2 border rounded"
                    list="list-jobs"
                    value={job.title || ""}
                    onChange={(e) =>
                      updateItem("experiences", job.id, "title", e.target.value)
                    }
                    onBlur={(e) => addToHistory("jobTitles", e.target.value)}
                  />

                  <input
                    placeholder="اسم الشركة"
                    className="p-2 border rounded"
                    list="list-companies"
                    value={job.company || ""}
                    onChange={(e) =>
                      updateItem(
                        "experiences",
                        job.id,
                        "company",
                        e.target.value
                      )
                    }
                    onBlur={(e) => addToHistory("companies", e.target.value)}
                  />

                  <input
                    placeholder="المكان"
                    className="p-2 border rounded"
                    list="list-locations"
                    value={job.location || ""}
                    onChange={(e) =>
                      updateItem(
                        "experiences",
                        job.id,
                        "location",
                        e.target.value
                      )
                    }
                    onBlur={(e) => addToHistory("locations", e.target.value)}
                  />

                  <div className="flex gap-2">
                    <input
                      placeholder="من"
                      className={`p-2 border rounded w-full ${
                        !job.start ? "border-red-400 bg-red-50/40" : ""
                      }`}
                      value={job.start || ""}
                      onChange={(e) =>
                        updateItem(
                          "experiences",
                          job.id,
                          "start",
                          e.target.value
                        )
                      }
                    />
                    <input
                      placeholder="إلى"
                      className={`p-2 border rounded w-full ${
                        !job.end ? "border-red-400 bg-red-50/40" : ""
                      }`}
                      value={job.end || ""}
                      onChange={(e) =>
                        updateItem("experiences", job.id, "end", e.target.value)
                      }
                    />
                  </div>
                </div>

                {job.company !== "AI_AUTO_DETECT_TOP_COMPANY_IN_EGYPT" && (
                  <textarea
                    placeholder="اكتب المهام بالعربي أو الإنجليزي (الذكاء الاصطناعي هيترجمها وينسقها)"
                    className="w-full p-3 border rounded-lg h-24 focus:ring-2 focus:ring-blue-500 outline-none"
                    value={job.descriptionRaw || ""}
                    onChange={(e) =>
                      updateItem(
                        "experiences",
                        job.id,
                        "descriptionRaw",
                        e.target.value
                      )
                    }
                  />
                )}

                {job.company === "AI_AUTO_DETECT_TOP_COMPANY_IN_EGYPT" && (
                  <div className="bg-orange-50 text-orange-800 text-xs p-2 rounded border border-orange-200">
                    سيقوم الـ AI باختيار اسم شركة حقيقية وتأليف المهام المناسبة
                    (Auto-Generate).
                  </div>
                )}
              </div>
            ))}

            <button
              onClick={() =>
                addItem("experiences", {
                  title: "",
                  company: "",
                  location: "",
                  start: "",
                  end: "",
                  descriptionRaw: "",
                })
              }
              className="flex items-center gap-2 text-blue-600 font-bold text-sm hover:bg-blue-50 px-4 py-2 rounded-lg transition"
            >
              <Plus size={16} /> إضافة وظيفة
            </button>
          </div>

          {/* Courses */}
          <div className="mb-8">
            <h3 className="font-bold text-lg mb-4 text-slate-700 border-b pb-2">
              الكورسات (Courses){" "}
              <span className="text-xs font-normal text-gray-400">
                (اختياري)
              </span>
            </h3>

            {(data.courses || []).map((course, i) => (
              <div
                key={course.id || i}
                className="grid md:grid-cols-3 gap-3 mb-2 bg-slate-50 p-3 rounded-lg relative"
              >
                <button
                  onClick={() => removeItem("courses", course.id)}
                  className="absolute top-2 left-2 text-red-400 hover:text-red-600"
                >
                  <Trash2 size={16} />
                </button>

                <input
                  placeholder="اسم الكورس"
                  className="p-2 border rounded"
                  list="list-courseNames"
                  value={course.name || ""}
                  onChange={(e) =>
                    updateItem("courses", course.id, "name", e.target.value)
                  }
                  onBlur={(e) => addToHistory("courseNames", e.target.value)}
                />

                <input
                  placeholder="الجهة المانحة"
                  className="p-2 border rounded"
                  list="list-providers"
                  value={course.provider || ""}
                  onChange={(e) =>
                    updateItem("courses", course.id, "provider", e.target.value)
                  }
                  onBlur={(e) => addToHistory("providers", e.target.value)}
                />

                <input
                  placeholder="التاريخ (Month Year)"
                  className={`p-2 border rounded ${
                    !course.date ? "border-red-400 bg-red-50/40" : ""
                  }`}
                  value={course.date || ""}
                  onChange={(e) =>
                    updateItem("courses", course.id, "date", e.target.value)
                  }
                />
              </div>
            ))}

            <button
              onClick={() =>
                addItem("courses", { name: "", provider: "", date: "" })
              }
              className="flex items-center gap-2 text-blue-600 font-bold text-sm hover:bg-blue-50 px-4 py-2 rounded-lg transition"
            >
              <Plus size={16} /> إضافة كورس
            </button>
          </div>

          {/* ✅ Optimize for JD (ONE STEP) */}
          <div className="mb-6 bg-slate-50 border rounded-2xl p-5">
            <label className="flex items-center gap-2 font-black text-slate-800">
              <input
                type="checkbox"
                checked={optimizeForJD}
                onChange={(e) => setOptimizeForJD(e.target.checked)}
                className="w-4 h-4"
              />
              Optimize for Job Description (خلّي الـ CV مناسب لإعلان وظيفة معين)
            </label>

            <div className="text-sm text-slate-600 mt-2">
              اكتب هنا متطلبات الوظيفة اللي هتقدم عليهااو اعلان الوظيفه
              والمتطلبات
            </div>

            <textarea
              value={jobDescription}
              onChange={(e) => setJobDescription(e.target.value)}
              disabled={!optimizeForJD}
              className={`mt-3 w-full h-36 p-4 border rounded-xl bg-white text-left text-sm ${
                optimizeForJD && !(jobDescription || "").trim()
                  ? "border-red-300"
                  : ""
              } ${!optimizeForJD ? "opacity-60" : ""}`}
              dir="ltr"
              placeholder={`Example:
• Required skills: React, JavaScript, Tailwind
• Experience: 2+ years
• Responsibilities: build UI, work with APIs
• Soft skills: teamwork, problem solving
or paste the full job description here`}
            />
          </div>

          <div className="sticky bottom-4 pt-4 border-t bg-white">
            <button
              onClick={preparePrompt}
              className="w-full py-4 bg-gradient-to-r from-blue-700 to-indigo-700 text-white rounded-xl font-bold text-xl shadow-lg shadow-blue-200 flex items-center justify-center gap-3 hover:scale-[1.01] transition transform active:scale-95"
            >
              <Bot size={28} /> تجميع البيانات وتحويلها (AI)
            </button>
          </div>
        </div>
      )}

      {/* Modal (Prompt) */}
      {showModal && (
        <div className="fixed inset-0 bg-black/70 backdrop-blur-sm flex items-center justify-center z-50 p-4 animate-[fadeIn_0.2s]">
          <div className="bg-white rounded-2xl p-6 w-full max-w-2xl shadow-2xl border border-gray-200">
            <div className="flex justify-between items-center mb-6">
              <h3 className="text-2xl font-black text-slate-800">
                جاهز للإرسال!
              </h3>
              <button
                onClick={() => setShowModal(false)}
                className="bg-gray-100 p-2 rounded-full hover:bg-red-100 text-gray-500 hover:text-red-500 transition"
              >
                <X size={20} />
              </button>
            </div>

            <div
              className={`mb-6 p-4 rounded-xl text-center font-bold border ${
                copyStatus.includes("تلقائياً")
                  ? "bg-green-50 border-green-200 text-green-700"
                  : "bg-yellow-50 border-yellow-200 text-yellow-700"
              }`}
            >
              {copyStatus}
            </div>

            <p className="mb-3 text-slate-600 font-medium">
              تم تجهيز الأوامر. اتبع الخطوات:
            </p>

            <ol className="list-decimal list-inside mb-6 space-y-2 text-sm text-slate-600 bg-slate-50 p-4 rounded-xl border">
              <li>تم نسخ النص تلقائياً (أو اضغط نسخ يدوياً).</li>
              <li>اضغط زر "فتح ChatGPT" بالأسفل.</li>
              <li>
                اعمل <b>Paste (لصق)</b> في الشات هناك.
              </li>
              <li>انسخ الكود اللي الـ AI هيطلعه وارجع هنا.</li>
            </ol>

            <div className="grid grid-cols-2 gap-3">
              <button
                onClick={copyToClipboard}
                className="py-3 bg-slate-200 text-slate-800 rounded-xl font-bold hover:bg-slate-300 flex items-center justify-center gap-2"
              >
                <Copy size={18} /> نسخ يدوياً
              </button>

              <button
                onClick={() => {
                  window.open("https://chat.openai.com", "_blank");
                  setActiveTab("process");
                  setShowModal(false);
                }}
                className="py-3 bg-green-600 text-white rounded-xl font-bold hover:bg-green-700 flex items-center justify-center gap-2 shadow-lg shadow-green-200"
              >
                <Bot size={20} /> فتح ChatGPT
              </button>
            </div>
          </div>
        </div>
      )}

      {/* STEP 2 */}
      {activeTab === "process" && (
        <div className="bg-white p-8 rounded-2xl shadow-lg text-center animate-[fadeIn_0.3s] max-w-3xl mx-auto">
          <div className="w-20 h-20 bg-purple-100 text-purple-600 rounded-full flex items-center justify-center mx-auto mb-6">
            <Copy size={40} />
          </div>
          <h2 className="text-3xl font-black mb-2 text-slate-800">
            لصق كود الـ AI
          </h2>
          <p className="text-slate-500 mb-6">
            هات الكود (JSON Block) اللي ChatGPT طلعه وحطه هنا.
          </p>

          <textarea
            value={pastedJson}
            onChange={(e) => setPastedJson(e.target.value)}
            placeholder='Paste JSON here... { "name": "..." }'
            className="w-full h-64 p-5 border-2 border-dashed border-purple-300 rounded-2xl bg-purple-50/50 text-left font-mono text-sm mb-6 focus:border-purple-600 focus:bg-white outline-none transition"
            dir="ltr"
          />

          <button
            onClick={handleJsonImport}
            className="w-full py-4 bg-green-600 text-white rounded-xl font-bold text-xl shadow-xl hover:bg-green-700 transition flex items-center justify-center gap-3"
          >
            <CheckCircle size={28} /> إنشاء السي في
          </button>

          <button
            onClick={() => setActiveTab("input")}
            className="mt-6 text-slate-400 hover:text-slate-600 underline text-sm"
          >
            العودة لتعديل البيانات
          </button>
        </div>
      )}

      {/* STEP 3 */}
      {activeTab === "preview" && finalCV && (
        <div className="animate-[fadeIn_0.3s]">
          <div className="flex justify-between items-center mb-6 bg-white p-4 rounded-xl shadow-sm">
            <button
              onClick={() => setActiveTab("process")}
              className="flex items-center gap-2 text-slate-600 hover:text-black font-bold"
            >
              <ArrowLeft /> رجوع
            </button>

            <div className="flex gap-2">
              <button
                onClick={downloadWord}
                className="bg-blue-600 text-white px-6 py-3 rounded-full font-bold shadow-lg hover:bg-blue-700 transition flex items-center gap-2"
              >
                <FileText size={20} /> تحميل Word (ATS Safe)
              </button>

              <button
                onClick={downloadPDF}
                className="bg-slate-900 text-white px-6 py-3 rounded-full font-bold shadow-lg hover:bg-black transition flex items-center gap-2"
              >
                <Download size={20} /> تحميل PDF
              </button>
            </div>
          </div>

          <div className="shadow-2xl overflow-auto bg-gray-300 p-8 rounded-xl flex justify-center">
            <div
              id="cv-document"
              style={{
                width: "816px",
                background: "white",
                padding: "28px 32px",
                fontFamily: "Times New Roman, Times, serif",
                color: "#0f172a",
              }}
            >
              <style>{`
                #cv-document { box-sizing: border-box; }
                #cv-document * { box-sizing: border-box; }

                .cv-header{
                  text-align:center;
                  display:flex;
                  flex-direction:column;
                  align-items:center;
                  gap:14px;
                  padding-bottom:14px;
                }

                .header-name{
                  font-size:46px;
                  font-weight:900;
                  line-height:1.15;
                  margin:0;
                }

                .contact-info{
                  display:inline-block;
                  font-size:14px;
                  line-height:1.35;
                  padding:10px 16px;
                  background:#e2e8f0;
                  max-width:100%;
                  word-break:break-word;
                }

                .section-title{
                  font-weight:800;
                  letter-spacing:1px;
                  text-transform:uppercase;
                  border-bottom:2px solid #cbd5e1;
                  padding-bottom:6px;
                  margin-top:18px;
                  margin-bottom:10px;
                  font-size:14px;
                }

                .row-header{
                  display:flex;
                  justify-content:space-between;
                  font-weight:800;
                  font-size:14px;
                  gap:12px;
                }

                .row-subheader{
                  font-style:italic;
                  margin-top:4px;
                  font-size:14px;
                }

                .standard-list{
                  margin-top:6px;
                  padding-left:18px;
                  font-size:14px;
                  line-height:1.45;
                }

                .course-row{
                  display:flex;
                  justify-content:space-between;
                  font-size:14px;
                  gap:12px;
                }

                .skills-grid{
                  display:grid;
                  grid-template-columns:1fr 1fr;
                  gap:6px;
                  font-size:14px;
                }

                .skill-item{
                  display:flex;
                  gap:8px;
                  align-items:flex-start;
                }

                .skill-dot{
                  width:8px;
                  height:8px;
                  border-radius:50%;
                  background:#0f172a;
                  margin-top:6px;
                  flex:0 0 8px;
                }
              `}</style>

              <div className="cv-header">
                <div className="header-name">{finalCV.name}</div>
                <div className="contact-info">{finalCV.contact}</div>
              </div>

              {finalCV.summary && (
                <div>
                  <div className="section-title">Professional Summary</div>
                  <p>{finalCV.summary}</p>
                </div>
              )}

              {finalCV.education && finalCV.education.length > 0 && (
                <div>
                  <div className="section-title">Education</div>
                  {finalCV.education.map((edu, i) => (
                    <div key={i} style={{ marginBottom: "4pt" }}>
                      <div className="row-header">
                        <span>
                          {edu.school}, {edu.location}
                        </span>
                        <span>{edu.date}</span>
                      </div>
                      <div>{edu.degree}</div>
                    </div>
                  ))}
                </div>
              )}

              {finalCV.courses && finalCV.courses.length > 0 && (
                <div>
                  <div className="section-title">Relevant Courses</div>
                  {finalCV.courses.map((course, i) => (
                    <div key={i} style={{ marginBottom: "2pt" }}>
                      <div className="course-row">
                        <span>
                          <strong>{course.name}</strong> – {course.provider}
                        </span>
                        <span>{course.date}</span>
                      </div>
                    </div>
                  ))}
                </div>
              )}

              {finalCV.experience && finalCV.experience.length > 0 && (
                <div>
                  <div className="section-title">Work Experience</div>
                  {finalCV.experience.map((exp, i) => (
                    <div key={i} style={{ marginBottom: "14pt" }}>
                      <div className="row-header">
                        <span>
                          {exp.company}, {exp.location}
                        </span>
                        <span>{exp.dates}</span>
                      </div>
                      <div className="row-subheader">{exp.title}</div>
                      <ul className="standard-list">
                        {(exp.bullets || []).map((b, idx) => (
                          <li key={idx}>{b}</li>
                        ))}
                      </ul>
                    </div>
                  ))}
                </div>
              )}

              {(finalCV.skills || finalCV.languages) && (
                <div>
                  <div className="section-title">Skills</div>
                  {finalCV.languages && (
                    <p style={{ marginBottom: "5px" }}>
                      <strong>Languages:</strong> {finalCV.languages}
                    </p>
                  )}
                  {finalCV.skills && (
                    <div className="skills-grid">
                      {finalCV.skills.map((skill, i) => (
                        <div key={i} className="skill-item">
                          <span className="skill-dot"></span>
                          {skill}
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              )}
            </div>
          </div>
        </div>
      )}

      {activeTab === "preview" && !finalCV && (
        <div className="bg-white p-8 rounded-2xl shadow-lg text-center max-w-2xl mx-auto">
          <div className="text-slate-700 font-black text-2xl mb-2">
            مفيش CV لسه
          </div>
          <div className="text-slate-500 mb-6">
            روح Step 2 واعمل Paste للـ JSON وبعدين Create.
          </div>
          <button
            onClick={() => setActiveTab("process")}
            className="px-6 py-3 bg-slate-900 text-white rounded-xl font-bold"
          >
            روح Step 2
          </button>
        </div>
      )}
    </div>
  );
};
