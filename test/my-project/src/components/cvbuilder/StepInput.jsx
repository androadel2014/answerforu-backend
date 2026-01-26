// src/components/cvbuilder/StepInput.jsx
import React, { useMemo } from "react";
import { Save, Trash2, Plus, Globe, Bot } from "lucide-react";

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
    autoSave: "Auto-save + consistent formatting",
    resumeScore: "Resume Score:",
    scoreTip:
      "Rule-based score: contact + title + education + experience + details",
    tab1: "1. Data",
    tab2: "2. Process",
    tab3: "3. Download",
    deleteData: "Delete data",
    targetTitle: "🎯 Target Job",
    jobTitle: "Job title",
    companyOptional: "Company (optional)",
    locationState: "State / Location",
    requiredPersonal: "Personal Info",
    fullName: "Full name",
    email: "Email",
    phone: "Phone",
    address: "Address",
    phonePh: "+1 703...",
    languagesOpt: "Languages (optional)",
    languagesPh: "e.g. Arabic: Native, English: Fluent",
    educationTitle: "Education",
    addEdu: "Add education",
    degreePh: "Degree (Bachelor...)",
    majorPh: "Major",
    schoolPh: "School",
    locationPh: "Location",
    gradPh: "Graduation (Month Year)",
    experienceTitle: "Experience",
    addHomeExp: "Add home-country exp (auto)",
    addJob: "Add job",
    jobTitlePh: "Job title",
    companyPh: "Company",
    from: "From",
    to: "To",
    dutiesPh: "Write duties in Arabic or English (AI will translate & format)",
    autoGenHint:
      "AI will pick a real company name and generate suitable duties (Auto-Generate).",
    coursesTitle: "Courses",
    optional: "(optional)",
    addCourse: "Add course",
    courseNamePh: "Course name",
    providerPh: "Provider",
    courseDatePh: "Date (Month Year)",
    optimizeTitle:
      "Optimize for Job Description (tailor CV for a specific job post)",
    optimizeHint: "Paste the job requirements / posting text here.",
    jdPlaceholder: `Example:
• Required skills: React, JavaScript, Tailwind
• Experience: 2+ years
• Responsibilities: build UI, work with APIs
• Soft skills: teamwork, problem solving
or paste the full job description here`,
    buildAI: "Build prompt (AI)",
    excellent: "Excellent",
    good: "Good",
    needsWork: "Needs Work",
  },
  ar: {
    autoSave: "حفظ تلقائي + تنسيق مضمون",
    resumeScore: "تقييم السيرة:",
    scoreTip:
      "Rule-based score: contact + title + education + experience + details",
    tab1: "1. البيانات",
    tab2: "2. المعالجة",
    tab3: "3. التحميل",
    deleteData: "حذف البيانات",
    targetTitle: "🎯 الوظيفة المستهدفة",
    jobTitle: "المسمى الوظيفي",
    companyOptional: "الشركة (اختياري)",
    locationState: "الولاية / المكان",
    requiredPersonal: "البيانات الشخصية",
    fullName: "الاسم الكامل",
    email: "البريد الإلكتروني",
    phone: "رقم الهاتف",
    address: "العنوان",
    phonePh: "+1 703...",
    languagesOpt: "اللغات (اختياري)",
    languagesPh: "مثلاً: Arabic: Native, English: Fluent",
    educationTitle: "التعليم (Education)",
    addEdu: "إضافة مؤهل",
    degreePh: "الدرجة (Bachelor...)",
    majorPh: "التخصص (Major)",
    schoolPh: "الجامعة",
    locationPh: "المكان",
    gradPh: "تاريخ التخرج (Month Year)",
    experienceTitle: "الخبرات (Experience)",
    addHomeExp: "إضافة خبرة البلد الأم (أوتوماتيك)",
    addJob: "إضافة وظيفة",
    jobTitlePh: "المسمى الوظيفي",
    companyPh: "اسم الشركة",
    from: "من",
    to: "إلى",
    dutiesPh:
      "اكتب المهام بالعربي أو الإنجليزي (الذكاء الاصطناعي هيترجمها وينسقها)",
    autoGenHint:
      "سيقوم الـ AI باختيار اسم شركة حقيقية وتأليف المهام المناسبة (Auto-Generate).",
    coursesTitle: "الكورسات (Courses)",
    optional: "(اختياري)",
    addCourse: "إضافة كورس",
    courseNamePh: "اسم الكورس",
    providerPh: "الجهة المانحة",
    courseDatePh: "التاريخ (Month Year)",
    optimizeTitle:
      "Optimize for Job Description (خلّي الـ CV مناسب لإعلان وظيفة معين)",
    optimizeHint:
      "اكتب هنا متطلبات الوظيفة اللي هتقدم عليها او اعلان الوظيفة والمتطلبات",
    jdPlaceholder: `Example:
• Required skills: React, JavaScript, Tailwind
• Experience: 2+ years
• Responsibilities: build UI, work with APIs
• Soft skills: teamwork, problem solving
or paste the full job description here`,
    buildAI: "تجميع البيانات وتحويلها (AI)",
    excellent: "ممتاز",
    good: "جيد",
    needsWork: "محتاج تحسين",
  },
  es: {
    autoSave: "Guardado automático + formato consistente",
    resumeScore: "Puntuación del CV:",
    scoreTip:
      "Puntuación por reglas: contacto + título + educación + experiencia + detalles",
    tab1: "1. Datos",
    tab2: "2. Proceso",
    tab3: "3. Descargar",
    deleteData: "Borrar datos",
    targetTitle: "🎯 Puesto objetivo",
    jobTitle: "Puesto",
    companyOptional: "Empresa (opcional)",
    locationState: "Estado / Ubicación",
    requiredPersonal: "Datos personales",
    fullName: "Nombre completo",
    email: "Email",
    phone: "Teléfono",
    address: "Dirección",
    phonePh: "+1 703...",
    languagesOpt: "Idiomas (opcional)",
    languagesPh: "p. ej.: Arabic: Native, English: Fluent",
    educationTitle: "Educación",
    addEdu: "Agregar educación",
    degreePh: "Título (Bachelor...)",
    majorPh: "Especialidad",
    schoolPh: "Universidad",
    locationPh: "Ubicación",
    gradPh: "Graduación (Month Year)",
    experienceTitle: "Experiencia",
    addHomeExp: "Agregar exp. país de origen (auto)",
    addJob: "Agregar trabajo",
    jobTitlePh: "Puesto",
    companyPh: "Empresa",
    from: "Desde",
    to: "Hasta",
    dutiesPh: "Escribe tareas en árabe o inglés (IA traducirá y formateará)",
    autoGenHint:
      "La IA elegirá una empresa real y generará tareas adecuadas (Auto-Generate).",
    coursesTitle: "Cursos",
    optional: "(opcional)",
    addCourse: "Agregar curso",
    courseNamePh: "Nombre del curso",
    providerPh: "Proveedor",
    courseDatePh: "Fecha (Month Year)",
    optimizeTitle:
      "Optimize for Job Description (ajusta el CV a una oferta específica)",
    optimizeHint: "Pega aquí la oferta / requisitos del puesto.",
    jdPlaceholder: `Example:
• Required skills: React, JavaScript, Tailwind
• Experience: 2+ years
• Responsibilities: build UI, work with APIs
• Soft skills: teamwork, problem solving
or paste the full job description here`,
    buildAI: "Generar prompt (IA)",
    excellent: "Excelente",
    good: "Bueno",
    needsWork: "Necesita mejorar",
  },
};

const t = (lang, key) => {
  const L = normLang(lang);
  return (I18N[L] && I18N[L][key]) || I18N.en[key] || key;
};

export function StepInput({
  lang,
  data,
  history,
  resumeScore,
  scoreLabel,
  Tooltip,

  // handlers
  addToHistory,
  updateData,
  updateTarget,
  addItem,
  removeItem,
  updateItem,
  clearData,

  // experience helpers
  getExperienceArray,
  addHomeCountryExp,

  // optimize JD
  optimizeForJD,
  setOptimizeForJD,
  jobDescription,
  setJobDescription,

  // main action
  onPreparePrompt,
  activeTab,
  setActiveTab,
}) {
  const L = normLang(lang);
  const dir = dirForLang(L);

  const scoreLabelLocalized = useMemo(() => {
    const s = String(scoreLabel || "").toLowerCase();
    if (s.includes("excellent")) return t(L, "excellent");
    if (s.includes("good")) return t(L, "good");
    if (s.includes("needs")) return t(L, "needsWork");
    return scoreLabel || "";
  }, [L, scoreLabel]);

  return (
    <div
      dir={dir}
      className="bg-white p-4 sm:p-6 md:p-8 rounded-2xl shadow-lg border border-slate-100 animate-[fadeIn_0.3s]"
    >
      {/* Header */}
      <div className="flex flex-col gap-3 mb-5 bg-white p-3 sm:p-4 rounded-xl shadow-sm border border-slate-200">
        <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-3">
          <div className="flex gap-2 text-sm text-green-700 items-center font-bold">
            <Save size={18} /> {t(L, "autoSave")}
          </div>

          <div className="flex items-center gap-2">
            <span className="text-xs font-bold text-slate-500">
              {t(L, "resumeScore")}
            </span>
            <Tooltip text={t(L, "scoreTip")}>
              <span
                className={`text-xs font-black px-3 py-1 rounded-full border ${
                  resumeScore >= 85
                    ? "bg-green-50 text-green-700 border-green-200"
                    : resumeScore >= 70
                    ? "bg-blue-50 text-blue-700 border-blue-200"
                    : "bg-yellow-50 text-yellow-800 border-yellow-200"
                }`}
              >
                {resumeScore}/100 • {scoreLabelLocalized}
              </span>
            </Tooltip>
          </div>
        </div>

        {/* Tabs (mobile-friendly scroll) */}
        <div className="flex items-center gap-2 justify-between">
          <div className="flex gap-2 bg-slate-100 p-1 rounded-full w-full overflow-x-auto no-scrollbar">
            <button
              onClick={() => setActiveTab("input")}
              className={`shrink-0 px-4 sm:px-6 py-2 rounded-full font-bold text-sm transition ${
                activeTab === "input"
                  ? "bg-white shadow text-blue-600"
                  : "text-slate-500"
              }`}
              type="button"
            >
              {t(L, "tab1")}
            </button>
            <button
              onClick={() => setActiveTab("process")}
              className={`shrink-0 px-4 sm:px-6 py-2 rounded-full font-bold text-sm transition ${
                activeTab === "process"
                  ? "bg-white shadow text-purple-600"
                  : "text-slate-500"
              }`}
              type="button"
            >
              {t(L, "tab2")}
            </button>
            <button
              onClick={() => setActiveTab("preview")}
              className={`shrink-0 px-4 sm:px-6 py-2 rounded-full font-bold text-sm transition ${
                activeTab === "preview"
                  ? "bg-white shadow text-green-600"
                  : "text-slate-500"
              }`}
              type="button"
            >
              {t(L, "tab3")}
            </button>
          </div>

          <button
            onClick={clearData}
            className="shrink-0 text-xs text-red-600 hover:text-red-800 font-bold border border-red-200 px-3 py-2 rounded-lg bg-white"
            type="button"
          >
            {t(L, "deleteData")}
          </button>
        </div>
      </div>

      {/* Target Job */}
      <div className="bg-blue-50/50 p-4 sm:p-6 rounded-xl border border-blue-100 mb-7">
        <h3 className="font-bold text-blue-900 mb-4 flex items-center gap-2">
          {t(L, "targetTitle")}
        </h3>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          <div className="space-y-1">
            <label className="text-xs font-bold text-gray-500">
              {t(L, "jobTitle")}
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
              {t(L, "companyOptional")}
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
              {t(L, "locationState")}
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
      <div className="mb-7">
        <h3 className="font-bold text-lg mb-4 text-slate-700 border-b pb-2 flex items-center gap-2">
          <span className="text-red-500">*</span> {t(L, "requiredPersonal")}
        </h3>

        <div className="grid grid-cols-1 md:grid-cols-2 gap-4 sm:gap-6">
          <div className="space-y-2">
            <label className="text-sm font-bold text-slate-600">
              {t(L, "fullName")}
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
              {t(L, "email")}
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
              inputMode="email"
              autoComplete="email"
            />
          </div>

          <div className="space-y-2">
            <label className="text-sm font-bold text-slate-600">
              {t(L, "phone")}
            </label>
            <input
              type="tel"
              className="w-full p-3 bg-slate-50 rounded-xl border border-slate-200 focus:ring-2 focus:ring-blue-500 outline-none"
              list="list-phones"
              placeholder={t(L, "phonePh")}
              value={data.personalInfo?.phone || ""}
              onChange={(e) =>
                updateData("personalInfo", "phone", e.target.value)
              }
              onBlur={(e) => addToHistory("phones", e.target.value)}
              inputMode="tel"
              autoComplete="tel"
            />
          </div>

          <div className="space-y-2">
            <label className="text-sm font-bold text-slate-600">
              {t(L, "address")}
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
              autoComplete="street-address"
            />
          </div>
        </div>
      </div>

      {/* Languages */}
      <div className="mb-7">
        <h3 className="font-bold text-lg mb-4 text-slate-700 border-b pb-2">
          {t(L, "languagesOpt")}
        </h3>
        <input
          placeholder={t(L, "languagesPh")}
          className="p-3 border rounded-lg w-full"
          value={data.languages || ""}
          onChange={(e) => updateData("languages", null, e.target.value)}
        />
      </div>

      {/* Education */}
      <div className="mb-7">
        <h3 className="font-bold text-lg mb-4 text-slate-700 border-b pb-2 flex items-center gap-2">
          <span className="text-red-500">*</span> {t(L, "educationTitle")}
        </h3>

        {(data.education || []).map((edu, i) => (
          <div
            key={edu.id || i}
            className="grid grid-cols-1 md:grid-cols-3 gap-3 mb-4 bg-slate-50 p-4 rounded-xl relative border border-slate-200"
          >
            <button
              onClick={() => removeItem("education", edu.id)}
              className="absolute top-2 left-2 text-red-500/70 hover:text-red-700"
              type="button"
              aria-label="delete"
            >
              <Trash2 size={18} />
            </button>

            <input
              placeholder={t(L, "degreePh")}
              className="p-2 border rounded"
              list="list-degrees"
              value={edu.degree || ""}
              onChange={(e) =>
                updateItem("education", edu.id, "degree", e.target.value)
              }
              onBlur={(e) => addToHistory("degrees", e.target.value)}
            />

            <input
              placeholder={t(L, "majorPh")}
              className="p-2 border rounded"
              list="list-majors"
              value={edu.major || ""}
              onChange={(e) =>
                updateItem("education", edu.id, "major", e.target.value)
              }
              onBlur={(e) => addToHistory("majors", e.target.value)}
            />

            <input
              placeholder={t(L, "schoolPh")}
              className="p-2 border rounded"
              list="list-schools"
              value={edu.school || ""}
              onChange={(e) =>
                updateItem("education", edu.id, "school", e.target.value)
              }
              onBlur={(e) => addToHistory("schools", e.target.value)}
            />

            <input
              placeholder={t(L, "locationPh")}
              className="p-2 border rounded"
              list="list-locations"
              value={edu.location || ""}
              onChange={(e) =>
                updateItem("education", edu.id, "location", e.target.value)
              }
              onBlur={(e) => addToHistory("locations", e.target.value)}
            />

            <input
              placeholder={t(L, "gradPh")}
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
          type="button"
        >
          <Plus size={16} /> {t(L, "addEdu")}
        </button>
      </div>

      {/* Experience */}
      <div className="mb-7">
        <div className="flex flex-col sm:flex-row sm:justify-between sm:items-center gap-3 mb-4 border-b pb-2">
          <h3 className="font-bold text-lg text-slate-700 flex items-center gap-2">
            <span className="text-red-500">*</span> {t(L, "experienceTitle")}
          </h3>
          <button
            onClick={addHomeCountryExp}
            className="text-xs bg-orange-100 text-orange-800 px-3 py-2 rounded-full font-bold hover:bg-orange-200 transition flex items-center gap-1 w-full sm:w-auto justify-center"
            type="button"
          >
            <Globe size={14} /> {t(L, "addHomeExp")}
          </button>
        </div>

        {getExperienceArray().map((job, i) => (
          <div
            key={job.id || `exp-${i}`}
            className="mb-6 bg-slate-50 p-4 rounded-xl relative border border-slate-200"
          >
            <button
              onClick={() => removeItem("experiences", job.id)}
              className="absolute top-2 left-2 text-red-500/70 hover:text-red-700"
              type="button"
              aria-label="delete"
            >
              <Trash2 size={18} />
            </button>

            <div className="grid grid-cols-1 md:grid-cols-2 gap-3 mb-3">
              <input
                placeholder={t(L, "jobTitlePh")}
                className="p-2 border rounded"
                list="list-jobs"
                value={job.title || ""}
                onChange={(e) =>
                  updateItem("experiences", job.id, "title", e.target.value)
                }
                onBlur={(e) => addToHistory("jobTitles", e.target.value)}
              />

              <input
                placeholder={t(L, "companyPh")}
                className="p-2 border rounded"
                list="list-companies"
                value={job.company || ""}
                onChange={(e) =>
                  updateItem("experiences", job.id, "company", e.target.value)
                }
                onBlur={(e) => addToHistory("companies", e.target.value)}
              />

              <input
                placeholder={t(L, "locationPh")}
                className="p-2 border rounded"
                list="list-locations"
                value={job.location || ""}
                onChange={(e) =>
                  updateItem("experiences", job.id, "location", e.target.value)
                }
                onBlur={(e) => addToHistory("locations", e.target.value)}
              />

              <div className="flex gap-2">
                <input
                  placeholder={t(L, "from")}
                  className={`p-2 border rounded w-full ${
                    !job.start ? "border-red-400 bg-red-50/40" : ""
                  }`}
                  value={job.start || ""}
                  onChange={(e) =>
                    updateItem("experiences", job.id, "start", e.target.value)
                  }
                />
                <input
                  placeholder={t(L, "to")}
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

            {job.company !== "AI_AUTO_DETECT_TOP_COMPANY_IN_EGYPT" ? (
              <textarea
                placeholder={t(L, "dutiesPh")}
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
                dir={dir} // keep natural writing direction for user
              />
            ) : (
              <div className="bg-orange-50 text-orange-800 text-xs p-2 rounded border border-orange-200">
                {t(L, "autoGenHint")}
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
          type="button"
        >
          <Plus size={16} /> {t(L, "addJob")}
        </button>
      </div>

      {/* Courses */}
      <div className="mb-7">
        <h3 className="font-bold text-lg mb-4 text-slate-700 border-b pb-2">
          {t(L, "coursesTitle")}{" "}
          <span className="text-xs font-normal text-gray-400">
            {t(L, "optional")}
          </span>
        </h3>

        {(data.courses || []).map((course, i) => (
          <div
            key={course.id || i}
            className="grid grid-cols-1 md:grid-cols-3 gap-3 mb-2 bg-slate-50 p-3 rounded-lg relative border border-slate-200"
          >
            <button
              onClick={() => removeItem("courses", course.id)}
              className="absolute top-2 left-2 text-red-500/70 hover:text-red-700"
              type="button"
              aria-label="delete"
            >
              <Trash2 size={16} />
            </button>

            <input
              placeholder={t(L, "courseNamePh")}
              className="p-2 border rounded"
              list="list-courseNames"
              value={course.name || ""}
              onChange={(e) =>
                updateItem("courses", course.id, "name", e.target.value)
              }
              onBlur={(e) => addToHistory("courseNames", e.target.value)}
            />

            <input
              placeholder={t(L, "providerPh")}
              className="p-2 border rounded"
              list="list-providers"
              value={course.provider || ""}
              onChange={(e) =>
                updateItem("courses", course.id, "provider", e.target.value)
              }
              onBlur={(e) => addToHistory("providers", e.target.value)}
            />

            <input
              placeholder={t(L, "courseDatePh")}
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
          type="button"
        >
          <Plus size={16} /> {t(L, "addCourse")}
        </button>
      </div>

      {/* Optimize for JD */}
      <div className="mb-6 bg-slate-50 border rounded-2xl p-4 sm:p-5">
        <label className="flex items-start gap-3 font-black text-slate-800">
          <input
            type="checkbox"
            checked={optimizeForJD}
            onChange={(e) => setOptimizeForJD(e.target.checked)}
            className="w-4 h-4 mt-1"
          />
          <span>{t(L, "optimizeTitle")}</span>
        </label>

        <div className="text-sm text-slate-600 mt-2">
          {t(L, "optimizeHint")}
        </div>

        <textarea
          value={jobDescription}
          onChange={(e) => setJobDescription(e.target.value)}
          disabled={!optimizeForJD}
          className={`mt-3 w-full h-36 p-4 border rounded-xl bg-white text-sm ${
            optimizeForJD && !(jobDescription || "").trim()
              ? "border-red-300"
              : ""
          } ${!optimizeForJD ? "opacity-60" : ""}`}
          dir="ltr"
          placeholder={t(L, "jdPlaceholder")}
        />
      </div>

      {/* Bottom Action (mobile-safe sticky) */}
      <div className="sticky bottom-3 sm:bottom-4 pt-4 border-t bg-white">
        <button
          onClick={onPreparePrompt}
          type="button"
          className="w-full py-4 bg-gradient-to-r from-blue-700 to-indigo-700 text-white rounded-xl font-bold text-lg sm:text-xl shadow-lg shadow-blue-200 flex items-center justify-center gap-3 hover:scale-[1.01] transition transform active:scale-95"
        >
          <Bot size={26} /> {t(L, "buildAI")}
        </button>
      </div>
    </div>
  );
}

export default StepInput;
