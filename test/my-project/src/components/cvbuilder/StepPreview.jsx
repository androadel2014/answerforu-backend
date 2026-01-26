// src/components/cvbuilder/StepPreview.jsx
import React from "react";
import { ArrowLeft, ArrowRight, FileText, Download } from "lucide-react";

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
    back: "Back",
    downloadWord: "Download Word (ATS Safe)",
    downloadPDF: "Download PDF",
    ps: "Professional Summary",
    edu: "Education",
    courses: "Relevant Courses",
    exp: "Work Experience",
    skills: "Skills",
    languages: "Languages:",
  },
  ar: {
    back: "رجوع",
    downloadWord: "تحميل Word (ATS Safe)",
    downloadPDF: "تحميل PDF",
    ps: "Professional Summary",
    edu: "Education",
    courses: "Relevant Courses",
    exp: "Work Experience",
    skills: "Skills",
    languages: "Languages:",
  },
  es: {
    back: "Volver",
    downloadWord: "Descargar Word (ATS Safe)",
    downloadPDF: "Descargar PDF",
    ps: "Professional Summary",
    edu: "Education",
    courses: "Relevant Courses",
    exp: "Work Experience",
    skills: "Skills",
    languages: "Languages:",
  },
};

const t = (lang, key) => {
  const L = normLang(lang);
  return (I18N[L] && I18N[L][key]) || I18N.en[key] || key;
};

export function StepPreview({
  lang = "en",
  finalCV,
  onBack,
  onDownloadWord,
  onDownloadPDF,
}) {
  if (!finalCV) return null;

  const L = normLang(lang);
  const dir = dirForLang(L);

  const BackIcon = isRTL(L) ? ArrowRight : ArrowLeft;

  return (
    <div dir={dir} className="animate-[fadeIn_0.3s]">
      <div className="flex flex-col sm:flex-row sm:justify-between sm:items-center gap-3 mb-4 sm:mb-6 bg-white p-3 sm:p-4 rounded-xl shadow-sm border border-slate-100">
        <button
          onClick={onBack}
          type="button"
          className="flex items-center gap-2 text-slate-600 hover:text-black font-bold w-full sm:w-auto justify-center sm:justify-start py-2"
        >
          <BackIcon size={18} /> {t(L, "back")}
        </button>

        <div className="flex flex-col sm:flex-row gap-2 w-full sm:w-auto">
          <button
            onClick={onDownloadWord}
            type="button"
            className="w-full sm:w-auto bg-blue-600 text-white px-5 py-3 rounded-xl sm:rounded-full font-bold shadow-lg hover:bg-blue-700 transition flex items-center justify-center gap-2 active:scale-[0.99]"
          >
            <FileText size={20} /> {t(L, "downloadWord")}
          </button>

          <button
            onClick={onDownloadPDF}
            type="button"
            className="w-full sm:w-auto bg-slate-900 text-white px-5 py-3 rounded-xl sm:rounded-full font-bold shadow-lg hover:bg-black transition flex items-center justify-center gap-2 active:scale-[0.99]"
          >
            <Download size={20} /> {t(L, "downloadPDF")}
          </button>
        </div>
      </div>

      {/* Preview wrapper (mobile-first) */}
      <div className="shadow-2xl overflow-hidden bg-gray-300 p-3 sm:p-6 md:p-8 rounded-xl">
        {/* Mobile: allow horizontal scroll ONLY for the paper */}
        <div className="overflow-auto flex justify-start sm:justify-center">
          <div
            id="cv-document"
            style={{
              width: "816px",
              minWidth: "816px",
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
                word-break:break-word;
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
                <div className="section-title">{t(L, "ps")}</div>
                <p>{finalCV.summary}</p>
              </div>
            )}

            {finalCV.education && finalCV.education.length > 0 && (
              <div>
                <div className="section-title">{t(L, "edu")}</div>
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
                <div className="section-title">{t(L, "courses")}</div>
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
                <div className="section-title">{t(L, "exp")}</div>
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
                <div className="section-title">{t(L, "skills")}</div>
                {finalCV.languages && (
                  <p style={{ marginBottom: "5px" }}>
                    <strong>{t(L, "languages")}</strong> {finalCV.languages}
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
    </div>
  );
}

export default StepPreview;
