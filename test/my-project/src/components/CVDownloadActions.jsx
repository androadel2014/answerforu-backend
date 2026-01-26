import React from "react";

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
    summary: "Professional Summary",
    education: "Education",
    courses: "Relevant Courses",
    experience: "Work Experience",
    skills: "Skills",
    languages: "Languages:",
  },
  ar: {
    summary: "Professional Summary",
    education: "Education",
    courses: "Relevant Courses",
    experience: "Work Experience",
    skills: "Skills",
    languages: "Languages:",
  },
  es: {
    summary: "Professional Summary",
    education: "Education",
    courses: "Relevant Courses",
    experience: "Work Experience",
    skills: "Skills",
    languages: "Languages:",
  },
};

const t = (lang, key) => {
  const L = normLang(lang);
  return (I18N[L] && I18N[L][key]) || I18N.en[key] || key;
};

const sectionTitleStyle = {
  fontWeight: 900,
  letterSpacing: "1px",
  textTransform: "uppercase",
  borderBottom: "2px solid #cbd5e1",
  padding: "10px 0 8px",
  marginBottom: "10px",
  fontSize: "14px",
};

export const CVDocument = ({ lang = "en", finalCV }) => {
  if (!finalCV) return null;

  const L = normLang(lang);
  const dir = dirForLang(L);

  // Keep the resume itself LTR (ATS + english output), but page dir can still be set by parent
  // If you ever allow non-English resume output, switch this to `dir`.
  const resumeDir = "ltr";

  return (
    <div dir={resumeDir}>
      <div
        className="header-name"
        style={{
          fontSize: "36px",
          fontWeight: 900,
          textAlign: "center",
          wordBreak: "break-word",
        }}
      >
        {finalCV.name}
      </div>

      <div
        className="contact-info"
        style={{
          marginTop: "10px",
          textAlign: "center",
          background: "#e2e8f0",
          padding: "8px 12px",
          borderRadius: "10px",
          fontSize: "14px",
          fontWeight: 600,
          wordBreak: "break-word",
        }}
      >
        {finalCV.contact}
      </div>

      {/* SUMMARY */}
      {finalCV.summary && (
        <div style={{ marginTop: "18px" }}>
          <div className="section-title" style={sectionTitleStyle}>
            {t(L, "summary")}
          </div>
          <p
            style={{
              fontSize: "14px",
              lineHeight: 1.45,
              textAlign: "justify",
              margin: 0,
            }}
          >
            {finalCV.summary}
          </p>
        </div>
      )}

      {/* EDUCATION */}
      {finalCV.education && finalCV.education.length > 0 && (
        <div style={{ marginTop: "18px" }}>
          <div className="section-title" style={sectionTitleStyle}>
            {t(L, "education")}
          </div>
          {finalCV.education.map((edu, i) => (
            <div key={i} style={{ marginBottom: "10px" }}>
              <div
                style={{
                  display: "flex",
                  justifyContent: "space-between",
                  fontWeight: 800,
                  fontSize: "14px",
                  gap: "12px",
                }}
              >
                <span style={{ minWidth: 0, wordBreak: "break-word" }}>
                  {edu.school}
                  {edu.location ? `, ${edu.location}` : ""}
                </span>
                <span style={{ whiteSpace: "nowrap" }}>{edu.date}</span>
              </div>
              <div style={{ fontSize: "14px" }}>{edu.degree}</div>
            </div>
          ))}
        </div>
      )}

      {/* COURSES */}
      {finalCV.courses && finalCV.courses.length > 0 && (
        <div style={{ marginTop: "18px" }}>
          <div className="section-title" style={sectionTitleStyle}>
            {t(L, "courses")}
          </div>

          {finalCV.courses.map((course, i) => (
            <div
              key={i}
              style={{
                marginBottom: "6px",
                display: "flex",
                justifyContent: "space-between",
                fontSize: "14px",
                gap: "12px",
              }}
            >
              <span style={{ minWidth: 0, wordBreak: "break-word" }}>
                <strong>{course.name}</strong>
                {course.provider ? ` – ${course.provider}` : ""}
              </span>
              <span style={{ whiteSpace: "nowrap" }}>{course.date}</span>
            </div>
          ))}
        </div>
      )}

      {/* EXPERIENCE */}
      {finalCV.experience && finalCV.experience.length > 0 && (
        <div style={{ marginTop: "18px" }}>
          <div className="section-title" style={sectionTitleStyle}>
            {t(L, "experience")}
          </div>

          {finalCV.experience.map((exp, i) => (
            <div key={i} style={{ marginBottom: "14pt" }}>
              <div
                style={{
                  display: "flex",
                  justifyContent: "space-between",
                  fontWeight: 800,
                  fontSize: "14px",
                  gap: "12px",
                }}
              >
                <span style={{ minWidth: 0, wordBreak: "break-word" }}>
                  {exp.company}
                  {exp.location ? `, ${exp.location}` : ""}
                </span>
                <span style={{ whiteSpace: "nowrap" }}>{exp.dates}</span>
              </div>

              <div
                style={{
                  fontStyle: "italic",
                  marginTop: "4px",
                  fontSize: "14px",
                }}
              >
                {exp.title}
              </div>

              <ul
                style={{
                  marginTop: "6px",
                  paddingLeft: "18px",
                  fontSize: "14px",
                  lineHeight: 1.45,
                }}
              >
                {(exp.bullets || []).map((b, idx) => (
                  <li key={idx}>{b}</li>
                ))}
              </ul>
            </div>
          ))}
        </div>
      )}

      {/* SKILLS */}
      {(finalCV.skills || finalCV.languages) && (
        <div style={{ marginTop: "18px" }}>
          <div className="section-title" style={sectionTitleStyle}>
            {t(L, "skills")}
          </div>

          {finalCV.languages && (
            <p style={{ marginBottom: "8px", fontSize: "14px" }}>
              <strong>{t(L, "languages")}</strong> {finalCV.languages}
            </p>
          )}

          {finalCV.skills && (
            <div
              style={{
                display: "grid",
                gridTemplateColumns: "1fr 1fr",
                gap: "6px",
                fontSize: "14px",
              }}
            >
              {finalCV.skills.map((skill, i) => (
                <div
                  key={i}
                  style={{
                    display: "flex",
                    gap: "8px",
                    alignItems: "flex-start",
                    minWidth: 0,
                  }}
                >
                  <span
                    style={{
                      width: "8px",
                      height: "8px",
                      borderRadius: "50%",
                      background: "#0f172a",
                      marginTop: "6px",
                      flexShrink: 0,
                    }}
                  />
                  <span style={{ minWidth: 0, wordBreak: "break-word" }}>
                    {skill}
                  </span>
                </div>
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  );
};
