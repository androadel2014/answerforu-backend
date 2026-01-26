// src/components/DetailPage.jsx
import React from "react";
import { DATA } from "./Data";
import { CheckCircle, Clock, Star } from "lucide-react";

const normLang = (v) => {
  const s = String(v || "en").toLowerCase();
  if (s.startsWith("ar")) return "ar";
  if (s.startsWith("es")) return "es";
  return "en";
};
const isRTL = (lang) => normLang(lang) === "ar";
const dirForLang = (lang) => (isRTL(lang) ? "rtl" : "ltr");

function pickLabel(label, lang) {
  if (!label) return "";
  if (typeof label === "string") return label;
  const L = normLang(lang);
  return (
    label?.[L] ||
    label?.en ||
    label?.ar ||
    label?.es ||
    Object.values(label || {})[0] ||
    ""
  );
}

function pickArray(v, lang) {
  if (!v) return [];
  if (Array.isArray(v)) return v;
  const L = normLang(lang);
  const arr = v?.[L] || v?.en || v?.ar || v?.es;
  return Array.isArray(arr) ? arr : [];
}

export const DetailPage = ({ page, lang = "en" }) => {
  const L = normLang(lang);
  const dir = dirForLang(L);

  const info = DATA.nav.find((i) => i.id === page) || DATA.nav[0];

  const content = DATA.sections_content?.[page] || {
    title: info.label,
    desc: { ar: "قريبًا", en: "Soon", es: "Pronto" },
    steps: [],
    tips: { ar: [], en: [], es: [] },
  };

  const badgeLabel = pickLabel(info.label, L);
  const title = pickLabel(content.title, L);
  const desc = pickLabel(content.desc, L);

  const emptyTitle =
    L === "ar"
      ? "جاري إضافة المحتوى"
      : L === "es"
      ? "Contenido en preparación"
      : "Content is being added";

  const emptyDesc =
    L === "ar"
      ? "نعمل على تجهيز هذا القسم بأفضل المعلومات."
      : L === "es"
      ? "Estamos preparando esta sección con la mejor información."
      : "We’re preparing this section with the best information.";

  const tipsTitle =
    L === "ar" ? "نصائح ذهبية" : L === "es" ? "Consejos Pro" : "Pro Tips";

  const tips = pickArray(content.tips, L);

  return (
    <div
      dir={dir}
      className="px-4 sm:px-6 lg:px-12 pb-28 lg:pb-12 max-w-5xl mx-auto animate-[fadeIn_0.3s_ease-out]"
    >
      <div className="py-8 sm:py-10 lg:py-16">
        <div className="inline-flex items-center gap-2 px-4 py-2 rounded-full bg-white border border-slate-200 text-slate-600 text-sm font-bold mb-5 sm:mb-6 shadow-sm max-w-full">
          <span className={`w-2 h-2 rounded-full ${info.color}`}></span>
          <span className="truncate max-w-[70vw] sm:max-w-none">
            {badgeLabel}
          </span>
        </div>

        <h1 className="text-3xl sm:text-4xl lg:text-5xl font-black text-slate-900 mb-5 sm:mb-6 leading-tight">
          {title}
        </h1>

        <p className="text-base sm:text-lg lg:text-xl text-slate-500 max-w-2xl leading-relaxed">
          {desc}
        </p>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6 sm:gap-8">
        <div className="lg:col-span-2 space-y-6 sm:space-y-8">
          {content.steps && content.steps.length > 0 ? (
            content.steps.map((step, idx) => {
              const stepTitle = pickLabel(step.title, L);
              const items = pickArray(step.items, L);

              return (
                <div
                  key={idx}
                  className="bg-white rounded-3xl p-5 sm:p-8 border border-slate-100 shadow-sm hover:shadow-md transition-shadow relative overflow-hidden"
                >
                  <div
                    className={`
                      absolute top-0 w-24 h-24 bg-slate-50 rounded-bl-full -mt-4
                      ${dir === "rtl" ? "left-0 -ml-4" : "right-0 -mr-4"}
                    `}
                  />

                  <div className="relative z-10">
                    <div className="flex items-center gap-4 mb-5 sm:mb-6">
                      <div className="w-10 h-10 rounded-full bg-slate-900 text-white flex items-center justify-center font-bold text-lg">
                        {idx + 1}
                      </div>
                      <h3 className="text-lg sm:text-xl font-bold text-slate-900">
                        {stepTitle}
                      </h3>
                    </div>

                    <ul className="space-y-3 sm:space-y-4">
                      {items.map((item, i) => (
                        <li key={i} className="flex items-start gap-3">
                          <CheckCircle
                            size={20}
                            className="text-emerald-500 shrink-0 mt-0.5"
                          />
                          <span className="text-slate-600 font-medium text-sm sm:text-base">
                            {item}
                          </span>
                        </li>
                      ))}
                    </ul>
                  </div>
                </div>
              );
            })
          ) : (
            <div className="bg-white rounded-3xl p-10 sm:p-12 text-center border border-slate-100">
              <div className="w-16 h-16 bg-slate-50 rounded-full flex items-center justify-center mx-auto mb-4 text-slate-400">
                <Clock size={32} />
              </div>
              <h3 className="text-lg font-bold text-slate-900">{emptyTitle}</h3>
              <p className="text-slate-500">{emptyDesc}</p>
            </div>
          )}
        </div>

        <div className="space-y-6">
          <div className="bg-gradient-to-br from-slate-900 to-slate-800 text-white rounded-3xl p-6 sm:p-8 shadow-xl relative overflow-hidden">
            <div
              className={`
                absolute top-0 w-32 h-32 bg-white/10 rounded-full blur-2xl -mt-10
                ${dir === "rtl" ? "left-0 -ml-10" : "right-0 -mr-10"}
              `}
            />

            <div className="relative z-10">
              <div className="flex items-center gap-2 mb-6 text-yellow-400">
                <Star size={20} fill="currentColor" />
                <span className="font-bold uppercase tracking-widest text-xs">
                  {tipsTitle}
                </span>
              </div>

              <ul className="space-y-4">
                {tips.map((tip, i) => (
                  <li
                    key={i}
                    className="flex gap-3 text-sm sm:text-base text-slate-300 leading-relaxed border-b border-white/10 pb-3 last:border-0 last:pb-0"
                  >
                    <span className="text-blue-400 font-bold">•</span>
                    {tip}
                  </li>
                ))}

                {!tips.length && (
                  <li className="text-sm text-slate-300 opacity-80">
                    {L === "ar"
                      ? "لا توجد نصائح حالياً."
                      : L === "es"
                      ? "No hay consejos por ahora."
                      : "No tips yet."}
                  </li>
                )}
              </ul>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};
