// src/components/cvbuilder/StepProcess.jsx
import React from "react";
import { Copy, CheckCircle, ArrowLeft, ArrowRight } from "lucide-react";

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
    title: "Paste AI JSON",
    desc: "Copy the JSON block from ChatGPT and paste it here.",
    ph: 'Paste JSON here... { "name": "..." }',
    create: "Create CV",
    back: "Back to edit data",
  },
  ar: {
    title: "لصق كود الـ AI",
    desc: "هات الكود (JSON Block) اللي ChatGPT طلعه وحطه هنا.",
    ph: 'الصق JSON هنا... { "name": "..." }',
    create: "إنشاء السي في",
    back: "العودة لتعديل البيانات",
  },
  es: {
    title: "Pegar JSON de IA",
    desc: "Copia el bloque JSON de ChatGPT y pégalo aquí.",
    ph: 'Pega el JSON aquí... { "name": "..." }',
    create: "Crear CV",
    back: "Volver a editar datos",
  },
};

const t = (lang, key) => {
  const L = normLang(lang);
  return (I18N[L] && I18N[L][key]) || I18N.en[key] || key;
};

export function StepProcess({
  lang = "en",
  pastedJson,
  setPastedJson,
  onImportJson,
  onBackToInput,
}) {
  const L = normLang(lang);
  const dir = dirForLang(L);
  const BackIcon = isRTL(L) ? ArrowRight : ArrowLeft;

  return (
    <div
      dir={dir}
      className="bg-white p-5 sm:p-8 rounded-2xl shadow-lg text-center animate-[fadeIn_0.3s] max-w-3xl mx-auto border border-slate-100"
    >
      <div className="w-16 h-16 sm:w-20 sm:h-20 bg-purple-100 text-purple-600 rounded-full flex items-center justify-center mx-auto mb-5 sm:mb-6">
        <Copy size={32} className="sm:hidden" />
        <Copy size={40} className="hidden sm:block" />
      </div>

      <h2 className="text-2xl sm:text-3xl font-black mb-2 text-slate-800">
        {t(L, "title")}
      </h2>
      <p className="text-slate-500 mb-5 sm:mb-6 text-sm sm:text-base">
        {t(L, "desc")}
      </p>

      <textarea
        value={pastedJson}
        onChange={(e) => setPastedJson(e.target.value)}
        placeholder={t(L, "ph")}
        className="w-full h-64 p-4 sm:p-5 border-2 border-dashed border-purple-300 rounded-2xl bg-purple-50/50 text-left font-mono text-sm mb-5 sm:mb-6 focus:border-purple-600 focus:bg-white outline-none transition"
        dir="ltr"
      />

      <button
        onClick={onImportJson}
        type="button"
        className="w-full py-4 bg-green-600 text-white rounded-xl font-bold text-lg sm:text-xl shadow-xl hover:bg-green-700 transition flex items-center justify-center gap-3 active:scale-[0.99]"
      >
        <CheckCircle size={26} /> {t(L, "create")}
      </button>

      <button
        onClick={onBackToInput}
        type="button"
        className="mt-5 sm:mt-6 text-slate-500 hover:text-slate-700 text-sm font-semibold inline-flex items-center gap-2 underline"
      >
        <BackIcon size={16} /> {t(L, "back")}
      </button>
    </div>
  );
}

export default StepProcess;
