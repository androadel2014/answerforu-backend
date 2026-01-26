// src/components/cvbuilder/PromptModal.jsx
import React from "react";
import { X, Copy, Bot } from "lucide-react";

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
    ready: "Ready to send!",
    close: "Close",
    stepsTitle: "Prompt is ready. Follow these steps:",
    s1: "Text was copied automatically (or tap Copy manually).",
    s2: 'Tap "Open ChatGPT" below.',
    s3: "Paste in the chat there.",
    s4: "Copy the JSON result and come back here.",
    preview: "View text (optional)",
    copyManual: "Copy manually",
    openChatGPT: "Open ChatGPT",
  },
  ar: {
    ready: "جاهز للإرسال!",
    close: "إغلاق",
    stepsTitle: "تم تجهيز الأوامر. اتبع الخطوات:",
    s1: "تم نسخ النص تلقائياً (أو اضغط نسخ يدوياً).",
    s2: 'اضغط زر "فتح ChatGPT" بالأسفل.',
    s3: "اعمل لصق (Paste) في الشات هناك.",
    s4: "انسخ الكود اللي الـ AI هيطلعه وارجع هنا.",
    preview: "عرض النص (اختياري)",
    copyManual: "نسخ يدوياً",
    openChatGPT: "فتح ChatGPT",
  },
  es: {
    ready: "¡Listo para enviar!",
    close: "Cerrar",
    stepsTitle: "El prompt está listo. Sigue estos pasos:",
    s1: "El texto se copió automáticamente (o toca Copiar manualmente).",
    s2: 'Toca "Abrir ChatGPT" abajo.',
    s3: "Pega el texto en el chat allí.",
    s4: "Copia el resultado JSON y vuelve aquí.",
    preview: "Ver texto (opcional)",
    copyManual: "Copiar manualmente",
    openChatGPT: "Abrir ChatGPT",
  },
};

const t = (lang, key) => {
  const L = normLang(lang);
  return (I18N[L] && I18N[L][key]) || I18N.en[key] || key;
};

export function PromptModal({
  show,
  copyStatus,
  generatedPrompt,
  onClose,
  onCopyManual,
  onOpenChatGPT,
  lang = "en",
}) {
  if (!show) return null;

  const L = normLang(lang);
  const dir = dirForLang(L);

  const ok = (() => {
    const s = String(copyStatus || "").toLowerCase();
    // accept old Arabic messages or English/Spanish equivalents
    return (
      s.includes("تلقائ") ||
      s.includes("copied automatically") ||
      s.includes("copiado autom") ||
      s.includes("✅")
    );
  })();

  return (
    <div
      dir={dir}
      className="fixed inset-0 bg-black/70 backdrop-blur-sm flex items-end sm:items-center justify-center z-50 p-3 sm:p-4 overscroll-contain"
      role="dialog"
      aria-modal="true"
      aria-label={t(L, "ready")}
    >
      <div className="bg-white rounded-2xl p-4 sm:p-6 w-full max-w-2xl shadow-2xl border border-gray-200">
        <div className="flex justify-between items-start gap-3 mb-4 sm:mb-6">
          <h3 className="text-xl sm:text-2xl font-black text-slate-800">
            {t(L, "ready")}
          </h3>
          <button
            onClick={onClose}
            type="button"
            aria-label={t(L, "close")}
            className="shrink-0 bg-gray-100 p-2 rounded-full hover:bg-red-100 text-gray-500 hover:text-red-500 transition active:scale-[0.98]"
          >
            <X size={20} />
          </button>
        </div>

        <div
          className={`mb-4 sm:mb-6 p-3 sm:p-4 rounded-xl text-center font-bold border text-sm sm:text-base ${
            ok
              ? "bg-green-50 border-green-200 text-green-700"
              : "bg-yellow-50 border-yellow-200 text-yellow-700"
          }`}
        >
          {copyStatus}
        </div>

        <p className="mb-3 text-slate-600 font-medium text-sm sm:text-base">
          {t(L, "stepsTitle")}
        </p>

        <ol className="list-decimal list-inside mb-5 sm:mb-6 space-y-2 text-sm text-slate-600 bg-slate-50 p-3 sm:p-4 rounded-xl border">
          <li>{t(L, "s1")}</li>
          <li>{t(L, "s2")}</li>
          <li>{t(L, "s3")}</li>
          <li>{t(L, "s4")}</li>
        </ol>

        <details className="mb-5 sm:mb-6">
          <summary className="cursor-pointer text-sm font-bold text-slate-600">
            {t(L, "preview")}
          </summary>
          <pre className="mt-3 p-3 bg-slate-50 border rounded-xl text-xs overflow-auto whitespace-pre-wrap break-words text-left">
            {generatedPrompt}
          </pre>
        </details>

        <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
          <button
            onClick={onCopyManual}
            type="button"
            className="py-3 bg-slate-200 text-slate-800 rounded-xl font-bold hover:bg-slate-300 flex items-center justify-center gap-2 active:scale-[0.99]"
          >
            <Copy size={18} /> {t(L, "copyManual")}
          </button>

          <button
            onClick={onOpenChatGPT}
            type="button"
            className="py-3 bg-green-600 text-white rounded-xl font-bold hover:bg-green-700 flex items-center justify-center gap-2 shadow-lg shadow-green-200 active:scale-[0.99]"
          >
            <Bot size={20} /> {t(L, "openChatGPT")}
          </button>
        </div>
      </div>
    </div>
  );
}

// ✅ عشان مايتكسرش لو حد بيستورد default
export default PromptModal;
