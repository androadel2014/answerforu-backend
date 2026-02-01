// src/lib/lang.js
import i18n from "../i18n/index";

const isBrowser = typeof window !== "undefined";

export const SUPPORTED_LANGS = ["en", "ar", "es"];
export const DEFAULT_LANG = "en";

function safeLang(v) {
  const s = String(v || "")
    .toLowerCase()
    .trim();
  return SUPPORTED_LANGS.includes(s) ? s : DEFAULT_LANG;
}

export function applyLang(lang) {
  if (!isBrowser) return;

  const l = safeLang(lang);

  localStorage.setItem("lang", l);
  i18n.changeLanguage(l);

  document.documentElement.lang = l;
  document.documentElement.dir = l === "ar" ? "rtl" : "ltr";

  // optional hook for CSS/RTL tweaks
  document.documentElement.dataset.lang = l;
}

export function initLang() {
  if (!isBrowser) return DEFAULT_LANG;

  const stored = localStorage.getItem("lang");
  const detected = i18n?.language;
  const lang = safeLang(stored || detected || DEFAULT_LANG);

  applyLang(lang);
  return lang;
}
