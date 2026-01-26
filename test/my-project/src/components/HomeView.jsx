// src/components/HomeView.jsx
import React from "react";
import { Link, useNavigate } from "react-router-dom";
import { DATA } from "./Data";
import {
  Map,
  Sparkles,
  Building,
  Shield,
  Briefcase,
  ArrowRight,
} from "lucide-react";

const getDir = (lang) => (lang === "ar" ? "rtl" : "ltr");

function pickLabel(label, lang) {
  if (!label) return "";
  if (typeof label === "string") return label;
  return (
    label?.[lang] ||
    label?.en ||
    label?.ar ||
    label?.es ||
    Object.values(label || {})[0] ||
    ""
  );
}

export const HomeView = ({ lang }) => {
  const navigate = useNavigate();

  const T = {
    ar: {
      badge: "الدليل المحدث 2024",
      confidence: "بكل ثقة.",
      startJourney: "ابدأ رحلتك",
      cvBuilder: "مصمم الـ CV",
      browse: "تصفح كل الأقسام",
      statsTitle: "مجتمع ينمو كل يوم",
      stat1: "دليل شامل",
      stat2: "مستخدم مستفيد",
      stat3: "مجاني وموثوق",
      ready: "جاهز للبدء؟",
      cta: "انطلق الآن مجاناً",
      cvDesc: "أداة ذكية لصياغة وتنسيق السيرة الذاتية مجاناً",
    },
    en: {
      badge: "Updated 2024 Guide",
      confidence: "With Confidence.",
      startJourney: "Start Journey",
      cvBuilder: "CV Builder",
      browse: "Browse All Sections",
      statsTitle: "A Community Growing Every Day",
      stat1: "Comprehensive Guide",
      stat2: "Active User",
      stat3: "Free & Trusted",
      ready: "Ready to Start?",
      cta: "Start Now for Free",
      cvDesc: "AI tool to format your resume professionally",
    },
    es: {
      badge: "Guía actualizada 2024",
      confidence: "Con confianza.",
      startJourney: "Empieza tu camino",
      cvBuilder: "Creador de CV",
      browse: "Explorar secciones",
      statsTitle: "Una comunidad que crece cada día",
      stat1: "Guía completa",
      stat2: "Usuarios activos",
      stat3: "Gratis y confiable",
      ready: "¿Listo para empezar?",
      cta: "Empieza gratis ahora",
      cvDesc: "Herramienta para crear tu CV profesionalmente",
    },
  };

  const L = T[lang] || T.en;

  return (
    <div className="px-4 sm:px-6 lg:px-12 pb-28 lg:pb-12 max-w-7xl mx-auto">
      {/* Hero Section */}
      <div className="py-8 sm:py-10 lg:py-16 flex flex-col lg:flex-row items-center gap-10 lg:gap-12">
        <div className="flex-1 text-center lg:text-start z-10 w-full">
          <div className="inline-flex items-center gap-2 px-3 py-1 rounded-full bg-blue-100/50 border border-blue-200 text-blue-700 text-xs font-bold mb-6">
            <span className="relative flex h-2 w-2">
              <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-blue-400 opacity-75"></span>
              <span className="relative inline-flex rounded-full h-2 w-2 bg-blue-500"></span>
            </span>
            {L.badge}
          </div>

          <h1 className="text-4xl sm:text-5xl lg:text-7xl font-black text-slate-900 mb-5 sm:mb-6 tracking-tight leading-[1.1]">
            {pickLabel(DATA.hero.title, lang)} <br />
            <span className="text-transparent bg-clip-text bg-gradient-to-r from-blue-600 to-indigo-600">
              {L.confidence}
            </span>
          </h1>

          <p className="text-base sm:text-lg lg:text-xl text-slate-500 mb-8 sm:mb-10 max-w-xl mx-auto lg:mx-0 leading-relaxed font-medium">
            {pickLabel(DATA.hero.subtitle, lang)}
          </p>

          <div className="flex flex-col sm:flex-row gap-3 sm:gap-4 justify-center lg:justify-start">
            <button
              type="button"
              onClick={() => navigate("/start")}
              className="group bg-slate-900 text-white px-7 sm:px-8 py-4 rounded-2xl font-bold text-base sm:text-lg shadow-xl shadow-slate-200 hover:bg-slate-800 hover:scale-[1.02] transition-all flex items-center justify-center gap-3"
            >
              <Map
                size={20}
                className="group-hover:rotate-12 transition-transform"
              />
              {L.startJourney}
            </button>

            <Link
              to="/cv_builder"
              className="bg-white text-purple-700 border border-purple-200 px-7 sm:px-8 py-4 rounded-2xl font-bold text-base sm:text-lg hover:bg-purple-50 hover:border-purple-300 transition-all flex items-center justify-center gap-3"
            >
              <Sparkles size={20} />
              {L.cvBuilder}
            </Link>
          </div>
        </div>

        {/* Hero Graphic (hide on mobile) */}
        <div className="flex-1 relative hidden lg:block">
          <div className="grid grid-cols-2 gap-4 opacity-90 transform rotate-[-5deg] hover:rotate-0 transition-transform duration-700">
            <div className="bg-white p-6 rounded-3xl shadow-xl border border-slate-100 flex flex-col gap-3 animate-[float_4s_ease-in-out_infinite]">
              <div className="w-12 h-12 bg-rose-100 text-rose-600 rounded-2xl flex items-center justify-center">
                <Building size={24} />
              </div>
              <div className="h-2 w-20 bg-slate-100 rounded-full"></div>
              <div className="h-2 w-12 bg-slate-100 rounded-full"></div>
            </div>

            <div className="bg-blue-600 p-6 rounded-3xl shadow-xl shadow-blue-200 flex flex-col gap-3 mt-12 animate-[float_5s_ease-in-out_infinite]">
              <div className="w-12 h-12 bg-white/20 text-white rounded-2xl flex items-center justify-center">
                <Map size={24} />
              </div>
              <div className="h-2 w-20 bg-white/20 rounded-full"></div>
              <div className="h-2 w-16 bg-white/20 rounded-full"></div>
            </div>

            <div className="bg-white p-6 rounded-3xl shadow-xl border border-slate-100 flex flex-col gap-3 -mt-8 animate-[float_4.5s_ease-in-out_infinite]">
              <div className="w-12 h-12 bg-emerald-100 text-emerald-600 rounded-2xl flex items-center justify-center">
                <Shield size={24} />
              </div>
              <div className="h-2 w-24 bg-slate-100 rounded-full"></div>
            </div>

            <div className="bg-white p-6 rounded-3xl shadow-xl border border-slate-100 flex flex-col gap-3 animate-[float_3.5s_ease-in-out_infinite]">
              <div className="w-12 h-12 bg-amber-100 text-amber-600 rounded-2xl flex items-center justify-center">
                <Briefcase size={24} />
              </div>
              <div className="h-2 w-16 bg-slate-100 rounded-full"></div>
            </div>
          </div>
        </div>
      </div>

      {/* All Sections Grid */}
      <div className="mt-12 sm:mt-16">
        <div className="flex items-center gap-3 mb-6 sm:mb-8">
          <div className="h-8 w-1 bg-blue-600 rounded-full"></div>
          <h2 className="text-xl sm:text-2xl font-black text-slate-900">
            {L.browse}
          </h2>
        </div>

        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-5 sm:gap-6">
          {DATA.nav.slice(1).map((item) => (
            <button
              key={item.id}
              type="button"
              onClick={() => navigate(item.id === "home" ? "/" : `/${item.id}`)}
              className="text-left group bg-white p-6 lg:p-8 rounded-3xl border border-slate-100 shadow-sm hover:shadow-xl hover:border-blue-100 transition-all duration-300 cursor-pointer relative overflow-hidden"
            >
              <div
                className={`absolute top-0 right-0 w-32 h-32 ${item.color} opacity-5 rounded-bl-full -mr-6 -mt-6 transition-transform group-hover:scale-125`}
              ></div>

              <div className="flex items-start justify-between mb-6 relative z-10">
                <div
                  className={`w-14 h-14 ${
                    item?.color ?? ""
                  } bg-opacity-10 rounded-2xl flex items-center justify-center ${String(
                    item?.color ?? ""
                  ).replace(
                    "bg-",
                    "text-"
                  )} group-hover:scale-110 transition-transform duration-300`}
                >
                  <item.icon size={28} />
                </div>

                <div className="p-2 bg-slate-50 rounded-full text-slate-400 group-hover:bg-blue-600 group-hover:text-white transition-colors">
                  <ArrowRight
                    size={18}
                    className={getDir(lang) === "rtl" ? "rotate-180" : ""}
                  />
                </div>
              </div>

              <h3 className="text-lg sm:text-xl font-bold text-slate-900 mb-2 relative z-10">
                {pickLabel(item.label, lang)}
              </h3>

              <p className="text-slate-500 text-sm font-medium relative z-10 line-clamp-2">
                {pickLabel(DATA.sections_content?.[item.id]?.desc, lang) ||
                  (item.id === "cv_builder" ? L.cvDesc : "...")}
              </p>
            </button>
          ))}
        </div>
      </div>

      {/* Community Stats Section */}
      <div className="mt-14 sm:mt-20 bg-slate-900 rounded-3xl p-7 sm:p-8 lg:p-12 text-white text-center relative overflow-hidden">
        <div className="absolute top-0 right-0 w-64 h-64 bg-blue-600/30 rounded-full blur-3xl transform translate-x-1/3 -translate-y-1/3"></div>

        <div className="relative z-10">
          <h2 className="text-xl sm:text-2xl lg:text-3xl font-bold mb-7 sm:mb-8">
            {L.statsTitle}
          </h2>

          <div className="grid grid-cols-1 sm:grid-cols-3 gap-6 sm:gap-8">
            <div>
              <div className="text-4xl lg:text-5xl font-black text-blue-400 mb-2">
                50+
              </div>
              <div className="text-slate-400">{L.stat1}</div>
            </div>

            <div>
              <div className="text-4xl lg:text-5xl font-black text-emerald-400 mb-2">
                10k+
              </div>
              <div className="text-slate-400">{L.stat2}</div>
            </div>

            <div>
              <div className="text-4xl lg:text-5xl font-black text-purple-400 mb-2">
                100%
              </div>
              <div className="text-slate-400">{L.stat3}</div>
            </div>
          </div>
        </div>
      </div>

      {/* CTA Section */}
      <div className="mt-12 sm:mt-16 text-center">
        <h2 className="text-xl sm:text-2xl font-bold text-slate-900 mb-4">
          {L.ready}
        </h2>

        <button
          type="button"
          onClick={() => navigate("/start")}
          className="bg-blue-600 text-white px-9 sm:px-10 py-4 rounded-full font-bold text-base sm:text-lg hover:bg-blue-700 shadow-xl transition-all"
        >
          {L.cta}
        </button>
      </div>
    </div>
  );
};
