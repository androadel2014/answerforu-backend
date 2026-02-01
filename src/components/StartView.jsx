// src/components/StartView.jsx
import React, { useMemo } from "react";
import {
  FileText,
  Briefcase,
  DollarSign,
  AlertTriangle,
  ArrowRight,
} from "lucide-react";
import { useNavigate } from "react-router-dom";

const normLang = (v) => {
  const s = String(v || "en").toLowerCase();
  if (s.startsWith("ar")) return "ar";
  if (s.startsWith("es")) return "es";
  return "en";
};
const getDir = (lang) => (normLang(lang) === "ar" ? "rtl" : "ltr");

export const StartView = ({ lang = "en" }) => {
  const navigate = useNavigate();

  const LKEY = normLang(lang);

  const T = {
    ar: {
      title: "دليل المهاجر في أمريكا",
      subtitle: "معلومات منظمة مش خطوات إجبارية، اختار اللي يهمك",
      essentialsTitle: "الأساسيات في أمريكا",
      essentialsDesc: "معلومات عامة تساعدك تفهم الحياة اليومية في أمريكا",
      papersTitle: "الأوراق والـ IDs",
      papersDesc: "SSN، State ID، رخصة القيادة وكل ما يخص الأوراق",
      workTitle: "الشغل والدخل",
      workDesc: "أنواع الشغل والفرق بين W2 و 1099",
      moneyTitle: "الفلوس والضرايب",
      moneyDesc: "حسابات بنكية، كريدت، وضرايب",
      mistakesTitle: "أخطاء شائعة",
      mistakesDesc: "غلطات بتقابل أغلب المهاجرين",
    },
    en: {
      title: "Immigrant Guide to the U.S.",
      subtitle:
        "Organized information, not mandatory steps. Choose what you need.",
      essentialsTitle: "U.S. Essentials",
      essentialsDesc: "General information about daily life in the U.S.",
      papersTitle: "Documents & IDs",
      papersDesc: "SSN, State ID, driver license and official documents",
      workTitle: "Work & Income",
      workDesc: "Job types and W2 vs 1099 explained",
      moneyTitle: "Money Basics",
      moneyDesc: "Banking, credit and taxes",
      mistakesTitle: "Common Mistakes",
      mistakesDesc: "Common mistakes immigrants make",
    },
    es: {
      title: "Guía del inmigrante en EE. UU.",
      subtitle:
        "Información organizada, no pasos obligatorios. Elige lo que necesitas.",
      essentialsTitle: "Esenciales en EE. UU.",
      essentialsDesc: "Información general sobre la vida diaria en EE. UU.",
      papersTitle: "Documentos e IDs",
      papersDesc: "SSN, ID estatal, licencia de conducir y documentos",
      workTitle: "Trabajo e ingresos",
      workDesc: "Tipos de empleo y W2 vs 1099",
      moneyTitle: "Dinero básico",
      moneyDesc: "Banca, crédito e impuestos",
      mistakesTitle: "Errores comunes",
      mistakesDesc: "Errores comunes que cometen los inmigrantes",
    },
  };

  const L = T[LKEY] || T.en;

  const sections = useMemo(
    () => [
      {
        id: "essentials",
        title: L.essentialsTitle,
        desc: L.essentialsDesc,
        icon: FileText,
        to: "/essentials",
      },
      {
        id: "papers",
        title: L.papersTitle,
        desc: L.papersDesc,
        icon: FileText,
        to: "/papers",
      },
      {
        id: "work",
        title: L.workTitle,
        desc: L.workDesc,
        icon: Briefcase,
        to: "/work",
      },
      {
        id: "money",
        title: L.moneyTitle,
        desc: L.moneyDesc,
        icon: DollarSign,
        to: "/money",
      },
      {
        id: "mistakes",
        title: L.mistakesTitle,
        desc: L.mistakesDesc,
        icon: AlertTriangle,
        to: "/mistakes",
      },
    ],
    [
      L.essentialsTitle,
      L.essentialsDesc,
      L.papersTitle,
      L.papersDesc,
      L.workTitle,
      L.workDesc,
      L.moneyTitle,
      L.moneyDesc,
      L.mistakesTitle,
      L.mistakesDesc,
    ]
  );

  const dir = getDir(LKEY);

  return (
    <div
      className="px-4 sm:px-6 lg:px-12 py-10 sm:py-12 max-w-7xl mx-auto"
      dir={dir}
    >
      {/* Header */}
      <div className="mb-8 sm:mb-12">
        <h1 className="text-2xl sm:text-3xl lg:text-4xl font-black text-slate-900 mb-3 sm:mb-4">
          {L.title}
        </h1>
        <p className="text-slate-500 max-w-2xl">{L.subtitle}</p>
      </div>

      {/* Sections */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-5 sm:gap-6">
        {sections.map((item) => (
          <button
            key={item.id}
            type="button"
            onClick={() => navigate(item.to)}
            className={[
              "group bg-white p-6 rounded-3xl border border-slate-100 shadow-sm hover:shadow-xl hover:border-blue-100 transition-all cursor-pointer",
              dir === "rtl" ? "text-right" : "text-left",
            ].join(" ")}
          >
            <div className="flex items-start justify-between mb-6">
              <div className="w-14 h-14 bg-blue-100 text-blue-600 rounded-2xl flex items-center justify-center group-hover:scale-110 transition-transform">
                <item.icon size={26} />
              </div>

              <ArrowRight
                size={18}
                className={[
                  "text-slate-400 group-hover:text-blue-600",
                  dir === "rtl" ? "rotate-180" : "",
                ].join(" ")}
              />
            </div>

            <h3 className="text-lg sm:text-xl font-bold text-slate-900 mb-2">
              {item.title}
            </h3>
            <p className="text-slate-500 text-sm font-medium">{item.desc}</p>
          </button>
        ))}
      </div>
    </div>
  );
};
