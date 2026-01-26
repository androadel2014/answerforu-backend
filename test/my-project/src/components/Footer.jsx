// src/components/Footer.jsx
import React from "react";
import { Compass, Twitter, Facebook, Instagram } from "lucide-react";
import { useNavigate } from "react-router-dom";

const normLang = (v) => {
  const s = String(v || "en").toLowerCase();
  if (s.startsWith("ar")) return "ar";
  if (s.startsWith("es")) return "es";
  return "en";
};
const isRTL = (lang) => normLang(lang) === "ar";
const dirForLang = (lang) => (isRTL(lang) ? "rtl" : "ltr");

export const Footer = ({ lang = "en" }) => {
  const navigate = useNavigate();

  const go = (id) => {
    if (id === "home") navigate("/");
    else navigate(`/${id}`);
  };

  const T = {
    ar: {
      about:
        "منصتك الأولى للنجاح في أمريكا. معلومات موثوقة، أدوات عملية، ومجتمع داعم.",
      sections: "الأقسام",
      roadmap: "خارطة الطريق",
      jobs: "العمل والوظائف",
      housing: "السكن",
      support: "مساعدة",
      faq: "الأسئلة الشائعة",
      contact: "اتصل بنا",
      privacy: "سياسة الخصوصية",
      newsletter: "انضم للنشرة",
      email: "البريد الإلكتروني",
      subscribe: "اشترك",
      footer: "© 2024 Newcomer Guide. Built for the community.",
    },
    en: {
      about:
        "Your #1 platform for success in the USA. Trusted info, practical tools, and a supportive community.",
      sections: "Sections",
      roadmap: "Roadmap",
      jobs: "Jobs",
      housing: "Housing",
      support: "Support",
      faq: "FAQ",
      contact: "Contact Us",
      privacy: "Privacy Policy",
      newsletter: "Newsletter",
      email: "Email Address",
      subscribe: "Subscribe",
      footer: "© 2024 Newcomer Guide. Built for the community.",
    },
    es: {
      about:
        "Tu plataforma #1 para triunfar en EE. UU. Información confiable, herramientas prácticas y una comunidad de apoyo.",
      sections: "Secciones",
      roadmap: "Ruta",
      jobs: "Trabajo",
      housing: "Vivienda",
      support: "Soporte",
      faq: "Preguntas frecuentes",
      contact: "Contáctanos",
      privacy: "Política de privacidad",
      newsletter: "Boletín",
      email: "Correo electrónico",
      subscribe: "Suscribirse",
      footer: "© 2024 Newcomer Guide. Built for the community.",
    },
  };

  const L = T[normLang(lang)] || T.en;
  const dir = dirForLang(lang);

  const iconPos = dir === "rtl" ? "right-0" : "left-0";
  const inputPad = dir === "rtl" ? "pr-12" : "pl-12";

  return (
    <footer
      dir={dir}
      className="bg-slate-900 text-slate-400 pt-12 sm:pt-14 pb-8 mt-auto border-t border-slate-800"
    >
      <div className="container mx-auto px-4 sm:px-6 lg:px-12">
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-10 mb-12">
          {/* Brand */}
          <div className="col-span-1">
            <button
              type="button"
              onClick={() => go("home")}
              className="flex items-center gap-2 text-white mb-4"
            >
              <Compass size={24} className="text-blue-500" />
              <span className="text-xl font-bold">Newcomer</span>
            </button>

            <p className="text-sm leading-relaxed mb-6">{L.about}</p>

            <div className="flex gap-4">
              <Twitter
                size={20}
                className="hover:text-blue-400 cursor-pointer transition-colors"
              />
              <Facebook
                size={20}
                className="hover:text-blue-600 cursor-pointer transition-colors"
              />
              <Instagram
                size={20}
                className="hover:text-pink-500 cursor-pointer transition-colors"
              />
            </div>
          </div>

          {/* Sections */}
          <div>
            <h4 className="text-white font-bold mb-6">{L.sections}</h4>
            <ul className="space-y-3 text-sm">
              <li>
                <button
                  type="button"
                  onClick={() => go("start")}
                  className="hover:text-white transition-colors"
                >
                  {L.roadmap}
                </button>
              </li>
              <li>
                <button
                  type="button"
                  onClick={() => go("jobs")}
                  className="hover:text-white transition-colors"
                >
                  {L.jobs}
                </button>
              </li>
              <li>
                <button
                  type="button"
                  onClick={() => go("housing")}
                  className="hover:text-white transition-colors"
                >
                  {L.housing}
                </button>
              </li>
            </ul>
          </div>

          {/* Support */}
          <div>
            <h4 className="text-white font-bold mb-6">{L.support}</h4>
            <ul className="space-y-3 text-sm">
              <li className="hover:text-white transition-colors">{L.faq}</li>
              <li className="hover:text-white transition-colors">
                {L.contact}
              </li>
              <li className="hover:text-white transition-colors">
                {L.privacy}
              </li>
            </ul>
          </div>

          {/* Newsletter */}
          <div>
            <h4 className="text-white font-bold mb-6">{L.newsletter}</h4>
            <div className="flex flex-col gap-3">
              <div className="relative">
                <input
                  type="email"
                  placeholder={L.email}
                  className={classNames(
                    "w-full bg-slate-800 border-none rounded-xl px-4 py-3 text-sm text-white focus:ring-2 focus:ring-blue-500 outline-none",
                    inputPad
                  )}
                />
                <span
                  className={classNames(
                    "absolute top-1/2 -translate-y-1/2 text-slate-500 text-sm",
                    iconPos,
                    "px-4"
                  )}
                >
                  @
                </span>
              </div>

              <button
                type="button"
                className="bg-blue-600 text-white rounded-xl px-4 py-3 text-sm font-bold hover:bg-blue-700 transition-colors"
              >
                {L.subscribe}
              </button>
            </div>
          </div>
        </div>

        <div className="border-t border-slate-800 pt-8 text-center text-xs font-medium">
          <p>{L.footer}</p>
        </div>
      </div>
    </footer>
  );
};

const classNames = (...arr) => arr.filter(Boolean).join(" ");
