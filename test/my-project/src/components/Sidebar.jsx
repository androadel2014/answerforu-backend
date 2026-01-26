// src/components/Sidebar.jsx
import React, { useEffect, useState, useMemo } from "react";
import { DATA } from "./Data";
import { Compass, ChevronLeft, LogOut, User, ChevronRight } from "lucide-react";
import { getDir } from "./utils";
import { Link, useNavigate, useLocation } from "react-router-dom";

const normLang = (v) => {
  const s = String(v || "en").toLowerCase();
  if (s.startsWith("ar")) return "ar";
  if (s.startsWith("es")) return "es";
  return "en";
};

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

// ✅ Robust active id from pathname (don’t rely on prop "page")
function activeIdFromPath(pathname) {
  const p = pathname || "/";

  if (p === "/" || p === "/home") return "home";
  if (p.startsWith("/cv_builder")) return "cv_builder";
  if (p.startsWith("/start")) return "start";
  if (p.startsWith("/jobs")) return "jobs";

  const clean = p.startsWith("/") ? p.slice(1) : p;
  const first = clean.split("/")[0] || "home";
  return first;
}

const DICT = {
  ar: {
    menu: "القائمة الرئيسية",
    myAccount: "حسابي",
    logout: "تسجيل الخروج",
    signIn: "تسجيل الدخول",
    guide: "Guide",
  },
  en: {
    menu: "Main Menu",
    myAccount: "My Account",
    logout: "Sign Out",
    signIn: "Sign In",
    guide: "Guide",
  },
  es: {
    menu: "Menú",
    myAccount: "Mi cuenta",
    logout: "Salir",
    signIn: "Iniciar sesión",
    guide: "Guide",
  },
};

export const Sidebar = ({ lang = "en", page }) => {
  const navigate = useNavigate();
  const { pathname } = useLocation();

  const LKEY = normLang(lang);
  const dir = getDir(LKEY); // "rtl" | "ltr"
  const T = DICT[LKEY] || DICT.en;

  // ✅ prefer pathname (mobile-like behavior). fallback to prop page if provided.
  const active = useMemo(
    () => page || activeIdFromPath(pathname),
    [page, pathname]
  );

  const [user, setUser] = useState(null);

  const readUser = () => {
    try {
      const savedUser = localStorage.getItem("user");
      return savedUser ? JSON.parse(savedUser) : null;
    } catch {
      return null;
    }
  };

  useEffect(() => {
    setUser(readUser());

    const onStorage = () => setUser(readUser());
    const onAuthChanged = () => setUser(readUser());

    window.addEventListener("storage", onStorage);
    window.addEventListener("auth_changed", onAuthChanged);

    return () => {
      window.removeEventListener("storage", onStorage);
      window.removeEventListener("auth_changed", onAuthChanged);
    };
  }, []);

  const handleLogout = () => {
    localStorage.removeItem("token");
    localStorage.removeItem("authToken");
    localStorage.removeItem("jwt");
    localStorage.removeItem("user");
    localStorage.removeItem("cv_user");
    localStorage.removeItem("me");

    setUser(null);
    window.dispatchEvent(new Event("auth_changed"));
    navigate("/auth", { replace: true });
  };

  const ActiveChevron = dir === "rtl" ? ChevronRight : ChevronLeft;

  return (
    <aside
      dir={dir}
      className={[
        "hidden lg:flex flex-col w-72 h-screen fixed top-0 bg-white/80 backdrop-blur-xl shadow-sm z-50",
        dir === "rtl"
          ? "right-0 border-l border-white/20"
          : "left-0 border-r border-white/20",
      ].join(" ")}
    >
      <div className="h-24 flex items-center px-8 border-b border-slate-100/50">
        <div className="flex items-center gap-3 min-w-0">
          <div className="bg-blue-600 text-white p-2.5 rounded-xl shadow-lg shadow-blue-200 shrink-0">
            <Compass size={28} />
          </div>
          <div className="min-w-0">
            <Link
              to="/"
              className="text-xl font-black text-slate-800 tracking-tight block truncate"
            >
              Newcomer
            </Link>
            <span className="text-xs font-semibold text-blue-600 uppercase tracking-widest">
              {T.guide}
            </span>
          </div>
        </div>
      </div>

      <div className="flex-1 overflow-y-auto px-4 py-6 space-y-2 custom-scrollbar">
        <div className="px-4 mb-2 text-xs font-bold text-slate-400 uppercase tracking-wider">
          {T.menu}
        </div>

        {DATA?.nav?.map((item) => {
          const isActive = active === item.id;
          const path = item.id === "home" ? "/" : `/${item.id}`;
          const label = pickLabel(item.label, LKEY);

          return (
            <Link
              key={item.id}
              to={path}
              className={[
                "w-full flex items-center gap-4 px-4 py-3.5 rounded-2xl transition-all duration-300 group",
                isActive
                  ? "bg-gradient-to-r from-blue-600 to-indigo-600 text-white shadow-lg shadow-blue-200 transform scale-[1.02]"
                  : "text-slate-500 hover:bg-white hover:text-slate-900 hover:shadow-sm",
              ].join(" ")}
            >
              <item.icon
                size={20}
                className={
                  isActive
                    ? "text-white"
                    : "text-slate-400 group-hover:text-blue-500"
                }
                strokeWidth={isActive ? 2.5 : 2}
              />

              <span className="text-sm font-bold truncate">{label}</span>

              {isActive && (
                <ActiveChevron size={16} className="opacity-60 ml-auto" />
              )}
            </Link>
          );
        })}
      </div>

      <div className="p-4 bg-slate-50/50 border-t border-slate-100 space-y-3">
        {user ? (
          <div className="space-y-2">
            <Link
              to="/profile"
              className="w-full flex items-center gap-3 px-4 py-3 bg-white border border-slate-100 rounded-2xl shadow-sm hover:border-blue-300 transition-all group"
            >
              <div className="w-9 h-9 bg-blue-50 text-blue-600 rounded-xl flex items-center justify-center group-hover:bg-blue-600 group-hover:text-white transition-all shrink-0">
                <User size={18} />
              </div>

              <div
                className={[
                  "flex-1 overflow-hidden",
                  dir === "rtl" ? "text-right" : "text-left",
                ].join(" ")}
              >
                <p className="text-[10px] text-slate-400 font-bold uppercase leading-none mb-1">
                  {T.myAccount}
                </p>
                <p className="text-sm font-black text-slate-700 truncate leading-none group-hover:text-blue-600">
                  {user.username}
                </p>
              </div>
            </Link>

            <button
              onClick={handleLogout}
              type="button"
              className="w-full flex items-center justify-center gap-2 py-3.5 bg-white text-red-500 border border-red-50 rounded-2xl font-bold text-xs hover:bg-red-50 hover:text-red-600 transition-all cursor-pointer shadow-sm active:scale-[0.99]"
            >
              <LogOut size={14} />
              {T.logout}
            </button>
          </div>
        ) : (
          <Link
            to="/auth"
            className="w-full py-4 bg-slate-900 text-white rounded-2xl font-bold text-sm hover:bg-slate-800 transition-all shadow-lg block text-center active:scale-[0.99]"
          >
            {T.signIn}
          </Link>
        )}
      </div>
    </aside>
  );
};
