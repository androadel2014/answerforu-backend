// src/components/Header.jsx
import React, { useEffect, useMemo, useRef, useState } from "react";
import {
  Compass,
  Search,
  Globe,
  User,
  LogOut,
  ChevronDown,
} from "lucide-react";
import { Link, useNavigate } from "react-router-dom";

const normLang = (v) => {
  const s = String(v || "en").toLowerCase();
  if (s.startsWith("ar")) return "ar";
  if (s.startsWith("es")) return "es";
  return "en";
};
const isRTL = (lang) => normLang(lang) === "ar";
const dirForLang = (lang) => (isRTL(lang) ? "rtl" : "ltr");

const DICT = {
  ar: {
    searchPlaceholder: "ابحث عن أي شيء...",
    myAccount: "حسابي",
    logout: "خروج",
    signIn: "دخول",
  },
  en: {
    searchPlaceholder: "Search anything...",
    myAccount: "My Account",
    logout: "Logout",
    signIn: "Sign In",
  },
  es: {
    searchPlaceholder: "Buscar...",
    myAccount: "Mi cuenta",
    logout: "Salir",
    signIn: "Iniciar sesión",
  },
};

const LANGS = [
  { code: "ar", label: "العربية" },
  { code: "en", label: "English" },
  { code: "es", label: "Español" },
];

export const Header = ({ lang = "en", setLang }) => {
  const navigate = useNavigate();
  const LKEY = normLang(lang);
  const dir = dirForLang(LKEY);
  const L = DICT[LKEY] || DICT.en;

  const [user, setUser] = useState(null);

  // dropdown state
  const [open, setOpen] = useState(false);
  const ddRef = useRef(null);

  const currentLangLabel = useMemo(() => {
    return LANGS.find((x) => x.code === LKEY)?.label || "English";
  }, [LKEY]);

  const syncUser = () => {
    const token = localStorage.getItem("token");
    const savedUser = localStorage.getItem("user");

    if (token && savedUser) {
      try {
        setUser(JSON.parse(savedUser));
      } catch {
        setUser(null);
      }
    } else {
      setUser(null);
    }
  };

  useEffect(() => {
    syncUser();

    const onStorage = (e) => {
      if (e.key === "token" || e.key === "user") syncUser();
    };
    const onAuthChanged = () => syncUser();

    window.addEventListener("storage", onStorage);
    window.addEventListener("auth_changed", onAuthChanged);

    return () => {
      window.removeEventListener("storage", onStorage);
      window.removeEventListener("auth_changed", onAuthChanged);
    };
  }, []);

  // close dropdown on outside click / esc
  useEffect(() => {
    const onDown = (e) => {
      if (e.key === "Escape") setOpen(false);
    };
    const onClick = (e) => {
      if (!ddRef.current) return;
      if (!ddRef.current.contains(e.target)) setOpen(false);
    };
    window.addEventListener("keydown", onDown);
    window.addEventListener("mousedown", onClick);
    return () => {
      window.removeEventListener("keydown", onDown);
      window.removeEventListener("mousedown", onClick);
    };
  }, []);

  const handleLogout = () => {
    localStorage.removeItem("token");
    localStorage.removeItem("user");
    window.dispatchEvent(new Event("auth_changed"));
    navigate("/auth", { replace: true });
  };

  return (
    <header
      dir={dir}
      className="h-20 sm:h-24 flex items-center justify-between px-4 sm:px-6 lg:px-12 pt-3 sm:pt-4"
    >
      {/* Mobile logo */}
      <div className="lg:hidden flex items-center gap-2 min-w-0">
        <div className="bg-blue-600 text-white p-1.5 rounded-lg shrink-0">
          <Compass size={20} />
        </div>
        <span className="font-black text-xl text-slate-800 tracking-tight truncate">
          Newcomer
        </span>
      </div>

      {/* Search (desktop) */}
      <div className="hidden lg:flex items-center bg-white border border-slate-200 px-5 py-3.5 rounded-2xl w-96 shadow-sm">
        <Search size={20} className="text-slate-400" />
        <input
          type="text"
          placeholder={L.searchPlaceholder}
          className="bg-transparent border-none outline-none w-full mx-3 text-slate-700 placeholder-slate-400 font-medium"
        />
      </div>

      <div className="flex items-center gap-2 sm:gap-3">
        {/* Language dropdown */}
        <div className="relative" ref={ddRef}>
          <button
            type="button"
            onClick={() => setOpen((v) => !v)}
            className={[
              "flex items-center gap-2 px-3 py-2.5 bg-white rounded-xl border border-slate-200",
              "shadow-sm hover:shadow transition active:scale-[0.99]",
              open ? "ring-2 ring-blue-200 border-blue-200" : "",
            ].join(" ")}
            aria-haspopup="menu"
            aria-expanded={open}
          >
            <Globe size={18} className="text-blue-600" />
            <span className="text-sm font-extrabold text-slate-800 hidden sm:inline">
              {currentLangLabel}
            </span>
            <span className="text-sm font-extrabold text-slate-800 sm:hidden">
              {LKEY.toUpperCase()}
            </span>
            <ChevronDown
              size={16}
              className={[
                "text-slate-500 transition",
                open ? "rotate-180" : "",
              ].join(" ")}
            />
          </button>

          {open && (
            <div
              className={[
                "absolute mt-2 w-44 rounded-2xl border border-slate-200 bg-white shadow-xl overflow-hidden z-[9999]",
                dir === "rtl" ? "right-0" : "left-0",
              ].join(" ")}
              role="menu"
            >
              {LANGS.map((x) => {
                const active = x.code === LKEY;
                return (
                  <button
                    key={x.code}
                    type="button"
                    onClick={() => {
                      setLang?.(x.code);
                      setOpen(false);
                    }}
                    className={[
                      "w-full flex items-center justify-between px-4 py-3 text-sm",
                      "hover:bg-slate-50 transition",
                      active
                        ? "bg-blue-50 text-blue-700 font-extrabold"
                        : "text-slate-700 font-bold",
                    ].join(" ")}
                    role="menuitem"
                  >
                    <span>{x.label}</span>
                    {active && (
                      <span className="text-[11px] font-black">✓</span>
                    )}
                  </button>
                );
              })}
            </div>
          )}
        </div>

        {/* User */}
        {user ? (
          <div className="flex items-center gap-2 sm:gap-3">
            <Link
              to="/profile"
              className={[
                "flex items-center gap-3 bg-white border border-slate-200 p-1.5 rounded-2xl",
                dir === "rtl" ? "pl-4" : "pr-4",
              ].join(" ")}
            >
              <div
                className={[
                  "flex flex-col",
                  dir === "rtl"
                    ? "items-start text-left"
                    : "items-end text-right",
                ].join(" ")}
              >
                <span className="text-[10px] text-slate-400 font-bold uppercase">
                  {L.myAccount}
                </span>
                <span className="text-sm font-black text-slate-800 max-w-[140px] truncate">
                  {user.username}
                </span>
              </div>
              <div className="w-10 h-10 bg-blue-50 text-blue-600 rounded-xl flex items-center justify-center shrink-0">
                <User size={20} />
              </div>
            </Link>

            <button
              onClick={handleLogout}
              type="button"
              className="p-3 text-slate-400 hover:text-red-500 hover:bg-red-50 rounded-xl active:scale-[0.99]"
              title={L.logout}
            >
              <LogOut size={20} />
            </button>
          </div>
        ) : (
          <Link
            to="/auth"
            className="flex items-center gap-2 px-4 sm:px-6 py-2.5 bg-blue-600 text-white rounded-xl font-bold active:scale-[0.99]"
          >
            <User size={18} />
            <span className="text-sm">{L.signIn}</span>
          </Link>
        )}
      </div>
    </header>
  );
};
