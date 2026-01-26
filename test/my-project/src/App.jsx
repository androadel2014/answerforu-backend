// src/App.jsx
import React, { useState, useEffect, useMemo } from "react";
import { Toaster } from "react-hot-toast";
import ProfilePage from "./components/ProfilePage";
import { initLang, applyLang } from "./lib/lang";

import {
  BrowserRouter as Router,
  Routes,
  Route,
  Navigate,
  useLocation,
  useParams,
} from "react-router-dom";

import CommunityView from "./components/community/CommunityView";
import ItemDetailsView from "./components/ItemDetailsView";

import { Sidebar } from "./components/Sidebar";
import { Header } from "./components/Header";
import { HomeView } from "./components/HomeView";
import { HomeFeedView } from "./components/feed/HomeFeedView";

import CVBuilderView from "./components/cvbuilder/CVBuilderView";
import { CVEditView } from "./components/CVEditView";
import { AuthView } from "./components/AuthView";
import { ProfileView } from "./components/ProfileView";
import { DetailPage } from "./components/DetailPage";
import { Footer } from "./components/Footer";
import { MobileNav, MobileMenuOverlay } from "./components/MobileNav";
import { StartView } from "./components/StartView";

function ScrollToTop() {
  const { pathname } = useLocation();
  useEffect(() => window.scrollTo(0, 0), [pathname]);
  return null;
}

function DetailPageWrapper({ lang }) {
  const { pageId } = useParams();

  const knownPages = [
    "home",
    "cv_builder",
    "cv_edit",
    "auth",
    "profile",
    "start",
    "feed",
    "community",
  ];

  if (knownPages.includes(pageId)) return null;
  return <DetailPage page={pageId} lang={lang} />;
}

function ProtectedRoute({ children }) {
  const token = localStorage.getItem("token");
  if (!token) return <Navigate to="/auth" replace />;
  return children;
}

function AppContent() {
  const [lang, setLang] = useState(() => localStorage.getItem("lang") || "ar");
  const [menuOpen, setMenuOpen] = useState(false);
  const location = useLocation();

  // ✅ init language once (also sets document.dir + i18n language)
  useEffect(() => {
    const l = initLang();
    setLang(l);
  }, []);

  // ✅ close mobile menu whenever route changes (fix overlay staying open)
  useEffect(() => {
    setMenuOpen(false);
  }, [location.pathname]);

  // ✅ currentPage ذكي
  const currentPage = useMemo(() => {
    const p = location.pathname || "/";
    if (p === "/" || p === "/home") return "home";
    if (p.startsWith("/u/")) return "u";
    if (p === "/community" || p.startsWith("/community/")) return "community";
    return p.startsWith("/") ? p.substring(1) : p;
  }, [location.pathname]);

  // ✅ language setter used by Header
  const setAppLang = (l) => {
    applyLang(l);
    setLang(l);
  };

  return (
    <div
      className="flex min-h-screen bg-[#f8fafc]"
      dir={lang === "ar" ? "rtl" : "ltr"}
    >
      <ScrollToTop />

      <Toaster
        position="top-center"
        reverseOrder={false}
        containerStyle={{ zIndex: 999999 }}
        toastOptions={{
          duration: 2600,
          style: {
            background: "#ffffff",
            color: "#0f172a",
            border: "1px solid #e5e7eb",
            borderRadius: "16px",
            boxShadow:
              "0 10px 25px rgba(0,0,0,0.08), 0 2px 10px rgba(0,0,0,0.06)",
            fontFamily: "inherit",
            padding: "12px 14px",
            maxWidth: "420px",
          },
          success: {
            duration: 2200,
            style: { border: "1px solid rgba(16,185,129,0.25)" },
          },
          error: {
            duration: 3200,
            style: { border: "1px solid rgba(239,68,68,0.25)" },
          },
        }}
      />

      <Sidebar lang={lang} page={currentPage} />

      <div
        className={[
          "flex-1 flex flex-col min-h-screen w-full min-w-0",
          lang === "ar" ? "lg:mr-72" : "lg:ml-72",
        ].join(" ")}
      >
        <Header lang={lang} setLang={setAppLang} />

        {/* ✅ IMPORTANT: pb-28 for mobile nav spacing */}
        <main className="flex-1 w-full min-w-0 pb-28 lg:pb-0 safe-bottom">
          <Routes>
            <Route path="/" element={<HomeView lang={lang} />} />
            <Route path="/home" element={<Navigate to="/" replace />} />

            <Route path="/auth" element={<AuthView lang={lang} />} />
            <Route path="/start" element={<StartView lang={lang} />} />

            <Route path="/feed" element={<HomeFeedView />} />

            <Route path="/u/:userId" element={<ProfilePage lang={lang} />} />

            <Route path="/community" element={<CommunityView lang={lang} />} />

            <Route
              path="/marketplace/item/:id"
              element={<ItemDetailsView lang={lang} />}
            />

            <Route
              path="/cv_builder"
              element={
                <ProtectedRoute>
                  <CVBuilderView lang={lang} />
                </ProtectedRoute>
              }
            />

            <Route
              path="/cv_edit"
              element={
                <ProtectedRoute>
                  <CVEditView lang={lang} />
                </ProtectedRoute>
              }
            />

            <Route
              path="/profile"
              element={
                <ProtectedRoute>
                  <ProfileView lang={lang} />
                </ProtectedRoute>
              }
            />

            <Route
              path="/:pageId"
              element={<DetailPageWrapper lang={lang} />}
            />

            <Route path="*" element={<Navigate to="/" replace />} />
          </Routes>
        </main>

        <Footer lang={lang} />
      </div>
      <MobileNav lang={lang} toggleMenu={() => setMenuOpen(true)} />

      <MobileMenuOverlay
        lang={lang}
        isOpen={menuOpen}
        close={() => setMenuOpen(false)}
      />
    </div>
  );
}

export default function App() {
  return (
    <Router>
      <AppContent />
    </Router>
  );
}
