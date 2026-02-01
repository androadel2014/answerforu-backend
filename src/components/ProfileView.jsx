// هتتلغي

// src/components/ProfileView.jsx  (FULL FILE - copy/paste)
// ✅ 3 languages (AR/EN/ES) + RTL/LTR
// ✅ Profile edit stays هنا
// ✅ CVs moved to ONE component: CVManagerSection (reused by ProfilePageBody tab)

import React, { useEffect, useState } from "react";
import { User, Edit2, MapPin, Info } from "lucide-react";
import toast from "react-hot-toast";
import { useNavigate } from "react-router-dom";

import CVManagerSection from "./profile/CVManagerSection";

/* =========================
   i18n
========================= */
const STR = {
  ar: {
    loading: "جارٍ التحميل...",
    saveFailed: "فشل الحفظ",
    updated: "تم تحديث البيانات",
    connErr: "خطأ في الاتصال",
    editProfile: "تعديل البروفايل",
    saveChanges: "حفظ التغييرات",
    fullName: "الاسم الكامل",
    phone: "رقم الهاتف",
    address: "العنوان",
    bio: "نبذة شخصية",
  },
  en: {
    loading: "Loading...",
    saveFailed: "Save failed",
    updated: "Profile Updated",
    connErr: "Connection error",
    editProfile: "Edit Profile",
    saveChanges: "Save Changes",
    fullName: "Full Name",
    phone: "Phone Number",
    address: "Address",
    bio: "Bio",
  },
  es: {
    loading: "Cargando...",
    saveFailed: "Falló guardar",
    updated: "Perfil actualizado",
    connErr: "Error de conexión",
    editProfile: "Editar perfil",
    saveChanges: "Guardar cambios",
    fullName: "Nombre completo",
    phone: "Teléfono",
    address: "Dirección",
    bio: "Bio",
  },
};

const t = (lang, key) => (STR[lang] || STR.en)[key] || STR.en[key] || key;
const getDir = (lang) => (lang === "ar" ? "rtl" : "ltr");

export const ProfileView = ({ lang = "en" }) => {
  const navigate = useNavigate();
  const dir = getDir(lang);

  const API_BASE =
    import.meta.env.VITE_API_BASE_URL ||
    import.meta.env.VITE_API_BASE ||
    "http://localhost:5000";

  // =========================
  // Auth helpers
  // =========================
  const getToken = () =>
    localStorage.getItem("token") ||
    localStorage.getItem("authToken") ||
    localStorage.getItem("jwt") ||
    "";

  const authHeaders = (isJson = true) => {
    const token = getToken();
    return {
      ...(isJson ? { "Content-Type": "application/json" } : {}),
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
    };
  };

  const hardLogout = () => {
    localStorage.removeItem("token");
    localStorage.removeItem("authToken");
    localStorage.removeItem("jwt");
    localStorage.removeItem("user");
    window.dispatchEvent(new Event("auth_changed"));
    navigate("/auth", { replace: true });
  };

  const fetchFirstOk = async (urls, opts) => {
    let lastErr = null;
    for (const u of urls) {
      try {
        const res = await fetch(u, opts);
        if (res.status === 401) return { __unauth: true };
        const json = await res.json().catch(() => ({}));
        if (res.ok) return { ok: true, json };
        lastErr = json || { message: "Request failed" };
      } catch (e) {
        lastErr = e;
      }
    }
    return { ok: false, error: lastErr };
  };

  const pickUserObj = (payload) => {
    const p = payload || {};
    const u =
      p?.profile ||
      p?.user_profile ||
      p?.user ||
      p?.data?.profile ||
      p?.data?.user ||
      p?.data ||
      p ||
      {};

    const username =
      u.username ||
      u.display_name ||
      u.displayName ||
      u.full_name ||
      u.fullName ||
      "";

    const phone =
      u.phone ||
      u.phone_number ||
      u.phoneNumber ||
      u.mobile ||
      u.mobile_number ||
      "";

    const address =
      u.location || u.address || u.address_line || u.addressLine || "";

    const bio = u.bio || u.about || u.summary || "";

    return { ...(u || {}), username, phone, address, bio };
  };

  // =========================
  // State
  // =========================
  const [loading, setLoading] = useState(true);
  const [isEditing, setIsEditing] = useState(false);

  const [user, setUser] = useState(null);
  const [editData, setEditData] = useState({
    username: "",
    phone: "",
    address: "",
    bio: "",
  });

  // =========================
  // Load ME
  // =========================
  useEffect(() => {
    const token = getToken();
    if (!token) {
      setLoading(false);
      return hardLogout();
    }

    const init = async () => {
      setLoading(true);

      // hydrate from localStorage first
      const savedUser = localStorage.getItem("user");
      if (savedUser) {
        try {
          const obj = JSON.parse(savedUser);
          const meObj = pickUserObj(obj);
          setUser(meObj);
          setEditData({
            username: meObj.username || "",
            phone: meObj.phone || "",
            address: meObj.address || "",
            bio: meObj.bio || "",
          });
        } catch {}
      }

      try {
        const res = await fetchFirstOk(
          [
            `${API_BASE}/api/profile/me`,
            `${API_BASE}/api/me/profile`,
            `${API_BASE}/api/user/profile/me`,
            `${API_BASE}/api/users/me`,
            `${API_BASE}/api/user/me`,
            `${API_BASE}/api/me`,
          ],
          { headers: authHeaders(false) },
        );

        if (res.__unauth) return hardLogout();

        if (res.ok) {
          const meObj = pickUserObj(res.json);
          setUser(meObj);
          setEditData({
            username: meObj.username || "",
            phone: meObj.phone || "",
            address: meObj.address || "",
            bio: meObj.bio || "",
          });
          localStorage.setItem("user", JSON.stringify(meObj));
        }
      } catch (e) {
        console.error("Failed to load ME", e);
      }

      setLoading(false);
    };

    init();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // =========================
  // Save profile
  // =========================
  const handleSave = async () => {
    try {
      const res = await fetchFirstOk(
        [
          `${API_BASE}/api/users/me`,
          `${API_BASE}/api/user/me`,
          `${API_BASE}/api/me`,
          `${API_BASE}/api/profile/me`,
          `${API_BASE}/api/me/profile`,
          `${API_BASE}/api/user/profile/me`,
        ],
        {
          method: "PUT",
          headers: authHeaders(true),
          body: JSON.stringify({
            username: editData.username,
            display_name: editData.username,
            full_name: editData.username,

            phone: editData.phone,
            phone_number: editData.phone,

            location: editData.address,
            address: editData.address,

            bio: editData.bio,
            about: editData.bio,
            summary: editData.bio,
          }),
        },
      );

      if (res.__unauth) return hardLogout();

      if (!res.ok) {
        toast.error(
          res?.error?.message || res?.error?.error || t(lang, "saveFailed"),
        );
        return;
      }

      const updated = pickUserObj(res.json) || { ...(user || {}), ...editData };
      localStorage.setItem("user", JSON.stringify(updated));
      setUser(updated);

      toast.success(t(lang, "updated"));
      setIsEditing(false);
    } catch {
      toast.error(t(lang, "connErr"));
    }
  };

  // =========================
  // Guard / Loading
  // =========================
  if (loading) {
    return (
      <div className="p-20 text-center font-black text-slate-400" dir={dir}>
        {t(lang, "loading")}
      </div>
    );
  }

  if (!getToken()) return null;

  // =========================
  // Render
  // =========================
  return (
    <div className="max-w-4xl mx-auto p-4 sm:p-6 relative" dir={dir}>
      {/* ===== Profile Card ===== */}
      <div className="bg-white rounded-[2.5rem] shadow-2xl overflow-hidden border border-slate-100">
        <div className="bg-gradient-to-r from-blue-600 to-indigo-700 p-8 sm:p-10 text-white flex flex-col md:flex-row justify-between items-center gap-6">
          <div className="flex items-center gap-5">
            <div className="w-20 h-20 bg-white/20 rounded-2xl flex items-center justify-center border border-white/30">
              <User size={40} />
            </div>
            <div>
              <h2 className="text-3xl font-black">{user?.username || ""}</h2>
              <p className="opacity-80 font-medium">{user?.email || ""}</p>
            </div>
          </div>

          <button
            onClick={() => (isEditing ? handleSave() : setIsEditing(true))}
            className="bg-white text-blue-600 px-8 py-3 rounded-2xl font-black shadow-xl active:scale-95 transition-all"
          >
            {isEditing ? t(lang, "saveChanges") : t(lang, "editProfile")}
          </button>
        </div>

        <div className="p-6 sm:p-10 space-y-6">
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            <div className="space-y-2">
              <label className="text-xs font-black text-slate-400 uppercase tracking-widest flex items-center gap-2">
                <User size={14} /> {t(lang, "fullName")}
              </label>
              <input
                disabled={!isEditing}
                className="w-full p-4 bg-slate-50 rounded-2xl border-2 border-transparent focus:border-blue-500 outline-none transition-all font-bold text-slate-700 disabled:opacity-60"
                value={editData.username}
                onChange={(e) =>
                  setEditData({ ...editData, username: e.target.value })
                }
              />
            </div>

            <div className="space-y-2">
              <label className="text-xs font-black text-slate-400 uppercase tracking-widest flex items-center gap-2">
                <Edit2 size={14} /> {t(lang, "phone")}
              </label>
              <input
                disabled={!isEditing}
                className="w-full p-4 bg-slate-50 rounded-2xl border-2 border-transparent focus:border-blue-500 outline-none transition-all font-bold text-slate-700 disabled:opacity-60"
                value={editData.phone}
                onChange={(e) =>
                  setEditData({ ...editData, phone: e.target.value })
                }
              />
            </div>

            <div className="space-y-2 md:col-span-2">
              <label className="text-xs font-black text-slate-400 uppercase tracking-widest flex items-center gap-2">
                <MapPin size={14} /> {t(lang, "address")}
              </label>
              <input
                disabled={!isEditing}
                className="w-full p-4 bg-slate-50 rounded-2xl border-2 border-transparent focus:border-blue-500 outline-none transition-all font-bold text-slate-700 disabled:opacity-60"
                value={editData.address}
                onChange={(e) =>
                  setEditData({ ...editData, address: e.target.value })
                }
              />
            </div>

            <div className="space-y-2 md:col-span-2">
              <label className="text-xs font-black text-slate-400 uppercase tracking-widest flex items-center gap-2">
                <Info size={14} /> {t(lang, "bio")}
              </label>
              <textarea
                disabled={!isEditing}
                rows="3"
                className="w-full p-4 bg-slate-50 rounded-2xl border-2 border-transparent focus:border-blue-500 outline-none transition-all font-bold text-slate-700 disabled:opacity-60 resize-none"
                value={editData.bio}
                onChange={(e) =>
                  setEditData({ ...editData, bio: e.target.value })
                }
              />
            </div>
          </div>
        </div>
      </div>

      {/* ===== CVs (ONE shared component) ===== */}
      <div className="mt-6">
        <CVManagerSection lang={lang} />
      </div>
    </div>
  );
};

export default ProfileView;
