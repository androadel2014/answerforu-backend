import React, { useEffect, useMemo, useRef, useState } from "react";
import toast from "react-hot-toast";

const API_BASE =
  import.meta.env.VITE_API_BASE_URL ||
  import.meta.env.VITE_API_BASE ||
  "http://localhost:5000";

const classNames = (...arr) => arr.filter(Boolean).join(" ");

const CATEGORY_META = {
  general: {
    label: "General",
    badge: "bg-gray-100 text-gray-700 border-gray-200",
    dot: "bg-gray-400",
  },
  immigration: {
    label: "Immigration",
    badge: "bg-blue-50 text-blue-700 border-blue-200",
    dot: "bg-blue-500",
  },
  work: {
    label: "Work",
    badge: "bg-emerald-50 text-emerald-700 border-emerald-200",
    dot: "bg-emerald-500",
  },
  housing: {
    label: "Housing",
    badge: "bg-amber-50 text-amber-800 border-amber-200",
    dot: "bg-amber-500",
  },
  tax: {
    label: "Taxes",
    badge: "bg-purple-50 text-purple-700 border-purple-200",
    dot: "bg-purple-500",
  },
  questions: {
    label: "Questions",
    badge: "bg-indigo-50 text-indigo-700 border-indigo-200",
    dot: "bg-indigo-500",
  },
};
const getCategory = (key) => CATEGORY_META[key] || CATEGORY_META.general;

const getInitials = (name = "") => {
  const parts = String(name).trim().split(/\s+/).filter(Boolean);
  if (!parts.length) return "U";
  const a = (parts[0][0] || "").toUpperCase();
  const b = (parts[1]?.[0] || "").toUpperCase();
  return a + b || "U";
};

const absUrl = (u) => {
  const s = String(u || "").trim();
  if (!s) return "";
  if (s.startsWith("http://") || s.startsWith("https://")) return s;
  return `${API_BASE}${s.startsWith("/") ? "" : "/"}${s}`;
};

async function fetchMe(API_BASE, headers) {
  const urls = [
    `${API_BASE}/api/users/me`,
    `${API_BASE}/api/me`,
    `${API_BASE}/api/profile/me`,
    `${API_BASE}/api/auth/me`,
  ];

  let lastErr = null;
  for (const url of urls) {
    try {
      const res = await fetch(url, { headers });
      if (!res.ok) throw new Error(`me failed ${res.status}`);
      return await res.json();
    } catch (e) {
      lastErr = e;
      continue;
    }
  }
  throw lastErr || new Error("me failed");
}

export default function PostComposer({ onPosted }) {
  const token = localStorage.getItem("token");
  const isLoggedIn = !!token;
  const authHeaders = useMemo(
    () => (token ? { Authorization: `Bearer ${token}` } : {}),
    [token]
  );

  // ✅ بدل ما نعتمد على localStorage فقط
  const [me, setMe] = useState(() => {
    try {
      return JSON.parse(localStorage.getItem("user") || "null");
    } catch {
      return null;
    }
  });

  // ✅ هات بياناتي من السيرفر عشان avatar
  useEffect(() => {
    if (!token) return;

    (async () => {
      try {
        const data = await fetchMe(API_BASE, authHeaders);

        // السيرفر ممكن يرجّع {user: {...}} أو {...}
        const u = data?.user || data;

        if (u) {
          setMe((prev) => ({ ...(prev || {}), ...u }));
          // ✅ حدث localStorage عشان باقي الموقع يستفيد
          try {
            const prev =
              JSON.parse(localStorage.getItem("user") || "null") || {};
            localStorage.setItem("user", JSON.stringify({ ...prev, ...u }));
          } catch {}
        }
      } catch {
        // ignore (هتفضل initials)
      }
    })();
  }, [token, authHeaders]);

  const displayName =
    me?.username || me?.name || me?.fullName || me?.email || "You";

  const avatarRaw =
    me?.author_avatar ||
    me?.avatar_url ||
    me?.avatar ||
    me?.photo ||
    me?.image_url ||
    me?.image ||
    me?.profile_image ||
    "";

  const avatarSrc = absUrl(avatarRaw);

  const [avatarOk, setAvatarOk] = useState(true);
  useEffect(() => {
    setAvatarOk(true);
  }, [avatarSrc]);

  const [content, setContent] = useState("");
  const [category, setCategory] = useState("general");
  const [posting, setPosting] = useState(false);

  const fileInputRef = useRef(null);
  const [postImages, setPostImages] = useState([]); // File[]
  const [postImagePreviews, setPostImagePreviews] = useState([]); // string[]

  const pickPostImages = (filesList) => {
    const files = Array.from(filesList || []);
    if (!files.length) return;

    const nextFiles = [];
    const nextPreviews = [];

    for (const f of files) {
      if (!String(f.type || "").startsWith("image/")) {
        toast.error("Please choose image files only");
        continue;
      }
      if (f.size && f.size > 8 * 1024 * 1024) {
        toast.error("Image is too large (max 8MB)");
        continue;
      }
      nextFiles.push(f);
      nextPreviews.push(URL.createObjectURL(f));
    }

    const mergedFiles = [...postImages, ...nextFiles].slice(0, 10);
    const mergedPreviews = [...postImagePreviews, ...nextPreviews].slice(0, 10);

    if (postImagePreviews.length + nextPreviews.length > 10) {
      const extras = [...postImagePreviews, ...nextPreviews].slice(10);
      extras.forEach((u) => {
        try {
          URL.revokeObjectURL(u);
        } catch {}
      });
    }

    setPostImages(mergedFiles);
    setPostImagePreviews(mergedPreviews);

    if (fileInputRef.current) fileInputRef.current.value = "";
  };

  const removePickedImage = (idx) => {
    const u = postImagePreviews[idx];
    try {
      if (u) URL.revokeObjectURL(u);
    } catch {}

    setPostImages((arr) => arr.filter((_, i) => i !== idx));
    setPostImagePreviews((arr) => arr.filter((_, i) => i !== idx));
  };

  const clearPickedImages = () => {
    postImagePreviews.forEach((u) => {
      try {
        URL.revokeObjectURL(u);
      } catch {}
    });
    setPostImages([]);
    setPostImagePreviews([]);
    if (fileInputRef.current) fileInputRef.current.value = "";
  };

  useEffect(() => {
    return () => {
      postImagePreviews.forEach((u) => {
        try {
          URL.revokeObjectURL(u);
        } catch {}
      });
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const createPost = async () => {
    if (!isLoggedIn) return toast.error("Login first");
    if (!content.trim()) return toast.error("Write something");

    try {
      setPosting(true);

      if (postImages.length) {
        const fd = new FormData();
        fd.append("content", content.trim());
        fd.append("category", category);
        postImages.forEach((f) => fd.append("images", f));

        const res = await fetch(`${API_BASE}/api/posts`, {
          method: "POST",
          headers: { ...authHeaders },
          body: fd,
        });
        if (!res.ok) throw new Error("create post failed");
      } else {
        const res = await fetch(`${API_BASE}/api/posts`, {
          method: "POST",
          headers: { "Content-Type": "application/json", ...authHeaders },
          body: JSON.stringify({ content: content.trim(), category }),
        });
        if (!res.ok) throw new Error("create post failed");
      }

      setContent("");
      setCategory("general");
      clearPickedImages();

      toast.success("Posted");
      if (onPosted) onPosted();
    } catch (e) {
      console.error(e);
      toast.error("Post failed");
    } finally {
      setPosting(false);
    }
  };

  return (
    <div className="bg-white rounded-2xl shadow-sm border border-gray-100 p-4 mb-6">
      <div className="flex gap-3">
        <div className="w-11 h-11 rounded-full overflow-hidden bg-gray-900 text-white flex items-center justify-center font-bold shrink-0">
          {avatarSrc && avatarOk ? (
            <img
              src={avatarSrc}
              alt="avatar"
              className="w-full h-full object-cover"
              onError={() => setAvatarOk(false)}
            />
          ) : (
            getInitials(displayName) // AM
          )}
        </div>

        <div className="flex-1">
          <textarea
            value={content}
            onChange={(e) => setContent(e.target.value)}
            placeholder="✍️ احكي تجربتك، اسأل سؤال، أو شارك معلومة تساعد غيرك…"
            className="w-full resize-none outline-none bg-gray-50 border border-gray-200 rounded-2xl p-4 text-[15px] leading-6 focus:bg-white focus:border-gray-300 transition"
            rows={3}
          />

          <div className="mt-3 flex items-center gap-2 flex-wrap">
            <input
              ref={fileInputRef}
              type="file"
              accept="image/*"
              multiple
              onChange={(e) => pickPostImages(e.target.files)}
              className="hidden"
            />
            <button
              type="button"
              onClick={() => fileInputRef.current?.click()}
              className="px-4 py-2 rounded-xl text-sm font-semibold bg-white border border-gray-200 hover:bg-gray-50"
            >
              📷 Add photos {postImages.length ? `(${postImages.length})` : ""}
            </button>

            {postImages.length > 0 && (
              <button
                type="button"
                onClick={clearPickedImages}
                className="px-4 py-2 rounded-xl text-sm font-semibold bg-red-50 text-red-700 border border-red-200 hover:bg-red-100"
              >
                Remove all
              </button>
            )}

            <span className="text-xs text-gray-500">
              Up to 10 images, 8MB each.
            </span>
          </div>

          {postImagePreviews.length > 0 && (
            <div className="mt-3 grid grid-cols-3 sm:grid-cols-5 gap-2">
              {postImagePreviews.map((src, i) => (
                <div key={src} className="relative">
                  <img
                    src={src}
                    alt=""
                    className="w-full h-24 object-cover rounded-xl border border-gray-200"
                  />
                  <button
                    type="button"
                    onClick={() => removePickedImage(i)}
                    className="absolute top-1 right-1 w-7 h-7 rounded-full bg-black/70 text-white flex items-center justify-center text-xs hover:bg-black"
                    title="Remove"
                  >
                    ✕
                  </button>
                </div>
              ))}
            </div>
          )}

          <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-3 mt-3">
            <div className="flex items-center gap-2">
              <select
                value={category}
                onChange={(e) => setCategory(e.target.value)}
                className="bg-white border border-gray-200 rounded-xl px-3 py-2 text-sm shadow-sm"
              >
                <option value="general">General</option>
                <option value="questions">Questions</option>
                <option value="immigration">Immigration</option>
                <option value="work">Work</option>
                <option value="housing">Housing</option>
                <option value="tax">Taxes</option>
              </select>

              <span
                className={classNames(
                  "inline-flex items-center gap-2 px-3 py-2 rounded-xl text-xs border",
                  getCategory(category).badge
                )}
              >
                <span
                  className={classNames(
                    "w-2 h-2 rounded-full",
                    getCategory(category).dot
                  )}
                />
                {getCategory(category).label}
              </span>
            </div>

            <button
              type="button"
              onClick={createPost}
              disabled={posting}
              className={classNames(
                "rounded-xl px-5 py-2 font-semibold text-sm shadow-sm transition",
                posting
                  ? "bg-gray-200 text-gray-500 cursor-not-allowed"
                  : "bg-gray-900 text-white hover:bg-black"
              )}
            >
              {posting ? "Posting..." : "Post"}
            </button>
          </div>

          {!isLoggedIn && (
            <button
              type="button"
              onClick={() =>
                toast("Login to post, like, comment, save and share.")
              }
              className="mt-3 text-xs text-gray-600 underline"
            >
              You are browsing as a guest (click to know more)
            </button>
          )}
        </div>
      </div>
    </div>
  );
}
