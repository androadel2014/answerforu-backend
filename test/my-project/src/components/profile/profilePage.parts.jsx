// src/components/profilePage.parts.jsx
import React, { useEffect, useMemo } from "react";
import { useNavigate } from "react-router-dom";
import toast from "react-hot-toast";
import {
  X,
  Star,
  Users,
  UserPlus,
  MessageCircle,
  Briefcase,
  Store,
  Trash2,
  ChevronDown,
  ChevronUp,
  ThumbsUp,
  MapPin,
  Phone,
  Link as LinkIcon,
} from "lucide-react";

// ✅ FIX PATH (this file is in src/components/, i18n is in src/i18n/)
import STR from "../../i18n/profile.strings";

/* =========================
   i18n + dir
========================= */
export const tt = (lang, key) => {
  const dict = (STR && (STR[lang] || STR.en)) || {};
  const en = (STR && STR.en) || {};
  const v = dict[key] ?? en[key];

  // ✅ fallbacks for new avatar UI (won't break if keys missing)
  if (v !== undefined) return v;

  const fallback = {
    avatar: lang === "ar" ? "الصورة" : lang === "es" ? "Avatar" : "Avatar",
    change: lang === "ar" ? "تغيير" : lang === "es" ? "Cambiar" : "Change",
    delete: lang === "ar" ? "حذف" : lang === "es" ? "Eliminar" : "Delete",
    deleteQ:
      lang === "ar"
        ? "متأكد إنك عايز تحذف؟"
        : lang === "es"
        ? "¿Seguro que deseas eliminar?"
        : "Are you sure you want to delete?",
  };

  return fallback[key] || key;
};

export const getDir = (lang) => (lang === "ar" ? "rtl" : "ltr");

/* =========================
   API / Auth
========================= */
export const getAPIBase = () =>
  import.meta.env.VITE_API_BASE_URL ||
  import.meta.env.VITE_API_BASE ||
  "http://localhost:5000";

export const authHeaders = () => {
  const token = localStorage.getItem("token");
  return token ? { Authorization: `Bearer ${token}` } : {};
};

export const isAuthed = () => !!localStorage.getItem("token");

export function classNames(...xs) {
  return xs.filter(Boolean).join(" ");
}

export const getInitials = (name = "") => {
  const parts = String(name).trim().split(/\s+/).filter(Boolean);
  if (!parts.length) return "U";
  const a = (parts[0][0] || "").toUpperCase();
  const b = (parts[1]?.[0] || "").toUpperCase();
  return a + b || "U";
};

export const safeUrl = (u) => {
  const s = String(u || "").trim();
  if (!s) return "";
  if (/^https?:\/\//i.test(s)) return s;
  return "https://" + s;
};

export const extractNumericId = (id) => {
  if (id === null || id === undefined) return null;
  const s = String(id);
  const m = s.match(/\d+$/);
  return m ? Number(m[0]) : null;
};

// ✅ robust current user id getter (if you store it)
export const getAuthUserId = () => {
  try {
    const direct = localStorage.getItem("userId");
    if (direct) return String(direct);

    const u = localStorage.getItem("user");
    if (!u) return null;
    const obj = JSON.parse(u);
    const id = obj?.id ?? obj?.user_id ?? obj?._id ?? obj?.uid ?? null;
    return id !== null && id !== undefined ? String(id) : null;
  } catch {
    return null;
  }
};

// ✅ normalize post id comparisons
export const getPostId = (p) =>
  p?.id ?? p?.post_id ?? p?._id ?? p?.postId ?? null;

export const normId = (v) => {
  if (v === null || v === undefined) return "";
  const n = extractNumericId(v);
  return n !== null ? String(n) : String(v);
};

// ✅ timestamp formatter
export const formatTime = (value) => {
  if (!value) return "";
  try {
    const s = String(value).trim();

    // لو جاي "YYYY-MM-DD HH:mm:ss" حوّله ISO
    const iso = s.includes(" ") && !s.includes("T") ? s.replace(" ", "T") : s;

    // لو مفيش timezone (لا Z ولا +hh:mm) اعتبره UTC
    const hasTZ = /Z$|[+-]\d{2}:\d{2}$/.test(iso);
    const d = new Date(hasTZ ? iso : iso + "Z");

    if (Number.isNaN(d.getTime())) return "";

    const abs = d.toLocaleString(undefined, {
      month: "short",
      day: "2-digit",
      year: "numeric",
      hour: "2-digit",
      minute: "2-digit",
    });

    const diff = Date.now() - d.getTime();
    const mins = Math.floor(diff / 60000);

    if (mins < 1) return `${abs}`;
    if (mins < 60) return `${mins}m • ${abs}`;
    const hrs = Math.floor(mins / 60);
    if (hrs < 24) return `${hrs}h • ${abs}`;
    const days = Math.floor(hrs / 24);
    return `${days}d • ${abs}`;
  } catch {
    return "";
  }
};

// ✅ category badge helper (keep as is)
export function getCategory(cat) {
  const key = String(cat || "General").trim() || "General";

  const map = {
    General: {
      label: "General",
      badge: "bg-gray-50 border-gray-200 text-gray-700",
      dot: "bg-gray-500",
    },
    Taxes: {
      label: "Taxes",
      badge: "bg-emerald-50 border-emerald-200 text-emerald-700",
      dot: "bg-emerald-500",
    },
    Housing: {
      label: "Housing",
      badge: "bg-indigo-50 border-indigo-200 text-indigo-700",
      dot: "bg-indigo-500",
    },
    Work: {
      label: "Work",
      badge: "bg-blue-50 border-blue-200 text-blue-700",
      dot: "bg-blue-500",
    },
    Immigration: {
      label: "Immigration",
      badge: "bg-amber-50 border-amber-200 text-amber-700",
      dot: "bg-amber-500",
    },
    Questions: {
      label: "Questions",
      badge: "bg-fuchsia-50 border-fuchsia-200 text-fuchsia-700",
      dot: "bg-fuchsia-500",
    },
  };

  return map[key] || map.General;
}

// ✅ confirm toast (localized)
export const toastConfirm = ({ lang = "en", title, confirmText } = {}) =>
  new Promise((resolve) => {
    toast.custom(
      (t) => (
        <div className="bg-white border border-gray-200 shadow-xl rounded-2xl px-4 py-3 w-[360px] max-w-[92vw]">
          <div className="flex items-start gap-3">
            <div className="w-9 h-9 rounded-full bg-red-50 text-red-600 flex items-center justify-center font-bold">
              !
            </div>
            <div className="flex-1">
              <div className="font-semibold text-gray-900">{title}</div>
              <div className="text-xs text-gray-500 mt-1">
                {tt(lang, "cannotUndo")}
              </div>

              <div className="flex items-center gap-2 mt-3 justify-end">
                <button
                  type="button"
                  className="px-3 py-1.5 rounded-xl text-sm font-semibold bg-gray-100 text-gray-800 hover:bg-gray-200"
                  onClick={() => {
                    toast.dismiss(t.id);
                    resolve(false);
                  }}
                >
                  {tt(lang, "cancel")}
                </button>
                <button
                  type="button"
                  className="px-3 py-1.5 rounded-xl text-sm font-semibold bg-red-600 text-white hover:bg-red-700"
                  onClick={() => {
                    toast.dismiss(t.id);
                    resolve(true);
                  }}
                >
                  {confirmText}
                </button>
              </div>
            </div>
          </div>
        </div>
      ),
      { duration: 999999 }
    );
  });

export async function tryFetch(url, options = {}) {
  const res = await fetch(url, options);
  let data = null;
  try {
    data = await res.json();
  } catch {}

  if (!res.ok) {
    const msg = data?.message || `Request failed (${res.status})`;
    const err = new Error(msg);
    err.status = res.status;
    err.data = data;
    throw err;
  }
  return data;
}

export async function tryFetchFallback(urls, options = {}) {
  let lastErr = null;

  for (const u of urls) {
    try {
      return await tryFetch(u, options);
    } catch (e) {
      lastErr = e;
      const s = e?.status;
      if (s === 404 || s === 500 || s === 401 || s === 403) continue;
      throw e;
    }
  }

  throw lastErr || new Error("Request failed");
}

export function Modal({ title, open, onClose, children, footer }) {
  useEffect(() => {
    if (!open) return;

    const body = document.body;
    const docEl = document.documentElement;

    const scrollY = window.scrollY || docEl.scrollTop || body.scrollTop || 0;

    const prevOverflow = body.style.overflow;
    const prevPosition = body.style.position;
    const prevTop = body.style.top;
    const prevWidth = body.style.width;

    // ✅ lock background scroll (no "scroll behind modal")
    body.style.overflow = "hidden";
    body.style.position = "fixed";
    body.style.top = `-${scrollY}px`;
    body.style.width = "100%";

    return () => {
      // ✅ restore
      body.style.overflow = prevOverflow;
      body.style.position = prevPosition;
      body.style.top = prevTop;
      body.style.width = prevWidth;

      // ✅ restore scroll position
      const y = Math.abs(parseInt(body.style.top || "0", 10)) || scrollY;
      window.scrollTo(0, y);
    };
  }, [open]);

  useEffect(() => {
    if (!open) return;
    const onKey = (e) => {
      if (e.key === "Escape") onClose?.();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [open, onClose]);

  if (!open) return null;

  return (
    <div className="fixed inset-0 z-50">
      <div
        className="absolute inset-0 bg-black/50"
        onClick={onClose}
        role="button"
        tabIndex={-1}
        aria-label="Close modal"
      />

      {/* ✅ modal container scrolls (not page behind) */}
      <div className="absolute inset-0 overflow-y-auto">
        <div className="min-h-full flex items-start justify-center p-4">
          <div className="relative w-full max-w-2xl rounded-2xl bg-white shadow-2xl border my-8">
            <div className="flex items-center justify-between px-5 py-4 border-b">
              <div className="font-semibold">{title}</div>
              <button
                onClick={onClose}
                className="p-2 rounded-xl hover:bg-gray-100"
                aria-label="Close"
              >
                <X size={18} />
              </button>
            </div>

            {/* ✅ content area can scroll if too tall */}
            <div className="p-5 max-h-[75vh] overflow-y-auto">{children}</div>

            {footer ? (
              <div className="px-5 py-4 border-t bg-white">{footer}</div>
            ) : null}
          </div>
        </div>
      </div>
    </div>
  );
}

/* =========================
   Helpers
========================= */
export const absUrl = (API_BASE, u) => {
  const s = String(u || "").trim();
  if (!s) return "";
  if (s.startsWith("http://") || s.startsWith("https://")) return s;
  return `${API_BASE}${s.startsWith("/") ? "" : "/"}${s}`;
};

// =========================
// Post update payload helpers (content + media)
// =========================
export const toArr = (v) => {
  if (!v) return [];
  if (Array.isArray(v)) return v.filter(Boolean);
  if (typeof v === "string") {
    const s = v.trim();
    if (!s) return [];
    if (s.startsWith("[")) {
      try {
        const a = JSON.parse(s);
        return Array.isArray(a) ? a.filter(Boolean) : [];
      } catch {
        return [];
      }
    }
    return [s].filter(Boolean);
  }
  return [];
};

export const uniq = (arr) => Array.from(new Set((arr || []).filter(Boolean)));

export const buildUpdateFormData = ({
  content,
  keepMedia = [],
  removeMedia = [],
  files = [],
}) => {
  const fd = new FormData();

  // ✅ نفس PostComposer
  fd.append("content", String(content || "").trim());

  // ✅ optional (لو السيرفر بيدعمهم)
  fd.append("keep_media_urls", JSON.stringify(uniq(keepMedia)));
  fd.append("remove_media_urls", JSON.stringify(uniq(removeMedia)));

  // ✅ أهم نقطة: ارفع الصور مرة واحدة وبنفس اسم الحقل
  const list = Array.isArray(files) ? files : Array.from(files || []);
  list.filter(Boolean).forEach((f) => fd.append("images", f)); // ✅ نفس PostComposer

  return fd;
};

export const normalizePostForMedia = (API_BASE, p) => {
  const toArray = (v) => {
    if (!v) return [];
    if (Array.isArray(v)) return v;
    if (typeof v === "string") {
      const s = v.trim();
      if (!s) return [];
      if (s.startsWith("[")) {
        try {
          const arr = JSON.parse(s);
          return Array.isArray(arr) ? arr : [];
        } catch {
          return [];
        }
      }
      return [s];
    }
    return [];
  };

  const abs = (u) => {
    const s = String(u || "").trim();
    if (!s) return "";
    if (s.startsWith("http://") || s.startsWith("https://")) return s;
    return `${API_BASE}${s.startsWith("/") ? "" : "/"}${s}`;
  };

  // ✅ collect ALL possible media keys coming from different endpoints
  let media = [
    ...toArray(p?.media),
    ...toArray(p?.media_urls),
    ...toArray(p?.media_url),
    ...toArray(p?.image_url),
    ...toArray(p?.image),
    ...toArray(p?.images),
  ].filter(Boolean);

  // ✅ if media_urls is a JSON string (very common)
  if (
    !media.length &&
    typeof p?.media_urls === "string" &&
    p.media_urls.trim()
  ) {
    try {
      const arr = JSON.parse(p.media_urls);
      if (Array.isArray(arr)) media = arr;
    } catch {}
  }

  media = media.map(abs).filter(Boolean);

  return { ...p, media, media_urls: null, media_url: null };
};

/* =========================
   ✅ FIX: comments ids
========================= */
export const normalizeFeedPostId = (raw) => {
  const s = String(raw ?? "").trim();
  if (!s) return null;

  if (s.startsWith("p_")) {
    const n = Number(s.slice(2));
    return Number.isFinite(n) && n > 0 ? n : null;
  }

  if (s.startsWith("pp_")) return null;

  const n = Number(s);
  return Number.isFinite(n) && n > 0 ? n : null;
};

/* =========================
   Small Components
========================= */
export function StatsPanel({
  lang,
  ratingAvg,
  ratingCount,
  followers,
  following,
  posts,
  services,
  products,
}) {
  const rounded = Math.round(Number(ratingAvg || 0));
  const safeAvg = Number.isFinite(Number(ratingAvg)) ? Number(ratingAvg) : 0;

  return (
    <div className="rounded-2xl border bg-gradient-to-b from-gray-50 to-white p-4 shadow-sm">
      <div className="rounded-2xl border bg-white p-3">
        <div className="flex items-center justify-between gap-3">
          <div className="flex items-center gap-3">
            <div className="w-11 h-11 rounded-2xl bg-gray-900 text-white flex items-center justify-center">
              <Star size={18} />
            </div>
            <div>
              <div className="text-xs text-gray-500">
                {tt(lang, "overallRating")}
              </div>
              <div className="text-lg font-extrabold leading-tight">
                {safeAvg.toFixed(1)}
              </div>
            </div>
          </div>

          <div className="text-right">
            <div className="flex items-center justify-end gap-1">
              {Array.from({ length: 5 }).map((_, i) => (
                <Star
                  key={i}
                  size={16}
                  className={i < rounded ? "text-yellow-500" : "text-gray-300"}
                />
              ))}
            </div>
            <div className="mt-1 text-xs text-gray-500">
              {ratingCount} {tt(lang, "review")}
              {ratingCount === 1 ? "" : "s"}
            </div>
          </div>
        </div>
      </div>

      <div className="mt-3 grid grid-cols-2 gap-3">
        <StatCard
          icon={<Users size={16} />}
          label="Followers"
          value={followers}
        />
        <StatCard
          icon={<UserPlus size={16} />}
          label="Following"
          value={following}
        />
      </div>

      <div className="mt-3 grid grid-cols-3 gap-3">
        <StatMini
          icon={<MessageCircle size={16} />}
          label={tt(lang, "posts")}
          value={posts}
        />
        <StatMini
          icon={<Briefcase size={16} />}
          label={tt(lang, "services")}
          value={services}
        />
        <StatMini
          icon={<Store size={16} />}
          label={tt(lang, "products")}
          value={products}
        />
      </div>

      <div className="mt-3 flex items-center justify-center">
        <span className="text-[11px] px-2 py-1 rounded-full bg-gray-100 border text-gray-600">
          Profile stats
        </span>
      </div>
    </div>
  );
}

export function StatCard({ icon, label, value }) {
  return (
    <div className="rounded-2xl border bg-white p-3 hover:shadow-sm transition">
      <div className="flex items-center gap-2 text-gray-700">
        <div className="w-9 h-9 rounded-xl bg-gray-50 border flex items-center justify-center">
          {icon}
        </div>
        <div className="text-xs font-semibold">{label}</div>
      </div>
      <div className="mt-2 text-xl font-extrabold text-gray-900">{value}</div>
    </div>
  );
}

export function StatMini({ icon, label, value }) {
  return (
    <div className="rounded-2xl border bg-white p-3 hover:bg-gray-50 transition">
      <div className="flex items-center justify-between gap-2">
        <div className="text-[11px] font-semibold text-gray-600">{label}</div>
        <div className="text-gray-500">{icon}</div>
      </div>
      <div className="mt-1 text-base font-extrabold text-gray-900">{value}</div>
    </div>
  );
}

export function TabPill({ active, onClick, icon, label }) {
  return (
    <button
      onClick={onClick}
      className={classNames(
        "px-3 py-2 rounded-xl border flex items-center gap-2 text-sm transition",
        active
          ? "bg-black text-white border-black"
          : "bg-white hover:bg-gray-50"
      )}
    >
      {icon}
      {label}
    </button>
  );
}

export function Field({ label, value, onChange, placeholder }) {
  return (
    <div>
      <label className="text-sm font-medium">{label}</label>
      <input
        className="mt-1 w-full border rounded-2xl p-3 outline-none focus:ring-2 focus:ring-black/10"
        value={value}
        onChange={(e) => onChange(e.target.value)}
        placeholder={placeholder}
      />
    </div>
  );
}

/* =========================
   Comments (Feed-like)
========================= */
export const VISUAL_MAX_DEPTH = 6;
export const INDENT_PX = 16;

export function normalizeComment(c) {
  if (!c) return null;
  const id = c.id ?? c.comment_id ?? c._id;
  const parentId = c.parent_id ?? c.parentId ?? c.reply_to ?? c.replyTo ?? null;
  const postId = c.post_id ?? c.postId ?? c.threadKey ?? c.postID ?? null;

  const authorId = c.author_id ?? c.user_id ?? c.userId ?? c.uid ?? null;

  return {
    ...c,
    id,
    post_id: postId,
    parent_id: parentId,
    author_id: authorId,
    content:
      c.content ?? c.text ?? c.comment ?? c.body ?? c.message ?? c.value ?? "",
    author_name:
      c.author_name ??
      c.user_name ??
      c.username ??
      c.author ??
      c.name ??
      "User",
    author_avatar:
      c.author_avatar ?? c.avatar_url ?? c.avatar ?? c.user_avatar ?? "",
    created_at: c.created_at ?? c.createdAt ?? c.time ?? c.timestamp ?? null,
    likes_count: c.likes_count ?? c.likesCount ?? c.likes ?? c.likeCount ?? 0,
    is_liked: !!(c.is_liked ?? c.isLiked ?? c.liked),
    can_delete: !!(c.can_delete ?? c.canDelete),
  };
}

export function buildCommentTree(flat = []) {
  const map = new Map();
  const roots = [];

  (Array.isArray(flat) ? flat : []).forEach((raw) => {
    const c = normalizeComment(raw);
    if (!c || !c.id) return;
    map.set(c.id, { ...c, replies: [] });
  });

  map.forEach((node) => {
    if (node.parent_id && map.has(node.parent_id)) {
      map.get(node.parent_id).replies.push(node);
    } else {
      roots.push(node);
    }
  });

  const sortByTime = (a, b) => {
    const ta = new Date(a.created_at || 0).getTime();
    const tb = new Date(b.created_at || 0).getTime();
    return ta - tb;
  };

  const sortDeep = (arr) => {
    arr.sort(sortByTime);
    arr.forEach((x) => sortDeep(x.replies || []));
  };

  sortDeep(roots);
  return roots;
}

export function CommentNode({
  lang,
  node,
  depth,
  showRepliesMap,
  onToggleReplies,
  onReply,
  onDelete,
  onLike,
  canAct,
}) {
  const id = node.id;
  const d = Math.min(depth, VISUAL_MAX_DEPTH);
  const marginLeft = d * INDENT_PX;

  const hasReplies = Array.isArray(node.replies) && node.replies.length > 0;
  const showReplies = showRepliesMap[id] ?? true;

  const authorName = node.author_name || "User";
  const avatar = node.author_avatar || "";

  const API_BASE = getAPIBase();
  const avatarSrc = absUrl(API_BASE, avatar);

  const [avatarOk, setAvatarOk] = React.useState(true);
  React.useEffect(() => {
    setAvatarOk(true);
  }, [avatarSrc]);

  const meId = getAuthUserId();
  const nodeAuthorId =
    node.author_id ?? node.user_id ?? node.userId ?? node.uid ?? null;

  const canDelete =
    !!node.can_delete ||
    (canAct &&
      meId &&
      nodeAuthorId !== null &&
      String(nodeAuthorId) === String(meId)) ||
    authorName === "You";

  const likes = Number(node.likes_count ?? 0) || 0;
  const liked = !!node.is_liked;

  return (
    <div style={{ marginLeft }} className="relative">
      {depth > 0 ? (
        <div className="absolute left-0 top-0 bottom-0 w-[2px] bg-gray-200 rounded-full" />
      ) : null}

      <div className={classNames("flex gap-2", depth > 0 ? "pl-3" : "")}>
        <div className="w-9 h-9 rounded-full overflow-hidden bg-gray-900 text-white flex items-center justify-center font-bold shrink-0">
          {avatarSrc && avatarOk ? (
            <img
              src={avatarSrc}
              alt="avatar"
              className="w-full h-full object-cover"
              onError={() => setAvatarOk(false)}
            />
          ) : (
            getInitials(authorName)
          )}
        </div>

        <div className="flex-1">
          <div className="bg-white border rounded-2xl px-4 py-3">
            <div className="flex items-start justify-between gap-2">
              <div className="font-semibold text-sm">{authorName}</div>
              <div className="text-xs text-gray-500">
                {formatTime(node.created_at) || ""}
              </div>
            </div>

            <div className="mt-1 text-sm whitespace-pre-wrap text-gray-900">
              {node.content}
            </div>

            <div className="mt-2 flex items-center gap-3 text-xs text-gray-600">
              <button
                onClick={() =>
                  canAct
                    ? onLike(id, liked)
                    : toast.error(tt(lang, "loginFirst"))
                }
                className={classNames(
                  "inline-flex items-center gap-1 hover:text-gray-900",
                  liked ? "text-gray-900 font-semibold" : ""
                )}
              >
                <ThumbsUp size={14} />
                {tt(lang, "like")} {likes ? `(${likes})` : ""}
              </button>

              <button
                onClick={() =>
                  canAct ? onReply(id) : toast.error(tt(lang, "loginFirst"))
                }
                className="hover:text-gray-900"
              >
                {tt(lang, "reply")}
              </button>

              {canDelete ? (
                <button
                  onClick={() => onDelete(id)}
                  className="hover:text-red-600 text-red-500"
                >
                  {tt(lang, "delete")}
                </button>
              ) : null}
            </div>
          </div>

          {hasReplies ? (
            <div className="mt-2">
              <button
                onClick={() => onToggleReplies(id)}
                className="text-xs font-semibold text-gray-700 hover:text-gray-900 inline-flex items-center gap-2"
              >
                {showReplies
                  ? tt(lang, "hideReplies")
                  : `${tt(lang, "viewReplies")} (${node.replies.length})`}
                {showReplies ? (
                  <ChevronUp size={14} />
                ) : (
                  <ChevronDown size={14} />
                )}
              </button>

              {showReplies ? (
                <div className="mt-2 space-y-2">
                  {node.replies.map((r) => (
                    <CommentNode
                      key={r.id}
                      lang={lang}
                      node={r}
                      depth={depth + 1}
                      showRepliesMap={showRepliesMap}
                      onToggleReplies={onToggleReplies}
                      onReply={onReply}
                      onDelete={onDelete}
                      onLike={onLike}
                      canAct={canAct}
                    />
                  ))}
                </div>
              ) : null}
            </div>
          ) : null}
        </div>
      </div>
    </div>
  );
}

/* =========================
   Tabs
========================= */
export function PostsTab({
  lang,
  API_BASE,
  profile,
  items,
  isMe,
  canAct,
  onDelete,
  onUpdate,
  refreshCurrentTab,
  PostCardComp,
  PostComposerComp,
}) {
  const safeItems = Array.isArray(items) ? items : [];

  return (
    <div className="space-y-4">
      {isMe ? (
        <PostComposerComp
          onPosted={async () => {
            await refreshCurrentTab?.();
          }}
        />
      ) : null}

      {safeItems.map((p, idx) => {
        const rawId = getPostId(p);
        const key = String(rawId ?? `row_${idx}`);
        return (
          <PostCardComp
            key={key}
            lang={lang}
            API_BASE={API_BASE}
            post={p}
            profile={profile}
            isMe={isMe}
            canAct={canAct}
            onDelete={() => onDelete(rawId)}
            onUpdate={(payload) => onUpdate(rawId, payload)}
            // inject (from same file)
            tt={tt}
            Modal={Modal}
            toastConfirm={toastConfirm}
            CommentNode={CommentNode}
            buildCommentTree={buildCommentTree}
            tryFetchFallback={tryFetchFallback}
            authHeaders={authHeaders}
            getAuthUserId={getAuthUserId}
            getInitials={getInitials}
            formatTime={formatTime}
            getPostId={getPostId}
            normalizeFeedPostId={normalizeFeedPostId}
            extractNumericId={extractNumericId}
          />
        );
      })}

      {!safeItems.length ? (
        <div className="text-sm text-gray-500">{tt(lang, "noPosts")}</div>
      ) : null}
    </div>
  );
}

export function ServicesTab({ lang, items, isMe, onDelete }) {
  const navigate = useNavigate();
  const safeItems = Array.isArray(items) ? items : [];

  const toServiceId = (s) => String(s?.id ?? s?.service_id ?? "").trim();
  const toTitle = (s) => String(s?.title ?? s?.name ?? "Service").trim();

  const openDetails = (s) => {
    const id = toServiceId(s);
    if (!id) return;
    navigate(`/marketplace/item/${id}`, { state: { type: "services" } });
  };

  return (
    <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
      {safeItems.map((s) => {
        const id = toServiceId(s);
        const title = toTitle(s);

        const priceType = String(s?.price_type || "").toLowerCase();
        const priceValue =
          s?.price_value !== null && s?.price_value !== undefined
            ? String(s.price_value)
            : "";

        const priceText =
          priceType === "fixed"
            ? `Fixed${priceValue ? ` • $${priceValue}` : ""}`
            : priceType === "starting_at"
            ? `Starting at${priceValue ? ` • $${priceValue}` : ""}`
            : `Negotiable${priceValue ? ` • $${priceValue}` : ""}`;

        const location =
          String(s?.location || s?.city || "").trim() ||
          [s?.city, s?.state].filter(Boolean).join(", ");

        const desc = String(s?.description || s?.notes || "").trim();

        return (
          <button
            key={id || title}
            type="button"
            onClick={() => openDetails(s)}
            className="text-left rounded-2xl border bg-white hover:bg-gray-50 transition p-4 w-full"
          >
            <div className="flex items-start justify-between gap-3">
              <div className="min-w-0">
                <div className="flex items-center gap-2">
                  <Briefcase size={16} className="text-gray-600" />
                  <div className="font-semibold text-gray-900 truncate">
                    {title}
                  </div>
                </div>

                <div className="mt-1 text-sm text-gray-600 flex flex-wrap gap-2">
                  {s?.category ? (
                    <span className="inline-flex items-center px-2 py-0.5 rounded-full border bg-gray-50 text-xs">
                      {s.category}
                    </span>
                  ) : null}
                  <span className="text-xs text-gray-500">•</span>
                  <span className="text-sm text-gray-700">{priceText}</span>
                  {location ? (
                    <>
                      <span className="text-xs text-gray-500">•</span>
                      <span className="text-sm text-gray-700">{location}</span>
                    </>
                  ) : null}
                </div>

                {desc ? (
                  <div className="mt-2 text-sm text-gray-700 line-clamp-3 whitespace-pre-wrap">
                    {desc}
                  </div>
                ) : null}

                <div className="mt-3 grid grid-cols-2 gap-2 text-xs text-gray-600">
                  {s?.phone ? (
                    <div className="rounded-xl border bg-gray-50 px-3 py-2">
                      📞 {s.phone}
                    </div>
                  ) : null}
                  {s?.website || s?.link || s?.url ? (
                    <div className="rounded-xl border bg-gray-50 px-3 py-2 truncate">
                      🔗 {String(s.website || s.link || s.url)}
                    </div>
                  ) : null}
                </div>
              </div>

              {isMe ? (
                <button
                  type="button"
                  onClick={(e) => {
                    e.stopPropagation();
                    onDelete(s.id ?? s.service_id);
                  }}
                  className="p-2 rounded-xl hover:bg-white text-red-600 border bg-white shrink-0"
                  title={tt(lang, "delete")}
                >
                  <Trash2 size={16} />
                </button>
              ) : null}
            </div>
          </button>
        );
      })}

      {!safeItems.length ? (
        <div className="text-sm text-gray-500">{tt(lang, "noServices")}</div>
      ) : null}
    </div>
  );
}

export function ProductsTab({ lang, items, isMe, onDelete }) {
  const navigate = useNavigate();
  const safeItems = Array.isArray(items) ? items : [];

  const toProductId = (p) => String(p?.id ?? p?.product_id ?? "").trim();
  const toTitle = (p) => String(p?.title ?? p?.name ?? "Product").trim();

  const getFirstImage = (p) => {
    if (Array.isArray(p?.images) && p.images[0]) return p.images[0];
    if (typeof p?.images === "string" && p.images.trim())
      return p.images.trim();
    if (p?.image_url) return p.image_url;
    return "";
  };

  const openDetails = (p) => {
    const id = toProductId(p);
    if (!id) return;
    navigate(`/marketplace/item/${id}`, { state: { type: "products" } });
  };

  return (
    <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
      {safeItems.map((p) => {
        const id = toProductId(p);
        const title = toTitle(p);

        const img = getFirstImage(p);
        const location =
          String(p?.location || p?.city || "").trim() ||
          [p?.city, p?.state].filter(Boolean).join(", ");

        const price =
          p?.price !== null && p?.price !== undefined ? Number(p.price) : null;
        const currency = String(p?.currency || "USD");
        const desc = String(p?.description || p?.notes || "").trim();

        return (
          <button
            key={id || title}
            type="button"
            onClick={() => openDetails(p)}
            className="text-left rounded-2xl border bg-white hover:bg-gray-50 transition overflow-hidden"
          >
            {img ? (
              <img src={img} alt="" className="w-full h-44 object-cover" />
            ) : (
              <div className="w-full h-44 bg-gray-100" />
            )}

            <div className="p-4">
              <div className="flex items-start justify-between gap-3">
                <div className="min-w-0">
                  <div className="flex items-center gap-2">
                    <Store size={16} className="text-gray-600" />
                    <div className="font-semibold text-gray-900 truncate">
                      {title}
                    </div>
                  </div>

                  <div className="mt-1 text-sm text-gray-600 flex flex-wrap gap-2">
                    {price !== null && Number.isFinite(price) ? (
                      <span className="text-sm text-gray-900 font-semibold">
                        {currency} {price}
                      </span>
                    ) : (
                      <span className="text-sm text-gray-500">Price: —</span>
                    )}
                    {location ? (
                      <>
                        <span className="text-xs text-gray-500">•</span>
                        <span className="text-sm text-gray-700">
                          {location}
                        </span>
                      </>
                    ) : null}
                  </div>

                  {desc ? (
                    <div className="mt-2 text-sm text-gray-700 line-clamp-3 whitespace-pre-wrap">
                      {desc}
                    </div>
                  ) : null}

                  {Array.isArray(p?.images) && p.images.length > 1 ? (
                    <div className="mt-3 text-xs text-gray-500">
                      +{p.images.length - 1} more photos
                    </div>
                  ) : null}
                </div>

                {isMe ? (
                  <button
                    type="button"
                    onClick={(e) => {
                      e.stopPropagation();
                      onDelete(p.id ?? p.product_id);
                    }}
                    className="p-2 rounded-xl hover:bg-white text-red-600 border bg-white shrink-0"
                    title={tt(lang, "delete")}
                  >
                    <Trash2 size={16} />
                  </button>
                ) : null}
              </div>
            </div>
          </button>
        );
      })}

      {!safeItems.length ? (
        <div className="text-sm text-gray-500">{tt(lang, "noProducts")}</div>
      ) : null}
    </div>
  );
}

export function ReviewsTab({ lang, items, ratingAvg, ratingCount }) {
  const safeItems = Array.isArray(items) ? items : [];
  const dist = useMemo(() => {
    const buckets = { 1: 0, 2: 0, 3: 0, 4: 0, 5: 0 };
    safeItems.forEach((r) => {
      const n = Math.max(1, Math.min(5, Number(r.rating) || 0));
      if (n) buckets[n] += 1;
    });
    return buckets;
  }, [safeItems]);

  const max = Math.max(1, ...Object.values(dist));

  return (
    <div className="space-y-4">
      <div className="border rounded-2xl bg-white p-4">
        <div className="flex flex-col md:flex-row md:items-center gap-4">
          <div className="flex items-center gap-4">
            <div className="w-20 h-20 rounded-2xl bg-gray-900 text-white flex items-center justify-center">
              <div className="text-2xl font-extrabold">
                {Number(ratingAvg || 0).toFixed(1)}
              </div>
            </div>
            <div>
              <div className="font-semibold text-gray-900">
                {tt(lang, "overallRating")}
              </div>
              <div className="text-sm text-gray-500">
                {ratingCount} {tt(lang, "review")}
                {ratingCount === 1 ? "" : "s"}
              </div>
              <div className="mt-1 flex items-center gap-1">
                {Array.from({ length: 5 }).map((_, i) => (
                  <Star
                    key={i}
                    size={18}
                    className={
                      i < Math.round(Number(ratingAvg || 0))
                        ? "text-yellow-500"
                        : "text-gray-300"
                    }
                  />
                ))}
              </div>
            </div>
          </div>

          <div className="md:ml-auto flex-1 max-w-md">
            {[5, 4, 3, 2, 1].map((n) => (
              <div key={n} className="flex items-center gap-2 mb-2">
                <div className="w-10 text-xs text-gray-700 font-semibold">
                  {n}★
                </div>
                <div className="flex-1 h-2 rounded-full bg-gray-100 overflow-hidden border">
                  <div
                    className="h-full bg-gray-900"
                    style={{ width: `${Math.round((dist[n] / max) * 100)}%` }}
                  />
                </div>
                <div className="w-10 text-xs text-gray-500 text-right">
                  {dist[n]}
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>

      <div className="space-y-3">
        {safeItems.map((r) => (
          <div
            key={r.id ?? r.review_id}
            className="border rounded-2xl p-4 bg-white"
          >
            <div className="flex items-center justify-between">
              <div className="font-semibold">{r.author_name || "User"}</div>
              <div className="flex items-center gap-1 text-sm">
                {Array.from({ length: 5 }).map((_, i) => (
                  <Star
                    key={i}
                    size={16}
                    className={
                      i < Number(r.rating) ? "text-yellow-500" : "text-gray-300"
                    }
                  />
                ))}
              </div>
            </div>
            <div className="text-xs text-gray-500 mt-1">
              {formatTime(r.created_at || r.createdAt) || r.created_at || ""}
            </div>
            <div className="mt-2 whitespace-pre-wrap">{r.comment}</div>
          </div>
        ))}

        {!safeItems.length ? (
          <div className="text-sm text-gray-500">{tt(lang, "noReviews")}</div>
        ) : null}
      </div>
    </div>
  );
}

/* =========================
   (Optional) Re-exports for icons used in body
========================= */
export const Icons = { MapPin, Phone, LinkIcon };
