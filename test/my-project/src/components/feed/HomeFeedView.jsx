import React, {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import { Link } from "react-router-dom";

import PostComposer from "../../components/feed/PostComposer";
import toast from "react-hot-toast";

const API_BASE = (
  import.meta.env.VITE_API_BASE_URL ||
  import.meta.env.VITE_API_BASE ||
  "http://localhost:5000"
).replace(/\/+$/, "");

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

const formatTime = (value) => {
  if (!value) return "Just now";
  try {
    const d = new Date(value);
    if (Number.isNaN(d.getTime())) return "Just now";
    const diff = Date.now() - d.getTime();
    const mins = Math.floor(diff / 60000);
    if (mins < 1) return "Just now";
    if (mins < 60) return `${mins}m`;
    const hrs = Math.floor(mins / 60);
    if (hrs < 24) return `${hrs}h`;
    const days = Math.floor(hrs / 24);
    return `${days}d`;
  } catch {
    return "Just now";
  }
};

const SkeletonPost = () => (
  <div className="bg-white rounded-2xl shadow-sm border border-gray-100 p-4 mb-4">
    <div className="flex items-center gap-3">
      <div className="w-10 h-10 rounded-full bg-gray-200 animate-pulse" />
      <div className="flex-1">
        <div className="h-3 w-32 bg-gray-200 rounded animate-pulse mb-2" />
        <div className="h-3 w-24 bg-gray-200 rounded animate-pulse" />
      </div>
    </div>
    <div className="mt-4 space-y-2">
      <div className="h-3 w-full bg-gray-200 rounded animate-pulse" />
      <div className="h-3 w-11/12 bg-gray-200 rounded animate-pulse" />
      <div className="h-3 w-9/12 bg-gray-200 rounded animate-pulse" />
    </div>
    <div className="mt-4 flex gap-2">
      <div className="h-9 w-24 bg-gray-200 rounded-xl animate-pulse" />
      <div className="h-9 w-28 bg-gray-200 rounded-xl animate-pulse" />
    </div>
  </div>
);

const buildCommentTree = (flat = []) => {
  const map = new Map();
  const roots = [];
  (flat || []).forEach((c) => map.set(c.id, { ...c, replies: [] }));
  map.forEach((node) => {
    const parentId = node.parent_comment_id ?? node.parentId ?? null;
    if (parentId && map.has(parentId)) map.get(parentId).replies.push(node);
    else roots.push(node);
  });

  const sortRec = (arr) => {
    arr.sort((a, b) => {
      const ta = new Date(a.created_at || a.createdAt || 0).getTime() || 0;
      const tb = new Date(b.created_at || b.createdAt || 0).getTime() || 0;
      return ta - tb;
    });
    arr.forEach((x) => sortRec(x.replies || []));
  };
  sortRec(roots);
  return roots;
};

// ✅ Professional confirm toast
const toastConfirm = ({
  title = "Are you sure?",
  confirmText = "Delete",
} = {}) =>
  new Promise((resolve) => {
    toast.custom(
      (t) => (
        <div className="bg-white border border-gray-200 shadow-xl rounded-2xl px-4 py-3 w-[360px]">
          <div className="flex items-start gap-3">
            <div className="w-9 h-9 rounded-full bg-red-50 text-red-600 flex items-center justify-center font-bold">
              !
            </div>
            <div className="flex-1">
              <div className="font-semibold text-gray-900">{title}</div>
              <div className="text-xs text-gray-500 mt-1">
                This action can’t be undone.
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
                  Cancel
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

/* =========================
   ✅ Facebook-like Thread UI
========================= */
const VISUAL_MAX_DEPTH = 2;
const INDENT_PX = 16;

const CommentNode = ({
  node,
  depth,
  postId,
  myUserId,
  profileHref,
  replyOpen,
  replyDraft,
  commentSending,
  onToggleReply,
  onReplyDraftChange,
  onSendReply,
  onDeleteComment,
  onLikeComment,
  absUrl, // ✅ add this
}) => {
  const replyKey = `${postId}:${node.id}`;
  const threadKey = `thread:${postId}:${node.id}`;

  const userUrl = profileHref(node.user_id || node.userId);
  const displayName = node.user_name || "User";
  const avatarSrc = absUrl(node.author_avatar || node.user_avatar || "");

  const isOwner =
    myUserId && (node.user_id === myUserId || node.userId === myUserId);

  const likeCount =
    node.likeCount ??
    node.likesCount ??
    node.like_count ??
    node.likes_count ??
    0;

  const likedByMe = !!(node.likedByMe ?? node.liked_by_me);

  const replies = node.replies || [];
  const hasReplies = replies.length > 0;
  const isThreadOpen = !!replyOpen[threadKey];

  const cappedDepth = Math.min(depth, VISUAL_MAX_DEPTH);

  return (
    <div
      className={classNames("relative", cappedDepth > 0 && "pl-4")}
      style={cappedDepth > 0 ? { paddingLeft: INDENT_PX } : undefined}
    >
      {cappedDepth > 0 && (
        <div className="absolute left-[6px] top-0 bottom-0 w-[2px] bg-gray-200 rounded-full" />
      )}

      <div className="flex gap-3">
        <Link
          to={userUrl}
          className="w-9 h-9 rounded-full overflow-hidden bg-gray-900 text-white flex items-center justify-center font-bold shrink-0"
          title="Open profile"
          onClick={(e) => e.stopPropagation()}
        >
          <AvatarImg src={avatarSrc} name={displayName} />
        </Link>

        <div className="flex-1">
          <div className="bg-gray-50 border border-gray-200 rounded-2xl px-4 py-3">
            <div className="flex items-center justify-between gap-2">
              <Link
                to={userUrl}
                className="text-sm font-semibold text-gray-900 hover:underline"
                onClick={(e) => e.stopPropagation()}
              >
                {node.user_name || "User"}
              </Link>
              <span className="text-xs text-gray-400">
                {formatTime(node.created_at || node.createdAt)}
              </span>
            </div>

            <div className="text-sm text-gray-800 mt-1 whitespace-pre-wrap">
              {node.comment}
            </div>
          </div>

          <div className="mt-2 flex items-center gap-4 text-xs text-gray-500">
            <button
              type="button"
              onClick={(e) => {
                e.stopPropagation();
                onLikeComment?.(postId, node.id);
              }}
              className={classNames(
                "hover:text-gray-900",
                likedByMe && "text-blue-600 font-semibold"
              )}
              title="Like"
            >
              👍 Like {likeCount > 0 ? `(${likeCount})` : ""}
            </button>

            <button
              type="button"
              onClick={(e) => {
                e.stopPropagation();
                onToggleReply(postId, node.id);
              }}
              className="hover:text-gray-900"
            >
              Reply
            </button>

            {isOwner && (
              <button
                type="button"
                onClick={(e) => {
                  e.stopPropagation();
                  onDeleteComment(postId, node.id);
                }}
                className="hover:text-red-600"
              >
                Delete
              </button>
            )}

            {hasReplies && (
              <button
                type="button"
                onClick={(e) => {
                  e.stopPropagation();
                  onToggleReply(postId, threadKey);
                }}
                className="ml-auto text-gray-600 hover:text-gray-900 font-semibold"
              >
                {isThreadOpen
                  ? "Hide replies"
                  : `View replies (${replies.length})`}
              </button>
            )}
          </div>

          {replyOpen[replyKey] && (
            <div
              className="mt-2 flex gap-2"
              onClick={(e) => e.stopPropagation()}
            >
              <input
                value={replyDraft[replyKey] || ""}
                onChange={(e) => onReplyDraftChange(replyKey, e.target.value)}
                onKeyDown={(e) => e.stopPropagation()}
                placeholder="Write a reply..."
                className="flex-1 bg-white border border-gray-200 rounded-2xl px-4 py-3 text-sm outline-none focus:border-gray-300"
              />
              <button
                type="button"
                onClick={(e) => {
                  e.stopPropagation();
                  onSendReply(postId, node.id);
                }}
                disabled={!!commentSending[replyKey]}
                className={classNames(
                  "rounded-2xl px-4 text-sm font-semibold shadow-sm transition",
                  commentSending[replyKey]
                    ? "bg-gray-200 text-gray-500 cursor-not-allowed"
                    : "bg-gray-900 text-white hover:bg-black"
                )}
              >
                {commentSending[replyKey] ? "Sending..." : "Send"}
              </button>
            </div>
          )}

          {hasReplies && isThreadOpen && (
            <div className="mt-3 space-y-3">
              {replies.map((r) => (
                <CommentNode
                  key={r.id}
                  node={r}
                  depth={depth + 1}
                  postId={postId}
                  myUserId={myUserId}
                  profileHref={profileHref}
                  replyOpen={replyOpen}
                  replyDraft={replyDraft}
                  commentSending={commentSending}
                  onToggleReply={onToggleReply}
                  onReplyDraftChange={onReplyDraftChange}
                  onSendReply={onSendReply}
                  onDeleteComment={onDeleteComment}
                  onLikeComment={onLikeComment}
                  absUrl={absUrl}
                />
              ))}
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

const AvatarImg = ({ src, name }) => {
  if (!src) return <>{getInitials(name)}</>;
  return (
    <img
      src={src}
      alt="avatar"
      className="w-full h-full object-cover"
      onError={(e) => {
        e.currentTarget.remove(); // يشيل الصورة لو فشلت
      }}
    />
  );
};

export const HomeFeedView = () => {
  const token = localStorage.getItem("token");
  const isLoggedIn = !!token;
  const authHeaders = () => (token ? { Authorization: `Bearer ${token}` } : {});

  const me = useMemo(() => {
    try {
      return JSON.parse(localStorage.getItem("user") || "null");
    } catch {
      return null;
    }
  }, []);
  const myUserId = me?.id ?? null;

  const [posts, setPosts] = useState([]);
  const [loading, setLoading] = useState(true);

  const [filter, setFilter] = useState("");
  const [search, setSearch] = useState("");

  const [openComments, setOpenComments] = useState({});
  const [comments, setComments] = useState({});
  const [commentDraft, setCommentDraft] = useState({});
  const [commentSending, setCommentSending] = useState({});
  const [replyOpen, setReplyOpen] = useState({});
  const [replyDraft, setReplyDraft] = useState({});
  const [commentCounts, setCommentCounts] = useState({});

  const [menuOpen, setMenuOpen] = useState({});

  // ✅ lightbox
  const [lightbox, setLightbox] = useState({ open: false, urls: [], i: 0 });

  const [savedIds, setSavedIds] = useState(() => {
    try {
      const raw = localStorage.getItem("savedPosts");
      const arr = raw ? JSON.parse(raw) : [];
      return new Set(Array.isArray(arr) ? arr : []);
    } catch {
      return new Set();
    }
  });

  const persistSaved = (setObj) => {
    try {
      localStorage.setItem("savedPosts", JSON.stringify([...setObj]));
    } catch {}
  };

  const profileHref = (userId) => (userId ? `/u/${userId}` : "/u/0");

  const pickMediaUrl = (item) => {
    if (!item) return "";
    if (typeof item === "string") return item.trim();

    // object shapes
    if (typeof item === "object") {
      return (
        String(
          item.url ||
            item.path ||
            item.src ||
            item.location ||
            item.file ||
            item.filename ||
            ""
        ).trim() || ""
      );
    }
    return String(item).trim();
  };

  const absUrl = (u) => {
    let s = pickMediaUrl(u);
    if (!s) return "";

    // keep blob/data urls
    if (s.startsWith("blob:") || s.startsWith("data:")) return s;

    // normalize slashes (windows paths)
    s = s.replace(/\\/g, "/");

    // already absolute
    if (s.startsWith("http://") || s.startsWith("https://")) return s;

    // protocol-relative
    if (s.startsWith("//")) return `${window.location.protocol}${s}`;

    // already same origin (sometimes backend returns full frontend origin)
    if (s.startsWith(window.location.origin)) return s;

    // already API_BASE
    if (s.startsWith(API_BASE)) return s;

    // remove leading "./"
    if (s.startsWith("./")) s = s.slice(2);

    // common: "/uploads/.."
    if (s.startsWith("/")) return `${API_BASE}${s}`;

    // common: "uploads/.."
    return `${API_BASE}/${s}`;
  };

  const normalizePost = (p) => {
    const likeCount =
      p.likeCount ?? p.likesCount ?? p.like_count ?? p.likes_count ?? 0;
    const commentCount =
      p.commentCount ??
      p.commentsCount ??
      p.comment_count ??
      p.comments_count ??
      0;

    const rawMedia =
      Array.isArray(p.media) && p.media.length
        ? p.media
        : (() => {
            try {
              const arr = JSON.parse(p.media_urls || "[]");
              return Array.isArray(arr) ? arr : [];
            } catch {
              return [];
            }
          })();

    const media = (rawMedia || []).map((x) => pickMediaUrl(x)).filter(Boolean);

    return {
      ...p,
      likeCount: Number(likeCount) || 0,
      commentCount: Number(commentCount) || 0,
      likedByMe: !!(p.likedByMe ?? p.liked_by_me),
      media,
    };
  };

  const normalizeComment = (c) => {
    const likeCount =
      c.likeCount ?? c.likesCount ?? c.like_count ?? c.likes_count ?? 0;
    return {
      ...c,
      likeCount: Number(likeCount) || 0,
      likedByMe: !!(c.likedByMe ?? c.liked_by_me),
    };
  };

  const tryFetch = async (urls, options) => {
    let last = null;
    for (const url of urls) {
      try {
        const res = await fetch(url, options);
        last = res;
        if (res.status !== 404) return res;
      } catch {
        last = null;
      }
    }
    return last;
  };

  // ✅ Close any open menus on outside click
  useEffect(() => {
    const onDocClick = () => setMenuOpen({});
    document.addEventListener("click", onDocClick);
    return () => document.removeEventListener("click", onDocClick);
  }, []);

  const filteredPosts = useMemo(() => {
    const s = search.trim().toLowerCase();
    if (!s) return posts;
    return posts.filter((p) => {
      const a = (p.user_name || "").toLowerCase();
      const b = (p.content || "").toLowerCase();
      const c = (p.category || "").toLowerCase();
      return a.includes(s) || b.includes(s) || c.includes(s);
    });
  }, [posts, search]);

  const fetchComments = async (postId) => {
    const res = await fetch(`${API_BASE}/api/posts/${postId}/comments`, {
      headers: authHeaders(),
    });
    const data = await res.json();
    const list = Array.isArray(data) ? data.map(normalizeComment) : [];
    setComments((m) => ({ ...m, [postId]: list }));
    setCommentCounts((prev) => ({ ...prev, [postId]: list.length }));
    return list;
  };

  const loadPosts = async () => {
    try {
      setLoading(true);
      const q = filter ? `?category=${filter}` : "";
      const res = await fetch(`${API_BASE}/api/posts${q}`, {
        headers: authHeaders(),
      });
      const data = await res.json();
      const arr = Array.isArray(data) ? data.map(normalizePost) : [];
      setPosts(arr);

      setCommentCounts((prev) => {
        const next = { ...prev };
        arr.forEach((p) => (next[p.id] = Number(p.commentCount) || 0));
        return next;
      });

      const firstWithComments = arr
        .filter((p) => (Number(p.commentCount) || 0) > 0)
        .slice(0, 3);

      if (firstWithComments.length) {
        setOpenComments((prev) => {
          const next = { ...prev };
          firstWithComments.forEach((p) => (next[p.id] = true));
          return next;
        });
        await Promise.allSettled(
          firstWithComments.map((p) => fetchComments(p.id))
        );
      }
    } catch (e) {
      console.error(e);
      toast.error("Failed to load posts");
      setPosts([]);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadPosts();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [filter]);

  const likePost = async (id) => {
    if (!isLoggedIn) return toast.error("Login first");

    setPosts((prev) =>
      prev.map((p) =>
        p.id === id
          ? {
              ...p,
              likedByMe: !p.likedByMe,
              likeCount: p.likedByMe
                ? Math.max(0, (p.likeCount || 0) - 1)
                : (p.likeCount || 0) + 1,
            }
          : p
      )
    );

    try {
      await fetch(`${API_BASE}/api/posts/${id}/like`, {
        method: "POST",
        headers: authHeaders(),
      });
    } catch {
      loadPosts();
    }
  };

  const toggleComments = async (postId) => {
    const open = !openComments[postId];
    setOpenComments((m) => ({ ...m, [postId]: open }));

    if (open) {
      try {
        await fetchComments(postId);
      } catch {
        setComments((m) => ({ ...m, [postId]: [] }));
        setCommentCounts((prev) => ({ ...prev, [postId]: 0 }));
      }
    }
  };

  const addComment = async (postId) => {
    if (!isLoggedIn) return toast.error("Login first");
    const text = (commentDraft[postId] || "").trim();
    if (!text) return;

    setCommentSending((m) => ({ ...m, [postId]: true }));
    try {
      const res = await fetch(`${API_BASE}/api/posts/${postId}/comments`, {
        method: "POST",
        headers: { "Content-Type": "application/json", ...authHeaders() },
        body: JSON.stringify({ comment: text }),
      });
      if (!res.ok) throw new Error("comment failed");

      setCommentDraft((m) => ({ ...m, [postId]: "" }));
      await fetchComments(postId);
      toast.success("Comment added");
      setOpenComments((m) => ({ ...m, [postId]: true }));
    } catch (e) {
      console.error(e);
      toast.error("Comment failed");
    } finally {
      setCommentSending((m) => ({ ...m, [postId]: false }));
    }
  };

  const toggleReply = (postId, commentIdOrKey) => {
    const key = String(commentIdOrKey).startsWith("thread:")
      ? String(commentIdOrKey)
      : `${postId}:${commentIdOrKey}`;

    setReplyOpen((m) => ({ ...m, [key]: !m[key] }));
  };

  const sendReply = async (postId, parentCommentId) => {
    if (!isLoggedIn) return toast.error("Login first");
    const key = `${postId}:${parentCommentId}`;
    const text = (replyDraft[key] || "").trim();
    if (!text) return;

    setCommentSending((m) => ({ ...m, [key]: true }));
    try {
      const res = await fetch(`${API_BASE}/api/posts/${postId}/comments`, {
        method: "POST",
        headers: { "Content-Type": "application/json", ...authHeaders() },
        body: JSON.stringify({
          comment: text,
          parent_comment_id: parentCommentId,
        }),
      });
      if (!res.ok) throw new Error("reply failed");

      setReplyDraft((m) => ({ ...m, [key]: "" }));
      setReplyOpen((m) => ({ ...m, [key]: false }));
      await fetchComments(postId);
      toast.success("Reply added");
      setOpenComments((m) => ({ ...m, [postId]: true }));
    } catch (e) {
      console.error(e);
      toast.error("Reply failed");
    } finally {
      setCommentSending((m) => ({ ...m, [key]: false }));
    }
  };

  const deletePost = async (postId) => {
    if (!isLoggedIn) return toast.error("Login first");

    setMenuOpen((m) => ({ ...m, [postId]: false }));

    const ok = await toastConfirm({
      title: "Delete this post?",
      confirmText: "Delete",
    });
    if (!ok) return;

    const prevPosts = posts;
    setPosts((p) => p.filter((x) => x.id !== postId));

    try {
      const res = await tryFetch(
        [
          `${API_BASE}/api/posts/${postId}`,
          `${API_BASE}/api/posts/delete/${postId}`,
          `${API_BASE}/api/delete-post/${postId}`,
        ],
        { method: "DELETE", headers: authHeaders() }
      );

      if (!res || !res.ok) {
        setPosts(prevPosts);
        if (res?.status === 401)
          return toast.error("Unauthorized (login again)");
        if (res?.status === 403)
          return toast.error("You can't delete this post");
        if (res?.status === 404)
          return toast.error(
            "Delete route not found (404) — backend needs route"
          );
        return toast.error("Delete failed");
      }

      toast.success("Post deleted");
      loadPosts();
    } catch (e) {
      console.error(e);
      setPosts(prevPosts);
      toast.error("Delete failed");
    }
  };

  const deleteComment = async (postId, commentId) => {
    if (!isLoggedIn) return toast.error("Login first");

    const ok = await toastConfirm({
      title: "Delete this comment?",
      confirmText: "Delete",
    });
    if (!ok) return;

    try {
      const res = await tryFetch(
        [
          `${API_BASE}/api/posts/${postId}/comments/${commentId}`,
          `${API_BASE}/api/comments/${commentId}`,
          `${API_BASE}/api/delete-comment/${commentId}`,
        ],
        { method: "DELETE", headers: authHeaders() }
      );

      if (!res || !res.ok) {
        if (res?.status === 401)
          return toast.error("Unauthorized (login again)");
        if (res?.status === 403)
          return toast.error("You can't delete this comment");
        if (res?.status === 404)
          return toast.error(
            "Delete route not found (404) — backend needs route"
          );
        return toast.error("Delete failed");
      }

      toast.success("Comment deleted");
      await fetchComments(postId);
      loadPosts();
    } catch (e) {
      console.error(e);
      toast.error("Delete failed");
    }
  };

  const likeComment = async (postId, commentId) => {
    if (!isLoggedIn) return toast.error("Login first");

    setComments((prev) => {
      const list = Array.isArray(prev[postId]) ? prev[postId] : [];
      const next = list.map((c) => {
        if (c.id !== commentId) return c;
        const likedNow = !(c.likedByMe ?? c.liked_by_me);
        const current = Number(
          c.likeCount ?? c.likesCount ?? c.like_count ?? c.likes_count ?? 0
        );
        return {
          ...c,
          likedByMe: likedNow,
          likeCount: likedNow ? current + 1 : Math.max(0, current - 1),
        };
      });
      return { ...prev, [postId]: next };
    });

    try {
      const res = await tryFetch(
        [
          `${API_BASE}/api/posts/${postId}/comments/${commentId}/like`,
          `${API_BASE}/api/comments/${commentId}/like`,
        ],
        { method: "POST", headers: authHeaders() }
      );

      if (!res || !res.ok) {
        await fetchComments(postId);
      }
    } catch {
      await fetchComments(postId);
    }
  };

  const toggleSave = async (postId) => {
    if (!isLoggedIn) return toast.error("Login first");

    const isSaved = savedIds.has(postId);
    const next = new Set(savedIds);
    if (isSaved) next.delete(postId);
    else next.add(postId);

    setSavedIds(next);
    persistSaved(next);

    toast.success(isSaved ? "Unsaved" : "Saved");
  };

  const sharePost = async (postId) => {
    const url = `${window.location.origin}/feed?postId=${postId}`;
    const text = "Check this post on AnswerForU";

    try {
      if (navigator.share) {
        await navigator.share({ title: "AnswerForU", text, url });
        toast.success("Shared");
      } else {
        await navigator.clipboard.writeText(url);
        toast.success("Link copied");
      }
    } catch {
      try {
        await navigator.clipboard.writeText(url);
        toast.success("Link copied");
      } catch {
        toast("Copy link: " + url);
      }
    }
  };

  const openLightbox = (urls, i) =>
    setLightbox({ open: true, urls: urls.map(absUrl), i });

  const closeLightbox = useCallback(
    () => setLightbox({ open: false, urls: [], i: 0 }),
    []
  );

  const nextLightbox = useCallback(() => {
    setLightbox((s) => ({
      ...s,
      i: s.urls.length ? (s.i + 1) % s.urls.length : 0,
    }));
  }, []);

  const prevLightbox = useCallback(() => {
    setLightbox((s) => ({
      ...s,
      i: s.urls.length ? (s.i - 1 + s.urls.length) % s.urls.length : 0,
    }));
  }, []);

  // ✅ keyboard navigation for lightbox (← → Esc)
  useEffect(() => {
    if (!lightbox.open) return;

    const onKeyDown = (e) => {
      if (e.key === "ArrowRight") {
        e.preventDefault();
        nextLightbox();
        return;
      }
      if (e.key === "ArrowLeft") {
        e.preventDefault();
        prevLightbox();
        return;
      }
      if (e.key === "Escape") {
        e.preventDefault();
        closeLightbox();
      }
    };

    window.addEventListener("keydown", onKeyDown, { passive: false });
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [lightbox.open, nextLightbox, prevLightbox, closeLightbox]);

  return (
    <div className="min-h-screen bg-gray-50">
      <div className="max-w-5xl mx-auto px-4 py-6">
        {/* Lightbox */}
        {lightbox.open && (
          <div
            className="fixed inset-0 z-[9999] bg-black/70 flex items-center justify-center p-4"
            onClick={closeLightbox}
          >
            <div
              className="relative w-full max-w-4xl"
              onClick={(e) => e.stopPropagation()}
            >
              <img
                src={lightbox.urls[lightbox.i]}
                alt=""
                className="w-full max-h-[80vh] object-contain rounded-2xl bg-black"
              />
              {lightbox.urls.length > 1 && (
                <>
                  <button
                    type="button"
                    onClick={prevLightbox}
                    className="absolute left-2 top-1/2 -translate-y-1/2 px-3 py-2 rounded-xl bg-white/90 hover:bg-white text-gray-900 font-bold"
                  >
                    ‹
                  </button>
                  <button
                    type="button"
                    onClick={nextLightbox}
                    className="absolute right-2 top-1/2 -translate-y-1/2 px-3 py-2 rounded-xl bg-white/90 hover:bg-white text-gray-900 font-bold"
                  >
                    ›
                  </button>
                </>
              )}
              <button
                type="button"
                onClick={closeLightbox}
                className="absolute right-2 top-2 px-3 py-2 rounded-xl bg-white/90 hover:bg-white text-gray-900 font-bold"
              >
                ✕
              </button>
            </div>
          </div>
        )}

        {/* Header */}
        <div className="flex items-start justify-between gap-3 mb-5">
          <div>
            <h1 className="text-2xl sm:text-3xl font-extrabold tracking-tight text-gray-900">
              AnswerForU Feed
            </h1>
            <p className="text-sm text-gray-500 mt-1">
              Share experiences, ask questions, and help newcomers.
            </p>
          </div>

          <div className="w-52 sm:w-72">
            <div className="bg-white border border-gray-200 rounded-2xl px-3 py-2 shadow-sm">
              <div className="flex items-center gap-2">
                <span className="text-gray-400">🔎</span>
                <input
                  value={search}
                  onChange={(e) => setSearch(e.target.value)}
                  placeholder="Search posts..."
                  className="w-full outline-none text-sm"
                />
              </div>
            </div>
          </div>
        </div>

        {/* Filter tabs */}
        <div className="flex items-center justify-between gap-3 mb-4">
          <div className="flex items-center gap-2 flex-wrap">
            {[
              { value: "", label: "All" },
              { value: "questions", label: "Questions" },
              { value: "immigration", label: "Immigration" },
              { value: "work", label: "Work" },
              { value: "housing", label: "Housing" },
              { value: "tax", label: "Taxes" },
              { value: "general", label: "General" },
            ].map((t) => {
              const active = filter === t.value;
              return (
                <button
                  key={t.value || "all"}
                  type="button"
                  onClick={() => setFilter(t.value)}
                  className={classNames(
                    "px-4 py-2 rounded-full text-sm border transition shadow-sm",
                    active
                      ? "bg-gray-900 text-white border-gray-900"
                      : "bg-white text-gray-700 border-gray-200 hover:bg-gray-100"
                  )}
                >
                  {t.label}
                </button>
              );
            })}
          </div>
        </div>

        <PostComposer onPosted={loadPosts} />

        {/* Posts */}
        {loading ? (
          <>
            <SkeletonPost />
            <SkeletonPost />
            <SkeletonPost />
          </>
        ) : filteredPosts.length === 0 ? (
          <div className="bg-white rounded-2xl shadow-sm border border-gray-100 p-6 text-center">
            <div className="text-3xl mb-2">🗣️</div>
            <p className="font-semibold text-gray-900">No posts yet</p>
            <p className="text-sm text-gray-500 mt-1">
              Be the first to share a question or experience.
            </p>
          </div>
        ) : (
          filteredPosts.map((p) => {
            const cat = getCategory(p.category);
            const open = !!openComments[p.id];
            const saved = savedIds.has(p.id);

            const count =
              typeof commentCounts[p.id] === "number"
                ? commentCounts[p.id]
                : typeof p.commentCount === "number"
                ? p.commentCount
                : 0;

            const isOwner =
              myUserId && (p.user_id === myUserId || p.userId === myUserId);

            const postProfileUrl = profileHref(p.user_id || p.userId);

            const flat = Array.isArray(comments[p.id]) ? comments[p.id] : [];
            const tree = buildCommentTree(flat);

            const likeCount =
              typeof p.likeCount === "number"
                ? p.likeCount
                : Number(
                    p.likeCount ??
                      p.likesCount ??
                      p.like_count ??
                      p.likes_count ??
                      0
                  ) || 0;

            const media = Array.isArray(p.media) ? p.media : [];
            const displayName = p.user_name || "Unknown";
            const avatarSrc = absUrl(p.author_avatar || p.user_avatar || "");

            return (
              <div
                key={p.id}
                className="bg-white rounded-2xl shadow-sm border border-gray-100 p-4 mb-4"
              >
                {/* Post Header */}
                <div className="flex items-start justify-between gap-3">
                  <div className="flex items-center gap-3">
                    <Link
                      to={postProfileUrl}
                      className="w-11 h-11 rounded-full overflow-hidden bg-gray-900 text-white flex items-center justify-center font-bold shrink-0"
                      title="Open profile"
                      onClick={(e) => e.stopPropagation()}
                    >
                      <AvatarImg src={avatarSrc} name={displayName} />
                    </Link>

                    <div>
                      <div className="flex items-center gap-2 flex-wrap">
                        <Link
                          to={postProfileUrl}
                          className="font-bold text-gray-900 leading-5 hover:underline"
                          onClick={(e) => e.stopPropagation()}
                        >
                          {p.user_name || "Unknown"}
                        </Link>

                        <span
                          className={classNames(
                            "inline-flex items-center gap-2 px-2.5 py-1 rounded-full text-xs border",
                            cat.badge
                          )}
                        >
                          <span
                            className={classNames(
                              "w-2 h-2 rounded-full",
                              cat.dot
                            )}
                          />
                          {cat.label}
                        </span>

                        <span className="text-xs text-gray-400">
                          • {formatTime(p.created_at || p.createdAt)}
                        </span>
                      </div>
                      <div className="text-xs text-gray-500 mt-0.5">
                        Newcomer Guide Community
                      </div>
                    </div>
                  </div>

                  {/* Menu */}
                  <div className="relative">
                    <button
                      type="button"
                      className="text-gray-400 hover:text-gray-700 rounded-lg px-2 py-1"
                      title="More"
                      onClick={(e) => {
                        e.stopPropagation();
                        setMenuOpen((m) => ({ ...m, [p.id]: !m[p.id] }));
                      }}
                    >
                      ⋯
                    </button>

                    {menuOpen[p.id] && (
                      <div
                        className="absolute right-0 mt-2 w-44 bg-white border border-gray-200 rounded-xl shadow-lg overflow-hidden z-20"
                        onClick={(e) => e.stopPropagation()}
                      >
                        <button
                          type="button"
                          onClick={() => {
                            setMenuOpen((m) => ({ ...m, [p.id]: false }));
                            toggleSave(p.id);
                          }}
                          className="w-full text-left px-4 py-2 text-sm hover:bg-gray-50"
                        >
                          {saved ? "Unsave" : "Save"}
                        </button>

                        <button
                          type="button"
                          onClick={() => {
                            setMenuOpen((m) => ({ ...m, [p.id]: false }));
                            sharePost(p.id);
                          }}
                          className="w-full text-left px-4 py-2 text-sm hover:bg-gray-50"
                        >
                          Share
                        </button>

                        {isOwner && (
                          <button
                            type="button"
                            onClick={() => deletePost(p.id)}
                            className="w-full text-left px-4 py-2 text-sm hover:bg-red-50 text-red-600"
                          >
                            Delete post
                          </button>
                        )}
                      </div>
                    )}
                  </div>
                </div>

                {/* Content */}
                <div className="mt-3 text-[15px] leading-6 text-gray-800 whitespace-pre-wrap">
                  {p.content}
                </div>

                {/* ✅ Media grid */}
                {media.length > 0 && (
                  <div className="mt-3 grid grid-cols-2 sm:grid-cols-3 gap-2">
                    {media.slice(0, 6).map((u, idx) => {
                      const src = absUrl(u);
                      const extra = media.length - 6;
                      const isLastWithMore = idx === 5 && extra > 0;

                      return (
                        <button
                          key={`${u}-${idx}`}
                          type="button"
                          onClick={() => openLightbox(media, idx)}
                          className="relative rounded-2xl overflow-hidden border border-gray-200 bg-gray-50"
                        >
                          <img
                            src={src}
                            alt=""
                            className="w-full h-44 object-cover"
                            loading="lazy"
                            onError={(e) => {
                              // fallback: hide broken img + show gray placeholder
                              e.currentTarget.style.display = "none";
                              const box = e.currentTarget.parentElement;
                              if (
                                box &&
                                !box.querySelector("[data-img-fallback]")
                              ) {
                                const div = document.createElement("div");
                                div.setAttribute("data-img-fallback", "1");
                                div.className =
                                  "absolute inset-0 flex items-center justify-center text-gray-500 text-sm font-semibold";
                                div.textContent = "Image not available";
                                box.appendChild(div);
                              }
                            }}
                          />

                          {isLastWithMore && (
                            <div className="absolute inset-0 bg-black/45 flex items-center justify-center text-white font-extrabold text-2xl">
                              +{extra}
                            </div>
                          )}
                        </button>
                      );
                    })}
                  </div>
                )}

                {/* Counts */}
                <div className="mt-3 flex items-center justify-between text-sm text-gray-500">
                  {likeCount > 0 && (
                    <div className="flex items-center gap-2">
                      <span className="inline-flex items-center gap-1">
                        <span className="w-6 h-6 rounded-full bg-gray-900 text-white flex items-center justify-center text-xs">
                          👍
                        </span>
                        <span className="font-medium text-gray-700">
                          {likeCount}
                        </span>
                      </span>
                    </div>
                  )}

                  {count > 0 && (
                    <button
                      type="button"
                      onClick={() => toggleComments(p.id)}
                      className="hover:text-gray-800"
                    >
                      {count} comments
                    </button>
                  )}
                </div>

                <div className="mt-3 border-t border-gray-100" />

                {/* Actions */}
                <div className="grid grid-cols-3 gap-2 mt-2">
                  <button
                    type="button"
                    onClick={() => likePost(p.id)}
                    className={classNames(
                      "flex items-center justify-center gap-2 rounded-xl py-2 text-sm font-semibold border transition",
                      p.likedByMe
                        ? "bg-gray-900 text-white border-gray-900"
                        : "bg-white text-gray-700 border-gray-200 hover:bg-gray-100"
                    )}
                  >
                    👍 Like
                  </button>

                  <button
                    type="button"
                    onClick={() => toggleComments(p.id)}
                    className={classNames(
                      "flex items-center justify-center gap-2 rounded-xl py-2 text-sm font-semibold border transition",
                      open
                        ? "bg-gray-100 text-gray-900 border-gray-200"
                        : "bg-white text-gray-700 border-gray-200 hover:bg-gray-100"
                    )}
                  >
                    💬 Comment{" "}
                    <span className="text-xs opacity-80">({count})</span>
                  </button>

                  <button
                    type="button"
                    onClick={() => sharePost(p.id)}
                    className="flex items-center justify-center gap-2 rounded-xl py-2 text-sm font-semibold border transition bg-white text-gray-700 border-gray-200 hover:bg-gray-100"
                  >
                    🔁 Share
                  </button>
                </div>

                {/* Comments */}
                {open && (
                  <div className="mt-4">
                    <div className="space-y-3">
                      {flat.length === 0 ? (
                        <div className="text-sm text-gray-500 bg-gray-50 border border-gray-200 rounded-2xl px-4 py-3">
                          No comments yet. Be the first to comment.
                        </div>
                      ) : (
                        <div className="space-y-4">
                          {tree.map((node) => (
                            <CommentNode
                              key={node.id}
                              node={node}
                              depth={0}
                              postId={p.id}
                              myUserId={myUserId}
                              profileHref={profileHref}
                              replyOpen={replyOpen}
                              replyDraft={replyDraft}
                              commentSending={commentSending}
                              onToggleReply={toggleReply}
                              onReplyDraftChange={(k, v) =>
                                setReplyDraft((m) => ({ ...m, [k]: v }))
                              }
                              onSendReply={sendReply}
                              onDeleteComment={deleteComment}
                              onLikeComment={likeComment}
                              absUrl={absUrl} // ✅ add this
                            />
                          ))}
                        </div>
                      )}
                    </div>

                    {/* Add comment */}
                    <div
                      className="mt-4 flex gap-2"
                      onClick={(e) => e.stopPropagation()}
                    >
                      <input
                        value={commentDraft[p.id] || ""}
                        onChange={(e) =>
                          setCommentDraft((m) => ({
                            ...m,
                            [p.id]: e.target.value,
                          }))
                        }
                        onKeyDown={(e) => e.stopPropagation()}
                        placeholder="Write a comment..."
                        className="flex-1 bg-white border border-gray-200 rounded-2xl px-4 py-3 text-sm outline-none focus:border-gray-300"
                      />
                      <button
                        type="button"
                        onClick={(e) => {
                          e.stopPropagation();
                          addComment(p.id);
                        }}
                        disabled={!!commentSending[p.id]}
                        className={classNames(
                          "rounded-2xl px-4 text-sm font-semibold shadow-sm transition",
                          commentSending[p.id]
                            ? "bg-gray-200 text-gray-500 cursor-not-allowed"
                            : "bg-gray-900 text-white hover:bg-black"
                        )}
                      >
                        {commentSending[p.id] ? "Sending..." : "Send"}
                      </button>
                    </div>
                  </div>
                )}
              </div>
            );
          })
        )}
      </div>
    </div>
  );
};
