// src/pages/ProfilePostsView.jsx (FULL FILE - copy/paste)
// ✅ Shows posts for a specific profile userId (same UI as HomeFeedView)
// ✅ Includes comments under posts (facebook-like thread UI)
// ✅ Uses fallback endpoints safely (skips 404/500/401/403 and keeps trying)

import React, { useEffect, useMemo, useState } from "react";
import { useParams } from "react-router-dom";
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
   Facebook-like Thread UI
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
}) => {
  const replyKey = `${postId}:${node.id}`;
  const threadKey = `thread:${postId}:${node.id}`;

  const userUrl = profileHref(node.user_id || node.userId);

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
        <a
          href={userUrl}
          className="w-9 h-9 rounded-full bg-gray-200 flex items-center justify-center font-bold text-gray-700 shrink-0"
          title="Open profile"
          onClick={(e) => e.stopPropagation()}
        >
          {getInitials(node.user_name)}
        </a>

        <div className="flex-1">
          <div className="bg-gray-50 border border-gray-200 rounded-2xl px-4 py-3">
            <div className="flex items-center justify-between gap-2">
              <a
                href={userUrl}
                className="text-sm font-semibold text-gray-900 hover:underline"
                onClick={(e) => e.stopPropagation()}
              >
                {node.user_name || "User"}
              </a>
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
                />
              ))}
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

export const ProfilePostsView = ({ lang }) => {
  const { userId } = useParams();
  const profileUserId = String(userId || "").trim();

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

  const [openComments, setOpenComments] = useState({});
  const [comments, setComments] = useState({});
  const [commentDraft, setCommentDraft] = useState({});
  const [commentSending, setCommentSending] = useState({});
  const [replyOpen, setReplyOpen] = useState({});
  const [replyDraft, setReplyDraft] = useState({});
  const [commentCounts, setCommentCounts] = useState({});

  const [menuOpen, setMenuOpen] = useState({});

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

  const profileHref = (uid) => (uid ? `/u/${uid}` : "/u/0");

  const normalizePost = (p) => {
    const likeCount =
      p.likeCount ?? p.likesCount ?? p.like_count ?? p.likes_count ?? 0;
    const commentCount =
      p.commentCount ??
      p.commentsCount ??
      p.comment_count ??
      p.comments_count ??
      0;

    return {
      ...p,
      likeCount: Number(likeCount) || 0,
      commentCount: Number(commentCount) || 0,
      likedByMe: !!(p.likedByMe ?? p.liked_by_me),
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

  // ✅ NEW: skip these statuses and keep trying other endpoints
  const TRY_NEXT_STATUSES = new Set([404, 500, 401, 403]);

  const tryFetchFallback = async (urls, options) => {
    let lastErr = null;
    let lastRes = null;
    let lastUrl = null;

    for (const url of urls) {
      lastUrl = url;
      try {
        const res = await fetch(url, options);
        lastRes = res;

        // ✅ accept only OK
        if (res.ok) return res;

        // ✅ keep trying on common mismatch statuses
        if (TRY_NEXT_STATUSES.has(res.status)) continue;

        // any other status: stop
        return res;
      } catch (e) {
        lastErr = e;
        continue;
      }
    }

    if (lastErr) console.warn("tryFetchFallback last error:", lastErr);
    if (lastRes)
      console.warn("tryFetchFallback last response:", {
        url: lastUrl,
        status: lastRes.status,
      });

    return lastRes; // may be null
  };

  useEffect(() => {
    const onDocClick = () => setMenuOpen({});
    document.addEventListener("click", onDocClick);
    return () => document.removeEventListener("click", onDocClick);
  }, []);

  const safeJson = async (res) => {
    try {
      const text = await res.text();
      if (!text) return null;
      return JSON.parse(text);
    } catch {
      return null;
    }
  };

  const fetchComments = async (postId) => {
    const res = await fetch(`${API_BASE}/api/posts/${postId}/comments`, {
      headers: authHeaders(),
    });
    const data = await safeJson(res);
    const list = Array.isArray(data) ? data.map(normalizeComment) : [];
    setComments((m) => ({ ...m, [postId]: list }));
    setCommentCounts((prev) => ({ ...prev, [postId]: list.length }));
    return list;
  };

  const loadProfilePosts = async () => {
    try {
      setLoading(true);

      // ✅ include the endpoints you saw in console + older ones
      const urls = [
        `${API_BASE}/api/profile/${profileUserId}/posts`,
        `${API_BASE}/api/profile_posts/${profileUserId}`,
        `${API_BASE}/api/profile-posts/${profileUserId}`,

        `${API_BASE}/api/users/${profileUserId}/posts`,
        `${API_BASE}/api/posts/user/${profileUserId}`,
        `${API_BASE}/api/posts?userId=${profileUserId}`,
        `${API_BASE}/api/posts?user_id=${profileUserId}`,
      ];

      // try with auth first
      let res = await tryFetchFallback(urls, { headers: authHeaders() });

      // if unauthorized / forbidden, try without auth (maybe public)
      if (res && (res.status === 401 || res.status === 403)) {
        res = await tryFetchFallback(urls, { headers: {} });
      }

      if (!res || !res.ok) {
        setPosts([]);
        return;
      }

      const data = await safeJson(res);
      const arr = Array.isArray(data) ? data.map(normalizePost) : [];
      setPosts(arr);

      setCommentCounts((prev) => {
        const next = { ...prev };
        arr.forEach((p) => (next[p.id] = Number(p.commentCount) || 0));
        return next;
      });
    } catch (e) {
      console.error(e);
      toast.error("Failed to load profile posts");
      setPosts([]);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadProfilePosts();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [profileUserId]);

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
      loadProfilePosts();
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
      const res = await tryFetchFallback(
        [
          `${API_BASE}/api/posts/${postId}`,
          `${API_BASE}/api/posts/delete/${postId}`,
          `${API_BASE}/api/delete-post/${postId}`,
        ],
        { method: "DELETE", headers: authHeaders() }
      );

      if (!res || !res.ok) {
        setPosts(prevPosts);
        if (res?.status === 401) return toast.error("Unauthorized");
        if (res?.status === 403) return toast.error("Forbidden");
        if (res?.status === 404) return toast.error("Delete route not found");
        return toast.error("Delete failed");
      }

      toast.success("Post deleted");
      loadProfilePosts();
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
      const res = await tryFetchFallback(
        [
          `${API_BASE}/api/posts/${postId}/comments/${commentId}`,
          `${API_BASE}/api/comments/${commentId}`,
          `${API_BASE}/api/delete-comment/${commentId}`,
        ],
        { method: "DELETE", headers: authHeaders() }
      );

      if (!res || !res.ok) {
        if (res?.status === 401) return toast.error("Unauthorized");
        if (res?.status === 403) return toast.error("Forbidden");
        if (res?.status === 404) return toast.error("Delete route not found");
        return toast.error("Delete failed");
      }

      toast.success("Comment deleted");
      await fetchComments(postId);
      loadProfilePosts();
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
      const res = await tryFetchFallback(
        [
          `${API_BASE}/api/posts/${postId}/comments/${commentId}/like`,
          `${API_BASE}/api/comments/${commentId}/like`,
        ],
        { method: "POST", headers: authHeaders() }
      );

      if (!res || !res.ok) await fetchComments(postId);
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
    try {
      if (navigator.share) {
        await navigator.share({ title: "AnswerForU", url });
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

  const titleName = useMemo(() => {
    const first = posts?.[0];
    return first?.user_name || `User ${profileUserId}`;
  }, [posts, profileUserId]);

  return (
    <div className="min-h-screen bg-gray-50">
      <div className="max-w-3xl mx-auto px-4 py-6">
        <div className="mb-5">
          <h2 className="text-xl sm:text-2xl font-extrabold text-gray-900">
            Posts by {titleName}
          </h2>
          <p className="text-sm text-gray-500 mt-1">
            Same feed UI — filtered by profile.
          </p>
        </div>

        {loading ? (
          <>
            <SkeletonPost />
            <SkeletonPost />
          </>
        ) : posts.length === 0 ? (
          <div className="bg-white rounded-2xl shadow-sm border border-gray-100 p-6 text-center">
            <div className="text-3xl mb-2">🧾</div>
            <p className="font-semibold text-gray-900">No posts yet</p>
            <p className="text-sm text-gray-500 mt-1">
              This user hasn’t posted anything yet.
            </p>
          </div>
        ) : (
          posts.map((p) => {
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

            return (
              <div
                key={p.id}
                className="bg-white rounded-2xl shadow-sm border border-gray-100 p-4 mb-4"
              >
                <div className="flex items-start justify-between gap-3">
                  <div className="flex items-center gap-3">
                    <a
                      href={postProfileUrl}
                      className="w-11 h-11 rounded-full bg-gray-200 flex items-center justify-center font-bold text-gray-700"
                      title="Open profile"
                    >
                      {getInitials(p.user_name)}
                    </a>

                    <div>
                      <div className="flex items-center gap-2 flex-wrap">
                        <a
                          href={postProfileUrl}
                          className="font-bold text-gray-900 leading-5 hover:underline"
                        >
                          {p.user_name || "Unknown"}
                        </a>

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

                <div className="mt-3 text-[15px] leading-6 text-gray-800 whitespace-pre-wrap">
                  {p.content}
                </div>

                <div className="mt-3 flex items-center justify-between text-sm text-gray-500">
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

                  <button
                    type="button"
                    onClick={() => toggleComments(p.id)}
                    className="hover:text-gray-800"
                  >
                    {count} comments
                  </button>
                </div>

                <div className="mt-3 border-t border-gray-100" />

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
                            />
                          ))}
                        </div>
                      )}
                    </div>

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
