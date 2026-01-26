// src/components/profile/PostCard.jsx
import React, { useEffect, useMemo, useState } from "react";
import toast from "react-hot-toast";
import {
  Pencil,
  Trash2,
  SendHorizontal,
  ChevronDown,
  ChevronUp,
  ThumbsUp,
  Share2,
  MessageCircle,
  X,
} from "lucide-react";
import EditPostModal from "../profile/EditPostModal";

const classNames = (...xs) => xs.filter(Boolean).join(" ");

const toArray = (v) => {
  if (!v) return [];
  if (Array.isArray(v)) return v.filter(Boolean);
  if (typeof v === "string") {
    const s = v.trim();
    if (!s) return [];
    if (s.startsWith("[")) {
      try {
        const arr = JSON.parse(s);
        return Array.isArray(arr) ? arr.filter(Boolean) : [];
      } catch {
        return [];
      }
    }
    return [s].filter(Boolean);
  }
  return [];
};

const uniq = (arr) => Array.from(new Set((arr || []).filter(Boolean)));

export default function PostCard({
  lang,
  API_BASE,
  post,
  profile,
  isMe,
  canAct,
  onDelete,
  onUpdate,

  // inject from parent
  tt,
  Modal,
  toastConfirm,
  CommentNode,
  buildCommentTree,
  tryFetchFallback,
  authHeaders,
  getAuthUserId,
  getInitials,
  formatTime,
  getPostId,
  normalizeFeedPostId,
  extractNumericId,
}) {
  const rawPostId = getPostId(post);

  const numericPostId = useMemo(() => {
    const s = String(rawPostId ?? "").trim();
    if (!s) return null;
    if (s.startsWith("pp_")) return null;

    const direct = normalizeFeedPostId(s);
    if (direct) return direct;

    const extracted = extractNumericId(s);
    return extracted || null;
  }, [rawPostId, normalizeFeedPostId, extractNumericId]);

  const postIdForComments = numericPostId ? String(numericPostId) : null;
  const created = formatTime(post.created_at || post.createdAt) || "";

  const authorName =
    post.author_name ||
    post.user_name ||
    post.username ||
    profile?.display_name ||
    profile?.username ||
    "User";

  const absMediaUrl = (u) => {
    const s = String(u || "").trim();
    if (!s) return "";
    if (s === "null" || s === "undefined") return "";
    if (s.startsWith("http://") || s.startsWith("https://")) return s;
    return `${API_BASE}${s.startsWith("/") ? "" : "/"}${s}`;
  };

  // ✅ get my avatar from storage (for comment input + optimistic comment)
  const getMyAvatarFromStorage = () => {
    try {
      const tryRead = (k) => {
        const v = localStorage.getItem(k);
        if (!v) return null;
        try {
          return JSON.parse(v);
        } catch {
          return v;
        }
      };

      const userObj = tryRead("user");
      const userId = localStorage.getItem("userId");

      const candidates = [
        // common shapes
        userObj?.avatar_url,
        userObj?.avatar,
        userObj?.photo_url,
        userObj?.photoUrl,
        userObj?.profile?.avatar_url,
        userObj?.profile?.avatar,
        userObj?.user_profile?.avatar_url,
        userObj?.user_profile?.avatar,
        userObj?.userProfile?.avatar_url,
        userObj?.userProfile?.avatar,

        // sometimes stored separately
        tryRead("avatar_url"),
        tryRead("avatarUrl"),
        tryRead("profile_avatar_url"),
      ]
        .map((x) => (typeof x === "string" ? x : ""))
        .map((x) => x.trim())
        .filter(Boolean);

      // last fallback: if profile page is mine, use its profile.avatar_url
      if (profile?.avatar_url) candidates.push(String(profile.avatar_url));

      // even last: users/me response cached?
      const me = tryRead("me");
      if (me?.avatar_url) candidates.push(String(me.avatar_url));

      // if still nothing and userId exists maybe some apps store per-id key
      if (userId) {
        const v1 = tryRead(`avatar_url_${userId}`);
        if (typeof v1 === "string" && v1.trim()) candidates.push(v1.trim());
      }

      const first = candidates[0] || "";
      return absMediaUrl(first);
    } catch {
      return "";
    }
  };

  const myAvatar = useMemo(() => getMyAvatarFromStorage(), [API_BASE]); // eslint-disable-line

  // ✅ author avatar (supports new backend fields)
  const authorAvatarRaw =
    post.author_avatar ||
    post.user_avatar ||
    post.avatar_url ||
    profile?.avatar_url ||
    post.userAvatar ||
    post.avatar ||
    "";

  const authorAvatar = absMediaUrl(authorAvatarRaw);

  // prevent "blank circle" when img fails
  const [authorAvatarOk, setAuthorAvatarOk] = useState(true);
  useEffect(() => {
    setAuthorAvatarOk(true);
  }, [authorAvatar]);

  // =========================
  // Media (current)
  // =========================
  const currentImages = useMemo(() => {
    const baseMedia = toArray(post.media);
    const images = [
      ...baseMedia,
      ...(baseMedia.length ? [] : toArray(post.media_urls)),
      ...toArray(post.media_url),
      ...toArray(post.image_url),
      ...toArray(post.image),
      ...toArray(post.images),
    ]
      .map(absMediaUrl)
      .filter(Boolean);

    return uniq(images);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [post]);

  // =========================
  // Edit Modal (EXTERNAL)
  // =========================
  const [editOpen, setEditOpen] = useState(false);

  function openEdit() {
    setEditOpen(true);
  }

  async function handleSaveEdit({
    content,
    keepMedia,
    removeMedia,
    files,
    mediaPreview,
  }) {
    await onUpdate({
      content,
      keepMedia,
      removeMedia,
      files,
      media: Array.isArray(mediaPreview) ? mediaPreview : undefined,
    });
  }

  // =========================
  // Post Actions (Like / Share)
  // =========================
  const canLikePost = !!postIdForComments;

  const [postLiked, setPostLiked] = useState(
    !!(post.likedByMe ?? post.liked_by_me)
  );
  const [postLikeCount, setPostLikeCount] = useState(
    Number(
      post.likeCount ??
        post.likesCount ??
        post.like_count ??
        post.likes_count ??
        0
    ) || 0
  );

  useEffect(() => {
    setPostLiked(!!(post.likedByMe ?? post.liked_by_me));
    setPostLikeCount(
      Number(
        post.likeCount ??
          post.likesCount ??
          post.like_count ??
          post.likes_count ??
          0
      ) || 0
    );
  }, [post]);

  async function onToggleLikePost() {
    if (!canAct) return toast.error(tt(lang, "loginFirst"));
    if (!canLikePost) return toast.error(tt(lang, "commentsUnavailable"));

    const prevLiked = postLiked;
    setPostLiked(!prevLiked);
    setPostLikeCount((n) => (!prevLiked ? n + 1 : Math.max(0, n - 1)));

    try {
      const pid = postIdForComments;
      await tryFetchFallback(
        [
          `${API_BASE}/api/posts/${encodeURIComponent(pid)}/like`,
          `${API_BASE}/api/post/${encodeURIComponent(pid)}/like`,
          `${API_BASE}/api/posts/${encodeURIComponent(pid)}/like`,
        ],
        { method: "POST", headers: { ...authHeaders() } }
      );
    } catch {
      setPostLiked(prevLiked);
      setPostLikeCount((n) => (prevLiked ? n + 1 : Math.max(0, n - 1)));
    }
  }

  async function onSharePost() {
    const pid = postIdForComments || rawPostId;
    const url = `${window.location.origin}/feed?postId=${encodeURIComponent(
      String(pid)
    )}`;

    try {
      if (navigator.share) {
        await navigator.share({ title: "AnswerForU", url });
        toast.success(tt(lang, "shared"));
        return;
      }
      await navigator.clipboard.writeText(url);
      toast.success(tt(lang, "linkCopied"));
    } catch {
      try {
        await navigator.clipboard.writeText(url);
        toast.success(tt(lang, "linkCopied"));
      } catch {
        toast("Copy link: " + url);
      }
    }
  }

  // =========================
  // Comments
  // =========================
  const [commentsLoading, setCommentsLoading] = useState(false);
  const [comments, setComments] = useState([]);
  const [openComments, setOpenComments] = useState(true);

  const [draft, setDraft] = useState("");
  const [replyTo, setReplyTo] = useState(null);
  const [showRepliesMap, setShowRepliesMap] = useState({});

  const tree = useMemo(
    () => buildCommentTree(comments),
    [comments, buildCommentTree]
  );

  const makeCommentGetUrls = (pid) => [
    `${API_BASE}/api/post_comments/${encodeURIComponent(pid)}`,
    `${API_BASE}/api/comments/${encodeURIComponent(pid)}`,
    `${API_BASE}/api/comments/post/${encodeURIComponent(pid)}`,
    `${API_BASE}/api/posts/${encodeURIComponent(pid)}/comments`,
    `${API_BASE}/api/post/${encodeURIComponent(pid)}/comments`,
  ];

  const makeCommentPostUrls = (pid) => [
    `${API_BASE}/api/comments`,
    `${API_BASE}/api/comments/post/${encodeURIComponent(pid)}`,
    `${API_BASE}/api/posts/${encodeURIComponent(pid)}/comments`,
    `${API_BASE}/api/post/${encodeURIComponent(pid)}/comments`,
  ];

  const makeCommentDeleteUrls = (pid, commentId) => [
    `${API_BASE}/api/comments/${commentId}`,
    `${API_BASE}/api/comment/${commentId}`,
    `${API_BASE}/api/posts/${encodeURIComponent(pid)}/comments/${commentId}`,
    `${API_BASE}/api/post/${encodeURIComponent(pid)}/comments/${commentId}`,
  ];

  async function loadComments() {
    if (!postIdForComments) {
      setComments([]);
      return;
    }

    setCommentsLoading(true);
    try {
      const urls = makeCommentGetUrls(postIdForComments);
      const r = await tryFetchFallback(urls, { headers: { ...authHeaders() } });

      const items =
        r?.comments ||
        r?.items ||
        r?.data ||
        r?.results ||
        (Array.isArray(r) ? r : []);

      setComments(Array.isArray(items) ? items : []);
    } catch {
      setComments([]);
    } finally {
      setCommentsLoading(false);
    }
  }

  useEffect(() => {
    loadComments();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [String(postIdForComments ?? "")]);

  async function onSendComment() {
    if (!canAct) return toast.error(tt(lang, "loginFirst"));
    if (!postIdForComments) return toast.error(tt(lang, "commentsDisabled"));

    const content = String(draft || "").trim();
    if (!content) return;

    const payload = replyTo
      ? { content, parent_id: replyTo, post_id: postIdForComments }
      : { content, post_id: postIdForComments };

    const optimistic = {
      id: "tmp_" + Math.random().toString(16).slice(2),
      post_id: postIdForComments,
      parent_id: replyTo || null,
      content,
      author_name: "You",
      author_avatar: myAvatar || "",
      created_at: new Date().toISOString(),
      likes_count: 0,
      is_liked: false,
      can_delete: true,
      author_id: getAuthUserId(),
    };

    setComments((xs) => [...xs, optimistic]);
    setDraft("");
    setReplyTo(null);

    try {
      const urls = makeCommentPostUrls(postIdForComments);

      await tryFetchFallback(urls, {
        method: "POST",
        headers: { "Content-Type": "application/json", ...authHeaders() },
        body: JSON.stringify(payload),
      });

      await loadComments();
    } catch (e) {
      setComments((xs) => xs.filter((c) => c.id !== optimistic.id));
      toast.error(e.message || "Failed to send comment");
    }
  }

  async function onDeleteComment(commentId) {
    const ok = await toastConfirm({
      lang,
      title: tt(lang, "deleteCommentQ"),
      confirmText: tt(lang, "confirmDelete"),
    });
    if (!ok) return;

    const prev = comments;
    setComments((xs) => xs.filter((c) => (c.id ?? c.comment_id) !== commentId));

    try {
      if (!postIdForComments) throw new Error("Missing post id for comments");
      const urls = makeCommentDeleteUrls(postIdForComments, commentId);

      await tryFetchFallback(urls, {
        method: "DELETE",
        headers: { ...authHeaders() },
      });

      toast.success(tt(lang, "deleted"));
      await loadComments();
    } catch (e) {
      setComments(prev);
      toast.error(e.message || tt(lang, "deleteFailed"));
    }
  }

  async function onToggleLikeComment(commentId, currentlyLiked) {
    if (!canAct) return toast.error(tt(lang, "loginFirst"));

    setComments((xs) =>
      xs.map((c) => {
        const id = c.id ?? c.comment_id;
        if (id !== commentId) return c;
        const liked = !currentlyLiked;
        const likes =
          Number(c.likes_count ?? c.likesCount ?? c.likes ?? 0) || 0;
        return {
          ...c,
          is_liked: liked,
          likes_count: liked ? likes + 1 : Math.max(0, likes - 1),
        };
      })
    );

    try {
      await tryFetchFallback(
        [
          `${API_BASE}/api/comments/${commentId}/like`,
          `${API_BASE}/api/comment/${commentId}/like`,
          `${API_BASE}/api/like/comment/${commentId}`,
        ],
        {
          method: currentlyLiked ? "DELETE" : "POST",
          headers: { "Content-Type": "application/json", ...authHeaders() },
        }
      );
    } catch {
      await loadComments();
    }
  }

  function toggleReplies(commentId) {
    setShowRepliesMap((m) => ({ ...m, [commentId]: !m[commentId] }));
  }

  // my avatar bubble fallback control
  const [myAvatarOk, setMyAvatarOk] = useState(true);
  useEffect(() => {
    setMyAvatarOk(true);
  }, [myAvatar]);

  return (
    <div className="border rounded-2xl bg-white overflow-hidden">
      {/* ✅ External edit modal only */}
      <EditPostModal
        open={editOpen}
        onClose={() => setEditOpen(false)}
        lang={lang}
        tt={tt}
        API_BASE={API_BASE}
        post={post}
        Modal={Modal}
        onSave={handleSaveEdit}
      />

      {/* ===== Card Header ===== */}
      <div className="px-4 py-3 flex items-start gap-3">
        <div className="w-10 h-10 rounded-full overflow-hidden bg-gray-900 text-white flex items-center justify-center font-bold">
          {authorAvatar && authorAvatarOk ? (
            <img
              src={authorAvatar}
              alt="a"
              className="w-full h-full object-cover"
              onError={() => setAuthorAvatarOk(false)}
            />
          ) : (
            getInitials(authorName)
          )}
        </div>

        <div className="flex-1">
          <div className="flex items-center justify-between gap-2">
            <div>
              <div className="font-semibold leading-tight">{authorName}</div>
              <div className="text-xs text-gray-500">{created}</div>
            </div>

            {isMe ? (
              <div className="flex items-center gap-1">
                <button
                  onClick={openEdit}
                  className="p-2 rounded-xl hover:bg-gray-50 text-gray-700"
                  title={tt(lang, "edit")}
                >
                  <Pencil size={16} />
                </button>
                <button
                  onClick={onDelete}
                  className="p-2 rounded-xl hover:bg-gray-50 text-red-600"
                  title={tt(lang, "delete")}
                >
                  <Trash2 size={16} />
                </button>
              </div>
            ) : null}
          </div>

          <div className="mt-2 whitespace-pre-wrap text-gray-900">
            {post.content}
          </div>

          {/* Post media */}
          {currentImages.length ? (
            <div className="mt-3 grid grid-cols-1 sm:grid-cols-2 gap-2">
              {currentImages.map((src, i) => (
                <img
                  key={src + "_" + i}
                  src={src}
                  alt={`media-${i}`}
                  className="w-full max-h-[420px] object-cover rounded-2xl border"
                  loading="lazy"
                />
              ))}
            </div>
          ) : null}

          {/* Actions */}
          <div className="mt-3 grid grid-cols-3 gap-2">
            <button
              type="button"
              onClick={onToggleLikePost}
              className={classNames(
                "flex items-center justify-center gap-2 rounded-xl py-2 text-sm font-semibold border transition",
                postLiked
                  ? "bg-black text-white border-black"
                  : "bg-white text-gray-700 border-gray-200 hover:bg-gray-50"
              )}
              disabled={!canLikePost}
              title={!canLikePost ? "Not available for profile posts" : ""}
            >
              <ThumbsUp size={16} />
              {tt(lang, "like")}
              {postLikeCount ? ` (${postLikeCount})` : ""}
            </button>

            <button
              type="button"
              onClick={() => setOpenComments(true)}
              className="flex items-center justify-center gap-2 rounded-xl py-2 text-sm font-semibold border transition bg-white text-gray-700 border-gray-200 hover:bg-gray-50"
            >
              <MessageCircle size={16} />
              {tt(lang, "comments")}
            </button>

            <button
              type="button"
              onClick={onSharePost}
              className="flex items-center justify-center gap-2 rounded-xl py-2 text-sm font-semibold border transition bg-white text-gray-700 border-gray-200 hover:bg-gray-50"
            >
              <Share2 size={16} />
              {tt(lang, "share")}
            </button>
          </div>
        </div>
      </div>

      {/* ===== Comments ===== */}
      <div className="border-t bg-gray-50/60">
        <div className="px-4 py-3 flex items-center justify-between">
          <button
            onClick={() => setOpenComments((v) => !v)}
            className="text-sm font-semibold text-gray-800 flex items-center gap-2"
          >
            {tt(lang, "comments")}
            {openComments ? <ChevronUp size={16} /> : <ChevronDown size={16} />}
          </button>

          {!postIdForComments ? (
            <span className="text-xs text-gray-500">
              {tt(lang, "commentsUnavailable")}
            </span>
          ) : null}
        </div>

        {openComments ? (
          <div className="px-4 pb-4">
            {commentsLoading ? (
              <div className="animate-pulse space-y-3">
                <div className="h-12 rounded-2xl bg-gray-200" />
                <div className="h-12 rounded-2xl bg-gray-200" />
              </div>
            ) : (
              <div className="space-y-3">
                {tree.map((node) => (
                  <CommentNode
                    key={node.id}
                    lang={lang}
                    node={node}
                    depth={0}
                    showRepliesMap={showRepliesMap}
                    onToggleReplies={toggleReplies}
                    onReply={(id) => setReplyTo(id)}
                    onDelete={(id) => onDeleteComment(id)}
                    onLike={(id, liked) => onToggleLikeComment(id, liked)}
                    canAct={canAct}
                  />
                ))}

                {!tree.length ? (
                  <div className="text-sm text-gray-500">
                    {tt(lang, "noComments")}
                  </div>
                ) : null}
              </div>
            )}

            <div className="mt-4 flex items-start gap-2">
              <div className="w-10 h-10 rounded-full overflow-hidden bg-white border flex items-center justify-center font-bold">
                {myAvatar && myAvatarOk ? (
                  <img
                    src={myAvatar}
                    alt="me"
                    className="w-full h-full object-cover"
                    onError={() => setMyAvatarOk(false)}
                  />
                ) : (
                  getInitials("You")
                )}
              </div>

              <div className="flex-1">
                {replyTo ? (
                  <div className="mb-2 flex items-center justify-between text-xs bg-white border rounded-xl px-3 py-2">
                    <span className="text-gray-700">
                      {tt(lang, "replying")} (#{String(replyTo).slice(0, 6)})
                    </span>
                    <button
                      onClick={() => setReplyTo(null)}
                      className="text-gray-600 hover:text-gray-900"
                    >
                      <X size={14} />
                    </button>
                  </div>
                ) : null}

                <div className="flex items-center gap-2">
                  <input
                    value={draft}
                    onChange={(e) => setDraft(e.target.value)}
                    placeholder={tt(lang, "writeComment")}
                    className="w-full bg-white border rounded-2xl px-4 py-3 outline-none focus:ring-2 focus:ring-black/10"
                  />
                  <button
                    onClick={onSendComment}
                    className="px-4 py-3 rounded-2xl bg-black text-white hover:bg-gray-900 flex items-center gap-2"
                  >
                    <SendHorizontal size={16} />
                    {tt(lang, "send")}
                  </button>
                </div>

                {!canAct ? (
                  <div className="text-xs text-gray-500 mt-2">
                    {tt(lang, "loginFirst")}
                  </div>
                ) : null}
              </div>
            </div>
          </div>
        ) : null}
      </div>
    </div>
  );
}
