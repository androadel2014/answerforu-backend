// backend/src/feed.js (FULL FILE - FINAL + SANITIZE FIX)
// ✅ FIXES:
// - PUT/PATCH /api/posts/:id supports multipart/form-data (images + keep/remove)
// - remove works حتى لو اليوزر بعت absolute URL (http://localhost:5000/uploads/...)
// - ✅ NEW: sanitize media list so empty/bad entries never get stored (prevents blank boxes)
// - stores media_urls JSON + media_url first image
// - returns `media: []` in GET list + single
// - keeps your existing routes (comments/likes/delete/aliases) intact
// ✅ NEW (THIS PATCH):
// - include author avatar in posts + comments (from user_profile, fallback users)
// ✅ CRITICAL FIX (THIS PATCH):
// - SAVE post images into backend/uploads/posts (same folder served by server.js /uploads)

const fs = require("fs");
const path = require("path");
const multer = require("multer");

module.exports = function registerFeed({
  app,
  db,
  dbGet,
  dbAll,
  dbRun,
  authRequired,
  authOptional,
  safeTrim,
  toInt,
  parseAnyPostId: parseAnyPostIdOverride, // optional override
  deleteFeedPostOwnedBy,
}) {
  const parseId = parseAnyPostIdOverride || parseAnyPostIdLocal;

  /* =====================
     Uploads (posts images)
     ✅ IMPORTANT: store inside backend/uploads/posts
  ===================== */
  const UPLOAD_DIR = path.resolve(__dirname, "..", "uploads", "posts");
  try {
    fs.mkdirSync(UPLOAD_DIR, { recursive: true });
  } catch {}

  const storage = multer.diskStorage({
    destination: (_req, _file, cb) => cb(null, UPLOAD_DIR),
    filename: (_req, file, cb) => {
      const ext = path.extname(file.originalname || "").toLowerCase() || ".jpg";
      const safeExt = [".jpg", ".jpeg", ".png", ".webp", ".gif"].includes(ext)
        ? ext
        : ".jpg";
      const name =
        Date.now() + "_" + Math.random().toString(16).slice(2) + safeExt;
      cb(null, name);
    },
  });

  const upload = multer({
    storage,
    limits: { fileSize: 8 * 1024 * 1024 }, // 8MB per file
    fileFilter: (_req, file, cb) => {
      const t = String(file.mimetype || "").toLowerCase();
      if (t.startsWith("image/")) return cb(null, true);
      cb(new Error("Only images allowed"));
    },
  });

  const parseJsonArray = (v) => {
    try {
      const arr = JSON.parse(v || "[]");
      return Array.isArray(arr) ? arr : [];
    } catch {
      return [];
    }
  };

  const uniq = (arr) => Array.from(new Set((arr || []).filter(Boolean)));

  const parseListFromBody = (v) => {
    if (!v) return [];
    if (Array.isArray(v)) return v.filter(Boolean).map(String);
    const s = String(v).trim();
    if (!s) return [];
    if (s.startsWith("[")) return parseJsonArray(s).map(String);
    if (s.includes(","))
      return s
        .split(",")
        .map((x) => String(x).trim())
        .filter(Boolean);
    return [s];
  };

  const isMultipart = (req) =>
    String(req.headers["content-type"] || "").includes("multipart/form-data");

  const normalizePostIdFromParam = (param) => {
    const parsed = parseId(param);
    return parsed?.kind === "feed"
      ? parsed.id
      : parsed?.kind === "numeric"
      ? parsed.id
      : null;
  };

  const fileUrlFromUpload = (file) => `/uploads/posts/${file.filename}`;

  // ✅ validate media (prevents "" / "null" / "media-1" / weird stuff)
  const isValidMediaUrl = (u) => {
    const s = String(u || "").trim();
    if (!s) return false;
    if (s === "null" || s === "undefined") return false;
    if (/^media-\d+$/i.test(s)) return false;
    // allow relative uploads OR absolute http(s)
    if (s.startsWith("/uploads/")) return true;
    if (s.startsWith("http://") || s.startsWith("https://")) return true;
    // also accept "/uploads/posts/.."
    if (s.includes("/uploads/")) return true;
    return false;
  };

  // ✅ normalize url for compare (absolute vs relative) + sanitize invalid => ""
  const normalizeMediaKey = (u) => {
    const s0 = String(u || "").trim();
    if (!isValidMediaUrl(s0)) return "";
    // convert absolute => pathname
    if (s0.startsWith("http://") || s0.startsWith("https://")) {
      try {
        const url = new URL(s0);
        const p = String(url.pathname || "").trim();
        return isValidMediaUrl(p) ? p : "";
      } catch {
        return ""; // invalid absolute url
      }
    }
    // if contains /uploads/ in a longer string, cut from it
    if (s0.includes("/uploads/")) {
      const i = s0.indexOf("/uploads/");
      if (i >= 0) {
        const p = s0.slice(i).trim();
        return isValidMediaUrl(p) ? p : "";
      }
    }
    return s0;
  };

  const sanitizeMediaList = (arr) =>
    uniq((arr || []).map(normalizeMediaKey).filter(Boolean)).slice(0, 10);

  /* =====================
     FEED POSTS
  ===================== */

  app.get("/api/posts", authOptional, (req, res) => {
    const category = safeTrim(req.query.category);
    const userId = req.user?.id || 0;

    const where = category ? "WHERE p.category = ?" : "";
    const params = category ? [category] : [];

    dbAll(
      `
      SELECT
        p.*,
        u.username AS user_name,
        COALESCE(up.avatar_url, u.avatar_url) AS user_avatar,
        (SELECT COUNT(*) FROM post_likes WHERE post_id = p.id) AS likeCount,
        (SELECT COUNT(*) FROM post_likes WHERE post_id = p.id AND user_id = ?) AS likedByMe,
        (SELECT COUNT(*) FROM post_comments WHERE post_id = p.id) AS commentCount
      FROM posts p
      LEFT JOIN users u ON u.id = p.user_id
      LEFT JOIN user_profile up ON up.user_id = p.user_id
      ${where}
      ORDER BY p.id DESC
      `,
      [userId, ...params],
      (err, rows) => {
        if (err)
          return res.status(500).json({ message: "Failed to load posts" });

        res.json(
          (rows || []).map((r) => {
            const media = sanitizeMediaList(parseJsonArray(r.media_urls));
            return {
              ...r,
              likedByMe: !!r.likedByMe,
              commentCount: Number(r.commentCount || 0),
              likeCount: Number(r.likeCount || 0),
              media,
              // ✅ normalized field names for frontend
              author_name: r.user_name || null,
              author_avatar: r.user_avatar || null,
            };
          })
        );
      }
    );
  });

  function getSingleFeedPost(req, res) {
    const id = normalizePostIdFromParam(req.params.id);
    if (!id) return res.status(400).json({ message: "Bad id" });

    const userId = req.user?.id || 0;

    dbGet(
      `
      SELECT
        p.*,
        u.username AS user_name,
        COALESCE(up.avatar_url, u.avatar_url) AS user_avatar,
        (SELECT COUNT(*) FROM post_likes WHERE post_id = p.id) AS likeCount,
        (SELECT COUNT(*) FROM post_likes WHERE post_id = p.id AND user_id = ?) AS likedByMe,
        (SELECT COUNT(*) FROM post_comments WHERE post_id = p.id) AS commentCount
      FROM posts p
      LEFT JOIN users u ON u.id = p.user_id
      LEFT JOIN user_profile up ON up.user_id = p.user_id
      WHERE p.id = ?
      `,
      [userId, id],
      (err, row) => {
        if (err) return res.status(500).json({ message: "Failed" });
        if (!row) return res.status(404).json({ message: "Post not found" });

        const media = sanitizeMediaList(parseJsonArray(row.media_urls));
        res.json({
          ...row,
          likedByMe: !!row.likedByMe,
          commentCount: Number(row.commentCount || 0),
          likeCount: Number(row.likeCount || 0),
          media,
          // ✅ normalized field names for frontend
          author_name: row.user_name || null,
          author_avatar: row.user_avatar || null,
        });
      }
    );
  }
  app.get("/api/posts/:id", authOptional, getSingleFeedPost);
  app.get("/api/post/:id", authOptional, getSingleFeedPost);

  app.post(
    "/api/posts",
    authRequired,
    (req, res, next) => {
      if (isMultipart(req)) return upload.any()(req, res, next);

      return next();
    },
    (req, res) => {
      const content = safeTrim(req.body?.content);
      const category = safeTrim(req.body?.category);

      if (!content) return res.status(400).json({ message: "Empty post" });

      const files = (Array.isArray(req.files) ? req.files : [])
        .filter((f) =>
          String(f.mimetype || "")
            .toLowerCase()
            .startsWith("image/")
        )
        .slice(0, 10);

      const mediaArr = sanitizeMediaList(files.map(fileUrlFromUpload));

      const media_url = mediaArr[0] || null;
      const media_urls = JSON.stringify(mediaArr);

      dbRun(
        `INSERT INTO posts (user_id, content, category, media_url, media_urls)
         VALUES (?, ?, ?, ?, ?)`,
        [req.user.id, content, category || null, media_url, media_urls],
        (err) => {
          if (err)
            return res.status(500).json({ message: "Create post failed" });
          res.json({ ok: true, media: mediaArr });
        }
      );
    }
  );

  function updateFeedPostCore(req, res) {
    const id = normalizePostIdFromParam(req.params.id);
    if (!id) return res.status(400).json({ message: "Bad id" });

    const content = safeTrim(
      req.body?.content ?? req.body?.text ?? req.body?.body ?? req.body?.message
    );
    const category = safeTrim(req.body?.category);

    if (!content) return res.status(400).json({ message: "Empty content" });

    const keepFromBodyRaw = uniq([
      ...parseListFromBody(req.body?.keep_media_urls),
      ...parseListFromBody(req.body?.keep_media),
      ...parseListFromBody(req.body?.keepMedia),
      ...parseListFromBody(req.body?.media_keep),
    ]);

    const removeFromBodyRaw = uniq([
      ...parseListFromBody(req.body?.remove_media_urls),
      ...parseListFromBody(req.body?.remove_media),
      ...parseListFromBody(req.body?.removeMedia),
      ...parseListFromBody(req.body?.media_remove),
    ]);

    const keepFromBody = sanitizeMediaList(keepFromBodyRaw);
    const removeFromBody = sanitizeMediaList(removeFromBodyRaw);

    const files = Array.isArray(req.files) ? req.files : [];
    const added = sanitizeMediaList(files.map(fileUrlFromUpload));

    dbGet(
      `SELECT id, user_id, media_urls FROM posts WHERE id = ?`,
      [id],
      (e0, row) => {
        if (e0) return res.status(500).json({ message: "Update failed" });
        if (!row) return res.status(404).json({ message: "Post not found" });
        if (Number(row.user_id) !== Number(req.user.id))
          return res.sendStatus(403);

        const existing = sanitizeMediaList(parseJsonArray(row.media_urls));

        let nextMedia = keepFromBody.length ? keepFromBody.slice() : existing;

        if (removeFromBody.length) {
          const rm = new Set(removeFromBody.map(normalizeMediaKey));
          nextMedia = nextMedia.filter((u) => !rm.has(normalizeMediaKey(u)));
        }

        nextMedia = sanitizeMediaList([...nextMedia, ...added]);

        const media_url = nextMedia[0] || null;
        const media_urls = JSON.stringify(nextMedia);

        dbRun(
          `UPDATE posts
           SET content = ?, category = COALESCE(?, category), media_url = ?, media_urls = ?
           WHERE id = ? AND user_id = ?`,
          [content, category || null, media_url, media_urls, id, req.user.id],
          function (e1) {
            if (e1) return res.status(500).json({ message: "Update failed" });
            if (this.changes === 0)
              return res.status(404).json({ message: "Post not found" });

            return res.json({ ok: true, media: nextMedia });
          }
        );
      }
    );
  }

  const maybeUploadForUpdate = (req, res, next) => {
    if (isMultipart(req)) return upload.any()(req, res, next);
    return next();
  };

  app.put(
    "/api/posts/:id",
    authRequired,
    maybeUploadForUpdate,
    updateFeedPostCore
  );
  app.patch(
    "/api/posts/:id",
    authRequired,
    maybeUploadForUpdate,
    updateFeedPostCore
  );

  function deletePostById(req, res) {
    const id = normalizePostIdFromParam(req.params.id);
    if (!id) return res.status(400).json({ message: "Bad id" });
    return deleteFeedPostOwnedBy(req.user.id, id, res);
  }

  app.delete("/api/posts/:id", authRequired, deletePostById);
  app.delete("/api/posts/delete/:id", authRequired, deletePostById);
  app.delete("/api/delete-post/:id", authRequired, deletePostById);
  app.delete("/api/post/:id", authRequired, deletePostById);
  app.post("/api/posts/:id/delete", authRequired, deletePostById);
  app.post("/api/posts/delete/:id", authRequired, deletePostById);

  function deleteMyPostCore(req, res) {
    const raw = safeTrim(req.params.postId || "");
    if (!raw) return res.status(400).json({ message: "Bad postId" });

    if (raw.startsWith("pp_")) {
      const postId = toInt(raw.slice(3));
      if (!postId) return res.status(400).json({ message: "Bad postId" });

      dbRun(
        `DELETE FROM profile_posts WHERE id = ? AND user_id = ?`,
        [postId, req.user.id],
        function (err) {
          if (err)
            return res.status(500).json({ message: "Delete post failed" });
          if (this.changes === 0)
            return res.status(404).json({ message: "Post not found" });
          return res.json({ ok: true });
        }
      );
      return;
    }

    if (raw.startsWith("p_")) {
      const postId = raw.slice(2);
      return deleteFeedPostOwnedBy(req.user.id, postId, res);
    }

    const numeric = toInt(raw);
    if (!numeric) return res.status(400).json({ message: "Bad postId" });

    dbGet(
      `SELECT id FROM posts WHERE id = ? AND user_id = ?`,
      [numeric, req.user.id],
      (e1, existsFeed) => {
        if (e1) return res.status(500).json({ message: "Delete post failed" });
        if (existsFeed) return deleteFeedPostOwnedBy(req.user.id, numeric, res);

        dbRun(
          `DELETE FROM profile_posts WHERE id = ? AND user_id = ?`,
          [numeric, req.user.id],
          function (err) {
            if (err)
              return res.status(500).json({ message: "Delete post failed" });
            if (this.changes === 0)
              return res.status(404).json({ message: "Post not found" });
            return res.json({ ok: true });
          }
        );
      }
    );
  }

  app.delete("/api/profile/me/posts/:postId", authRequired, deleteMyPostCore);
  app.delete("/api/me/profile/posts/:postId", authRequired, deleteMyPostCore);

  /* ========= LIKE Post ========= */
  app.post("/api/posts/:id/like", authRequired, (req, res) => {
    const postId = normalizePostIdFromParam(req.params.id);
    if (!postId) return res.status(400).json({ message: "Bad postId" });

    dbGet(`SELECT id FROM posts WHERE id = ?`, [postId], (e0, pRow) => {
      if (e0) return res.status(500).json({ message: "Like failed" });
      if (!pRow) return res.status(404).json({ message: "Post not found" });

      dbGet(
        `SELECT id FROM post_likes WHERE post_id = ? AND user_id = ?`,
        [postId, req.user.id],
        (err, row) => {
          if (err) return res.status(500).json({ message: "Like failed" });

          if (row) {
            dbRun(
              `DELETE FROM post_likes WHERE post_id = ? AND user_id = ?`,
              [postId, req.user.id],
              () => res.json({ liked: false })
            );
          } else {
            dbRun(
              `INSERT INTO post_likes (post_id, user_id) VALUES (?, ?)`,
              [postId, req.user.id],
              (e2) => {
                if (e2) return res.status(500).json({ message: "Like failed" });
                res.json({ liked: true });
              }
            );
          }
        }
      );
    });
  });

  /* =====================
     COMMENTS (LIKES + REPLIES)  (UNCHANGED)
     ✅ NEW: include avatar (from user_profile/users)
  ===================== */

  function getFeedPostCommentsCore(req, res, postIdOverride = null) {
    const postId =
      postIdOverride || (() => normalizePostIdFromParam(req.params.id))();
    if (!postId) return res.status(400).json({ message: "Bad postId" });

    const userId = req.user?.id || 0;

    dbAll(
      `
      SELECT
        c.*,
        u.username AS user_name,
        COALESCE(up.avatar_url, u.avatar_url) AS user_avatar,
        (SELECT COUNT(*) FROM post_comment_likes WHERE comment_id = c.id) AS likeCount,
        (SELECT COUNT(*) FROM post_comment_likes WHERE comment_id = c.id AND user_id = ?) AS likedByMe
      FROM post_comments c
      LEFT JOIN users u ON u.id = c.user_id
      LEFT JOIN user_profile up ON up.user_id = c.user_id
      WHERE c.post_id = ?
      ORDER BY c.id ASC
      `,
      [userId, postId],
      (err, rows) => {
        if (err)
          return res.status(500).json({ message: "Failed to load comments" });
        res.json(
          (rows || []).map((r) => ({
            ...r,
            likeCount: Number(r.likeCount || 0),
            likedByMe: !!r.likedByMe,
            // ✅ normalized field names for frontend
            author_name: r.user_name || null,
            author_avatar: r.user_avatar || null,
          }))
        );
      }
    );
  }

  function createFeedPostCommentCore(req, res, postIdOverride = null) {
    const postId =
      postIdOverride || (() => normalizePostIdFromParam(req.params.id))();
    if (!postId) return res.status(400).json({ message: "Bad postId" });

    const comment = safeTrim(
      req.body?.comment ??
        req.body?.content ??
        req.body?.text ??
        req.body?.message ??
        req.body?.body
    );

    if (!comment) return res.status(400).json({ message: "Empty comment" });

    const parentIdRaw =
      req.body?.parent_comment_id ??
      req.body?.parentId ??
      req.body?.parent_id ??
      req.body?.replyTo ??
      null;

    const parentId =
      parentIdRaw === null || parentIdRaw === "" ? null : toInt(parentIdRaw);

    dbGet(`SELECT id FROM posts WHERE id = ?`, [postId], (e0, pRow) => {
      if (e0) return res.status(500).json({ message: "Comment failed" });
      if (!pRow) return res.status(404).json({ message: "Post not found" });

      const insertNow = () => {
        dbRun(
          `INSERT INTO post_comments (post_id, user_id, comment, parent_comment_id)
           VALUES (?, ?, ?, ?)`,
          [postId, req.user.id, comment, parentId],
          (err) => {
            if (err) return res.status(500).json({ message: "Comment failed" });
            return res.json({ ok: true });
          }
        );
      };

      if (!parentId) return insertNow();

      dbGet(
        `SELECT id, post_id FROM post_comments WHERE id = ?`,
        [parentId],
        (e1, pr) => {
          if (e1) return res.status(500).json({ message: "Comment failed" });
          if (!pr)
            return res
              .status(404)
              .json({ message: "Parent comment not found" });
          if (Number(pr.post_id) !== postId)
            return res.status(400).json({ message: "Parent comment mismatch" });
          insertNow();
        }
      );
    });
  }

  app.get("/api/posts/:id/comments", authOptional, (req, res) =>
    getFeedPostCommentsCore(req, res)
  );
  app.post("/api/posts/:id/comments", authRequired, (req, res) =>
    createFeedPostCommentCore(req, res)
  );

  app.post("/api/comments", authRequired, (req, res) => {
    const postId = toInt(
      req.body?.post_id ??
        req.body?.postId ??
        req.body?.post_id_fk ??
        req.body?.post ??
        req.body?.id ??
        req.body?.post_id_ref
    );

    if (!postId) return res.status(400).json({ message: "Bad postId" });

    if (req.body && req.body.comment == null) {
      req.body.comment =
        req.body.content ??
        req.body.text ??
        req.body.message ??
        req.body.body ??
        "";
    }

    return createFeedPostCommentCore(req, res, postId);
  });

  app.get("/api/post_comments/:postId", authOptional, (req, res) => {
    const postId = toInt(req.params.postId);
    if (!postId) return res.status(400).json({ message: "Bad postId" });
    return getFeedPostCommentsCore(req, res, postId);
  });

  app.post("/api/post_comments/:postId", authRequired, (req, res) => {
    const postId = toInt(req.params.postId);
    if (!postId) return res.status(400).json({ message: "Bad postId" });
    return createFeedPostCommentCore(req, res, postId);
  });

  function deleteCommentCore(req, res) {
    const postId = toInt(req.params.postId);
    const commentId = toInt(req.params.commentId);
    if (!postId || !commentId)
      return res.status(400).json({ message: "Bad ids" });

    dbGet(
      `SELECT id, user_id, post_id FROM post_comments WHERE id = ? AND post_id = ?`,
      [commentId, postId],
      (err, row) => {
        if (err) return res.status(500).json({ message: "Delete failed" });
        if (!row) return res.status(404).json({ message: "Comment not found" });
        if (row.user_id !== req.user.id) return res.sendStatus(403);

        dbRun(
          `DELETE FROM post_comment_likes WHERE comment_id IN (SELECT id FROM post_comments WHERE parent_comment_id = ?)`,
          [commentId],
          () => {
            dbRun(
              `DELETE FROM post_comments WHERE parent_comment_id = ?`,
              [commentId],
              () => {
                dbRun(
                  `DELETE FROM post_comment_likes WHERE comment_id = ?`,
                  [commentId],
                  () => {
                    dbRun(
                      `DELETE FROM post_comments WHERE id = ?`,
                      [commentId],
                      function (err2) {
                        if (err2)
                          return res
                            .status(500)
                            .json({ message: "Delete failed" });
                        return res.json({ ok: true });
                      }
                    );
                  }
                );
              }
            );
          }
        );
      }
    );
  }

  app.delete(
    "/api/posts/:postId/comments/:commentId",
    authRequired,
    deleteCommentCore
  );
  app.delete(
    "/api/post_comments/:postId/:commentId",
    authRequired,
    deleteCommentCore
  );

  function toggleLikeComment(req, res) {
    const commentId = toInt(req.params.commentId);
    if (!commentId) return res.status(400).json({ message: "Bad commentId" });

    dbGet(
      `SELECT id FROM post_comments WHERE id = ?`,
      [commentId],
      (e1, cRow) => {
        if (e1) return res.status(500).json({ message: "Like failed" });
        if (!cRow)
          return res.status(404).json({ message: "Comment not found" });

        dbGet(
          `SELECT id FROM post_comment_likes WHERE comment_id = ? AND user_id = ?`,
          [commentId, req.user.id],
          (err, row) => {
            if (err) return res.status(500).json({ message: "Like failed" });

            if (row) {
              dbRun(
                `DELETE FROM post_comment_likes WHERE comment_id = ? AND user_id = ?`,
                [commentId, req.user.id],
                () => res.json({ liked: false })
              );
            } else {
              dbRun(
                `INSERT INTO post_comment_likes (comment_id, user_id) VALUES (?, ?)`,
                [commentId, req.user.id],
                (e2) => {
                  if (e2)
                    return res.status(500).json({ message: "Like failed" });
                  res.json({ liked: true });
                }
              );
            }
          }
        );
      }
    );
  }

  app.post(
    "/api/posts/:postId/comments/:commentId/like",
    authRequired,
    toggleLikeComment
  );
  app.post("/api/comments/:commentId/like", authRequired, toggleLikeComment);
  app.post(
    "/api/post_comments/:postId/:commentId/like",
    authRequired,
    toggleLikeComment
  );
};

// ✅ local parser that matches your routes expectation: { kind, id }
function parseAnyPostIdLocal(input) {
  const s = String(input ?? "").trim();
  if (!s) return { kind: "bad", id: null };

  if (/^p_\d+$/.test(s)) return { kind: "feed", id: parseInt(s.slice(2), 10) };

  if (/^pp_\d+$/.test(s))
    return { kind: "profile", id: parseInt(s.slice(3), 10) };

  if (/^\d+$/.test(s)) return { kind: "numeric", id: parseInt(s, 10) };

  const m = s.match(/(\d+)/);
  if (!m) return { kind: "bad", id: null };
  const n = parseInt(m[1], 10);
  return Number.isFinite(n) && n > 0
    ? { kind: "numeric", id: n }
    : { kind: "bad", id: null };
}

module.exports.parseAnyPostId = parseAnyPostIdLocal;
