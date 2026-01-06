// backend/src/feed.js (FULL FILE - FIXED)
// ✅ Fixes:
// - parseAnyPostId now returns { kind, id } as your routes expect
// - export parseAnyPostId correctly
// - keeps your existing routes as-is

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
  // ✅ ensure we always have the correct parser
  const parseId = parseAnyPostIdOverride || parseAnyPostIdLocal;

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
        (SELECT COUNT(*) FROM post_likes WHERE post_id = p.id) AS likeCount,
        (SELECT COUNT(*) FROM post_likes WHERE post_id = p.id AND user_id = ?) AS likedByMe,
        (SELECT COUNT(*) FROM post_comments WHERE post_id = p.id) AS commentCount
      FROM posts p
      LEFT JOIN users u ON u.id = p.user_id
      ${where}
      ORDER BY p.id DESC
      `,
      [userId, ...params],
      (err, rows) => {
        if (err)
          return res.status(500).json({ message: "Failed to load posts" });
        res.json(
          (rows || []).map((r) => ({
            ...r,
            likedByMe: !!r.likedByMe,
            commentCount: Number(r.commentCount || 0),
            likeCount: Number(r.likeCount || 0),
          }))
        );
      }
    );
  });

  function updateFeedPostCore(req, res) {
    const parsed = parseId(req.params.id);
    const content = safeTrim(req.body?.content);
    const category = safeTrim(req.body?.category);

    // allow /api/posts/p_17 too
    const id =
      parsed?.kind === "feed"
        ? parsed.id
        : parsed?.kind === "numeric"
        ? parsed.id
        : null;

    if (!id) return res.status(400).json({ message: "Bad id" });
    if (!content) return res.status(400).json({ message: "Empty content" });

    dbRun(
      `UPDATE posts SET content = ?, category = COALESCE(?, category) WHERE id = ? AND user_id = ?`,
      [content, category || null, id, req.user.id],
      function (err) {
        if (err) return res.status(500).json({ message: "Update failed" });
        if (this.changes === 0)
          return res.status(404).json({ message: "Post not found" });
        res.json({ ok: true });
      }
    );
  }

  app.put("/api/posts/:id", authRequired, updateFeedPostCore);
  app.patch("/api/posts/:id", authRequired, updateFeedPostCore);

  // ✅ NEW: GET single feed post (supports /api/posts/17 and /api/posts/p_17)
  function getSingleFeedPost(req, res) {
    const parsed = parseId(req.params.id);
    const id =
      parsed?.kind === "feed"
        ? parsed.id
        : parsed?.kind === "numeric"
        ? parsed.id
        : null;

    if (!id) return res.status(400).json({ message: "Bad id" });

    const userId = req.user?.id || 0;

    dbGet(
      `
      SELECT
        p.*,
        u.username AS user_name,
        (SELECT COUNT(*) FROM post_likes WHERE post_id = p.id) AS likeCount,
        (SELECT COUNT(*) FROM post_likes WHERE post_id = p.id AND user_id = ?) AS likedByMe,
        (SELECT COUNT(*) FROM post_comments WHERE post_id = p.id) AS commentCount
      FROM posts p
      LEFT JOIN users u ON u.id = p.user_id
      WHERE p.id = ?
      `,
      [userId, id],
      (err, row) => {
        if (err) return res.status(500).json({ message: "Failed" });
        if (!row) return res.status(404).json({ message: "Post not found" });
        res.json({
          ...row,
          likedByMe: !!row.likedByMe,
          commentCount: Number(row.commentCount || 0),
          likeCount: Number(row.likeCount || 0),
        });
      }
    );
  }
  app.get("/api/posts/:id", authOptional, getSingleFeedPost);
  app.get("/api/post/:id", authOptional, getSingleFeedPost);

  app.post("/api/posts", authRequired, (req, res) => {
    const content = safeTrim(req.body?.content);
    const category = safeTrim(req.body?.category);

    if (!content) return res.status(400).json({ message: "Empty post" });

    dbRun(
      `INSERT INTO posts (user_id, content, category) VALUES (?, ?, ?)`,
      [req.user.id, content, category || null],
      (err) => {
        if (err) return res.status(500).json({ message: "Create post failed" });
        res.json({ ok: true });
      }
    );
  });

  /* ========= DELETE Post ========= */
  function deletePostById(req, res) {
    const parsed = parseId(req.params.id);
    const id =
      parsed?.kind === "feed"
        ? parsed.id
        : parsed?.kind === "numeric"
        ? parsed.id
        : null;

    if (!id) return res.status(400).json({ message: "Bad id" });
    return deleteFeedPostOwnedBy(req.user.id, id, res);
  }

  app.delete("/api/posts/:id", authRequired, deletePostById);
  app.delete("/api/posts/delete/:id", authRequired, deletePostById);
  app.delete("/api/delete-post/:id", authRequired, deletePostById);
  app.delete("/api/post/:id", authRequired, deletePostById);
  app.post("/api/posts/:id/delete", authRequired, deletePostById);
  app.post("/api/posts/delete/:id", authRequired, deletePostById);

  // ✅ NEW: aliases for "weird" frontend delete tries
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
    const parsed = parseId(req.params.id);
    const postId =
      parsed?.kind === "feed"
        ? parsed.id
        : parsed?.kind === "numeric"
        ? parsed.id
        : null;

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
     COMMENTS (LIKES + REPLIES)
  ===================== */

  // ✅ (1) Core: GET comments for feed post
  function getFeedPostCommentsCore(req, res, postIdOverride = null) {
    const postId =
      postIdOverride ||
      (() => {
        const parsed = parseId(req.params.id);
        return parsed?.kind === "feed"
          ? parsed.id
          : parsed?.kind === "numeric"
          ? parsed.id
          : null;
      })();

    if (!postId) return res.status(400).json({ message: "Bad postId" });

    const userId = req.user?.id || 0;

    dbAll(
      `
      SELECT
        c.*,
        u.username AS user_name,
        (SELECT COUNT(*) FROM post_comment_likes WHERE comment_id = c.id) AS likeCount,
        (SELECT COUNT(*) FROM post_comment_likes WHERE comment_id = c.id AND user_id = ?) AS likedByMe
      FROM post_comments c
      LEFT JOIN users u ON u.id = c.user_id
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
          }))
        );
      }
    );
  }

  // ✅ (2) Core: POST comment
  function createFeedPostCommentCore(req, res, postIdOverride = null) {
    const postId =
      postIdOverride ||
      (() => {
        const parsed = parseId(req.params.id);
        return parsed?.kind === "feed"
          ? parsed.id
          : parsed?.kind === "numeric"
          ? parsed.id
          : null;
      })();

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

  // ✅ ROUTES: canonical
  app.get("/api/posts/:id/comments", authOptional, (req, res) =>
    getFeedPostCommentsCore(req, res)
  );
  app.post("/api/posts/:id/comments", authRequired, (req, res) =>
    createFeedPostCommentCore(req, res)
  );

  // ✅ Profile legacy: POST /api/comments  (postId in body)
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

    // force normalized fields so core works
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

  // ✅✅✅ (A) LEGACY ALIASES: fix 404 shown in your screenshot
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

  // feed route aliases like "p_17"
  if (/^p_\d+$/.test(s)) return { kind: "feed", id: parseInt(s.slice(2), 10) };

  // profile posts like "pp_18" (not a feed post)
  if (/^pp_\d+$/.test(s))
    return { kind: "profile", id: parseInt(s.slice(3), 10) };

  // numeric
  if (/^\d+$/.test(s)) return { kind: "numeric", id: parseInt(s, 10) };

  // fallback: extract digits
  const m = s.match(/(\d+)/);
  if (!m) return { kind: "bad", id: null };
  const n = parseInt(m[1], 10);
  return Number.isFinite(n) && n > 0
    ? { kind: "numeric", id: n }
    : { kind: "bad", id: null };
}

module.exports.parseAnyPostId = parseAnyPostIdLocal;
