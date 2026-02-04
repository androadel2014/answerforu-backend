// src/modules/social-profile.routes.js
const multer = require("multer");

const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 6 * 1024 * 1024 }, // 6MB
});

const crypto = require("crypto");

module.exports = function registerSocialProfileRoutes({
  app,
  authRequired,
  authOptional,
  dbAll,
  dbGet,
  dbRun,
  toInt,
  safeTrim,
  safeUrl,
  safeJsonParse,
  ensureProfileRow,
  deleteFeedPostOwnedBy,
  parseAnyPostId,
}) {
  function hasColumn(table, col, cb) {
    dbAll(`PRAGMA table_info(${table})`, [], (e, rows) => {
      if (e) return cb(false);
      const ok = (rows || []).some(
        (r) =>
          String(r?.name || "").toLowerCase() === String(col).toLowerCase(),
      );
      cb(ok);
    });
  }

  function addColumnIfMissing(table, colDef) {
    const col = String(colDef).trim().split(/\s+/)[0]; // first token = column name
    hasColumn(table, col, (exists) => {
      if (exists) return;
      dbRun(`ALTER TABLE ${table} ADD COLUMN ${colDef}`, [], () => {});
    });
  }

  /* =========================
     ✅ Ensure users.public_id (non-guessable)
  ========================= */
  const genPublicId = () => {
    // 12~ chars url-safe
    return crypto.randomBytes(9).toString("base64url");
  };

  function ensureUsersPublicIdSchema() {
    addColumnIfMissing("users", "public_id TEXT");

    try {
      dbRun(
        `CREATE UNIQUE INDEX IF NOT EXISTS idx_users_public_id ON users(public_id)`,
        [],
        () => {},
      );
    } catch {}

    // backfill for old users (best-effort, safe if run multiple times)
    try {
      dbAll(
        `SELECT id FROM users WHERE public_id IS NULL OR TRIM(public_id) = ''`,
        [],
        (e, rows) => {
          if (e) return;
          const list = Array.isArray(rows) ? rows : [];
          list.forEach((r) => {
            const id = toInt(r?.id);
            if (!id) return;

            const pid = genPublicId();
            dbRun(
              `UPDATE users SET public_id = ? WHERE id = ? AND (public_id IS NULL OR TRIM(public_id) = '')`,
              [pid, id],
              () => {},
            );
          });
        },
      );
    } catch {}
  }

  ensureUsersPublicIdSchema();

  function resolveUserKey(raw, cb) {
    const key = String(raw ?? "").trim();
    if (!key) return cb(new Error("Bad userId"));

    // numeric id
    const n = toInt(key);
    if (n) return cb(null, n);

    // public_id
    dbGet(
      `SELECT id FROM users WHERE public_id = ? LIMIT 1`,
      [key],
      (e1, row1) => {
        if (e1) return cb(e1);
        if (row1?.id) return cb(null, toInt(row1.id));

        // username fallback (optional, helps old links)
        dbGet(
          `
          SELECT u.id
          FROM user_profile up
          JOIN users u ON u.id = up.user_id
          WHERE LOWER(up.username) = LOWER(?) OR LOWER(up.display_name) = LOWER(?)
          LIMIT 1
          `,
          [key, key],
          (e2, row2) => {
            if (e2) return cb(e2);
            if (row2?.id) return cb(null, toInt(row2.id));
            return cb(
              Object.assign(new Error("Profile not found"), { status: 404 }),
            );
          },
        );
      },
    );
  }

  /* =========================
     ✅ Ensure USER-REVIEWS table (profile write-review)
  ========================= */
  try {
    dbRun(
      `
      CREATE TABLE IF NOT EXISTS reviews (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,       -- target user being reviewed
        author_id INTEGER NOT NULL,     -- reviewer (logged-in user)
        rating INTEGER NOT NULL,
        comment TEXT NOT NULL,
        created_at TEXT DEFAULT (datetime('now'))
      )
    `,
      [],
      () => {},
    );

    dbRun(
      `CREATE UNIQUE INDEX IF NOT EXISTS idx_reviews_unique ON reviews(user_id, author_id)`,
      [],
      () => {},
    );

    dbRun(
      `CREATE INDEX IF NOT EXISTS idx_reviews_user ON reviews(user_id)`,
      [],
      () => {},
    );

    dbRun(
      `CREATE INDEX IF NOT EXISTS idx_reviews_author ON reviews(author_id)`,
      [],
      () => {},
    );
  } catch {}

  function getProfileCore(req, res) {
    resolveUserKey(req.params.userId, (eResolve, targetId) => {
      if (eResolve) {
        const st = eResolve?.status || 400;
        return res
          .status(st)
          .json({ message: eResolve.message || "Bad userId" });
      }

      ensureProfileRow(targetId, (e0) => {
        if (e0) return res.status(500).json({ message: "Failed" });

        dbGet(
          `
          SELECT up.*, u.public_id AS public_id
          FROM user_profile up
          LEFT JOIN users u ON u.id = up.user_id
          WHERE up.user_id = ?
          `,
          [targetId],
          (e1, p) => {
            if (e1) return res.status(500).json({ message: "Failed" });
            if (!p)
              return res.status(404).json({ message: "Profile not found" });

            const meId = req.user?.id || 0;

            dbGet(
              `SELECT COUNT(*) c FROM follows WHERE following_id = ?`,
              [targetId],
              (eF1, rFollowers) => {
                if (eF1) return res.status(500).json({ message: "Failed" });

                dbGet(
                  `SELECT COUNT(*) c FROM follows WHERE follower_id = ?`,
                  [targetId],
                  (eF2, rFollowing) => {
                    if (eF2) return res.status(500).json({ message: "Failed" });

                    dbGet(
                      `
                      SELECT
                        (SELECT COUNT(*) FROM profile_posts WHERE user_id = ?) +
                        (SELECT COUNT(*) FROM posts WHERE user_id = ?) AS c
                      `,
                      [targetId, targetId],
                      (eP, rPosts) => {
                        if (eP)
                          return res.status(500).json({ message: "Failed" });

                        dbGet(
                          `SELECT COUNT(*) c FROM services WHERE user_id = ? AND is_active = 1`,
                          [targetId],
                          (eS, rServices) => {
                            if (eS)
                              return res
                                .status(500)
                                .json({ message: "Failed" });

                            dbGet(
                              `SELECT COUNT(*) c FROM products WHERE user_id = ? AND is_available = 1`,
                              [targetId],
                              (ePr, rProducts) => {
                                if (ePr)
                                  return res
                                    .status(500)
                                    .json({ message: "Failed" });

                                dbGet(
                                  `
                                  SELECT
                                    (
                                      SELECT COALESCE(AVG(x.stars), 0)
                                      FROM (
                                        SELECT r.rating AS stars
                                        FROM reviews r
                                        WHERE r.user_id = ?

                                        UNION ALL

                                        SELECT pr.stars AS stars
                                        FROM community_places cp
                                        JOIN place_reviews pr ON pr.place_id = cp.id
                                        WHERE cp.created_by = ?

                                        UNION ALL

                                        SELECT gr.stars AS stars
                                        FROM community_groups cg
                                        JOIN group_reviews gr ON gr.group_id = cg.id
                                        WHERE cg.created_by = ?
                                      ) x
                                    ) AS avg,
                                    (
                                      SELECT COUNT(1)
                                      FROM (
                                        SELECT r.id AS id
                                        FROM reviews r
                                        WHERE r.user_id = ?

                                        UNION ALL

                                        SELECT pr.id AS id
                                        FROM community_places cp
                                        JOIN place_reviews pr ON pr.place_id = cp.id
                                        WHERE cp.created_by = ?

                                        UNION ALL

                                        SELECT gr.id AS id
                                        FROM community_groups cg
                                        JOIN group_reviews gr ON gr.group_id = cg.id
                                        WHERE cg.created_by = ?
                                      ) y
                                    ) AS c
                                  `,
                                  [
                                    targetId,
                                    targetId,
                                    targetId,
                                    targetId,
                                    targetId,
                                    targetId,
                                  ],
                                  (eR, rAgg) => {
                                    if (eR)
                                      return res
                                        .status(500)
                                        .json({ message: "Failed" });

                                    const base = {
                                      profile: p,
                                      stats: {
                                        followers: Number(rFollowers?.c || 0),
                                        following: Number(rFollowing?.c || 0),
                                        posts: Number(rPosts?.c || 0),
                                        services: Number(rServices?.c || 0),
                                        products: Number(rProducts?.c || 0),
                                        ratingAvg: Number(rAgg?.avg || 0),
                                        ratingCount: Number(rAgg?.c || 0),
                                      },
                                      isMe: meId === targetId,
                                      isFollowing: false,
                                    };

                                    if (!meId) return res.json(base);

                                    dbGet(
                                      `SELECT 1 x FROM follows WHERE follower_id = ? AND following_id = ?`,
                                      [meId, targetId],
                                      (eF3, fRow) => {
                                        if (eF3)
                                          return res
                                            .status(500)
                                            .json({ message: "Failed" });
                                        return res.json({
                                          ...base,
                                          isFollowing: !!fRow,
                                        });
                                      },
                                    );
                                  },
                                );
                              },
                            );
                          },
                        );
                      },
                    );
                  },
                );
              },
            );
          },
        );
      });
    });
  }

  // ✅ me aliases
  app.get("/api/profile/me", authRequired, (req, res) => {
    req.params.userId = String(req.user.id);
    return getProfileCore(req, res);
  });
  app.get("/api/profiles/me", authRequired, (req, res) => {
    req.params.userId = String(req.user.id);
    return getProfileCore(req, res);
  });

  // ✅ public id route (non-guessable)
  app.get("/api/u/:userId", authOptional, getProfileCore);

  // public profile (now supports numeric OR public_id OR username)
  app.get("/api/profile/:userId", authOptional, getProfileCore);
  app.get("/api/profiles/:userId", authOptional, getProfileCore);

  // update my profile
  app.put("/api/profile/me", authRequired, (req, res) => {
    const userId = req.user.id;

    ensureProfileRow(userId, (e0) => {
      if (e0) return res.status(500).json({ message: "Failed" });

      const body = req.body || {};
      const username = safeTrim(body.username);
      const display_name = safeTrim(body.display_name);
      const bio = safeTrim(body.bio);
      const location = safeTrim(body.location);
      const phone = safeTrim(body.phone);
      const whatsapp = safeTrim(body.whatsapp);
      const website = safeUrl(body.website);

      function doUpdate() {
        dbRun(
          `
          UPDATE user_profile
          SET
            username = COALESCE(?, username),
            display_name = COALESCE(?, display_name),
            bio = COALESCE(?, bio),
            location = COALESCE(?, location),
            phone = COALESCE(?, phone),
            whatsapp = COALESCE(?, whatsapp),
            website = COALESCE(?, website),
            updated_at = datetime('now')
          WHERE user_id = ?
          `,
          [
            username || null,
            display_name || null,
            bio || null,
            location || null,
            phone || null,
            whatsapp || null,
            website || null,
            userId,
          ],
          function (e2) {
            if (e2)
              return res.status(500).json({ message: "Profile update failed" });

            dbGet(
              `
              SELECT up.*, u.public_id AS public_id
              FROM user_profile up
              LEFT JOIN users u ON u.id = up.user_id
              WHERE up.user_id = ?
              `,
              [userId],
              (e3, p) => {
                if (e3 || !p)
                  return res
                    .status(500)
                    .json({ message: "Profile update failed" });
                return res.json({ ok: true, profile: p });
              },
            );
          },
        );
      }

      if (!username) return doUpdate();

      dbGet(
        `SELECT user_id FROM user_profile WHERE username = ? AND user_id != ?`,
        [username, userId],
        (e1, row) => {
          if (e1) return res.status(500).json({ message: "Failed" });
          if (row)
            return res.status(400).json({ message: "Username already taken" });
          doUpdate();
        },
      );
    });
  });

  /* =========================
     ✅ Avatar/Cover Upload (FIX 404)
     Frontend sends POST FormData: avatar/cover
     Backend currently supports PUT with avatar_url/cover_url only
     -> Add POST + aliases + keep PUT/DELETE
  ========================= */

  addColumnIfMissing("user_profile", "avatar_url TEXT");
  addColumnIfMissing("user_profile", "cover_url TEXT");

  // ✅ simplistic local upload (base64 data URL) - no external libs
  // NOTE: This keeps your current DB schema: avatar_url/cover_url is TEXT
  function readFileAsDataUrl(file) {
    if (!file) return null;

    // multer usually: { buffer, mimetype }
    if (file.buffer && file.mimetype) {
      const b64 = file.buffer.toString("base64");
      return `data:${file.mimetype};base64,${b64}`;
    }

    // in case you send raw string (rare)
    if (typeof file === "string") return file;

    return null;
  }

  // ✅ accept: (1) JSON body avatar_url/cover_url (PUT)
  //         (2) multipart/form-data "avatar" / "cover" (POST)
  function pickAvatarUrlFromReq(req) {
    const urlFromBody = safeUrl(req.body?.avatar_url);
    if (urlFromBody) return urlFromBody;

    const f = req.file || (req.files && (req.files.avatar || req.files.cover));
    if (f && Array.isArray(f)) return readFileAsDataUrl(f[0]);
    return readFileAsDataUrl(f);
  }

  function pickCoverUrlFromReq(req) {
    const urlFromBody = safeUrl(req.body?.cover_url);
    if (urlFromBody) return urlFromBody;

    const f = req.file || (req.files && (req.files.cover || req.files.avatar));
    if (f && Array.isArray(f)) return readFileAsDataUrl(f[0]);
    return readFileAsDataUrl(f);
  }

  function respondMeProfile(userId, res, failMsg) {
    dbGet(
      `
      SELECT up.*, u.public_id AS public_id
      FROM user_profile up
      LEFT JOIN users u ON u.id = up.user_id
      WHERE up.user_id = ?
      `,
      [userId],
      (e2, p) => {
        if (e2 || !p)
          return res.status(500).json({ message: failMsg || "Failed" });
        res.json({
          ok: true,
          profile: p,
          avatar_url: p.avatar_url || "",
          cover_url: p.cover_url || "",
        });
      },
    );
  }

  // -------------------------
  // ✅ Avatar handlers
  // -------------------------
  function avatarSetCore(req, res) {
    const userId = req.user.id;

    ensureProfileRow(userId, (e0) => {
      if (e0) return res.status(500).json({ message: "Failed" });

      const avatar_url = pickAvatarUrlFromReq(req);
      if (!avatar_url)
        return res
          .status(400)
          .json({ message: "Missing avatar (file) or avatar_url" });

      dbRun(
        `UPDATE user_profile SET avatar_url = ?, updated_at = datetime('now') WHERE user_id = ?`,
        [avatar_url, userId],
        function (e1) {
          if (e1) return res.status(500).json({ message: "Update failed" });
          return respondMeProfile(userId, res, "Update failed");
        },
      );
    });
  }

  function avatarDeleteCore(req, res) {
    const userId = req.user.id;

    ensureProfileRow(userId, (e0) => {
      if (e0) return res.status(500).json({ message: "Failed" });

      dbRun(
        `UPDATE user_profile SET avatar_url = NULL, updated_at = datetime('now') WHERE user_id = ?`,
        [userId],
        function (e1) {
          if (e1) return res.status(500).json({ message: "Delete failed" });
          return respondMeProfile(userId, res, "Delete failed");
        },
      );
    });
  }

  // -------------------------
  // ✅ Cover handlers
  // -------------------------
  function coverSetCore(req, res) {
    const userId = req.user.id;

    ensureProfileRow(userId, (e0) => {
      if (e0) return res.status(500).json({ message: "Failed" });

      const cover_url = pickCoverUrlFromReq(req);
      if (!cover_url)
        return res
          .status(400)
          .json({ message: "Missing cover (file) or cover_url" });

      dbRun(
        `UPDATE user_profile SET cover_url = ?, updated_at = datetime('now') WHERE user_id = ?`,
        [cover_url, userId],
        function (e1) {
          if (e1) return res.status(500).json({ message: "Update failed" });
          return respondMeProfile(userId, res, "Update failed");
        },
      );
    });
  }

  function coverDeleteCore(req, res) {
    const userId = req.user.id;

    ensureProfileRow(userId, (e0) => {
      if (e0) return res.status(500).json({ message: "Failed" });

      dbRun(
        `UPDATE user_profile SET cover_url = NULL, updated_at = datetime('now') WHERE user_id = ?`,
        [userId],
        function (e1) {
          if (e1) return res.status(500).json({ message: "Delete failed" });
          return respondMeProfile(userId, res, "Delete failed");
        },
      );
    });
  }

  // ✅ IMPORTANT: Frontend calls POST for upload
  // We don't assume multer exists. If you already have upload middleware, keep it.
  // If you have: const upload = ...  then wrap: app.post(path, authRequired, upload.single("avatar"), avatarSetCore)
  // For now, just accept JSON (avatar_url/cover_url) or any body parser that provides req.file/req.files.

  // ---- canonical routes ----

  app.put("/api/profile/me/avatar", authRequired, avatarSetCore);
  app.delete("/api/profile/me/avatar", authRequired, avatarDeleteCore);

  app.post(
    "/api/profile/me/avatar",
    authRequired,
    upload.single("avatar"),
    avatarSetCore,
  );

  app.post(
    "/api/profile/me/cover",
    authRequired,
    upload.single("cover"),
    coverSetCore,
  );

  app.put("/api/profile/me/cover", authRequired, coverSetCore);
  app.delete("/api/profile/me/cover", authRequired, coverDeleteCore);

  // ---- aliases (your frontend tries these) ----
  [
    "/api/profile/avatar",
    "/api/me/profile/avatar",
    "/api/user/profile/me/avatar",
  ].forEach((p) => {
    app.post(p, authRequired, upload.single("avatar"), avatarSetCore);
    app.put(p, authRequired, avatarSetCore);
    app.delete(p, authRequired, avatarDeleteCore);
  });

  [
    "/api/profile/cover",
    "/api/me/profile/cover",
    "/api/user/profile/me/cover",
  ].forEach((p) => {
    app.post(p, authRequired, upload.single("cover"), coverSetCore);
    app.put(p, authRequired, coverSetCore);
    app.delete(p, authRequired, coverDeleteCore);
  });

  // follow / unfollow (accept numeric OR public_id OR username)
  function followCore(req, res) {
    const me = req.user.id;

    resolveUserKey(req.params.userId, (eResolve, target) => {
      if (eResolve) {
        const st = eResolve?.status || 400;
        return res
          .status(st)
          .json({ message: eResolve.message || "Bad userId" });
      }

      if (me === target)
        return res.status(400).json({ message: "Cannot follow yourself" });

      dbRun(
        `INSERT OR IGNORE INTO follows (follower_id, following_id) VALUES (?, ?)`,
        [me, target],
        (err) => {
          if (err) return res.status(500).json({ message: "Follow failed" });
          res.json({ ok: true });
        },
      );
    });
  }

  function unfollowCore(req, res) {
    const me = req.user.id;

    resolveUserKey(req.params.userId, (eResolve, target) => {
      if (eResolve) {
        const st = eResolve?.status || 400;
        return res
          .status(st)
          .json({ message: eResolve.message || "Bad userId" });
      }

      dbRun(
        `DELETE FROM follows WHERE follower_id = ? AND following_id = ?`,
        [me, target],
        (err) => {
          if (err) return res.status(500).json({ message: "Unfollow failed" });
          res.json({ ok: true });
        },
      );
    });
  }

  app.post("/api/profile/:userId/follow", authRequired, followCore);
  app.delete("/api/profile/:userId/follow", authRequired, unfollowCore);
  app.post("/api/profiles/:userId/follow", authRequired, followCore);
  app.delete("/api/profiles/:userId/follow", authRequired, unfollowCore);
  app.post("/api/u/:userId/follow", authRequired, followCore);
  app.delete("/api/u/:userId/follow", authRequired, unfollowCore);

  // posts tab (profile_posts + posts) (accept numeric OR public_id OR username)
  function getProfilePostsCore(req, res) {
    resolveUserKey(req.params.userId, (eResolve, userId) => {
      if (eResolve) {
        const st = eResolve?.status || 400;
        return res
          .status(st)
          .json({ message: eResolve.message || "Bad userId" });
      }

      const sql = `
        SELECT *
        FROM (
          SELECT
            ('pp_' || pp.id) AS id,
            pp.user_id AS user_id,
            pp.content AS content,
            pp.media_url AS media_url,
            NULL AS media_urls,
            pp.created_at AS created_at,
            NULL AS category,
            'profile' AS source,
            u.username AS user_name,
            up.avatar_url AS user_avatar
          FROM profile_posts pp
          LEFT JOIN users u ON u.id = pp.user_id
          LEFT JOIN user_profile up ON up.user_id = pp.user_id
          WHERE pp.user_id = ?

          UNION ALL

          SELECT
            ('p_' || p.id) AS id,
            p.user_id AS user_id,
            p.content AS content,
            NULL AS media_url,
            p.media_urls AS media_urls,
            p.created_at AS created_at,
            p.category AS category,
            'feed' AS source,
            u2.username AS user_name,
            up2.avatar_url AS user_avatar
          FROM posts p
          LEFT JOIN users u2 ON u2.id = p.user_id
          LEFT JOIN user_profile up2 ON up2.user_id = p.user_id
          WHERE p.user_id = ?
        )
        ORDER BY datetime(created_at) DESC
        LIMIT 200
      `;

      dbAll(sql, [userId, userId], (err, rows) => {
        if (err)
          return res.status(500).json({ message: "Failed to load posts" });

        const normalized = (rows || []).map((r) => {
          let media = [];

          if (r.media_url) media = [r.media_url];

          if (r.media_urls) {
            try {
              const arr = JSON.parse(r.media_urls);
              if (Array.isArray(arr)) media = arr;
            } catch {}
          }

          return { ...r, media, media_url: null, media_urls: null };
        });

        res.json({ posts: normalized });
      });
    });
  }

  app.get("/api/profile/:userId/posts", authOptional, getProfilePostsCore);
  app.get("/api/profiles/:userId/posts", authOptional, getProfilePostsCore);
  app.get("/api/u/:userId/posts", authOptional, getProfilePostsCore);

  app.get("/api/profile_posts/:userId", authOptional, getProfilePostsCore);
  app.get(
    "/api/profile_posts/:userId/posts",
    authOptional,
    getProfilePostsCore,
  );
  app.get("/api/profile-posts/:userId", authOptional, getProfilePostsCore);
  app.get(
    "/api/profile-posts/:userId/posts",
    authOptional,
    getProfilePostsCore,
  );
  app.get("/api/users/:userId/posts", authOptional, getProfilePostsCore);

  // create my profile post
  app.post("/api/profile/me/posts", authRequired, (req, res) => {
    const content = safeTrim(req.body?.content);
    const media_url = safeUrl(req.body?.media_url);
    if (!content) return res.status(400).json({ message: "Empty post" });

    dbRun(
      `INSERT INTO profile_posts (user_id, content, media_url) VALUES (?, ?, ?)`,
      [req.user.id, content, media_url || null],
      function (err) {
        if (err) return res.status(500).json({ message: "Create post failed" });
        res.json({ ok: true, id: `pp_${this.lastID}` });
      },
    );
  });

  // get my single post
  app.get("/api/profile/me/posts/:postId", authRequired, (req, res) => {
    const parsed = parseAnyPostId(req.params.postId);
    if (parsed.kind === "bad")
      return res.status(400).json({ message: "Bad postId" });

    if (parsed.kind === "profile") {
      return dbGet(
        `SELECT id, user_id, content, media_url, created_at
         FROM profile_posts
         WHERE id = ? AND user_id = ?`,
        [parsed.id, req.user.id],
        (err, row) => {
          if (err) return res.status(500).json({ message: "Failed" });
          if (!row) return res.status(404).json({ message: "Not found" });
          res.json({ ...row, id: `pp_${row.id}`, source: "profile" });
        },
      );
    }

    if (parsed.kind === "feed") {
      return dbGet(
        `SELECT p.*, u.username AS user_name
         FROM posts p
         LEFT JOIN users u ON u.id = p.user_id
         WHERE p.id = ? AND p.user_id = ?`,
        [parsed.id, req.user.id],
        (err, row) => {
          if (err) return res.status(500).json({ message: "Failed" });
          if (!row) return res.status(404).json({ message: "Not found" });
          res.json({ ...row, id: `p_${row.id}`, source: "feed" });
        },
      );
    }

    dbGet(
      `SELECT p.*, u.username AS user_name
       FROM posts p
       LEFT JOIN users u ON u.id = p.user_id
       WHERE p.id = ? AND p.user_id = ?`,
      [parsed.id, req.user.id],
      (e1, feedRow) => {
        if (e1) return res.status(500).json({ message: "Failed" });
        if (feedRow)
          return res.json({
            ...feedRow,
            id: `p_${feedRow.id}`,
            source: "feed",
          });

        dbGet(
          `SELECT id, user_id, content, media_url, created_at
           FROM profile_posts
           WHERE id = ? AND user_id = ?`,
          [parsed.id, req.user.id],
          (e2, profRow) => {
            if (e2) return res.status(500).json({ message: "Failed" });
            if (!profRow) return res.status(404).json({ message: "Not found" });
            res.json({ ...profRow, id: `pp_${profRow.id}`, source: "profile" });
          },
        );
      },
    );
  });

  // update my post
  function updateMyPostCore(req, res) {
    const parsed = parseAnyPostId(req.params.postId);
    const content = safeTrim(req.body?.content);

    if (parsed.kind === "bad")
      return res.status(400).json({ message: "Bad postId" });
    if (!content) return res.status(400).json({ message: "Empty content" });

    if (parsed.kind === "profile") {
      return dbRun(
        `UPDATE profile_posts SET content = ? WHERE id = ? AND user_id = ?`,
        [content, parsed.id, req.user.id],
        function (err) {
          if (err) return res.status(500).json({ message: "Update failed" });
          if (this.changes === 0)
            return res.status(404).json({ message: "Post not found" });
          return res.json({ ok: true });
        },
      );
    }

    if (parsed.kind === "feed") {
      return dbRun(
        `UPDATE posts SET content = ? WHERE id = ? AND user_id = ?`,
        [content, parsed.id, req.user.id],
        function (err) {
          if (err) return res.status(500).json({ message: "Update failed" });
          if (this.changes === 0)
            return res.status(404).json({ message: "Post not found" });
          return res.json({ ok: true });
        },
      );
    }

    dbGet(
      `SELECT id FROM posts WHERE id = ? AND user_id = ?`,
      [parsed.id, req.user.id],
      (e1, existsFeed) => {
        if (e1) return res.status(500).json({ message: "Update failed" });

        if (existsFeed) {
          return dbRun(
            `UPDATE posts SET content = ? WHERE id = ? AND user_id = ?`,
            [content, parsed.id, req.user.id],
            function (err) {
              if (err)
                return res.status(500).json({ message: "Update failed" });
              if (this.changes === 0)
                return res.status(404).json({ message: "Post not found" });
              return res.json({ ok: true });
            },
          );
        }

        return dbRun(
          `UPDATE profile_posts SET content = ? WHERE id = ? AND user_id = ?`,
          [content, parsed.id, req.user.id],
          function (err) {
            if (err) return res.status(500).json({ message: "Update failed" });
            if (this.changes === 0)
              return res.status(404).json({ message: "Post not found" });
            return res.json({ ok: true });
          },
        );
      },
    );
  }

  app.put("/api/profile/me/posts/:postId", authRequired, updateMyPostCore);
  app.patch("/api/profile/me/posts/:postId", authRequired, updateMyPostCore);

  // delete my post
  app.delete("/api/profile/me/posts/:postId", authRequired, (req, res) => {
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
          res.json({ ok: true });
        },
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
            res.json({ ok: true });
          },
        );
      },
    );
  });

  // services tab (accept numeric OR public_id OR username)
  function getServicesCore(req, res) {
    resolveUserKey(req.params.userId, (eResolve, userId) => {
      if (eResolve) {
        const st = eResolve?.status || 400;
        return res
          .status(st)
          .json({ message: eResolve.message || "Bad userId" });
      }

      dbAll(
        `
        SELECT *
        FROM services
        WHERE user_id = ? AND is_active = 1
        ORDER BY id DESC
        LIMIT 200
        `,
        [userId],
        (err, rows) => {
          if (err)
            return res.status(500).json({ message: "Failed to load services" });
          res.json({ services: rows || [] });
        },
      );
    });
  }

  app.get("/api/profile/:userId/services", authOptional, getServicesCore);
  app.get("/api/profiles/:userId/services", authOptional, getServicesCore);
  app.get("/api/u/:userId/services", authOptional, getServicesCore);

  app.post("/api/profile/me/services", authRequired, (req, res) => {
    const title = safeTrim(req.body?.title);
    const description = safeTrim(req.body?.description);
    const category = safeTrim(req.body?.category);
    const price_type = safeTrim(req.body?.price_type) || "negotiable";
    const price_value =
      req.body?.price_value === null || req.body?.price_value === undefined
        ? null
        : Number(req.body?.price_value);
    const location = safeTrim(req.body?.location);

    if (!title) return res.status(400).json({ message: "Missing title" });

    dbRun(
      `
      INSERT INTO services (user_id, title, description, category, price_type, price_value, location, is_active)
      VALUES (?, ?, ?, ?, ?, ?, ?, 1)
      `,
      [
        req.user.id,
        title,
        description || null,
        category || null,
        price_type,
        Number.isFinite(price_value) ? price_value : null,
        location || null,
      ],
      function (err) {
        if (err)
          return res.status(500).json({ message: "Create service failed" });
        res.json({ ok: true, id: this.lastID });
      },
    );
  });

  app.delete("/api/profile/me/services/:id", authRequired, (req, res) => {
    const id = toInt(req.params.id);
    if (!id) return res.status(400).json({ message: "Bad id" });

    dbRun(
      `UPDATE services SET is_active = 0 WHERE id = ? AND user_id = ?`,
      [id, req.user.id],
      function (err) {
        if (err)
          return res.status(500).json({ message: "Delete service failed" });
        if (this.changes === 0)
          return res.status(404).json({ message: "Service not found" });
        res.json({ ok: true });
      },
    );
  });

  // products tab (accept numeric OR public_id OR username)
  function getProductsCore(req, res) {
    resolveUserKey(req.params.userId, (eResolve, userId) => {
      if (eResolve) {
        const st = eResolve?.status || 400;
        return res
          .status(st)
          .json({ message: eResolve.message || "Bad userId" });
      }

      dbAll(
        `
        SELECT *
        FROM products
        WHERE user_id = ? AND is_available = 1
        ORDER BY id DESC
        LIMIT 200
        `,
        [userId],
        (err, rows) => {
          if (err)
            return res.status(500).json({ message: "Failed to load products" });
          res.json({
            products: (rows || []).map((p) => ({
              ...p,
              images: safeJsonParse(p.images_json) || [],
            })),
          });
        },
      );
    });
  }

  app.get("/api/profile/:userId/products", authOptional, getProductsCore);
  app.get("/api/profiles/:userId/products", authOptional, getProductsCore);
  app.get("/api/u/:userId/products", authOptional, getProductsCore);

  app.post("/api/profile/me/products", authRequired, (req, res) => {
    const title = safeTrim(req.body?.title);
    const description = safeTrim(req.body?.description);
    const price =
      req.body?.price === null || req.body?.price === undefined
        ? null
        : Number(req.body?.price);
    const currency = safeTrim(req.body?.currency) || "USD";
    const images = Array.isArray(req.body?.images) ? req.body.images : [];
    const location = safeTrim(req.body?.location);

    if (!title) return res.status(400).json({ message: "Missing title" });

    dbRun(
      `
      INSERT INTO products (user_id, title, description, price, currency, images_json, location, is_available)
      VALUES (?, ?, ?, ?, ?, ?, ?, 1)
      `,
      [
        req.user.id,
        title,
        description || null,
        Number.isFinite(price) ? price : null,
        currency,
        JSON.stringify(images || []),
        location || null,
      ],
      function (err) {
        if (err)
          return res.status(500).json({ message: "Create product failed" });
        res.json({ ok: true, id: this.lastID });
      },
    );
  });

  app.delete("/api/profile/me/products/:id", authRequired, (req, res) => {
    const id = toInt(req.params.id);
    if (!id) return res.status(400).json({ message: "Bad id" });

    dbRun(
      `UPDATE products SET is_available = 0 WHERE id = ? AND user_id = ?`,
      [id, req.user.id],
      function (err) {
        if (err)
          return res.status(500).json({ message: "Delete product failed" });
        if (this.changes === 0)
          return res.status(404).json({ message: "Product not found" });
        res.json({ ok: true });
      },
    );
  });

  // =========================
  // ✅ reviews tab (MERGED) (accept numeric OR public_id OR username)
  // =========================
  function getReviewsCore(req, res) {
    resolveUserKey(req.params.userId, (eResolve, userId) => {
      if (eResolve) {
        const st = eResolve?.status || 400;
        return res
          .status(st)
          .json({ message: eResolve.message || "Bad userId" });
      }

      const sql = `
        SELECT *
        FROM (
          /* USER-to-USER reviews */
          SELECT
            ('ur_' || r.id) AS id,
            'user' AS kind,
            r.user_id AS target_user_id,
            r.author_id AS author_id,
            r.rating AS rating,
            r.rating AS stars,
            r.comment AS comment,
            r.comment AS text,
            r.created_at AS created_at,
            NULL AS item_type,
            NULL AS item_id,
            NULL AS item_name,
            u.username AS author_name,
            up.avatar_url AS author_avatar
          FROM reviews r
          LEFT JOIN users u ON u.id = r.author_id
          LEFT JOIN user_profile up ON up.user_id = r.author_id
          WHERE r.user_id = ?

          UNION ALL

          /* PLACE reviews on places created_by this user */
          SELECT
            ('pr_' || pr.id) AS id,
            'place' AS kind,
            cp.created_by AS target_user_id,
            pr.user_id AS author_id,
            pr.stars AS rating,
            pr.stars AS stars,
            pr.text AS comment,
            pr.text AS text,
            pr.created_at AS created_at,
            'places' AS item_type,
            cp.id AS item_id,
            cp.name AS item_name,
            COALESCE(u2.username, pr.name) AS author_name,
            up2.avatar_url AS author_avatar
          FROM community_places cp
          JOIN place_reviews pr ON pr.place_id = cp.id
          LEFT JOIN users u2 ON u2.id = pr.user_id
          LEFT JOIN user_profile up2 ON up2.user_id = pr.user_id
          WHERE cp.created_by = ?

          UNION ALL

          /* GROUP reviews on groups created_by this user */
          SELECT
            ('gr_' || gr.id) AS id,
            'group' AS kind,
            cg.created_by AS target_user_id,
            gr.user_id AS author_id,
            gr.stars AS rating,
            gr.stars AS stars,
            gr.text AS comment,
            gr.text AS text,
            gr.created_at AS created_at,
            'groups' AS item_type,
            cg.id AS item_id,
            cg.name AS item_name,
            COALESCE(u3.username, gr.name) AS author_name,
            up3.avatar_url AS author_avatar
          FROM community_groups cg
          JOIN group_reviews gr ON gr.group_id = cg.id
          LEFT JOIN users u3 ON u3.id = gr.user_id
          LEFT JOIN user_profile up3 ON up3.user_id = gr.user_id
          WHERE cg.created_by = ?
        )
        ORDER BY datetime(created_at) DESC
        LIMIT 500
      `;

      dbAll(sql, [userId, userId, userId], (err, rows) => {
        if (err)
          return res.status(500).json({ message: "Failed to load reviews" });

        const normalized = (rows || []).map((r) => ({
          ...r,
          rating: Number(r.rating || r.stars || 0),
          stars: Number(r.stars || r.rating || 0),
        }));

        res.json({ reviews: normalized });
      });
    });
  }

  app.get("/api/profile/:userId/reviews", authOptional, getReviewsCore);
  app.get("/api/profiles/:userId/reviews", authOptional, getReviewsCore);
  app.get("/api/u/:userId/reviews", authOptional, getReviewsCore);

  // ✅ user-to-user review create/update (accept numeric OR public_id OR username)
  app.post("/api/profile/:userId/reviews", authRequired, (req, res) => {
    resolveUserKey(req.params.userId, (eResolve, userId) => {
      if (eResolve) {
        const st = eResolve?.status || 400;
        return res
          .status(st)
          .json({ message: eResolve.message || "Bad userId" });
      }

      if (userId === req.user.id)
        return res.status(400).json({ message: "You cannot review yourself" });

      const rating = Number(req.body?.rating);
      const comment = safeTrim(req.body?.comment);

      if (!Number.isFinite(rating) || rating < 1 || rating > 5) {
        return res.status(400).json({ message: "Rating must be 1..5" });
      }
      if (!comment) return res.status(400).json({ message: "Empty comment" });

      dbRun(
        `
        INSERT INTO reviews (user_id, author_id, rating, comment)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(user_id, author_id) DO UPDATE SET
          rating = excluded.rating,
          comment = excluded.comment,
          created_at = datetime('now')
        `,
        [userId, req.user.id, Math.round(rating), comment],
        function (err) {
          if (err) return res.status(500).json({ message: "Review failed" });
          res.json({ ok: true });
        },
      );
    });
  });

  // ✅ same create via /api/u/:userId/reviews (public id OR username OR numeric)
  app.post("/api/u/:userId/reviews", authRequired, (req, res) => {
    // نفس منطق /api/profile/:userId/reviews
    resolveUserKey(req.params.userId, (eResolve, userId) => {
      if (eResolve) {
        const st = eResolve?.status || 400;
        return res
          .status(st)
          .json({ message: eResolve.message || "Bad userId" });
      }

      if (userId === req.user.id)
        return res.status(400).json({ message: "You cannot review yourself" });

      const rating = Number(req.body?.rating);
      const comment = safeTrim(req.body?.comment);

      if (!Number.isFinite(rating) || rating < 1 || rating > 5) {
        return res.status(400).json({ message: "Rating must be 1..5" });
      }
      if (!comment) return res.status(400).json({ message: "Empty comment" });

      dbRun(
        `
        INSERT INTO reviews (user_id, author_id, rating, comment)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(user_id, author_id) DO UPDATE SET
          rating = excluded.rating,
          comment = excluded.comment,
          created_at = datetime('now')
        `,
        [userId, req.user.id, Math.round(rating), comment],
        function (err) {
          if (err) return res.status(500).json({ message: "Review failed" });
          res.json({ ok: true });
        },
      );
    });
  });
};
