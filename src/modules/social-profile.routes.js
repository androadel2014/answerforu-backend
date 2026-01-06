// src/modules/social-profile.routes.js
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
  function getProfileCore(req, res) {
    const targetId = toInt(req.params.userId);
    if (!targetId) return res.status(400).json({ message: "Bad userId" });

    ensureProfileRow(targetId, (e0) => {
      if (e0) return res.status(500).json({ message: "Failed" });

      dbGet(
        `SELECT * FROM user_profile WHERE user_id = ?`,
        [targetId],
        (e1, p) => {
          if (e1) return res.status(500).json({ message: "Failed" });
          if (!p) return res.status(404).json({ message: "Profile not found" });

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
                            return res.status(500).json({ message: "Failed" });

                          dbGet(
                            `SELECT COUNT(*) c FROM products WHERE user_id = ? AND is_available = 1`,
                            [targetId],
                            (ePr, rProducts) => {
                              if (ePr)
                                return res
                                  .status(500)
                                  .json({ message: "Failed" });

                              dbGet(
                                `SELECT COALESCE(AVG(rating),0) avg FROM reviews WHERE user_id = ?`,
                                [targetId],
                                (eR1, rAvg) => {
                                  if (eR1)
                                    return res
                                      .status(500)
                                      .json({ message: "Failed" });

                                  dbGet(
                                    `SELECT COUNT(*) c FROM reviews WHERE user_id = ?`,
                                    [targetId],
                                    (eR2, rCnt) => {
                                      if (eR2)
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
                                          ratingAvg: Number(rAvg?.avg || 0),
                                          ratingCount: Number(rCnt?.c || 0),
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
                  );
                }
              );
            }
          );
        }
      );
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

  // public profile
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
      const avatar_url = safeUrl(body.avatar_url);
      const cover_url = safeUrl(body.cover_url);
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
            avatar_url = COALESCE(?, avatar_url),
            cover_url = COALESCE(?, cover_url),
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
            avatar_url || null,
            cover_url || null,
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
              `SELECT * FROM user_profile WHERE user_id = ?`,
              [userId],
              (e3, p) => {
                if (e3 || !p)
                  return res
                    .status(500)
                    .json({ message: "Profile update failed" });
                return res.json({ ok: true, profile: p });
              }
            );
          }
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
        }
      );
    });
  });

  // follow / unfollow
  function followCore(req, res) {
    const me = req.user.id;
    const target = toInt(req.params.userId);
    if (!target) return res.status(400).json({ message: "Bad userId" });
    if (me === target)
      return res.status(400).json({ message: "Cannot follow yourself" });

    dbRun(
      `INSERT OR IGNORE INTO follows (follower_id, following_id) VALUES (?, ?)`,
      [me, target],
      (err) => {
        if (err) return res.status(500).json({ message: "Follow failed" });
        res.json({ ok: true });
      }
    );
  }

  function unfollowCore(req, res) {
    const me = req.user.id;
    const target = toInt(req.params.userId);
    if (!target) return res.status(400).json({ message: "Bad userId" });

    dbRun(
      `DELETE FROM follows WHERE follower_id = ? AND following_id = ?`,
      [me, target],
      (err) => {
        if (err) return res.status(500).json({ message: "Unfollow failed" });
        res.json({ ok: true });
      }
    );
  }

  app.post("/api/profile/:userId/follow", authRequired, followCore);
  app.delete("/api/profile/:userId/follow", authRequired, unfollowCore);
  app.post("/api/profiles/:userId/follow", authRequired, followCore);
  app.delete("/api/profiles/:userId/follow", authRequired, unfollowCore);

  // posts tab (profile_posts + posts)
  function getProfilePostsCore(req, res) {
    const userId = toInt(req.params.userId);
    if (!userId) return res.status(400).json({ message: "Bad userId" });

    const sql = `
      SELECT *
      FROM (
        SELECT
          ('pp_' || pp.id) AS id,
          pp.user_id AS user_id,
          pp.content AS content,
          pp.media_url AS media_url,
          pp.created_at AS created_at,
          NULL AS category,
          'profile' AS source,
          u.username AS user_name
        FROM profile_posts pp
        LEFT JOIN users u ON u.id = pp.user_id
        WHERE pp.user_id = ?

        UNION ALL

        SELECT
          ('p_' || p.id) AS id,
          p.user_id AS user_id,
          p.content AS content,
          NULL AS media_url,
          p.created_at AS created_at,
          p.category AS category,
          'feed' AS source,
          u2.username AS user_name
        FROM posts p
        LEFT JOIN users u2 ON u2.id = p.user_id
        WHERE p.user_id = ?
      )
      ORDER BY datetime(created_at) DESC
      LIMIT 200
    `;

    dbAll(sql, [userId, userId], (err, rows) => {
      if (err) return res.status(500).json({ message: "Failed to load posts" });
      res.json({ posts: rows || [] });
    });
  }

  app.get("/api/profile/:userId/posts", authOptional, getProfilePostsCore);
  app.get("/api/profiles/:userId/posts", authOptional, getProfilePostsCore);
  app.get("/api/profile_posts/:userId", authOptional, getProfilePostsCore);
  app.get(
    "/api/profile_posts/:userId/posts",
    authOptional,
    getProfilePostsCore
  );
  app.get("/api/profile-posts/:userId", authOptional, getProfilePostsCore);
  app.get(
    "/api/profile-posts/:userId/posts",
    authOptional,
    getProfilePostsCore
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
      }
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
        }
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
        }
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
          }
        );
      }
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
        }
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
        }
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
            }
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
          }
        );
      }
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
            res.json({ ok: true });
          }
        );
      }
    );
  });

  // services tab
  function getServicesCore(req, res) {
    const userId = toInt(req.params.userId);
    if (!userId) return res.status(400).json({ message: "Bad userId" });

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
      }
    );
  }

  app.get("/api/profile/:userId/services", authOptional, getServicesCore);
  app.get("/api/profiles/:userId/services", authOptional, getServicesCore);

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
      }
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
      }
    );
  });

  // products tab
  function getProductsCore(req, res) {
    const userId = toInt(req.params.userId);
    if (!userId) return res.status(400).json({ message: "Bad userId" });

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
      }
    );
  }

  app.get("/api/profile/:userId/products", authOptional, getProductsCore);
  app.get("/api/profiles/:userId/products", authOptional, getProductsCore);

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
      }
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
      }
    );
  });

  // reviews tab
  function getReviewsCore(req, res) {
    const userId = toInt(req.params.userId);
    if (!userId) return res.status(400).json({ message: "Bad userId" });

    dbAll(
      `
      SELECT r.*, u.username AS author_name
      FROM reviews r
      LEFT JOIN users u ON u.id = r.author_id
      WHERE r.user_id = ?
      ORDER BY r.id DESC
      LIMIT 200
      `,
      [userId],
      (err, rows) => {
        if (err)
          return res.status(500).json({ message: "Failed to load reviews" });
        res.json({ reviews: rows || [] });
      }
    );
  }

  app.get("/api/profile/:userId/reviews", authOptional, getReviewsCore);
  app.get("/api/profiles/:userId/reviews", authOptional, getReviewsCore);

  app.post("/api/profile/:userId/reviews", authRequired, (req, res) => {
    const userId = toInt(req.params.userId);
    if (!userId) return res.status(400).json({ message: "Bad userId" });
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
      [userId, req.user.id, rating, comment],
      function (err) {
        if (err) return res.status(500).json({ message: "Review failed" });
        res.json({ ok: true });
      }
    );
  });
};
