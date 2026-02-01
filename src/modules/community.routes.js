// src/modules/community.routes.js
module.exports = function registerCommunityRoutes({
  app,
  db,
  authRequired,
  authOptional,
  isAdminReq,
  dbAll,
  dbGet,
  dbRun,
  safeTrim,
  toInt,
  safeAlterTable,
}) {
  // =====================
  // Helpers: promise wrappers on sqlite db
  // =====================
  function run(sql, params = []) {
    return new Promise((resolve, reject) => {
      db.run(sql, params, function (err) {
        if (err) reject(err);
        else resolve(this);
      });
    });
  }
  function all(sql, params = []) {
    return new Promise((resolve, reject) => {
      db.all(sql, params, (err, rows) => {
        if (err) reject(err);
        else resolve(rows || []);
      });
    });
  }
  function get(sql, params = []) {
    return new Promise((resolve, reject) => {
      db.get(sql, params, (err, row) => {
        if (err) reject(err);
        else resolve(row || null);
      });
    });
  }
  // =====================
  // ✅ Ownership guard (ONLY owner OR admin)
  // =====================
  async function assertOwnerOrAdmin(table, id, req, res) {
    const row = await get(`SELECT created_by FROM ${table} WHERE id = ?`, [id]);
    if (!row) {
      res.status(404).json({ error: "Not found" });
      return false;
    }

    const ownerId = Number(row.created_by || 0);
    const meId = Number(req.user?.id || 0);

    if (ownerId && meId && ownerId === meId) return true;
    if (typeof isAdminReq === "function" && isAdminReq(req)) return true;

    res.status(403).json({ error: "Forbidden" });
    return false;
  }

  // =====================
  // Tables
  // =====================
  async function ensureCommunityTables() {
    await run(`
      CREATE TABLE IF NOT EXISTS community_places (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        category TEXT,
        state TEXT,
        city TEXT,
        address TEXT,
        phone TEXT,
        website TEXT,
        notes TEXT,
        status TEXT,
        created_by INTEGER,
        reviewed_by INTEGER,
        reviewed_at TEXT,
        created_at TEXT DEFAULT (datetime('now')),
        updated_at TEXT DEFAULT (datetime('now'))
      )
    `);

    await run(`
      CREATE TABLE IF NOT EXISTS community_groups (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        platform TEXT,
        link TEXT,
        state TEXT,
        city TEXT,
        topic TEXT,
        notes TEXT,
        status TEXT,
        created_by INTEGER,
        reviewed_by INTEGER,
        reviewed_at TEXT,
        created_at TEXT DEFAULT (datetime('now')),
        updated_at TEXT DEFAULT (datetime('now'))
      )
    `);

    // safe alters (لو tables قديمة)
    safeAlterTable(`ALTER TABLE community_places ADD COLUMN category TEXT`);
    safeAlterTable(`ALTER TABLE community_places ADD COLUMN state TEXT`);
    safeAlterTable(`ALTER TABLE community_places ADD COLUMN city TEXT`);
    safeAlterTable(`ALTER TABLE community_places ADD COLUMN address TEXT`);
    safeAlterTable(`ALTER TABLE community_places ADD COLUMN phone TEXT`);
    safeAlterTable(`ALTER TABLE community_places ADD COLUMN website TEXT`);
    safeAlterTable(`ALTER TABLE community_places ADD COLUMN notes TEXT`);
    safeAlterTable(`ALTER TABLE community_places ADD COLUMN created_at TEXT`);
    safeAlterTable(`ALTER TABLE community_places ADD COLUMN updated_at TEXT`);
    safeAlterTable(`ALTER TABLE community_places ADD COLUMN status TEXT`);
    safeAlterTable(
      `ALTER TABLE community_places ADD COLUMN created_by INTEGER`
    );
    safeAlterTable(
      `ALTER TABLE community_places ADD COLUMN reviewed_by INTEGER`
    );
    safeAlterTable(`ALTER TABLE community_places ADD COLUMN reviewed_at TEXT`);

    safeAlterTable(`ALTER TABLE community_groups ADD COLUMN platform TEXT`);
    safeAlterTable(`ALTER TABLE community_groups ADD COLUMN link TEXT`);
    safeAlterTable(`ALTER TABLE community_groups ADD COLUMN state TEXT`);
    safeAlterTable(`ALTER TABLE community_groups ADD COLUMN city TEXT`);
    safeAlterTable(`ALTER TABLE community_groups ADD COLUMN topic TEXT`);
    safeAlterTable(`ALTER TABLE community_groups ADD COLUMN notes TEXT`);
    safeAlterTable(`ALTER TABLE community_groups ADD COLUMN created_at TEXT`);
    safeAlterTable(`ALTER TABLE community_groups ADD COLUMN updated_at TEXT`);
    safeAlterTable(`ALTER TABLE community_groups ADD COLUMN status TEXT`);
    safeAlterTable(
      `ALTER TABLE community_groups ADD COLUMN created_by INTEGER`
    );
    safeAlterTable(
      `ALTER TABLE community_groups ADD COLUMN reviewed_by INTEGER`
    );
    safeAlterTable(`ALTER TABLE community_groups ADD COLUMN reviewed_at TEXT`);
  }

  async function ensurePlaceReviewsTable() {
    await run(`
      CREATE TABLE IF NOT EXISTS place_reviews (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        place_id INTEGER NOT NULL,
        user_id INTEGER,
        name TEXT NOT NULL,
        stars INTEGER NOT NULL,
        text TEXT NOT NULL,
        created_at TEXT DEFAULT (datetime('now')),
        FOREIGN KEY(place_id) REFERENCES community_places(id) ON DELETE CASCADE
      )
    `);

    await run(`
      CREATE INDEX IF NOT EXISTS idx_place_reviews_place
      ON place_reviews(place_id)
    `);

    // remove duplicates before unique index
    await run(`
      DELETE FROM place_reviews
      WHERE id NOT IN (
        SELECT MAX(id)
        FROM place_reviews
        WHERE user_id IS NOT NULL
        GROUP BY place_id, user_id
      )
      AND user_id IS NOT NULL
    `);

    await run(`
      CREATE UNIQUE INDEX IF NOT EXISTS idx_place_reviews_unique
      ON place_reviews(place_id, user_id)
    `);
  }

  async function ensureGroupReviewsTable() {
    await run(`
      CREATE TABLE IF NOT EXISTS group_reviews (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        group_id INTEGER NOT NULL,
        user_id INTEGER,
        name TEXT NOT NULL,
        stars INTEGER NOT NULL,
        text TEXT NOT NULL,
        created_at TEXT DEFAULT (datetime('now')),
        FOREIGN KEY(group_id) REFERENCES community_groups(id) ON DELETE CASCADE
      )
    `);

    await run(`
      CREATE INDEX IF NOT EXISTS idx_group_reviews_group
      ON group_reviews(group_id)
    `);

    // remove duplicates before unique index
    await run(`
      DELETE FROM group_reviews
      WHERE id NOT IN (
        SELECT MAX(id)
        FROM group_reviews
        WHERE user_id IS NOT NULL
        GROUP BY group_id, user_id
      )
      AND user_id IS NOT NULL
    `);

    await run(`
      CREATE UNIQUE INDEX IF NOT EXISTS idx_group_reviews_unique
      ON group_reviews(group_id, user_id)
    `);
  }

  // =====================
  // Places (CRUD)
  // =====================
  app.get("/api/community/places", authOptional, async (req, res) => {
    try {
      const { q = "", state = "", city = "", category = "" } = req.query;

      const where = [];
      const params = [];

      const isAdmin = isAdminReq(req);
      if (!isAdmin) where.push(`COALESCE(status,'approved') = 'approved'`);

      const qq = String(q || "").trim();
      if (qq) {
        // ✅ safer with non-latin + avoid any weird chars crashing sqlite
        const like = `%${qq.replace(/[%_]/g, "\\$&")}%`;
        where.push(
          "(p.name LIKE ? ESCAPE '\\' OR p.notes LIKE ? ESCAPE '\\' OR p.address LIKE ? ESCAPE '\\')"
        );
        params.push(like, like, like);
      }

      if (state) {
        where.push("state = ?");
        params.push(state);
      }
      if (String(city).trim()) {
        where.push("LOWER(city) = LOWER(?)");
        params.push(String(city).trim());
      }
      if (category) {
        where.push("category = ?");
        params.push(category);
      }

      const sql = `
        SELECT
          p.*,
          COALESCE(AVG(r.stars), 0) AS avg_rating,
          COUNT(r.id) AS reviews_count
        FROM community_places p
        LEFT JOIN place_reviews r ON r.place_id = p.id
        ${where.length ? `WHERE ${where.join(" AND ")}` : ""}
        GROUP BY p.id
        ORDER BY p.id DESC
      `;

      const rows = await all(sql, params);
      res.json(rows);
    } catch (e) {
      console.error(e);
      res.status(500).json({ error: "Failed to load places" });
    }
  });

  app.get("/api/community/places/:id", authOptional, async (req, res) => {
    try {
      const id = toInt(req.params.id);
      if (!id) return res.status(400).json({ error: "Bad id" });

      const isAdmin = isAdminReq(req);

      const row = await get(
        `
        SELECT
          p.*,
          COALESCE(AVG(r.stars), 0) AS avg_rating,
          COUNT(r.id) AS reviews_count
        FROM community_places p
        LEFT JOIN place_reviews r ON r.place_id = p.id
        WHERE p.id = ?
        ${!isAdmin ? `AND COALESCE(p.status,'approved') = 'approved'` : ""}
        GROUP BY p.id
        LIMIT 1
        `,
        [id]
      );

      if (!row) return res.status(404).json({ error: "Not found" });
      res.json(row);
    } catch (e) {
      console.error(e);
      res.status(500).json({ error: "Failed" });
    }
  });

  app.post("/api/community/places", authRequired, async (req, res) => {
    try {
      const {
        name,
        category = "",
        state = "",
        city = "",
        address = "",
        phone = "",
        website = "",
        notes = "",
      } = req.body || {};

      if (!name || !String(name).trim())
        return res.status(400).json({ error: "Name is required" });

      const status = "approved";

      const r = await run(
        `INSERT INTO community_places (name, category, state, city, address, phone, website, notes, status, created_by)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [
          String(name).trim(),
          category,
          state,
          String(city || "").trim(),
          String(address || "").trim(),
          String(phone || "").trim(),
          String(website || "").trim(),
          String(notes || "").trim(),
          status,
          req.user.id,
        ]
      );

      const created = await get(`SELECT * FROM community_places WHERE id = ?`, [
        r.lastID,
      ]);
      res.json(created);
    } catch (e) {
      console.error(e);
      res.status(500).json({ error: "Failed to create place" });
    }
  });

  async function updatePlace(req, res) {
    try {
      const id = toInt(req.params.id);
      if (!id) return res.status(400).json({ error: "Bad id" });

      const existing = await get(
        `SELECT * FROM community_places WHERE id = ?`,
        [id]
      );
      if (!existing) return res.status(404).json({ error: "Not found" });
      // ✅ only owner/admin can edit
      const ok = await assertOwnerOrAdmin("community_places", id, req, res);
      if (!ok) return;

      const b = req.body || {};
      const next = {
        name: b.name ?? existing.name,
        category: b.category ?? existing.category,
        state: b.state ?? existing.state,
        city: b.city ?? existing.city,
        address: b.address ?? existing.address,
        phone: b.phone ?? existing.phone,
        website: b.website ?? existing.website,
        notes: b.notes ?? existing.notes,
      };

      if (!String(next.name || "").trim())
        return res.status(400).json({ error: "Name is required" });

      await run(
        `UPDATE community_places
         SET name=?, category=?, state=?, city=?, address=?, phone=?, website=?, notes=?, updated_at=datetime('now')
         WHERE id=?`,
        [
          String(next.name).trim(),
          next.category || "",
          next.state || "",
          String(next.city || "").trim(),
          String(next.address || "").trim(),
          String(next.phone || "").trim(),
          String(next.website || "").trim(),
          String(next.notes || "").trim(),
          id,
        ]
      );

      const updated = await get(`SELECT * FROM community_places WHERE id = ?`, [
        id,
      ]);
      res.json(updated);
    } catch (e) {
      console.error("updatePlace error:", e);
      res.status(500).json({ error: String(e?.message || "Failed to update") });
    }
  }

  app.put("/api/community/places/:id", authRequired, updatePlace);
  app.patch("/api/community/places/:id", authRequired, updatePlace);

  app.delete("/api/community/places/:id", authRequired, async (req, res) => {
    try {
      const id = toInt(req.params.id);
      if (!id) return res.status(400).json({ error: "Bad id" });

      const existing = await get(
        `SELECT * FROM community_places WHERE id = ?`,
        [id]
      );
      if (!existing) return res.status(404).json({ error: "Not found" });
      // ✅ only owner/admin can delete
      const ok = await assertOwnerOrAdmin("community_places", id, req, res);
      if (!ok) return;

      await run(`DELETE FROM community_places WHERE id = ?`, [id]);
      res.json({ ok: true });
    } catch (e) {
      console.error(e);
      res.status(500).json({ error: "Failed to delete place" });
    }
  });

  // =====================
  // Place Reviews
  // =====================
  app.get("/api/community/places/:id/reviews", authOptional, (req, res) => {
    const placeId = toInt(req.params.id);
    if (!placeId) return res.status(400).json({ error: "Bad place id" });

    dbAll(
      `
      SELECT
        pr.id,
        pr.place_id,
        pr.user_id,
        COALESCE(u.username, pr.name) AS user_name,
        pr.stars,
        pr.text,
        pr.created_at
      FROM place_reviews pr
      LEFT JOIN users u ON u.id = pr.user_id
      WHERE pr.place_id = ?
      ORDER BY pr.id DESC
      `,
      [placeId],
      (err, rows) => {
        if (err) return res.status(500).json({ error: "DB error" });
        return res.json(rows || []);
      }
    );
  });

  function upsertPlaceReview(req, res) {
    const placeId = toInt(req.params.id);
    if (!placeId) return res.status(400).json({ error: "Bad place id" });

    const text = safeTrim(req.body?.text);
    const stars = Number(req.body?.stars);

    if (!text) return res.status(400).json({ error: "text required" });
    if (!(stars >= 1 && stars <= 5))
      return res.status(400).json({ error: "stars must be 1..5" });

    dbGet(
      `SELECT id FROM community_places WHERE id = ?`,
      [placeId],
      (e0, row) => {
        if (e0) return res.status(500).json({ error: "DB error" });
        if (!row) return res.status(404).json({ error: "Place not found" });

        dbGet(
          `SELECT username FROM users WHERE id = ?`,
          [req.user.id],
          (eU, uRow) => {
            if (eU) return res.status(500).json({ error: "DB error" });

            const userName = safeTrim(uRow?.username) || `User ${req.user.id}`;

            dbRun(
              `
              INSERT INTO place_reviews (place_id, user_id, name, stars, text, created_at)
              VALUES (?, ?, ?, ?, ?, datetime('now'))
              ON CONFLICT(place_id, user_id) DO UPDATE SET
                name = excluded.name,
                stars = excluded.stars,
                text = excluded.text,
                created_at = datetime('now')
              `,
              [placeId, req.user.id, userName, Math.round(stars), text],
              function (err) {
                if (err) return res.status(500).json({ error: "DB error" });
                return res.status(201).json({ ok: true });
              }
            );
          }
        );
      }
    );
  }

  app.post(
    "/api/community/places/:id/reviews",
    authRequired,
    upsertPlaceReview
  );
  app.put("/api/community/places/:id/reviews", authRequired, upsertPlaceReview);
  app.patch(
    "/api/community/places/:id/reviews",
    authRequired,
    upsertPlaceReview
  );

  app.delete(
    "/api/community/places/:id/reviews/me",
    authRequired,
    (req, res) => {
      const placeId = toInt(req.params.id);
      if (!placeId) return res.status(400).json({ error: "Bad place id" });

      dbRun(
        `DELETE FROM place_reviews WHERE place_id = ? AND user_id = ?`,
        [placeId, req.user.id],
        function (err) {
          if (err) return res.status(500).json({ error: "DB error" });
          return res.json({ ok: true, deleted: this.changes || 0 });
        }
      );
    }
  );

  // =====================
  // Groups (CRUD)
  // =====================
  app.get("/api/community/groups", authOptional, async (req, res) => {
    try {
      const {
        q = "",
        state = "",
        city = "",
        platform = "",
        topic = "",
      } = req.query;

      const where = [];
      const params = [];

      const isAdmin = isAdminReq(req);
      if (!isAdmin) where.push(`COALESCE(status,'approved') = 'approved'`);

      if (String(q).trim()) {
        where.push("(name LIKE ? OR notes LIKE ?)");
        params.push(`%${q.trim()}%`, `%${q.trim()}%`);
      }
      if (state) {
        where.push("state = ?");
        params.push(state);
      }
      if (String(city).trim()) {
        where.push("LOWER(city)=LOWER(?)");
        params.push(String(city).trim());
      }
      if (platform) {
        where.push("platform = ?");
        params.push(platform);
      }
      if (topic) {
        where.push("topic = ?");
        params.push(topic);
      }

      const sql =
        `SELECT * FROM community_groups ` +
        (where.length ? `WHERE ${where.join(" AND ")} ` : "") +
        `ORDER BY id DESC`;

      const rows = await all(sql, params);
      res.json(rows);
    } catch (e) {
      console.error(e);
      res.status(500).json({ error: "Failed to load groups" });
    }
  });

  app.get("/api/community/groups/:id", authOptional, async (req, res) => {
    try {
      const id = toInt(req.params.id);
      if (!id) return res.status(400).json({ error: "Bad id" });

      const isAdmin = isAdminReq(req);

      const row = await get(
        `SELECT * FROM community_groups WHERE id = ?
         ${!isAdmin ? `AND COALESCE(status,'approved') = 'approved'` : ""}`,
        [id]
      );
      if (!row) return res.status(404).json({ error: "Not found" });
      res.json(row);
    } catch (e) {
      console.error(e);
      res.status(500).json({ error: "Failed" });
    }
  });

  app.post("/api/community/groups", authRequired, async (req, res) => {
    try {
      const {
        name,
        platform = "",
        link = "",
        state = "",
        city = "",
        topic = "",
        notes = "",
      } = req.body || {};

      if (!name || !String(name).trim())
        return res.status(400).json({ error: "Name is required" });
      if (!link || !String(link).trim())
        return res.status(400).json({ error: "Link is required" });

      const status = "approved";

      const r = await run(
        `INSERT INTO community_groups (name, platform, link, state, city, topic, notes, status, created_by)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [
          String(name).trim(),
          platform,
          String(link).trim(),
          state,
          String(city || "").trim(),
          topic,
          String(notes || "").trim(),
          status,
          req.user.id,
        ]
      );

      const created = await get(`SELECT * FROM community_groups WHERE id = ?`, [
        r.lastID,
      ]);
      res.json(created);
    } catch (e) {
      console.error(e);
      res.status(500).json({ error: "Failed to create group" });
    }
  });

  async function updateGroup(req, res) {
    try {
      const id = toInt(req.params.id);
      if (!id) return res.status(400).json({ error: "Bad id" });

      const existing = await get(
        `SELECT * FROM community_groups WHERE id = ?`,
        [id]
      );
      if (!existing) return res.status(404).json({ error: "Not found" });
      // ✅ only owner/admin can edit
      const ok = await assertOwnerOrAdmin("community_groups", id, req, res);
      if (!ok) return;

      const b = req.body || {};
      const next = {
        name: b.name ?? existing.name,
        platform: b.platform ?? existing.platform,
        link: b.link ?? existing.link,
        state: b.state ?? existing.state,
        city: b.city ?? existing.city,
        topic: b.topic ?? existing.topic,
        notes: b.notes ?? existing.notes,
      };

      if (!String(next.name || "").trim())
        return res.status(400).json({ error: "Name is required" });
      if (!String(next.link || "").trim())
        return res.status(400).json({ error: "Link is required" });

      await run(
        `UPDATE community_groups
         SET name=?, platform=?, link=?, state=?, city=?, topic=?, notes=?, updated_at=datetime('now')
         WHERE id=?`,
        [
          String(next.name).trim(),
          next.platform || "",
          String(next.link).trim(),
          next.state || "",
          String(next.city || "").trim(),
          String(next.topic || "").trim(),
          String(next.notes || "").trim(),
          id,
        ]
      );

      const updated = await get(`SELECT * FROM community_groups WHERE id = ?`, [
        id,
      ]);
      res.json(updated);
    } catch (e) {
      console.error("updateGroup error:", e);
      res.status(500).json({ error: String(e?.message || "Failed to update") });
    }
  }

  app.put("/api/community/groups/:id", authRequired, updateGroup);
  app.patch("/api/community/groups/:id", authRequired, updateGroup);

  app.delete("/api/community/groups/:id", authRequired, async (req, res) => {
    try {
      const id = toInt(req.params.id);
      if (!id) return res.status(400).json({ error: "Bad id" });

      const existing = await get(
        `SELECT * FROM community_groups WHERE id = ?`,
        [id]
      );
      if (!existing) return res.status(404).json({ error: "Not found" });
      // ✅ only owner/admin can delete
      const ok = await assertOwnerOrAdmin("community_groups", id, req, res);
      if (!ok) return;

      await run(`DELETE FROM community_groups WHERE id = ?`, [id]);
      res.json({ ok: true });
    } catch (e) {
      console.error(e);
      res.status(500).json({ error: "Failed to delete group" });
    }
  });

  // =====================
  // Group Reviews
  // =====================
  app.get("/api/community/groups/:id/reviews", authOptional, (req, res) => {
    const groupId = toInt(req.params.id);
    if (!groupId) return res.status(400).json({ error: "Bad group id" });

    dbAll(
      `
      SELECT
        gr.id,
        gr.group_id,
        gr.user_id,
        COALESCE(u.username, gr.name) AS user_name,
        gr.stars,
        gr.text,
        gr.created_at
      FROM group_reviews gr
      LEFT JOIN users u ON u.id = gr.user_id
      WHERE gr.group_id = ?
      ORDER BY gr.id DESC
      `,
      [groupId],
      (err, rows) => {
        if (err) return res.status(500).json({ error: "DB error" });
        return res.json(rows || []);
      }
    );
  });

  function upsertGroupReview(req, res) {
    const groupId = toInt(req.params.id);
    if (!groupId) return res.status(400).json({ error: "Bad group id" });

    const text = safeTrim(req.body?.text);
    const stars = Number(req.body?.stars);

    if (!text) return res.status(400).json({ error: "text required" });
    if (!(stars >= 1 && stars <= 5))
      return res.status(400).json({ error: "stars must be 1..5" });

    dbGet(
      `SELECT id FROM community_groups WHERE id = ?`,
      [groupId],
      (e0, row) => {
        if (e0) return res.status(500).json({ error: "DB error" });
        if (!row) return res.status(404).json({ error: "Group not found" });

        dbGet(
          `SELECT username FROM users WHERE id = ?`,
          [req.user.id],
          (eU, uRow) => {
            if (eU) return res.status(500).json({ error: "DB error" });

            const userName = safeTrim(uRow?.username) || `User ${req.user.id}`;

            dbRun(
              `
              INSERT INTO group_reviews (group_id, user_id, name, stars, text, created_at)
              VALUES (?, ?, ?, ?, ?, datetime('now'))
              ON CONFLICT(group_id, user_id) DO UPDATE SET
                name = excluded.name,
                stars = excluded.stars,
                text = excluded.text,
                created_at = datetime('now')
              `,
              [groupId, req.user.id, userName, Math.round(stars), text],
              function (err) {
                if (err) return res.status(500).json({ error: "DB error" });
                return res.status(201).json({ ok: true });
              }
            );
          }
        );
      }
    );
  }

  app.post(
    "/api/community/groups/:id/reviews",
    authRequired,
    upsertGroupReview
  );
  app.put("/api/community/groups/:id/reviews", authRequired, upsertGroupReview);
  app.patch(
    "/api/community/groups/:id/reviews",
    authRequired,
    upsertGroupReview
  );

  app.delete(
    "/api/community/groups/:id/reviews/me",
    authRequired,
    (req, res) => {
      const groupId = toInt(req.params.id);
      if (!groupId) return res.status(400).json({ error: "Bad group id" });

      dbRun(
        `DELETE FROM group_reviews WHERE group_id = ? AND user_id = ?`,
        [groupId, req.user.id],
        function (err) {
          if (err) return res.status(500).json({ error: "DB error" });
          return res.json({ ok: true, deleted: this.changes || 0 });
        }
      );
    }
  );

  // =====================
  // Init
  // =====================
  (async () => {
    try {
      await ensureCommunityTables();
      await ensurePlaceReviewsTable();
      await ensureGroupReviewsTable();
      console.log("✅ Community tables ready");
    } catch (e) {
      console.error("❌ Community init failed:", e);
    }
  })();
};
