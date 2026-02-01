// backend/src/modules/carry.js
/* =====================
   CARRY / SHIPMENTS (Hitchhiker style)
   ✅ New module only (no touching marketplace/community)
   ✅ Tables:
      - carry_listings
      - carry_requests
      - carry_messages
      - carry_reviews
===================== */

module.exports = function registerCarry(opts) {
  const { app, db, auth, safeTrim, safeJsonParse, toInt } = opts;
  const { authRequired, authOptional, isAdminReq } = auth;

  const all = (sql, params = []) =>
    new Promise((resolve, reject) => {
      db.all(sql, params, (err, rows) => (err ? reject(err) : resolve(rows)));
    });

  const get = (sql, params = []) =>
    new Promise((resolve, reject) => {
      db.get(sql, params, (err, row) => (err ? reject(err) : resolve(row)));
    });

  const run = (sql, params = []) =>
    new Promise((resolve, reject) => {
      db.run(sql, params, function (err) {
        if (err) return reject(err);
        resolve({ lastID: this.lastID, changes: this.changes });
      });
    });

  // =====================
  // DB ENSURE
  // =====================
  db.run(`
    CREATE TABLE IF NOT EXISTS carry_listings (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id INTEGER NOT NULL,

      role TEXT NOT NULL,              -- traveler | sender
      from_country TEXT,
      from_city TEXT,
      to_country TEXT,
      to_city TEXT,

      travel_date TEXT,
      arrival_date TEXT,

      available_weight REAL,
      item_type TEXT,
      description TEXT,

      reward_amount REAL,
      currency TEXT,

      status TEXT DEFAULT 'open',      -- open|matched|in_transit|delivered|completed|cancelled
      is_active INTEGER DEFAULT 1,

      data_json TEXT,
      created_at TEXT DEFAULT (datetime('now')),
      updated_at TEXT DEFAULT (datetime('now'))
    )
  `);

  db.run(`
    CREATE TABLE IF NOT EXISTS carry_requests (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      listing_id INTEGER NOT NULL,
      requester_id INTEGER NOT NULL,
      status TEXT DEFAULT 'pending',    -- pending|accepted|rejected|cancelled
      created_at TEXT DEFAULT (datetime('now'))
    )
  `);

  db.run(`
    CREATE TABLE IF NOT EXISTS carry_messages (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      listing_id INTEGER NOT NULL,
      sender_id INTEGER NOT NULL,
      message TEXT NOT NULL,
      created_at TEXT DEFAULT (datetime('now'))
    )
  `);

  db.run(`
    CREATE TABLE IF NOT EXISTS carry_reviews (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      listing_id INTEGER NOT NULL,
      reviewer_id INTEGER NOT NULL,
      reviewed_user_id INTEGER NOT NULL,
      rating INTEGER NOT NULL,
      comment TEXT,
      created_at TEXT DEFAULT (datetime('now'))
    )
  `);

  function mapListing(row) {
    const data = safeJsonParse(row?.data_json) || {};
    return {
      id: Number(row.id),
      user_id: Number(row.user_id),
      role: row.role,
      from_country: row.from_country || "",
      from_city: row.from_city || "",
      to_country: row.to_country || "",
      to_city: row.to_city || "",
      travel_date: row.travel_date || null,
      arrival_date: row.arrival_date || null,
      available_weight: row.available_weight ?? null,
      item_type: row.item_type || "",
      description: row.description || "",
      reward_amount: row.reward_amount ?? null,
      currency: row.currency || "USD",
      status: row.status || "open",
      is_active: Number(row.is_active || 0),
      created_at: row.created_at || null,
      updated_at: row.updated_at || null,
      data,
      raw: row,
    };
  }

  function clampRole(v) {
    const r = String(v || "")
      .trim()
      .toLowerCase();
    return r === "traveler" || r === "sender" ? r : null;
  }

  function canEdit(reqUserId, rowUserId, req) {
    if (!reqUserId) return false;
    if (Number(reqUserId) === Number(rowUserId)) return true;
    return isAdminReq(req);
  }

  // =====================
  // LISTINGS CRUD
  // =====================

  // Create
  app.post("/api/carry/listings", authRequired, async (req, res) => {
    try {
      const role = clampRole(req.body?.role);
      if (!role) return res.status(400).json({ error: "Bad role" });

      const from_country = safeTrim(req.body?.from_country) || null;
      const from_city = safeTrim(req.body?.from_city) || null;
      const to_country = safeTrim(req.body?.to_country) || null;
      const to_city = safeTrim(req.body?.to_city) || null;

      const travel_date = safeTrim(req.body?.travel_date) || null;
      const arrival_date = safeTrim(req.body?.arrival_date) || null;

      const available_weight =
        req.body?.available_weight == null
          ? null
          : Number(req.body.available_weight);

      const item_type = safeTrim(req.body?.item_type) || null;
      const description = safeTrim(req.body?.description) || null;

      const reward_amount =
        req.body?.reward_amount == null ? null : Number(req.body.reward_amount);

      const currency = safeTrim(req.body?.currency) || "USD";

      const data_json = JSON.stringify(req.body || {});

      const r = await run(
        `
        INSERT INTO carry_listings
        (user_id, role, from_country, from_city, to_country, to_city,
         travel_date, arrival_date, available_weight, item_type, description,
         reward_amount, currency, status, is_active, data_json, updated_at)
        VALUES
        (?, ?, ?, ?, ?, ?,
         ?, ?, ?, ?, ?,
         ?, ?, 'open', 1, ?, datetime('now'))
        `,
        [
          req.user.id,
          role,
          from_country,
          from_city,
          to_country,
          to_city,
          travel_date,
          arrival_date,
          Number.isFinite(available_weight) ? available_weight : null,
          item_type,
          description,
          Number.isFinite(reward_amount) ? reward_amount : null,
          currency,
          data_json,
        ]
      );

      const row = await get(`SELECT * FROM carry_listings WHERE id=?`, [
        r.lastID,
      ]);
      return res.json({ ok: true, item: mapListing(row) });
    } catch (e) {
      console.error("[carry] create listing", e);
      return res.status(500).json({ error: "Failed to create listing" });
    }
  });

  // List (filters)
  app.get("/api/carry/listings", authOptional, async (req, res) => {
    try {
      const where = ["is_active=1"];
      const params = [];

      const role = clampRole(req.query?.role);
      if (role) {
        where.push("role=?");
        params.push(role);
      }

      const q = String(req.query?.q || "").trim();
      if (q) {
        where.push(`(
          from_country LIKE ? OR from_city LIKE ? OR
          to_country LIKE ? OR to_city LIKE ? OR
          item_type LIKE ? OR description LIKE ?
        )`);
        params.push(`%${q}%`, `%${q}%`, `%${q}%`, `%${q}%`, `%${q}%`, `%${q}%`);
      }

      const from_country = String(req.query?.from_country || "").trim();
      if (from_country) {
        where.push("from_country=?");
        params.push(from_country);
      }

      const to_country = String(req.query?.to_country || "").trim();
      if (to_country) {
        where.push("to_country=?");
        params.push(to_country);
      }

      const sql = `
        SELECT * FROM carry_listings
        WHERE ${where.join(" AND ")}
        ORDER BY created_at DESC, id DESC
        LIMIT 200
      `;
      const rows = await all(sql, params);
      return res.json({ ok: true, items: rows.map(mapListing) });
    } catch (e) {
      console.error("[carry] list", e);
      return res.status(500).json({ error: "Failed to load" });
    }
  });

  // Get details
  app.get("/api/carry/listings/:id", authOptional, async (req, res) => {
    try {
      const id = toInt(req.params.id);
      if (!id) return res.status(400).json({ error: "Bad id" });

      const row = await get(
        `SELECT * FROM carry_listings WHERE id=? AND is_active=1`,
        [id]
      );
      if (!row) return res.status(404).json({ error: "Not found" });

      // attach requests count + last 20 messages + rating summary
      const reqCountRow = await get(
        `SELECT COUNT(*) AS c FROM carry_requests WHERE listing_id=?`,
        [id]
      );

      const msgs = await all(
        `SELECT * FROM carry_messages WHERE listing_id=? ORDER BY created_at DESC LIMIT 20`,
        [id]
      );

      const ratingRow = await get(
        `SELECT COALESCE(AVG(rating),0) AS avg_rating, COUNT(*) AS reviews_count
         FROM carry_reviews WHERE listing_id=?`,
        [id]
      );

      return res.json({
        ok: true,
        item: mapListing(row),
        requests_count: Number(reqCountRow?.c || 0),
        messages: msgs || [],
        avg_rating: Number(ratingRow?.avg_rating || 0),
        reviews_count: Number(ratingRow?.reviews_count || 0),
      });
    } catch (e) {
      console.error("[carry] details", e);
      return res.status(500).json({ error: "Failed" });
    }
  });

  // Update
  app.patch("/api/carry/listings/:id", authRequired, async (req, res) => {
    try {
      const id = toInt(req.params.id);
      if (!id) return res.status(400).json({ error: "Bad id" });

      const row = await get(
        `SELECT id, user_id FROM carry_listings WHERE id=?`,
        [id]
      );
      if (!row) return res.status(404).json({ error: "Not found" });

      if (!canEdit(req.user.id, row.user_id, req))
        return res.status(403).json({ error: "Forbidden" });

      const fields = {
        from_country: safeTrim(req.body?.from_country) || "",
        from_city: safeTrim(req.body?.from_city) || "",
        to_country: safeTrim(req.body?.to_country) || "",
        to_city: safeTrim(req.body?.to_city) || "",
        travel_date: safeTrim(req.body?.travel_date) || "",
        arrival_date: safeTrim(req.body?.arrival_date) || "",
        item_type: safeTrim(req.body?.item_type) || "",
        description: safeTrim(req.body?.description) || "",
        currency: safeTrim(req.body?.currency) || "",
      };

      const available_weight =
        req.body?.available_weight == null
          ? null
          : Number(req.body.available_weight);

      const reward_amount =
        req.body?.reward_amount == null ? null : Number(req.body.reward_amount);

      await run(
        `
        UPDATE carry_listings SET
          from_country=COALESCE(NULLIF(?,''), from_country),
          from_city=COALESCE(NULLIF(?,''), from_city),
          to_country=COALESCE(NULLIF(?,''), to_country),
          to_city=COALESCE(NULLIF(?,''), to_city),
          travel_date=COALESCE(NULLIF(?,''), travel_date),
          arrival_date=COALESCE(NULLIF(?,''), arrival_date),
          available_weight=COALESCE(?, available_weight),
          item_type=COALESCE(NULLIF(?,''), item_type),
          description=COALESCE(NULLIF(?,''), description),
          reward_amount=COALESCE(?, reward_amount),
          currency=COALESCE(NULLIF(?,''), currency),
          data_json=COALESCE(?, data_json),
          updated_at=datetime('now')
        WHERE id=?
        `,
        [
          fields.from_country,
          fields.from_city,
          fields.to_country,
          fields.to_city,
          fields.travel_date,
          fields.arrival_date,
          Number.isFinite(available_weight) ? available_weight : null,
          fields.item_type,
          fields.description,
          Number.isFinite(reward_amount) ? reward_amount : null,
          fields.currency,
          JSON.stringify(req.body || {}),
          id,
        ]
      );

      const updated = await get(`SELECT * FROM carry_listings WHERE id=?`, [
        id,
      ]);
      return res.json({ ok: true, item: mapListing(updated) });
    } catch (e) {
      console.error("[carry] update", e);
      return res.status(500).json({ error: "Failed to update" });
    }
  });

  // Delete (soft)
  app.delete("/api/carry/listings/:id", authRequired, async (req, res) => {
    try {
      const id = toInt(req.params.id);
      if (!id) return res.status(400).json({ error: "Bad id" });

      const row = await get(
        `SELECT id, user_id FROM carry_listings WHERE id=?`,
        [id]
      );
      if (!row) return res.status(404).json({ error: "Not found" });

      if (!canEdit(req.user.id, row.user_id, req))
        return res.status(403).json({ error: "Forbidden" });

      await run(
        `UPDATE carry_listings SET is_active=0, updated_at=datetime('now') WHERE id=?`,
        [id]
      );
      return res.json({ ok: true });
    } catch (e) {
      console.error("[carry] delete", e);
      return res.status(500).json({ error: "Failed to delete" });
    }
  });

  // =====================
  // REQUESTS (match)
  // =====================
  app.post(
    "/api/carry/listings/:id/request",
    authRequired,
    async (req, res) => {
      try {
        const listingId = toInt(req.params.id);
        if (!listingId) return res.status(400).json({ error: "Bad id" });

        const listing = await get(
          `SELECT id, user_id, status FROM carry_listings WHERE id=? AND is_active=1`,
          [listingId]
        );
        if (!listing) return res.status(404).json({ error: "Not found" });

        if (Number(listing.user_id) === Number(req.user.id))
          return res
            .status(400)
            .json({ error: "You can't request your own listing" });

        if (String(listing.status) !== "open")
          return res.status(400).json({ error: "Listing not open" });

        const existing = await get(
          `SELECT id, status FROM carry_requests WHERE listing_id=? AND requester_id=? LIMIT 1`,
          [listingId, req.user.id]
        );

        if (existing?.id && existing.status !== "cancelled")
          return res.status(400).json({ error: "Already requested" });

        if (existing?.id) {
          await run(`UPDATE carry_requests SET status='pending' WHERE id=?`, [
            existing.id,
          ]);
          return res.json({
            ok: true,
            request_id: existing.id,
            status: "pending",
          });
        }

        const r = await run(
          `INSERT INTO carry_requests (listing_id, requester_id, status) VALUES (?, ?, 'pending')`,
          [listingId, req.user.id]
        );

        return res.json({ ok: true, request_id: r.lastID, status: "pending" });
      } catch (e) {
        console.error("[carry] request", e);
        return res.status(500).json({ error: "Failed" });
      }
    }
  );

  // accept/reject (owner only)
  app.patch(
    "/api/carry/requests/:id/accept",
    authRequired,
    async (req, res) => {
      try {
        const requestId = toInt(req.params.id);
        if (!requestId) return res.status(400).json({ error: "Bad id" });

        const reqRow = await get(`SELECT * FROM carry_requests WHERE id=?`, [
          requestId,
        ]);
        if (!reqRow) return res.status(404).json({ error: "Not found" });

        const listing = await get(
          `SELECT id, user_id FROM carry_listings WHERE id=?`,
          [reqRow.listing_id]
        );
        if (!listing) return res.status(404).json({ error: "Listing missing" });

        if (!canEdit(req.user.id, listing.user_id, req))
          return res.status(403).json({ error: "Forbidden" });

        await run(`UPDATE carry_requests SET status='accepted' WHERE id=?`, [
          requestId,
        ]);
        await run(
          `UPDATE carry_listings SET status='matched', updated_at=datetime('now') WHERE id=?`,
          [listing.id]
        );

        return res.json({ ok: true });
      } catch (e) {
        console.error("[carry] accept", e);
        return res.status(500).json({ error: "Failed" });
      }
    }
  );

  app.patch(
    "/api/carry/requests/:id/reject",
    authRequired,
    async (req, res) => {
      try {
        const requestId = toInt(req.params.id);
        if (!requestId) return res.status(400).json({ error: "Bad id" });

        const reqRow = await get(`SELECT * FROM carry_requests WHERE id=?`, [
          requestId,
        ]);
        if (!reqRow) return res.status(404).json({ error: "Not found" });

        const listing = await get(
          `SELECT id, user_id FROM carry_listings WHERE id=?`,
          [reqRow.listing_id]
        );
        if (!listing) return res.status(404).json({ error: "Listing missing" });

        if (!canEdit(req.user.id, listing.user_id, req))
          return res.status(403).json({ error: "Forbidden" });

        await run(`UPDATE carry_requests SET status='rejected' WHERE id=?`, [
          requestId,
        ]);
        return res.json({ ok: true });
      } catch (e) {
        console.error("[carry] reject", e);
        return res.status(500).json({ error: "Failed" });
      }
    }
  );

  // =====================
  // MESSAGES
  // =====================
  app.get(
    "/api/carry/listings/:id/messages",
    authRequired,
    async (req, res) => {
      try {
        const listingId = toInt(req.params.id);
        if (!listingId) return res.status(400).json({ error: "Bad id" });

        const rows = await all(
          `SELECT * FROM carry_messages WHERE listing_id=? ORDER BY created_at ASC LIMIT 200`,
          [listingId]
        );
        return res.json({ ok: true, messages: rows || [] });
      } catch (e) {
        console.error("[carry] messages read", e);
        return res.status(500).json({ error: "Failed" });
      }
    }
  );

  app.post(
    "/api/carry/listings/:id/messages",
    authRequired,
    async (req, res) => {
      try {
        const listingId = toInt(req.params.id);
        if (!listingId) return res.status(400).json({ error: "Bad id" });

        const message = safeTrim(req.body?.message);
        if (!message) return res.status(400).json({ error: "Missing message" });

        const r = await run(
          `INSERT INTO carry_messages (listing_id, sender_id, message) VALUES (?, ?, ?)`,
          [listingId, req.user.id, message]
        );

        const row = await get(`SELECT * FROM carry_messages WHERE id=?`, [
          r.lastID,
        ]);
        return res.json({ ok: true, message: row });
      } catch (e) {
        console.error("[carry] messages write", e);
        return res.status(500).json({ error: "Failed" });
      }
    }
  );

  // =====================
  // REVIEWS
  // =====================
  function clampRating(v) {
    const n = Number(v);
    if (!Number.isFinite(n)) return null;
    const x = Math.round(n);
    return Math.max(1, Math.min(5, x));
  }

  app.post("/api/carry/listings/:id/review", authRequired, async (req, res) => {
    try {
      const listingId = toInt(req.params.id);
      if (!listingId) return res.status(400).json({ error: "Bad id" });

      const rating = clampRating(req.body?.rating ?? req.body?.stars);
      if (!rating) return res.status(400).json({ error: "Bad rating" });

      const comment = safeTrim(req.body?.comment) || null;

      const listing = await get(
        `SELECT id, user_id FROM carry_listings WHERE id=?`,
        [listingId]
      );
      if (!listing) return res.status(404).json({ error: "Not found" });

      // default: review the listing owner
      const reviewed_user_id = Number(
        req.body?.reviewed_user_id || listing.user_id
      );

      await run(
        `INSERT INTO carry_reviews (listing_id, reviewer_id, reviewed_user_id, rating, comment)
         VALUES (?, ?, ?, ?, ?)`,
        [listingId, req.user.id, reviewed_user_id, rating, comment]
      );

      return res.json({ ok: true });
    } catch (e) {
      console.error("[carry] review", e);
      return res.status(500).json({ error: "Failed" });
    }
  });
};
