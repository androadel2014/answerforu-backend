// backend/src/modules/carry.js
/* =====================
   CARRY / SHIPMENTS (Hitchhiker style)
   ✅ Tables:
      - carry_listings      (TRIPS: traveler trips)
      - carry_shipments     (SHIPMENTS: sender delivery requests)
      - carry_requests      (MATCH: shipment -> trip)
      - carry_messages
      - carry_reviews
   ✅ Accept/Reject supports BOTH: POST + PATCH (fix 404)
   ✅ Cancel request: requester can cancel pending
   ✅ Negotiate note: requester can update note while pending
   ✅ IMPORTANT: safe ALTER for existing DB (fix 500 from missing columns)
   ✅ FIX: messages include sender_name using best available field (username/full_name/email) with safe fallbacks
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
  // SAFE ALTER (fix existing sqlite tables)
  // =====================
  function safeAddColumn(table, colDef) {
    const sql = `ALTER TABLE ${table} ADD COLUMN ${colDef}`;
    db.run(sql, (err) => {
      if (!err) return;
      const msg = String(err.message || "");
      if (
        msg.includes("duplicate column") ||
        msg.includes("already exists") ||
        msg.includes("no such table")
      ) {
        return;
      }
      console.error("[carry] SAFE_ALTER_ERROR:", msg, "SQL:", sql);
    });
  }

  function nowSql() {
    return new Date().toISOString().slice(0, 19).replace("T", " ");
  }

  // =====================
  // DB ENSURE
  // =====================
  db.run(`
    CREATE TABLE IF NOT EXISTS carry_listings (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id INTEGER NOT NULL,

      role TEXT NOT NULL,              -- traveler | sender (legacy)
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

  // ✅ NEW: Shipments table (Sender creates shipments)
  db.run(`
    CREATE TABLE IF NOT EXISTS carry_shipments (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id INTEGER NOT NULL,

      from_country TEXT,
      from_city TEXT,
      to_country TEXT,
      to_city TEXT,

      deadline TEXT,
      item_title TEXT,
      item_desc TEXT,
      item_weight REAL,

      budget_amount REAL,
      budget_currency TEXT DEFAULT 'USD',

      status TEXT DEFAULT 'open',      -- open|matched|cancelled|completed
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

      -- NEW: link shipment + offer
      shipment_id INTEGER,
      offer_amount REAL,
      offer_currency TEXT,

      note TEXT,
      status TEXT DEFAULT 'pending',    -- pending|accepted|rejected|cancelled

      created_at TEXT DEFAULT (datetime('now')),
      updated_at TEXT DEFAULT (datetime('now')),

      UNIQUE(listing_id, requester_id) -- legacy (we will enforce shipment uniqueness via SELECT before insert)
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

  // ✅ FIX OLD DBS (missing columns cause 500)
  // carry_listings
  safeAddColumn("carry_listings", "status TEXT DEFAULT 'open'");
  safeAddColumn("carry_listings", "is_active INTEGER DEFAULT 1");
  safeAddColumn("carry_listings", "data_json TEXT");
  safeAddColumn("carry_listings", "created_at TEXT");
  safeAddColumn("carry_listings", "updated_at TEXT");
  safeAddColumn("carry_listings", "currency TEXT");
  safeAddColumn("carry_listings", "reward_amount REAL");

  // carry_shipments
  safeAddColumn("carry_shipments", "status TEXT DEFAULT 'open'");
  safeAddColumn("carry_shipments", "is_active INTEGER DEFAULT 1");
  safeAddColumn("carry_shipments", "data_json TEXT");
  safeAddColumn("carry_shipments", "created_at TEXT");
  safeAddColumn("carry_shipments", "updated_at TEXT");
  safeAddColumn("carry_shipments", "budget_currency TEXT");
  safeAddColumn("carry_shipments", "budget_amount REAL");
  safeAddColumn("carry_shipments", "item_weight REAL");
  safeAddColumn("carry_shipments", "deadline TEXT");
  safeAddColumn("carry_shipments", "item_title TEXT");
  safeAddColumn("carry_shipments", "item_desc TEXT");

  // carry_requests
  safeAddColumn("carry_requests", "note TEXT");
  safeAddColumn("carry_requests", "status TEXT DEFAULT 'pending'");
  safeAddColumn("carry_requests", "created_at TEXT");
  safeAddColumn("carry_requests", "updated_at TEXT");
  safeAddColumn("carry_requests", "shipment_id INTEGER");
  safeAddColumn("carry_requests", "offer_amount REAL");
  safeAddColumn("carry_requests", "offer_currency TEXT");

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
    };
  }

  function mapShipment(row) {
    const data = safeJsonParse(row?.data_json) || {};
    return {
      id: Number(row.id),
      user_id: Number(row.user_id),

      from_country: row.from_country || "",
      from_city: row.from_city || "",
      to_country: row.to_country || "",
      to_city: row.to_city || "",

      deadline: row.deadline || null,
      item_title: row.item_title || "",
      item_desc: row.item_desc || "",
      item_weight: row.item_weight ?? null,

      budget_amount: row.budget_amount ?? null,
      budget_currency: row.budget_currency || "USD",

      status: row.status || "open",
      is_active: Number(row.is_active || 0),
      created_at: row.created_at || null,
      updated_at: row.updated_at || null,
      data,
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
  // CHAT ACCESS RULE
  // =====================
  async function canAccessChat(listingId, req) {
    const userId = req?.user?.id;
    if (!userId) return false;

    const listing = await get(
      `SELECT id, user_id FROM carry_listings WHERE id=? AND is_active=1`,
      [listingId]
    );
    if (!listing) return false;

    if (isAdminReq(req)) return true;

    const isOwner = Number(listing.user_id) === Number(userId);

    // Owner: allow chat only after ANY accepted request exists
    if (isOwner) {
      const anyAccepted = await get(
        `SELECT id FROM carry_requests WHERE listing_id=? AND status='accepted' LIMIT 1`,
        [listingId]
      );
      return !!anyAccepted?.id;
    }

    // Requester: allow chat only if THEIR request accepted
    const acceptedMine = await get(
      `SELECT id FROM carry_requests
       WHERE listing_id=? AND requester_id=? AND status='accepted'
       LIMIT 1`,
      [listingId, userId]
    );
    return !!acceptedMine?.id;
  }

  // =====================
  // LISTINGS CRUD (TRIPS)
  // =====================
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

  app.get("/api/carry/listings/:id", authOptional, async (req, res) => {
    try {
      const id = toInt(req.params.id);
      if (!id) return res.status(400).json({ error: "Bad id" });

      const row = await get(
        `SELECT * FROM carry_listings WHERE id=? AND is_active=1`,
        [id]
      );
      if (!row) return res.status(404).json({ error: "Not found" });

      const reqCountRow = await get(
        `SELECT COUNT(*) AS c FROM carry_requests WHERE listing_id=?`,
        [id]
      );

      const is_owner =
        !!req.user?.id && Number(row.user_id) === Number(req.user.id);

      let my_request_status = null;
      let my_request_id = null;
      let my_request = null;

      if (req.user?.id && !is_owner) {
        const myReq = await get(
          `SELECT * FROM carry_requests
           WHERE listing_id=? AND requester_id=?
           ORDER BY id DESC LIMIT 1`,
          [id, req.user.id]
        );
        my_request_status = myReq?.status || null;
        my_request_id = myReq?.id || null;
        my_request = myReq || null;
      }

      const allowChat = req.user?.id ? await canAccessChat(id, req) : false;

      // ✅ messages WITH sender_name (best effort: username/full_name/email)
      let msgs = [];
      if (allowChat) {
        const attempts = [
          `
          SELECT
            m.*,
            COALESCE(
              NULLIF(u.username,''),
              NULLIF(u.full_name,''),
              NULLIF(u.email,''),
              'User'
            ) AS sender_name,
            u.username AS sender_username,
            u.full_name AS sender_full_name,
            u.email AS sender_email
          FROM carry_messages m
          LEFT JOIN users u ON u.id = m.sender_id
          WHERE m.listing_id=?
          ORDER BY m.created_at DESC
          LIMIT 50
          `,
          `
          SELECT
            m.*,
            COALESCE(
              NULLIF(u.username,''),
              NULLIF(u.email,''),
              'User'
            ) AS sender_name,
            u.username AS sender_username,
            u.email AS sender_email
          FROM carry_messages m
          LEFT JOIN users u ON u.id = m.sender_id
          WHERE m.listing_id=?
          ORDER BY m.created_at DESC
          LIMIT 50
          `,
        ];

        let tmp = [];
        let okRun = false;

        for (const sql of attempts) {
          try {
            tmp = await all(sql, [id]);
            okRun = true;
            break;
          } catch (e) {}
        }

        msgs = okRun
          ? tmp
          : await all(
              `SELECT * FROM carry_messages WHERE listing_id=? ORDER BY created_at DESC LIMIT 50`,
              [id]
            );
      }

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

        can_chat: allowChat,
        is_owner,
        my_request_status,
        my_request_id,
        my_request,
      });
    } catch (e) {
      console.error("[carry] details", e);
      return res.status(500).json({ error: "Failed" });
    }
  });

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
  // SHIPMENTS CRUD (Sender)
  // =====================
  app.post("/api/carry/shipments", authRequired, async (req, res) => {
    try {
      const from_country = safeTrim(req.body?.from_country) || null;
      const from_city = safeTrim(req.body?.from_city) || null;
      const to_country = safeTrim(req.body?.to_country) || null;
      const to_city = safeTrim(req.body?.to_city) || null;

      const deadline = safeTrim(req.body?.deadline) || null;

      const item_title = safeTrim(req.body?.item_title) || null;
      const item_desc = safeTrim(req.body?.item_desc) || null;

      const item_weight =
        req.body?.item_weight == null ? null : Number(req.body.item_weight);

      const budget_amount =
        req.body?.budget_amount == null ? null : Number(req.body.budget_amount);

      const budget_currency = safeTrim(req.body?.budget_currency) || "USD";
      const data_json = JSON.stringify(req.body || {});

      const r = await run(
        `
        INSERT INTO carry_shipments
        (user_id, from_country, from_city, to_country, to_city,
         deadline, item_title, item_desc, item_weight,
         budget_amount, budget_currency,
         status, is_active, data_json, updated_at)
        VALUES
        (?, ?, ?, ?, ?,
         ?, ?, ?, ?,
         ?, ?,
         'open', 1, ?, datetime('now'))
        `,
        [
          req.user.id,
          from_country,
          from_city,
          to_country,
          to_city,
          deadline,
          item_title,
          item_desc,
          Number.isFinite(item_weight) ? item_weight : null,
          Number.isFinite(budget_amount) ? budget_amount : null,
          budget_currency,
          data_json,
        ]
      );

      const row = await get(`SELECT * FROM carry_shipments WHERE id=?`, [
        r.lastID,
      ]);
      return res.json({ ok: true, item: mapShipment(row) });
    } catch (e) {
      console.error("[carry] create shipment", e);
      return res.status(500).json({ error: "Failed to create shipment" });
    }
  });

  app.get("/api/carry/shipments", authRequired, async (req, res) => {
    try {
      const rows = await all(
        `SELECT * FROM carry_shipments
         WHERE user_id=? AND is_active=1
         ORDER BY created_at DESC, id DESC
         LIMIT 200`,
        [req.user.id]
      );
      return res.json({ ok: true, items: (rows || []).map(mapShipment) });
    } catch (e) {
      console.error("[carry] list shipments", e);
      return res.status(500).json({ error: "Failed to load" });
    }
  });

  app.patch("/api/carry/shipments/:id", authRequired, async (req, res) => {
    try {
      const id = toInt(req.params.id);
      if (!id) return res.status(400).json({ error: "Bad id" });

      const row = await get(
        `SELECT id, user_id FROM carry_shipments WHERE id=?`,
        [id]
      );
      if (!row) return res.status(404).json({ error: "Not found" });

      if (!canEdit(req.user.id, row.user_id, req))
        return res.status(403).json({ error: "Forbidden" });

      const from_country = safeTrim(req.body?.from_country) || "";
      const from_city = safeTrim(req.body?.from_city) || "";
      const to_country = safeTrim(req.body?.to_country) || "";
      const to_city = safeTrim(req.body?.to_city) || "";
      const deadline = safeTrim(req.body?.deadline) || "";
      const item_title = safeTrim(req.body?.item_title) || "";
      const item_desc = safeTrim(req.body?.item_desc) || "";
      const budget_currency = safeTrim(req.body?.budget_currency) || "";

      const item_weight =
        req.body?.item_weight == null ? null : Number(req.body.item_weight);
      const budget_amount =
        req.body?.budget_amount == null ? null : Number(req.body.budget_amount);

      await run(
        `
        UPDATE carry_shipments SET
          from_country=COALESCE(NULLIF(?,''), from_country),
          from_city=COALESCE(NULLIF(?,''), from_city),
          to_country=COALESCE(NULLIF(?,''), to_country),
          to_city=COALESCE(NULLIF(?,''), to_city),
          deadline=COALESCE(NULLIF(?,''), deadline),
          item_title=COALESCE(NULLIF(?,''), item_title),
          item_desc=COALESCE(NULLIF(?,''), item_desc),
          item_weight=COALESCE(?, item_weight),
          budget_amount=COALESCE(?, budget_amount),
          budget_currency=COALESCE(NULLIF(?,''), budget_currency),
          data_json=COALESCE(?, data_json),
          updated_at=datetime('now')
        WHERE id=?
        `,
        [
          from_country,
          from_city,
          to_country,
          to_city,
          deadline,
          item_title,
          item_desc,
          Number.isFinite(item_weight) ? item_weight : null,
          Number.isFinite(budget_amount) ? budget_amount : null,
          budget_currency,
          JSON.stringify(req.body || {}),
          id,
        ]
      );

      const updated = await get(`SELECT * FROM carry_shipments WHERE id=?`, [
        id,
      ]);
      return res.json({ ok: true, item: mapShipment(updated) });
    } catch (e) {
      console.error("[carry] update shipment", e);
      return res.status(500).json({ error: "Failed to update" });
    }
  });

  app.delete("/api/carry/shipments/:id", authRequired, async (req, res) => {
    try {
      const id = toInt(req.params.id);
      if (!id) return res.status(400).json({ error: "Bad id" });

      const row = await get(
        `SELECT id, user_id FROM carry_shipments WHERE id=?`,
        [id]
      );
      if (!row) return res.status(404).json({ error: "Not found" });

      if (!canEdit(req.user.id, row.user_id, req))
        return res.status(403).json({ error: "Forbidden" });

      await run(
        `UPDATE carry_shipments SET is_active=0, updated_at=datetime('now') WHERE id=?`,
        [id]
      );
      return res.json({ ok: true });
    } catch (e) {
      console.error("[carry] delete shipment", e);
      return res.status(500).json({ error: "Failed to delete" });
    }
  });

  // =====================
  // REQUESTS (MATCH: shipment -> trip)
  // =====================

  app.get(
    "/api/carry/listings/:id/requests",
    authRequired,
    async (req, res) => {
      try {
        const listingId = toInt(req.params.id);
        if (!listingId) return res.status(400).json({ error: "Bad id" });

        const listing = await get(
          `SELECT id, user_id FROM carry_listings WHERE id=? AND is_active=1`,
          [listingId]
        );
        if (!listing) return res.status(404).json({ error: "Not found" });

        if (!canEdit(req.user.id, listing.user_id, req))
          return res.status(403).json({ error: "Forbidden" });

        let rows = [];
        try {
          rows = await all(
            `
          SELECT
            r.*,
            u.username AS requester_username,
            u.email AS requester_email,

            s.item_title AS shipment_item_title,
            s.item_weight AS shipment_item_weight,
            s.budget_amount AS shipment_budget_amount,
            s.budget_currency AS shipment_budget_currency,
            s.from_city AS shipment_from_city,
            s.from_country AS shipment_from_country,
            s.to_city AS shipment_to_city,
            s.to_country AS shipment_to_country,
            s.deadline AS shipment_deadline
          FROM carry_requests r
          LEFT JOIN users u ON u.id = r.requester_id
          LEFT JOIN carry_shipments s ON s.id = r.shipment_id
          WHERE r.listing_id=?
          ORDER BY r.id DESC
          LIMIT 100
          `,
            [listingId]
          );
        } catch {
          rows = await all(
            `SELECT * FROM carry_requests WHERE listing_id=? ORDER BY id DESC LIMIT 100`,
            [listingId]
          );
        }

        return res.json({ ok: true, requests: rows || [] });
      } catch (e) {
        console.error("[carry] list requests", e);
        return res.status(500).json({ error: "Failed" });
      }
    }
  );

  // Request / Negotiate (NOW REQUIRES shipment_id)
  app.post(
    "/api/carry/listings/:id/request",
    authRequired,
    async (req, res) => {
      try {
        const listingId = toInt(req.params.id);
        if (!listingId)
          return res.status(400).json({ ok: false, error: "bad_listing_id" });

        const me = req.user?.id;
        if (!me)
          return res.status(401).json({ ok: false, error: "unauthorized" });

        const listing = await get(
          `SELECT id, user_id FROM carry_listings WHERE id=? AND is_active=1`,
          [listingId]
        );
        if (!listing)
          return res.status(404).json({ ok: false, error: "not_found" });
        if (Number(listing.user_id) === Number(me))
          return res
            .status(400)
            .json({ ok: false, error: "cannot_request_own" });

        const shipmentId = toInt(req.body?.shipment_id);
        const offer_amount =
          req.body?.offer_amount == null ? null : Number(req.body.offer_amount);
        const offer_currency = safeTrim(req.body?.offer_currency) || "USD";

        if (!shipmentId) {
          return res
            .status(400)
            .json({ ok: false, error: "missing_shipment_id" });
        }

        const shipment = await get(
          `SELECT id, user_id, is_active FROM carry_shipments WHERE id=? AND is_active=1`,
          [shipmentId]
        );
        if (!shipment)
          return res
            .status(404)
            .json({ ok: false, error: "shipment_not_found" });
        if (Number(shipment.user_id) !== Number(me))
          return res
            .status(403)
            .json({ ok: false, error: "shipment_not_owner" });

        const noteUp = safeTrim(req.body?.note || "");

        // enforce unique by shipment_id (do NOT rely on legacy UNIQUE)
        const existing = await get(
          `SELECT * FROM carry_requests
         WHERE listing_id=? AND requester_id=? AND shipment_id=?
         LIMIT 1`,
          [listingId, me, shipmentId]
        );

        if (existing) {
          const st = String(existing.status || "").toLowerCase();
          if (st === "pending") {
            // allow updating note/offer while pending
            const nextNote = noteUp || existing.note || null;
            const nextOfferAmount = Number.isFinite(offer_amount)
              ? offer_amount
              : existing.offer_amount ?? null;
            const nextOfferCurrency =
              offer_currency || existing.offer_currency || "USD";

            await run(
              `UPDATE carry_requests
             SET note=?, offer_amount=?, offer_currency=?, updated_at=datetime('now')
             WHERE id=?`,
              [nextNote, nextOfferAmount, nextOfferCurrency, existing.id]
            );

            const updated = await get(
              `SELECT * FROM carry_requests WHERE id=?`,
              [existing.id]
            );
            return res.json({ ok: true, already: true, request: updated });
          }
          return res.json({ ok: true, already: true, request: existing });
        }

        const now = nowSql();
        await run(
          `INSERT INTO carry_requests
         (listing_id, requester_id, shipment_id, offer_amount, offer_currency, note, status, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, 'pending', ?, ?)`,
          [
            listingId,
            me,
            shipmentId,
            Number.isFinite(offer_amount) ? offer_amount : null,
            offer_currency,
            noteUp,
            now,
            now,
          ]
        );

        const created = await get(
          `SELECT * FROM carry_requests
         WHERE listing_id=? AND requester_id=? AND shipment_id=?
         ORDER BY id DESC LIMIT 1`,
          [listingId, me, shipmentId]
        );

        return res.json({ ok: true, already: false, request: created });
      } catch (e) {
        console.error("[carry] request listing error:", e);
        return res.status(500).json({ ok: false, error: "server_error" });
      }
    }
  );

  // ===== Accept / Reject core (supports PATCH + POST) =====
  async function acceptRequestCore(requestId, req) {
    const reqRow = await get(`SELECT * FROM carry_requests WHERE id=?`, [
      requestId,
    ]);
    if (!reqRow) return { status: 404, body: { error: "Not found" } };

    const listing = await get(
      `SELECT id, user_id FROM carry_listings WHERE id=?`,
      [reqRow.listing_id]
    );
    if (!listing) return { status: 404, body: { error: "Listing missing" } };

    if (!canEdit(req.user.id, listing.user_id, req))
      return { status: 403, body: { error: "Forbidden" } };

    const st = String(reqRow.status || "pending").toLowerCase();
    if (st !== "pending")
      return { status: 400, body: { error: `Request not pending (${st})` } };

    // reject other pending requests on same trip
    await run(
      `UPDATE carry_requests
       SET status='rejected', updated_at=datetime('now')
       WHERE listing_id=? AND id<>? AND status='pending'`,
      [listing.id, requestId]
    );

    // accept this request
    await run(
      `UPDATE carry_requests
       SET status='accepted', updated_at=datetime('now')
       WHERE id=?`,
      [requestId]
    );

    // trip becomes matched
    await run(
      `UPDATE carry_listings
       SET status='matched', updated_at=datetime('now')
       WHERE id=?`,
      [listing.id]
    );

    // shipment becomes matched (if exists)
    if (reqRow.shipment_id) {
      await run(
        `UPDATE carry_shipments
         SET status='matched', updated_at=datetime('now')
         WHERE id=?`,
        [reqRow.shipment_id]
      );
    }

    return { status: 200, body: { ok: true } };
  }

  async function rejectRequestCore(requestId, req) {
    const reqRow = await get(`SELECT * FROM carry_requests WHERE id=?`, [
      requestId,
    ]);
    if (!reqRow) return { status: 404, body: { error: "Not found" } };

    const listing = await get(
      `SELECT id, user_id FROM carry_listings WHERE id=?`,
      [reqRow.listing_id]
    );
    if (!listing) return { status: 404, body: { error: "Listing missing" } };

    if (!canEdit(req.user.id, listing.user_id, req))
      return { status: 403, body: { error: "Forbidden" } };

    const st = String(reqRow.status || "pending").toLowerCase();
    if (st !== "pending")
      return { status: 400, body: { error: `Request not pending (${st})` } };

    await run(
      `UPDATE carry_requests
       SET status='rejected', updated_at=datetime('now')
       WHERE id=?`,
      [requestId]
    );

    return { status: 200, body: { ok: true } };
  }

  const acceptHandler = async (req, res) => {
    try {
      const requestId = toInt(req.params.id);
      if (!requestId) return res.status(400).json({ error: "Bad id" });
      const out = await acceptRequestCore(requestId, req);
      return res.status(out.status).json(out.body);
    } catch (e) {
      console.error("[carry] accept", e);
      return res.status(500).json({ error: "Failed" });
    }
  };

  const rejectHandler = async (req, res) => {
    try {
      const requestId = toInt(req.params.id);
      if (!requestId) return res.status(400).json({ error: "Bad id" });
      const out = await rejectRequestCore(requestId, req);
      return res.status(out.status).json(out.body);
    } catch (e) {
      console.error("[carry] reject", e);
      return res.status(500).json({ error: "Failed" });
    }
  };

  // ✅ support BOTH (fix 404)
  app.patch("/api/carry/requests/:id/accept", authRequired, acceptHandler);
  app.post("/api/carry/requests/:id/accept", authRequired, acceptHandler);

  app.patch("/api/carry/requests/:id/reject", authRequired, rejectHandler);
  app.post("/api/carry/requests/:id/reject", authRequired, rejectHandler);

  // Cancel request (pending only)
  const cancelHandler = async (req, res) => {
    try {
      const requestId = toInt(req.params.id);
      if (!requestId)
        return res.status(400).json({ ok: false, error: "Bad id" });

      const me = req.user?.id;
      const row = await get(`SELECT * FROM carry_requests WHERE id=?`, [
        requestId,
      ]);
      if (!row) return res.status(404).json({ ok: false, error: "Not found" });

      const listing = await get(
        `SELECT id, user_id FROM carry_listings WHERE id=?`,
        [row.listing_id]
      );
      if (!listing)
        return res.status(404).json({ ok: false, error: "Listing missing" });

      const canCancel =
        Number(row.requester_id) === Number(me) ||
        canEdit(me, listing.user_id, req);

      if (!canCancel)
        return res.status(403).json({ ok: false, error: "Forbidden" });

      const st = String(row.status || "pending").toLowerCase();
      if (st !== "pending")
        return res
          .status(400)
          .json({ ok: false, error: `Cannot cancel (${st})` });

      await run(
        `UPDATE carry_requests SET status='cancelled', updated_at=datetime('now') WHERE id=?`,
        [requestId]
      );

      return res.json({ ok: true });
    } catch (e) {
      console.error("[carry] cancel", e);
      return res.status(500).json({ ok: false, error: "Failed" });
    }
  };

  app.delete("/api/carry/requests/:id", authRequired, cancelHandler);
  app.post("/api/carry/requests/:id/cancel", authRequired, cancelHandler);
  app.delete("/api/carry/requests/:id/cancel", authRequired, cancelHandler);

  // Update note (negotiation) - requester only (pending)
  app.patch("/api/carry/requests/:id/note", authRequired, async (req, res) => {
    try {
      const requestId = toInt(req.params.id);
      if (!requestId)
        return res.status(400).json({ ok: false, error: "Bad id" });

      const me = req.user?.id;
      const row = await get(`SELECT * FROM carry_requests WHERE id=?`, [
        requestId,
      ]);
      if (!row) return res.status(404).json({ ok: false, error: "Not found" });

      if (Number(row.requester_id) !== Number(me))
        return res.status(403).json({ ok: false, error: "Forbidden" });

      const st = String(row.status || "pending").toLowerCase();
      if (st !== "pending")
        return res
          .status(400)
          .json({ ok: false, error: `Cannot update (${st})` });

      const note = safeTrim(req.body?.note || "");
      if (!note)
        return res.status(400).json({ ok: false, error: "Missing note" });

      await run(
        `UPDATE carry_requests SET note=?, updated_at=datetime('now') WHERE id=?`,
        [note, requestId]
      );

      const updated = await get(`SELECT * FROM carry_requests WHERE id=?`, [
        requestId,
      ]);
      return res.json({ ok: true, request: updated });
    } catch (e) {
      console.error("[carry] update note", e);
      return res.status(500).json({ ok: false, error: "Failed" });
    }
  });

  // =====================
  // MESSAGES (private)
  // =====================
  app.get(
    "/api/carry/listings/:id/messages",
    authRequired,
    async (req, res) => {
      try {
        const listingId = toInt(req.params.id);
        if (!listingId) return res.status(400).json({ error: "Bad id" });

        const ok = await canAccessChat(listingId, req);
        if (!ok) return res.status(403).json({ error: "Forbidden" });

        const attempts = [
          `
        SELECT
          m.*,
          COALESCE(
            NULLIF(u.username,''),
            NULLIF(u.full_name,''),
            NULLIF(u.email,''),
            'User'
          ) AS sender_name,
          u.username AS sender_username,
          u.full_name AS sender_full_name,
          u.email AS sender_email
        FROM carry_messages m
        LEFT JOIN users u ON u.id = m.sender_id
        WHERE m.listing_id=?
        ORDER BY m.created_at ASC
        LIMIT 200
        `,
          `
        SELECT
          m.*,
          COALESCE(
            NULLIF(u.username,''),
            NULLIF(u.email,''),
            'User'
          ) AS sender_name,
          u.username AS sender_username,
          u.email AS sender_email
        FROM carry_messages m
        LEFT JOIN users u ON u.id = m.sender_id
        WHERE m.listing_id=?
        ORDER BY m.created_at ASC
        LIMIT 200
        `,
        ];

        let rows = [];
        let okRun = false;

        for (const sql of attempts) {
          try {
            rows = await all(sql, [listingId]);
            okRun = true;
            break;
          } catch (e) {}
        }

        if (!okRun) {
          rows = await all(
            `SELECT * FROM carry_messages WHERE listing_id=? ORDER BY created_at ASC LIMIT 200`,
            [listingId]
          );
        }

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

        const ok = await canAccessChat(listingId, req);
        if (!ok) return res.status(403).json({ error: "Forbidden" });

        const message = safeTrim(req.body?.message);
        if (!message) return res.status(400).json({ error: "Missing message" });

        const r = await run(
          `INSERT INTO carry_messages (listing_id, sender_id, message) VALUES (?, ?, ?)`,
          [listingId, req.user.id, message]
        );

        let row = null;

        const attempts = [
          `
        SELECT
          m.*,
          COALESCE(
            NULLIF(u.username,''),
            NULLIF(u.full_name,''),
            NULLIF(u.email,''),
            'User'
          ) AS sender_name,
          u.username AS sender_username,
          u.full_name AS sender_full_name,
          u.email AS sender_email
        FROM carry_messages m
        LEFT JOIN users u ON u.id = m.sender_id
        WHERE m.id=?
        `,
          `
        SELECT
          m.*,
          COALESCE(
            NULLIF(u.username,''),
            NULLIF(u.email,''),
            'User'
          ) AS sender_name,
          u.username AS sender_username,
          u.email AS sender_email
        FROM carry_messages m
        LEFT JOIN users u ON u.id = m.sender_id
        WHERE m.id=?
        `,
        ];

        for (const sql of attempts) {
          try {
            row = await get(sql, [r.lastID]);
            if (row) break;
          } catch (e) {}
        }

        if (!row) {
          row = await get(`SELECT * FROM carry_messages WHERE id=?`, [
            r.lastID,
          ]);
        }

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
