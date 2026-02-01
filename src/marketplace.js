// src/modules/marketplace.js
/* =====================
   MARKETPLACE (Compatibility Layer)
   ✅ prevents 404 from frontend Marketplace tabs
   ✅ supports: places, groups, services, products, jobs, housing
   ✅ supports BOTH styles:
      - /api/listings?type=jobs
      - /api/listings/jobs
      - /api/listings/:prefixedId   (place_1 / group_2 / service_3 / product_4 / jobs_5 / housing_6)
      - /api/listings/jobs/5        (legacy)  -> maps to jobs_5
   ✅ adds ratings (avg_rating + reviews_count) for: groups/services/products/jobs/housing (places via place_reviews)
   ✅ fixes price saving:
      - services uses pickPrice (price_value|price|amount|priceValue)
      - jobs/housing saves price_type/price_value/currency into marketplace_listings

   IMPORTANT:
   ✅ No regex routes like :type(places|...) or :id(\\d+) because path-to-regexp throws.
   ✅ Numeric id resolver moved to /api/listings/id/:id to avoid conflict with /api/listings/:id (prefixed)
===================== */

module.exports = function registerMarketplace(opts) {
  console.log("[marketplace] routes registered ✅");
  // =====================
  // DB MIGRATIONS (safe)
  // =====================
  const safeAlterTable = opts.safeAlterTable || (() => {});

  // لو الجدول موجود ومعندوش أعمدة جديدة، هنضيفها بأمان
  safeAlterTable(`ALTER TABLE marketplace_listings ADD COLUMN category TEXT`);
  safeAlterTable(`ALTER TABLE marketplace_listings ADD COLUMN created_at TEXT`);
  safeAlterTable(`ALTER TABLE marketplace_listings ADD COLUMN updated_at TEXT`);
  safeAlterTable(
    `ALTER TABLE marketplace_listings ADD COLUMN public_id INTEGER`
  );

  const { app, db, auth, safeTrim, safeJsonParse, toInt } = opts;
  const { authRequired, authOptional, isAdminReq } = auth;

  const all = (db, sql, params = []) =>
    new Promise((resolve, reject) => {
      db.all(sql, params, (err, rows) => (err ? reject(err) : resolve(rows)));
    });

  const get = (db, sql, params = []) =>
    new Promise((resolve, reject) => {
      db.get(sql, params, (err, row) => (err ? reject(err) : resolve(row)));
    });

  const run = (db, sql, params = []) =>
    new Promise((resolve, reject) => {
      db.run(sql, params, function (err) {
        if (err) return reject(err);
        resolve({ lastID: this.lastID, changes: this.changes });
      });
    });

  const typeToPrefix = {
    places: "places",
    groups: "groups",
    services: "services",
    products: "products",
    jobs: "jobs",
    housing: "housing",
  };

  function normType(v) {
    return String(v || "")
      .trim()
      .toLowerCase();
  }

  const ALLOWED_TYPES = new Set([
    "places",
    "groups",
    "services",
    "products",
    "jobs",
    "housing",
  ]);
  function typeGuard(req, res, next) {
    const t = normType(req.params.type);

    // ✅ prevent numeric from being treated as type
    if (/^\d+$/.test(t)) return res.status(404).json({ error: "Bad type" });

    if (!ALLOWED_TYPES.has(t))
      return res.status(404).json({ error: "Bad type" });
    req.params.type = t;
    next();
  }

  function toNumOrNull(v) {
    if (v === null || v === undefined) return null;
    const s = String(v).trim();
    if (!s) return null;
    const n = Number(s);
    return Number.isFinite(n) ? n : null;
  }

  function pickPrice(reqBody) {
    const price_value = toNumOrNull(
      reqBody?.price_value ??
        reqBody?.priceValue ??
        reqBody?.price ??
        reqBody?.amount
    );
    const price_type =
      safeTrim(reqBody?.price_type ?? reqBody?.priceType) || null;
    const currency = safeTrim(reqBody?.currency) || null;
    return { price_value, price_type, currency };
  }

  function asBoolInt(v) {
    return v ? 1 : 0;
  }

  function sendList(res, items) {
    return res.json({ ok: true, items: Array.isArray(items) ? items : [] });
  }
  function sendItem(res, item) {
    return res.json({ ok: true, item: item || null });
  }
  function sendOk(res, extra = {}) {
    return res.json({ ok: true, ...extra });
  }

  // =====================
  // DB ensure
  // =====================
  function ensureUnifiedListingsTable() {
    // ✅ Table واحد لكل الأنواع + id رقمي يونيك
    db.run(`
      CREATE TABLE IF NOT EXISTS marketplace_listings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        type TEXT NOT NULL,

        title TEXT,
        category TEXT,
        state TEXT,
        city TEXT,

        address TEXT,
        phone TEXT,
        website TEXT,
        link TEXT,
        contact TEXT,

        platform TEXT,
        topic TEXT,

        price_type TEXT,
        price_value REAL,
        currency TEXT,

        description TEXT,
        notes TEXT,

        images_json TEXT,
        data_json TEXT,

        status TEXT DEFAULT 'approved',
        is_active INTEGER DEFAULT 1,

        created_at TEXT DEFAULT (datetime('now')),
        updated_at TEXT DEFAULT (datetime('now'))
      )
    `);

    // ✅ safe add columns لو عندك DB قديمة
    const addCol = (name, defSql) => {
      db.run(
        `ALTER TABLE marketplace_listings ADD COLUMN ${name} ${defSql}`,
        () => {}
      );
    };

    addCol("user_id", "INTEGER");
    addCol("type", "TEXT");

    addCol("title", "TEXT");
    addCol("category", "TEXT");
    addCol("state", "TEXT");
    addCol("city", "TEXT");

    addCol("address", "TEXT");
    addCol("phone", "TEXT");
    addCol("website", "TEXT");
    addCol("link", "TEXT");
    addCol("contact", "TEXT");

    addCol("platform", "TEXT");
    addCol("topic", "TEXT");

    addCol("price_type", "TEXT");
    addCol("price_value", "REAL");
    addCol("currency", "TEXT");

    addCol("description", "TEXT");
    addCol("notes", "TEXT");

    addCol("images_json", "TEXT");
    addCol("data_json", "TEXT");

    addCol("status", "TEXT DEFAULT 'approved'");
    addCol("is_active", "INTEGER DEFAULT 1");
    addCol("created_at", "TEXT DEFAULT (datetime('now'))");
    addCol("updated_at", "TEXT DEFAULT (datetime('now'))");
  }

  ensureUnifiedListingsTable();

  function ensureMarketplaceReviewsTable() {
    db.run(`
      CREATE TABLE IF NOT EXISTS marketplace_reviews (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        listing_type TEXT NOT NULL,   -- any string (services/products/...)
        listing_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        stars INTEGER NOT NULL,
        comment TEXT,
        created_at TEXT DEFAULT (datetime('now'))
      )
    `);
  }
  ensureMarketplaceReviewsTable();

  // =====================
  // Mappers
  // =====================
  function mapListingRow(x) {
    const data = safeJsonParse(x.data_json) || {};
    const images = safeJsonParse(x.images_json) || [];

    // ✅ IMPORTANT:
    // - internal DB id: x.id
    // - stable public id used by frontend everywhere: COALESCE(public_id, id)
    const internalId = Number(x.id);
    const publicId = Number(x.public_id || x.id);
    const stableId = publicId;

    return {
      // ✅ frontend MUST use this id everywhere (details / edit / delete / reviews)
      id: stableId,

      // ✅ keep both for debugging / internal ops
      public_id: publicId,
      internal_id: internalId,

      type: x.type,

      title: x.title || "",
      category: x.category || "",

      state: x.state || "",
      city: x.city || "",

      link: x.link || "",
      contact: x.contact || "",

      address: x.address || "",
      phone: x.phone || "",
      website: x.website || "",

      platform: x.platform || "",
      topic: x.topic || "",

      price_type: x.price_type || null,
      price_value: x.price_value ?? null,
      currency: x.currency || null,

      description: x.description || "",
      notes: x.notes || "",

      images,

      status: x.status || "approved",
      is_active: Number(x.is_active || 0),
      created_at: x.created_at || null,
      updated_at: x.updated_at || null,

      avg_rating: Number(x.avg_rating || 0),
      reviews_count: Number(x.reviews_count || 0),

      data,
      raw: x,
    };
  }

  // =====================
  // Loaders (with ratings)
  // =====================
  async function loadMarketplaceByType(req, type) {
    const t = normType(type);
    const isAdmin = isAdminReq(req);

    const where = ["m.type = ?"];
    const params = [t];

    if (!isAdmin) where.push(`COALESCE(m.status,'approved')='approved'`);
    where.push("m.is_active = 1");

    const q = String(req.query.q || "").trim();
    const state = String(req.query.state || "").trim();
    const city = String(req.query.city || "").trim();
    const category = String(req.query.category || "").trim();

    // groups-only filters (still stored same table)
    const platform = String(req.query.platform || "").trim();
    const topic = String(req.query.topic || "").trim();

    if (q) {
      where.push(`(
      m.title LIKE ? OR
      m.notes LIKE ? OR
      m.description LIKE ? OR
      m.address LIKE ? OR
      m.category LIKE ?
    )`);
      params.push(`%${q}%`, `%${q}%`, `%${q}%`, `%${q}%`, `%${q}%`);
    }

    if (state) {
      where.push("m.state = ?");
      params.push(state);
    }
    if (city) {
      where.push("LOWER(m.city)=LOWER(?)");
      params.push(city);
    }
    if (category) {
      where.push("m.category = ?");
      params.push(category);
    }

    if (t === "groups") {
      if (platform) {
        where.push("m.platform = ?");
        params.push(platform);
      }
      if (topic) {
        where.push("m.topic = ?");
        params.push(topic);
      }
    }

    const sql = `
    SELECT
      m.*,
      COALESCE(AVG(r.stars), 0) AS avg_rating,
      COUNT(r.id) AS reviews_count
    FROM marketplace_listings m
    LEFT JOIN marketplace_reviews r
      ON r.listing_type = m.type AND r.listing_id = m.id
    WHERE ${where.join(" AND ")}
    GROUP BY m.id
    ORDER BY m.id DESC
    LIMIT 200
  `;

    const rows = await all(db, sql, params);
    return rows.map(mapListingRow);
  }

  // =====================
  // Create
  // =====================
  async function createListingCore(req, res) {
    try {
      if (!req.user || !req.user.id) {
        return res.status(401).json({
          error: "Unauthorized (missing req.user)",
          hint: "Token missing/invalid OR authRequired not attached correctly",
        });
      }

      const type = normType(req.body?.type);
      if (!type) return res.status(400).json({ error: "Missing type" });
      if (!ALLOWED_TYPES.has(type))
        return res.status(400).json({ error: "Bad type" });

      const title = safeTrim(req.body?.title || req.body?.name);
      if (!title) return res.status(400).json({ error: "Missing title" });

      const category = safeTrim(req.body?.category) || null;
      const state = safeTrim(req.body?.state) || null;
      const city = safeTrim(req.body?.city) || null;

      const link = safeTrim(req.body?.link || req.body?.url) || null;
      const contact =
        safeTrim(req.body?.contact || req.body?.phone || req.body?.email) ||
        null;

      const address = safeTrim(req.body?.address) || null;
      const phone = safeTrim(req.body?.phone) || null;
      const website = safeTrim(req.body?.website) || null;

      const platform = safeTrim(req.body?.platform) || null;
      const topic = safeTrim(req.body?.topic) || null;

      const description = safeTrim(req.body?.description) || null;
      const notes = safeTrim(req.body?.notes) || null;

      const images = Array.isArray(req.body?.images) ? req.body.images : null;

      const { price_value, price_type, currency } = pickPrice(req.body || {});
      const dataObj = req.body || {};

      const r = await run(
        db,
        `
        INSERT INTO marketplace_listings
          (user_id, type, title, category, state, city,
           link, contact,
           address, phone, website,
           platform, topic,
           price_type, price_value, currency,
           description, notes,
           images_json, data_json,
           status, is_active, updated_at)
        VALUES
          (?, ?, ?, ?, ?, ?,
           ?, ?,
           ?, ?, ?,
           ?, ?,
           ?, ?, ?,
           ?, ?,
           ?, ?,
           'approved', 1, datetime('now'))
        `,
        [
          req.user.id,
          type,
          title,
          category,
          state,
          city,
          link,
          contact,
          address,
          phone,
          website,
          platform,
          topic,
          price_type || null,
          price_value,
          currency || null,
          description,
          notes,
          images ? JSON.stringify(images) : null,
          JSON.stringify(dataObj),
        ]
      );
      // ✅ keep public_id stable for frontend shortId
      await run(
        db,
        `UPDATE marketplace_listings SET public_id = COALESCE(public_id, id) WHERE id = ?`,
        [r.lastID]
      );

      const createdRow = await get(
        db,
        `
        SELECT
          m.*,
          COALESCE(AVG(r.stars), 0) AS avg_rating,
          COUNT(r.id) AS reviews_count
        FROM marketplace_listings m
        LEFT JOIN marketplace_reviews r
          ON r.listing_type = m.type AND r.listing_id = m.id
        WHERE m.id = ?
        GROUP BY m.id
        LIMIT 1
        `,
        [r.lastID]
      );

      return res.json({
        ok: true,
        item: mapListingRow(createdRow),
      });
    } catch (e) {
      console.error("createListingCore error:", e);
      return res.status(500).json({
        error: "Failed to create listing",
        detail: String(e?.message || e),
        hint: "Check SQL column names / req.user / table schema",
      });
    }
  }

  // =====================
  // Update
  // =====================
  async function updateListingCore(req, res) {
    try {
      const raw = safeTrim(req.params.id);

      let id = null;
      let type = null;

      if (raw.includes("_")) {
        const parts = raw.split("_");
        const p = normType(parts[0]);
        // ✅ accept singular prefixes too
        const normalized =
          p === "place"
            ? "places"
            : p === "group"
            ? "groups"
            : p === "service"
            ? "services"
            : p === "product"
            ? "products"
            : p;
        type = normalized;
        id = toInt(parts[1]);
      } else {
        id = toInt(raw);
        if (id) {
          const row = await get(
            db,
            `SELECT id, type, user_id FROM marketplace_listings WHERE id=?`,
            [id]
          );
          if (!row) return res.status(404).json({ error: "Not found" });
          if (Number(row.user_id) !== Number(req.user.id))
            return res.status(403).json({ error: "Forbidden" });
          type = row.type;
        }
      }

      if (!id) return res.status(400).json({ error: "Bad id" });
      if (!ALLOWED_TYPES.has(type))
        return res.status(400).json({ error: "Bad type" });

      // ✅ read updates from body (these were missing)
      const title = safeTrim(req.body?.title || req.body?.name) || "";
      const category = safeTrim(req.body?.category) || "";
      const state = safeTrim(req.body?.state) || "";
      const city = safeTrim(req.body?.city) || "";

      const link = safeTrim(req.body?.link || req.body?.url) || "";
      const contact =
        safeTrim(req.body?.contact || req.body?.phone || req.body?.email) || "";

      const address = safeTrim(req.body?.address) || "";
      const phone = safeTrim(req.body?.phone) || "";
      const website = safeTrim(req.body?.website) || "";

      const platform = safeTrim(req.body?.platform) || "";
      const topic = safeTrim(req.body?.topic) || "";

      const description = safeTrim(req.body?.description) || "";
      const notes = safeTrim(req.body?.notes) || "";

      const images = Array.isArray(req.body?.images) ? req.body.images : null;

      const { price_value, price_type, currency } = pickPrice(req.body || {});

      await run(
        db,
        `
      UPDATE marketplace_listings SET
        title = COALESCE(NULLIF(?,''), title),
        category = COALESCE(NULLIF(?,''), category),
        state = COALESCE(NULLIF(?,''), state),
        city = COALESCE(NULLIF(?,''), city),

        link = COALESCE(NULLIF(?,''), link),
        contact = COALESCE(NULLIF(?,''), contact),

        address = COALESCE(NULLIF(?,''), address),
        phone = COALESCE(NULLIF(?,''), phone),
        website = COALESCE(NULLIF(?,''), website),

        platform = COALESCE(NULLIF(?,''), platform),
        topic = COALESCE(NULLIF(?,''), topic),

        price_type = COALESCE(NULLIF(?,''), price_type),
        price_value = COALESCE(?, price_value),
        currency = COALESCE(NULLIF(?,''), currency),

        description = COALESCE(NULLIF(?,''), description),
        notes = COALESCE(NULLIF(?,''), notes),

        images_json = COALESCE(?, images_json),
        data_json = COALESCE(?, data_json),

        updated_at = datetime('now')
      WHERE id = ? AND user_id = ? AND type = ?
      `,
        [
          title,
          category,
          state,
          city,

          link,
          contact,

          address,
          phone,
          website,

          platform,
          topic,

          price_type || "",
          price_value,
          currency || "",

          description,
          notes,

          images ? JSON.stringify(images) : null,
          JSON.stringify(req.body || {}),

          id,
          req.user.id,
          type,
        ]
      );

      const updated = await get(
        db,
        `SELECT
        m.*,
        COALESCE(AVG(r.stars), 0) AS avg_rating,
        COUNT(r.id) AS reviews_count
      FROM marketplace_listings m
      LEFT JOIN marketplace_reviews r
        ON r.listing_type = m.type AND r.listing_id = m.id
      WHERE m.id=?
      GROUP BY m.id
      LIMIT 1`,
        [id]
      );

      return res.json({ ok: true, item: mapListingRow(updated) });
    } catch (e) {
      console.error("PATCH /api/listings/:id error:", e);
      return res.status(500).json({ error: "Failed to update" });
    }
  }

  // =====================
  // Delete
  // =====================
  async function deleteListingCore(req, res) {
    try {
      const raw = safeTrim(req.params.id);

      let id = null;
      let type = null;

      if (raw.includes("_")) {
        const parts = raw.split("_");
        const p = normType(parts[0]);
        // ✅ accept singular prefixes too
        const normalized =
          p === "place"
            ? "places"
            : p === "group"
            ? "groups"
            : p === "service"
            ? "services"
            : p === "product"
            ? "products"
            : p;
        type = normalized;
        id = toInt(parts[1]);
      } else {
        id = toInt(raw);
        if (id) {
          const row = await get(
            db,
            `SELECT id, type, user_id FROM marketplace_listings WHERE id=?`,
            [id]
          );
          if (!row) return res.status(404).json({ error: "Not found" });
          if (Number(row.user_id) !== Number(req.user.id))
            return res.status(403).json({ error: "Forbidden" });
          type = row.type;
        }
      }

      if (!id) return res.status(400).json({ error: "Bad id" });
      if (!ALLOWED_TYPES.has(type))
        return res.status(400).json({ error: "Bad type" });

      await run(
        db,
        `DELETE FROM marketplace_listings WHERE id = ? AND user_id = ?`,
        [id, req.user.id]
      );

      return sendOk(res);
    } catch (e) {
      console.error("DELETE /api/listings/:id error:", e);
      return res.status(500).json({ error: "Failed to delete" });
    }
  }

  async function getPrefixedListing(req, res) {
    const raw = safeTrim(req.params.id);
    if (!raw) return res.status(400).json({ error: "Bad id" });

    const [p0, numStr] = raw.split("_");
    const p = normType(p0);
    const type =
      p === "place"
        ? "places"
        : p === "group"
        ? "groups"
        : p === "service"
        ? "services"
        : p === "product"
        ? "products"
        : p;

    const id = toInt(numStr);
    if (!id) return res.status(400).json({ error: "Bad id" });

    if (!ALLOWED_TYPES.has(type))
      return res.status(400).json({ error: "Bad type" });

    const isAdmin = isAdminReq(req);

    const where = ["m.id = ?", "m.type = ?"];
    const params = [id, type];
    if (!isAdmin) where.push(`COALESCE(m.status,'approved')='approved'`);
    where.push("m.is_active = 1");

    const row = await get(
      db,
      `
    SELECT
      m.*,
      COALESCE(AVG(r.stars), 0) AS avg_rating,
      COUNT(r.id) AS reviews_count
    FROM marketplace_listings m
    LEFT JOIN marketplace_reviews r
      ON r.listing_type = m.type AND r.listing_id = m.id
    WHERE ${where.join(" AND ")}
    GROUP BY m.id
    LIMIT 1
    `,
      params
    );

    if (!row) return res.status(404).json({ error: "Not found" });
    return sendItem(res, mapListingRow(row));
  }

  async function resolveNumericId(req, res) {
    try {
      const id = toInt(req.params.id);
      if (!id) return res.status(400).json({ error: "Bad id" });

      const isAdmin = isAdminReq(req);

      const where = ["m.id = ?"];
      const params = [id];

      if (!isAdmin) where.push(`COALESCE(m.status,'approved')='approved'`);
      where.push("m.is_active = 1");

      const row = await get(
        db,
        `
      SELECT
        m.*,
        COALESCE(AVG(r.stars), 0) AS avg_rating,
        COUNT(r.id) AS reviews_count
      FROM marketplace_listings m
      LEFT JOIN marketplace_reviews r
        ON r.listing_type = m.type AND r.listing_id = m.id
      WHERE ${where.join(" AND ")}
      GROUP BY m.id
      LIMIT 1
      `,
        params
      );
      if (!row) {
        // ✅ fallback 1: جرّب الجداول القديمة (بأسماء أوسع)
        const tryTables = [
          { table: "community_places", kind: "places" },
          { table: "community_groups", kind: "groups" },
          { table: "community_services", kind: "services" },

          { table: "places", kind: "places" },
          { table: "groups", kind: "groups" },
          { table: "services", kind: "services" },

          // ✅ extra guesses (won't break if missing)
          { table: "marketplace_places", kind: "places" },
          { table: "marketplace_groups", kind: "groups" },
          { table: "marketplace_services", kind: "services" },

          { table: "listings_places", kind: "places" },
          { table: "listings_groups", kind: "groups" },
          { table: "listings_services", kind: "services" },
        ];

        for (const t of tryTables) {
          try {
            const legacy = await get(
              db,
              `SELECT * FROM ${t.table} WHERE id = ?`,
              [id]
            );
            if (legacy) {
              // ✅ رجّع شكل موحّد قد ما نقدر (عشان الفرونت ما يقولش Place not found)
              const mapped = {
                id: `${t.kind}_${legacy.id}`,
                type: t.kind,

                title: legacy.title || legacy.name || "",
                category: legacy.category || "",
                state: legacy.state || "",
                city: legacy.city || "",

                link: legacy.link || legacy.url || "",
                contact: legacy.contact || legacy.phone || legacy.email || "",

                address: legacy.address || "",
                phone: legacy.phone || "",
                website: legacy.website || "",

                platform: legacy.platform || "",
                topic: legacy.topic || "",

                price_type: legacy.price_type || null,
                price_value:
                  legacy.price_value ?? legacy.price ?? legacy.amount ?? null,
                currency: legacy.currency || null,

                description: legacy.description || "",
                notes: legacy.notes || "",

                images:
                  safeJsonParse(legacy.images_json) ||
                  safeJsonParse(legacy.images) ||
                  [],

                status: legacy.status || "approved",
                is_active: Number(legacy.is_active ?? 1),

                created_at: legacy.created_at || legacy.createdAt || null,
                updated_at: legacy.updated_at || legacy.updatedAt || null,

                avg_rating: Number(legacy.avg_rating || legacy.avgRating || 0),
                reviews_count: Number(
                  legacy.reviews_count || legacy.reviewsCount || 0
                ),

                data: legacy,
                raw: legacy,
                _source_table: t.table,
              };

              return sendItem(res, mapped);
            }
          } catch (e) {
            // ignore missing table
          }
        }

        // ✅ fallback 2: لو الـ shortId بتاع الفرونت هو "legacy id" متخزن جوه data_json
        // و الـ marketplace_listings.id مختلف (وده اللي بيعمل 404 عندك)
        try {
          const like = `%${id}%`;
          const found = await get(
            db,
            `
      SELECT
        m.*,
        COALESCE(AVG(r.stars), 0) AS avg_rating,
        COUNT(r.id) AS reviews_count
      FROM marketplace_listings m
      LEFT JOIN marketplace_reviews r
        ON r.listing_type = m.type AND r.listing_id = m.id
      WHERE
        m.data_json LIKE ?
        OR m.data_json LIKE ?
        OR m.data_json LIKE ?
        OR m.data_json LIKE ?
      GROUP BY m.id
      ORDER BY m.id DESC
      LIMIT 1
      `,
            [
              `%"id":${id}%`,
              `%"placeId":${id}%`,
              `%"place_id":${id}%`,
              `%"legacy_id":${id}%`,
            ]
          );

          if (found) return sendItem(res, mapListingRow(found));
        } catch (e) {
          // ignore
        }

        return res.status(404).json({ error: "Not found" });
      }

      return sendItem(res, mapListingRow(row));
    } catch (e) {
      console.error("GET /api/listings/id/:id error:", e);
      return res.status(500).json({ error: "Failed to resolve id" });
    }
  }

  /* =========================================================
   ✅ ALIASES FIX (do not break existing)
   - supports:
     GET /api/marketplace/:kind/:id
     GET /api/listings/:id
     GET /api/marketplace/listings/:id
     GET /api/community/:kind/:id/reviews
     GET /api/marketplace/:kind/:id/reviews
========================================================= */

  function safeKind(k) {
    return String(k || "")
      .trim()
      .toLowerCase();
  }

  // ✅ resolve shortId (public_id OR id) -> internal marketplace_listings.id
  async function resolveInternalListingId(kind, shortId) {
    const k = safeKind(kind);
    const sid = Number(shortId);
    if (!sid) return null;

    // try match by public_id first, then id
    try {
      const row = await get(
        db,
        `SELECT id FROM marketplace_listings
       WHERE type = ? AND (public_id = ? OR id = ?)
       LIMIT 1`,
        [k, sid, sid]
      );
      return row?.id ? Number(row.id) : null;
    } catch (e) {
      return null;
    }
  }

  async function tryGetFromTables(getFn, id, tables) {
    for (const t of tables) {
      try {
        const row = await getFn(`SELECT * FROM ${t} WHERE id = ?`, [id]);
        if (row) return { row, table: t };
      } catch (e) {
        // table missing or query error -> ignore and continue
      }
    }
    return null;
  }

  async function tryAllFromTables(allFn, sqlList) {
    for (const { sql, params } of sqlList) {
      try {
        const rows = await allFn(sql, params);
        if (rows && Array.isArray(rows)) return rows;
      } catch (e) {
        // ignore and continue
      }
    }
    return [];
  }

  // ✅ unified "get item by id" (kind is only a hint)

  // ✅ legacy numeric id aliases (ONLY when numeric)
  // - keeps prefixed ids working: /api/listings/services_5
  // - keeps types working: /api/listings/services
  // - handles numeric id only: /api/listings/5
  app.get("/api/listings/:id", authOptional, async (req, res, next) => {
    const raw = safeTrim(req.params.id);
    if (!raw) return next();
    // prefixed -> handle here
    if (raw.includes("_")) return getPrefixedListing(req, res);

    // type -> let /api/listings/:type handle it
    if (ALLOWED_TYPES.has(normType(raw))) return next();

    // only numeric ids
    if (!/^\d+$/.test(raw)) return next();

    req.params.id = raw;
    return resolveNumericId(req, res);
  });

  app.get(
    "/api/marketplace/listings/:id",
    authOptional,
    async (req, res, next) => {
      const raw = safeTrim(req.params.id);
      if (!raw) return next();
      if (raw.includes("_")) return getPrefixedListing(req, res);
      if (ALLOWED_TYPES.has(normType(raw))) return next();
      if (!/^\d+$/.test(raw)) return next();

      req.params.id = raw;
      return resolveNumericId(req, res);
    }
  );

  // =====================
  // REVIEWS (single source of truth)
  // supports:
  //  GET  /api/community/:kind/:id/reviews
  //  GET  /api/marketplace/:kind/:id/reviews
  //  POST /api/community/:kind/:id/reviews
  //  POST /api/marketplace/:kind/:id/reviews
  // =====================

  function clampStars(v) {
    const n = Number(v);
    if (!Number.isFinite(n)) return null;
    const x = Math.round(n);
    return Math.max(1, Math.min(5, x));
  }
  function mapReviewRow(r) {
    if (!r) return r;

    const c = r.comment ?? "";
    const userName =
      r.user_name ??
      r.userName ??
      r.name ??
      r.full_name ??
      r.username ??
      r.email ??
      "User";

    return {
      ...r,
      user_name: userName,
      userName: userName,

      // comment aliases
      text: c,
      body: c,
      review: c,
      content: c,
      message: c,
      commentText: c,
      reviewText: c,
    };
  }
  async function fetchReviewsWithUser(kind, internalId) {
    // try join users (if table exists)
    try {
      const rows = await all(
        db,
        `
        SELECT
          r.*,
          COALESCE(u.username, u.email, 'User') AS user_name
        FROM marketplace_reviews r
        LEFT JOIN users u ON u.id = r.user_id
        WHERE r.listing_type = ? AND r.listing_id = ?
        ORDER BY r.created_at DESC
        `,
        [kind, internalId]
      );
      return rows;
    } catch (e) {
      // fallback: no users table (or columns mismatch)
      const rows = await all(
        db,
        `
        SELECT r.* FROM marketplace_reviews r
        WHERE r.listing_type = ? AND r.listing_id = ?
        ORDER BY r.created_at DESC
        `,
        [kind, internalId]
      );
      return rows;
    }
  }

  async function resolveInternalByAnyId(kind, anyId) {
    const k = safeKind(kind);
    const sid = Number(anyId);
    if (!sid) return null;

    try {
      const row = await get(
        db,
        `
        SELECT id, type FROM marketplace_listings
        WHERE (public_id = ? OR id = ?)
        LIMIT 1
        `,
        [sid, sid]
      );
      if (!row) return null;
      if (k && row.type && safeKind(row.type) !== k) return null; // kind mismatch protection
      return { internalId: Number(row.id), kind: safeKind(row.type) };
    } catch (e) {
      return null;
    }
  }

  // ✅ read reviews (uses internal listing id)
  async function handleReviewsAlias(req, res) {
    try {
      const kind = safeKind(req.params.kind);
      const shortId = Number(req.params.id);
      if (!shortId) return res.status(400).json({ error: "Invalid id" });

      const internalId = await resolveInternalListingId(kind, shortId);
      if (!internalId) {
        return res.json({
          ok: true,
          kind,
          item_id: shortId,
          listing_id: null,
          reviews: [],
          count: 0,
        });
      }

      const rows = await fetchReviewsWithUser(kind, internalId);

      return res.json({
        ok: true,
        kind,
        item_id: shortId,
        listing_id: internalId,
        reviews: rows.map(mapReviewRow),
        count: rows.length,
      });
    } catch (e) {
      console.error("[REVIEWS] read", e);
      return res.sendStatus(500);
    }
  }

  // ✅ write review (upsert) + return fresh list
  async function createOrUpdateMyReview(req, res) {
    try {
      const kind = safeKind(req.params.kind);
      const shortId = Number(req.params.id);
      if (!shortId) return res.status(400).json({ error: "Invalid id" });

      // ✅ accept many frontend shapes for rating/stars
      const stars = clampStars(
        req.body?.stars ??
          req.body?.rating ??
          req.body?.value ??
          req.body?.starsValue ??
          req.body?.ratingValue ??
          req.body?.stars?.value ??
          req.body?.rating?.value
      );
      if (!stars) {
        return res.status(400).json({
          error: "Invalid stars",
          received: req.body,
        });
      }

      // ✅ accept many frontend shapes for comment text
      const commentRaw =
        req.body?.comment ??
        req.body?.commentText ??
        req.body?.comment_text ??
        req.body?.reviewText ??
        req.body?.text ??
        req.body?.body ??
        req.body?.review ??
        req.body?.content ??
        req.body?.message ??
        req.body?.description ??
        req.body?.note ??
        req.body?.notes;

      // if comment is object/array, stringify it
      const commentStr =
        typeof commentRaw === "string"
          ? commentRaw
          : commentRaw == null
          ? ""
          : JSON.stringify(commentRaw);

      const comment = safeTrim(commentStr) || null;

      const internalId = await resolveInternalListingId(kind, shortId);
      if (!internalId)
        return res.status(404).json({ error: "Listing not found" });

      // ✅ upsert per user+listing+type
      const existing = await get(
        db,
        `SELECT id FROM marketplace_reviews
       WHERE listing_type=? AND listing_id=? AND user_id=?
       LIMIT 1`,
        [kind, internalId, req.user.id]
      );

      if (existing?.id) {
        await run(
          db,
          `UPDATE marketplace_reviews
         SET stars=?, comment=?, created_at=datetime('now')
         WHERE id=?`,
          [stars, comment, existing.id]
        );
      } else {
        await run(
          db,
          `INSERT INTO marketplace_reviews (listing_type, listing_id, user_id, stars, comment)
         VALUES (?, ?, ?, ?, ?)`,
          [kind, internalId, req.user.id, stars, comment]
        );
      }
      const rows = await all(
        db,
        `
        SELECT
          r.*,
          COALESCE(u.username, u.email, 'User') AS user_name
        FROM marketplace_reviews r
        LEFT JOIN users u ON u.id = r.user_id
        WHERE r.listing_type=? AND r.listing_id=?
        ORDER BY r.created_at DESC
        `,
        [kind, internalId]
      );

      return res.json({
        ok: true,
        kind,
        item_id: shortId,
        listing_id: internalId,
        reviews: rows.map(mapReviewRow),
        count: rows.length,
      });
    } catch (e) {
      console.error("[REVIEWS] write", e);
      return res.status(500).json({ error: "Failed to save review" });
    }
  }

  // ✅ delete my review (alias)
  // supports:
  //   DELETE /api/community/:kind/:id/reviews/me
  //   DELETE /api/marketplace/:kind/:id/reviews/me
  async function deleteMyReview(req, res) {
    try {
      const kind = safeKind(req.params.kind);
      const shortId = Number(req.params.id);
      if (!shortId) return res.status(400).json({ error: "Invalid id" });

      const internalId = await resolveInternalListingId(kind, shortId);
      if (!internalId)
        return res.status(404).json({ error: "Listing not found" });

      await run(
        db,
        `DELETE FROM marketplace_reviews
       WHERE listing_type=? AND listing_id=? AND user_id=?`,
        [kind, internalId, req.user.id]
      );
      const rows = await all(
        db,
        `
        SELECT
          r.*,
          COALESCE(u.username, u.email, 'User') AS user_name
        FROM marketplace_reviews r
        LEFT JOIN users u ON u.id = r.user_id
        WHERE r.listing_type=? AND r.listing_id=?
        ORDER BY r.created_at DESC
        `,
        [kind, internalId]
      );

      return res.json({
        ok: true,
        kind,
        item_id: shortId,
        listing_id: internalId,
        reviews: rows.map(mapReviewRow),
        count: rows.length,
      });
    } catch (e) {
      console.error("[REVIEWS] delete my review", e);
      return res.status(500).json({ error: "Failed to delete review" });
    }
  }
  // =========================================================
  // ✅ EXPLICIT COMMUNITY REVIEW ROUTES (prevents 404 forever)
  // Frontend calls:
  //   /api/community/groups/:id/reviews
  //   /api/community/places/:id/reviews
  // so we provide explicit handlers that route to marketplace_reviews
  // =========================================================

  // ---- GROUPS ----
  app.get("/api/community/groups/:id/reviews", (req, res) => {
    req.params.kind = "groups";
    return handleReviewsAlias(req, res);
  });
  app.post("/api/community/groups/:id/reviews", authRequired, (req, res) => {
    req.params.kind = "groups";
    return createOrUpdateMyReview(req, res);
  });
  app.delete(
    "/api/community/groups/:id/reviews/me",
    authRequired,
    (req, res) => {
      req.params.kind = "groups";
      return deleteMyReview(req, res);
    }
  );

  // ---- PLACES ----
  app.get("/api/community/places/:id/reviews", (req, res) => {
    req.params.kind = "places";
    return handleReviewsAlias(req, res);
  });
  app.post("/api/community/places/:id/reviews", authRequired, (req, res) => {
    req.params.kind = "places";
    return createOrUpdateMyReview(req, res);
  });
  app.delete(
    "/api/community/places/:id/reviews/me",
    authRequired,
    (req, res) => {
      req.params.kind = "places";
      return deleteMyReview(req, res);
    }
  );

  // ✅ keep generic aliases too (for services/products/jobs/housing or future)
  app.get("/api/community/:kind/:id/reviews", handleReviewsAlias);
  app.get("/api/marketplace/:kind/:id/reviews", handleReviewsAlias);

  // =====================
  // ✅ REVIEWS aliases by listing numeric id (no kind in URL)
  // supports:
  //   GET  /api/listings/:id/reviews
  //   GET  /api/marketplace/listings/:id/reviews
  //   POST /api/listings/:id/reviews
  //   POST /api/marketplace/listings/:id/reviews
  //   DELETE /api/listings/:id/reviews/me
  //   DELETE /api/marketplace/listings/:id/reviews/me
  // =====================

  function parsePrefixedId(raw) {
    const s = safeTrim(raw);
    if (!s || !s.includes("_")) return null;

    const [p0, numStr] = s.split("_");
    const p = normType(p0);

    const kind =
      p === "place"
        ? "places"
        : p === "group"
        ? "groups"
        : p === "service"
        ? "services"
        : p === "product"
        ? "products"
        : p;

    const internalId = toInt(numStr);
    if (!internalId) return null;
    if (!ALLOWED_TYPES.has(kind)) return null;

    return { kind, internalId };
  }

  async function handleReviewsByListingId(req, res) {
    try {
      const raw = String(req.params.id || "").trim();

      // ✅ prefixed: services_5 / places_10 ...
      const pref = parsePrefixedId(raw);
      if (pref) {
        const rows = await fetchReviewsWithUser(pref.kind, pref.internalId);
        return res.json({
          ok: true,
          kind: pref.kind,
          item_id: raw,
          listing_id: pref.internalId,
          reviews: rows.map(mapReviewRow),
          count: rows.length,
        });
      }

      // ✅ numeric: 12
      const shortId = Number(raw);
      if (!shortId) return res.status(400).json({ error: "Invalid id" });

      const resolved = await resolveInternalByAnyId("", shortId);
      if (!resolved?.internalId) {
        return res.json({
          ok: true,
          kind: null,
          item_id: shortId,
          listing_id: null,
          reviews: [],
          count: 0,
        });
      }

      const kind = resolved.kind;
      const internalId = resolved.internalId;

      const rows = await fetchReviewsWithUser(kind, internalId);

      return res.json({
        ok: true,
        kind,
        item_id: shortId,
        listing_id: internalId,
        reviews: rows.map(mapReviewRow),
        count: rows.length,
      });
    } catch (e) {
      console.error("[REVIEWS] by listing id read", e);
      return res.sendStatus(500);
    }
  }

  async function upsertReviewByListingId(req, res) {
    try {
      const raw = String(req.params.id || "").trim();

      // ✅ prefixed
      const pref = parsePrefixedId(raw);
      let kind = null;
      let internalId = null;

      if (pref) {
        kind = pref.kind;
        internalId = pref.internalId;
      } else {
        // ✅ numeric
        const shortId = Number(raw);
        if (!shortId) return res.status(400).json({ error: "Invalid id" });

        const resolved = await resolveInternalByAnyId("", shortId);
        if (!resolved?.internalId)
          return res.status(404).json({ error: "Listing not found" });

        kind = resolved.kind;
        internalId = resolved.internalId;
      }

      const stars = clampStars(
        req.body?.stars ??
          req.body?.rating ??
          req.body?.value ??
          req.body?.starsValue ??
          req.body?.ratingValue ??
          req.body?.stars?.value ??
          req.body?.rating?.value
      );
      if (!stars)
        return res
          .status(400)
          .json({ error: "Invalid stars", received: req.body });

      const commentRaw =
        req.body?.comment ??
        req.body?.commentText ??
        req.body?.comment_text ??
        req.body?.reviewText ??
        req.body?.text ??
        req.body?.body ??
        req.body?.review ??
        req.body?.content ??
        req.body?.message ??
        req.body?.description ??
        req.body?.note ??
        req.body?.notes;

      const commentStr =
        typeof commentRaw === "string"
          ? commentRaw
          : commentRaw == null
          ? ""
          : JSON.stringify(commentRaw);

      const comment = safeTrim(commentStr) || null;

      const existing = await get(
        db,
        `SELECT id FROM marketplace_reviews
         WHERE listing_type=? AND listing_id=? AND user_id=?
         LIMIT 1`,
        [kind, internalId, req.user.id]
      );

      if (existing?.id) {
        await run(
          db,
          `UPDATE marketplace_reviews
           SET stars=?, comment=?, created_at=datetime('now')
           WHERE id=?`,
          [stars, comment, existing.id]
        );
      } else {
        await run(
          db,
          `INSERT INTO marketplace_reviews (listing_type, listing_id, user_id, stars, comment)
           VALUES (?, ?, ?, ?, ?)`,
          [kind, internalId, req.user.id, stars, comment]
        );
      }

      const rows = await fetchReviewsWithUser(kind, internalId);

      return res.json({
        ok: true,
        kind,
        item_id: raw,
        listing_id: internalId,
        reviews: rows.map(mapReviewRow),
        count: rows.length,
      });
    } catch (e) {
      console.error("[REVIEWS] by listing id write", e);
      return res.status(500).json({ error: "Failed to save review" });
    }
  }

  async function deleteMyReviewByListingId(req, res) {
    try {
      const raw = String(req.params.id || "").trim();

      // ✅ prefixed
      const pref = parsePrefixedId(raw);
      let kind = null;
      let internalId = null;

      if (pref) {
        kind = pref.kind;
        internalId = pref.internalId;
      } else {
        // ✅ numeric
        const shortId = Number(raw);
        if (!shortId) return res.status(400).json({ error: "Invalid id" });

        const resolved = await resolveInternalByAnyId("", shortId);
        if (!resolved?.internalId)
          return res.status(404).json({ error: "Listing not found" });

        kind = resolved.kind;
        internalId = resolved.internalId;
      }

      await run(
        db,
        `DELETE FROM marketplace_reviews
         WHERE listing_type=? AND listing_id=? AND user_id=?`,
        [kind, internalId, req.user.id]
      );

      const rows = await fetchReviewsWithUser(kind, internalId);

      return res.json({
        ok: true,
        kind,
        item_id: raw,
        listing_id: internalId,
        reviews: rows.map(mapReviewRow),
        count: rows.length,
      });
    } catch (e) {
      console.error("[REVIEWS] by listing id delete", e);
      return res.status(500).json({ error: "Failed to delete review" });
    }
  }

  app.get("/api/listings/:id/reviews", authOptional, handleReviewsByListingId);
  app.get(
    "/api/marketplace/listings/:id/reviews",
    authOptional,
    handleReviewsByListingId
  );

  app.post("/api/listings/:id/reviews", authRequired, upsertReviewByListingId);
  app.post(
    "/api/marketplace/listings/:id/reviews",
    authRequired,
    upsertReviewByListingId
  );

  app.delete(
    "/api/listings/:id/reviews/me",
    authRequired,
    deleteMyReviewByListingId
  );
  app.delete(
    "/api/marketplace/listings/:id/reviews/me",
    authRequired,
    deleteMyReviewByListingId
  );

  app.delete(
    "/api/community/:kind/:id/reviews/me",
    authRequired,
    deleteMyReview
  );
  app.delete(
    "/api/marketplace/:kind/:id/reviews/me",
    authRequired,
    deleteMyReview
  );

  app.post(
    "/api/community/:kind/:id/reviews",
    authRequired,
    createOrUpdateMyReview
  );
  app.post(
    "/api/marketplace/:kind/:id/reviews",
    authRequired,
    createOrUpdateMyReview
  );

  // =====================
  // ROUTES (order matters)
  // =====================

  // ✅ create (query/body style)
  app.post("/api/listings", authRequired, createListingCore);
  app.post("/api/marketplace/listings", authRequired, createListingCore);

  // ✅ list (query style)
  app.get("/api/listings", authOptional, async (req, res) => {
    try {
      const type = normType(req.query.type);
      if (!type) return res.status(400).json({ error: "Missing type" });
      const items = await loadMarketplaceByType(req, type);
      return sendList(res, items);
    } catch (e) {
      console.error("GET /api/listings error:", e);
      return res.status(500).json({ error: "Failed to load listings" });
    }
  });

  app.get("/api/marketplace/listings", authOptional, async (req, res) => {
    try {
      const type = normType(req.query.type);
      if (!type) return res.status(400).json({ error: "Missing type" });
      const items = await loadMarketplaceByType(req, type);
      return sendList(res, items);
    } catch (e) {
      console.error("GET /api/marketplace/listings error:", e);
      return res.status(500).json({ error: "Failed to load listings" });
    }
  });

  // ✅ create (path style): /api/listings/jobs
  app.post("/api/listings/:type", authRequired, typeGuard, (req, res) => {
    req.body = { ...(req.body || {}), type: req.params.type };
    return createListingCore(req, res);
  });

  // ✅ create (path style - marketplace): /api/marketplace/jobs
  app.post("/api/marketplace/:type", authRequired, typeGuard, (req, res) => {
    req.body = { ...(req.body || {}), type: req.params.type };
    return createListingCore(req, res);
  });

  // ✅ list (path style): /api/listings/jobs
  app.get("/api/listings/:type", authOptional, typeGuard, async (req, res) => {
    try {
      const items = await loadMarketplaceByType(req, req.params.type);
      return sendList(res, items);
    } catch (e) {
      return res.status(500).json({ error: "Failed to load listings" });
    }
  });
  // ✅ marketplace all (includes jobs/housing too)
  app.get("/api/marketplace/all", authOptional, async (req, res) => {
    try {
      const isAdmin = isAdminReq(req);

      const where = ["m.is_active = 1"];
      const params = [];

      if (!isAdmin) where.push(`COALESCE(m.status,'approved')='approved'`);

      const q = String(req.query.q || "").trim();
      const state = String(req.query.state || "").trim();
      const city = String(req.query.city || "").trim();
      const category = String(req.query.category || "").trim();

      // optional type filter:
      // /api/marketplace/all?type=jobs
      // /api/marketplace/all?type=jobs,products
      const typeRaw = String(req.query.type || "").trim();
      const typeList = typeRaw
        ? typeRaw
            .split(",")
            .map((x) => normType(x))
            .filter((x) => ALLOWED_TYPES.has(x))
        : [];

      if (typeList.length === 1) {
        where.push("m.type = ?");
        params.push(typeList[0]);
      } else if (typeList.length > 1) {
        where.push(`m.type IN (${typeList.map(() => "?").join(",")})`);
        params.push(...typeList);
      }

      if (q) {
        where.push(`(
        m.title LIKE ? OR
        m.description LIKE ? OR
        m.notes LIKE ? OR
        m.address LIKE ? OR
        m.category LIKE ? OR
        m.city LIKE ? OR
        m.state LIKE ? OR
        m.platform LIKE ? OR
        m.topic LIKE ?
      )`);
        params.push(
          `%${q}%`,
          `%${q}%`,
          `%${q}%`,
          `%${q}%`,
          `%${q}%`,
          `%${q}%`,
          `%${q}%`,
          `%${q}%`,
          `%${q}%`
        );
      }

      if (state) {
        where.push("m.state = ?");
        params.push(state);
      }
      if (city) {
        where.push("LOWER(m.city)=LOWER(?)");
        params.push(city);
      }
      if (category) {
        where.push("m.category = ?");
        params.push(category);
      }

      const sql = `
      SELECT
        m.*,
        COALESCE(AVG(r.stars), 0) AS avg_rating,
        COUNT(r.id) AS reviews_count
      FROM marketplace_listings m
      LEFT JOIN marketplace_reviews r
        ON r.listing_type = m.type AND r.listing_id = m.id
      WHERE ${where.join(" AND ")}
      GROUP BY m.id
      ORDER BY m.created_at DESC, m.id DESC
      LIMIT 500
    `;

      const rows = await all(db, sql, params);
      return sendList(res, rows.map(mapListingRow));
    } catch (e) {
      console.error("GET /api/marketplace/all error:", e);
      return res.status(500).json({ error: "Failed to load marketplace all" });
    }
  });

  // ✅ list (path style - marketplace): /api/marketplace/jobs
  app.get(
    "/api/marketplace/:type",
    authOptional,
    typeGuard,
    async (req, res) => {
      try {
        const items = await loadMarketplaceByType(req, req.params.type);
        return sendList(res, items);
      } catch (e) {
        return res.status(500).json({ error: "Failed to load listings" });
      }
    }
  );

  // ✅ update/delete (legacy style): /api/listings/jobs/5  -> maps to jobs_5
  app.patch("/api/listings/:type/:id", authRequired, typeGuard, (req, res) => {
    const onlyNum = String(req.params.id || "").match(/(\d+)/);
    if (!onlyNum) return res.status(400).json({ error: "Bad id" });

    const prefix = typeToPrefix[req.params.type];
    req.params.id = `${prefix}_${onlyNum[1]}`;
    return updateListingCore(req, res);
  });

  app.delete("/api/listings/:type/:id", authRequired, typeGuard, (req, res) => {
    const onlyNum = String(req.params.id || "").match(/(\d+)/);
    if (!onlyNum) return res.status(400).json({ error: "Bad id" });

    const prefix = typeToPrefix[req.params.type];
    req.params.id = `${prefix}_${onlyNum[1]}`;
    return deleteListingCore(req, res);
  });

  // ✅ marketplace legacy update/delete: /api/marketplace/products/7
  app.patch(
    "/api/marketplace/:type/:id",
    authRequired,
    typeGuard,
    (req, res) => {
      const onlyNum = String(req.params.id || "").match(/(\d+)/);
      if (!onlyNum) return res.status(400).json({ error: "Bad id" });

      const prefix = typeToPrefix[req.params.type];
      req.params.id = `${prefix}_${onlyNum[1]}`;
      return updateListingCore(req, res);
    }
  );

  app.delete(
    "/api/marketplace/:type/:id",
    authRequired,
    typeGuard,
    (req, res) => {
      const onlyNum = String(req.params.id || "").match(/(\d+)/);
      if (!onlyNum) return res.status(400).json({ error: "Bad id" });

      const prefix = typeToPrefix[req.params.type];
      req.params.id = `${prefix}_${onlyNum[1]}`;
      return deleteListingCore(req, res);
    }
  );

  // ✅ numeric id resolver (moved away from /api/listings/:id to avoid conflict)
  app.get("/api/listings/id/:id", authOptional, resolveNumericId);
  app.get("/api/marketplace/listings/id/:id", authOptional, resolveNumericId);
  // ✅ short aliases (frontend expects these)

  // ✅ prefixed id update/delete
  // IMPORTANT: keep AFTER /:type/:id routes, to avoid swallowing them
  app.patch("/api/listings/:id", authRequired, updateListingCore);
  app.delete("/api/listings/:id", authRequired, deleteListingCore);

  app.patch("/api/marketplace/listings/:id", authRequired, updateListingCore);
  app.delete("/api/marketplace/listings/:id", authRequired, deleteListingCore);
  // ✅ details by numeric id (always returns rating)
  // ✅ details by numeric id OR public_id (always returns rating)
  app.get("/api/marketplace/item/:id", authOptional, async (req, res) => {
    try {
      const shortId = toInt(req.params.id);
      if (!shortId) return res.status(400).json({ error: "Bad id" });

      // ✅ resolve internal id using (public_id OR id)
      const resolved = await resolveInternalByAnyId("", shortId);
      if (!resolved?.internalId) {
        return res.status(404).json({ error: "Not found" });
      }

      const internalId = resolved.internalId;

      const row = await get(
        db,
        `
        SELECT
          m.*,
          COALESCE(AVG(r.stars), 0) AS avg_rating,
          COUNT(r.id) AS reviews_count
        FROM marketplace_listings m
        LEFT JOIN marketplace_reviews r
          ON r.listing_type = m.type AND r.listing_id = m.id
        WHERE m.id = ?
        GROUP BY m.id
        LIMIT 1
        `,
        [internalId]
      );

      if (!row) return res.status(404).json({ error: "Not found" });
      return res.json({ ok: true, item: mapListingRow(row) });
    } catch (e) {
      console.error("GET /api/marketplace/item/:id error:", e);
      return res.status(500).json({ error: "Failed" });
    }
  });
};
