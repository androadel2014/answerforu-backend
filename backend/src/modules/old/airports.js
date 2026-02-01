// backend/src/modules/airports.js
/* =====================
   AIRPORTS MODULE
   - SQLite table + indexes
   - Search endpoint for autocomplete
   - Seed endpoint from JSON file(s)
===================== */

const fs = require("fs");
const path = require("path");

module.exports = function registerAirports(opts) {
  const { app, db } = opts;

  const run = (sql, params = []) =>
    new Promise((resolve, reject) => {
      db.run(sql, params, function (err) {
        if (err) reject(err);
        else resolve(this);
      });
    });

  const all = (sql, params = []) =>
    new Promise((resolve, reject) => {
      db.all(sql, params, (err, rows) => (err ? reject(err) : resolve(rows)));
    });

  async function ensureAirportsSchema() {
    await run(`
      CREATE TABLE IF NOT EXISTS airports (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        iata TEXT,
        icao TEXT,
        name TEXT,
        city TEXT,
        country TEXT,
        country_code TEXT,
        lat REAL,
        lon REAL
      );
    `);

    await run(
      `CREATE INDEX IF NOT EXISTS idx_airports_iata ON airports(iata);`
    );
    await run(
      `CREATE INDEX IF NOT EXISTS idx_airports_icao ON airports(icao);`
    );
    await run(
      `CREATE INDEX IF NOT EXISTS idx_airports_city ON airports(city);`
    );
    await run(
      `CREATE INDEX IF NOT EXISTS idx_airports_name ON airports(name);`
    );
    await run(
      `CREATE INDEX IF NOT EXISTS idx_airports_country ON airports(country);`
    );
  }

  // run once on boot
  ensureAirportsSchema().catch((e) =>
    console.error("[AIRPORTS] schema init error", e)
  );

  function safeReadJsonArray(filePath) {
    if (!fs.existsSync(filePath)) {
      return { ok: false, error: "file_not_found", filePath };
    }

    const raw = fs.readFileSync(filePath, "utf8");
    const head = raw.slice(0, 200);

    let data;
    try {
      data = JSON.parse(raw);
    } catch (_e) {
      return { ok: false, error: "invalid_json", filePath, rawHead: head };
    }

    // ✅ Accept BOTH:
    // - Array: [ {...}, {...} ]
    // - Object map: { "00AK": {...}, "00AL": {...} }
    let arr = null;

    if (Array.isArray(data)) {
      arr = data;
    } else if (data && typeof data === "object") {
      arr = Object.values(data);
    } else {
      return {
        ok: false,
        error: "invalid_json_shape",
        filePath,
        type: typeof data,
        rawHead: head,
      };
    }

    if (!arr || arr.length === 0) {
      return { ok: false, error: "empty_array", filePath, rawHead: head };
    }

    return { ok: true, filePath, data: arr, count: arr.length };
  }

  function normalizeAirport(x) {
    // supports different shapes
    const iata =
      String(x.iata || x.IATA || x.code || x.Code || "").trim() || null;
    const icao = String(x.icao || x.ICAO || "").trim() || null;
    const name = String(x.name || x.Name || "").trim() || null;
    const city = String(x.city || x.City || "").trim() || null;
    const country =
      String(x.country_name || x.Country || x.country || "").trim() || null;

    const country_code =
      String(
        x.country_code || x.countryCode || x.CountryCode || x.country || ""
      ).trim() || null;

    const latRaw = x.lat ?? x.Latitude ?? x.latitude ?? null;
    const lonRaw = x.lon ?? x.Longitude ?? x.longitude ?? null;

    const lat = latRaw == null ? null : Number(latRaw);
    const lon = lonRaw == null ? null : Number(lonRaw);

    return {
      iata,
      icao,
      name,
      city,
      country,
      country_code,
      lat: Number.isFinite(lat) ? lat : null,
      lon: Number.isFinite(lon) ? lon : null,
    };
  }

  // 1) Search
  app.get("/api/airports/search", async (req, res) => {
    try {
      const q = String(req.query.q || "").trim();
      const limit = Math.min(
        50,
        Math.max(5, parseInt(req.query.limit || "20", 10))
      );

      if (q.length < 2) return res.json([]);

      const qLower = q.toLowerCase();
      const like = `%${qLower}%`;
      const starts = `${qLower}%`;

      const rows = await all(
        `
        SELECT iata, icao, name, city, country, country_code, lat, lon
        FROM airports
        WHERE
          (
            LOWER(COALESCE(iata,'')) LIKE ? OR
            LOWER(COALESCE(icao,'')) LIKE ? OR
            LOWER(COALESCE(city,'')) LIKE ? OR
            LOWER(COALESCE(name,'')) LIKE ? OR
            LOWER(COALESCE(country,'')) LIKE ?
          )
          AND COALESCE(name,'') <> ''
          AND (COALESCE(iata,'') <> '' OR COALESCE(icao,'') <> '')
        ORDER BY
          CASE
            WHEN LOWER(COALESCE(iata,'')) = ? THEN 0
            WHEN LOWER(COALESCE(iata,'')) LIKE ? THEN 1
            WHEN LOWER(COALESCE(icao,'')) LIKE ? THEN 2
            WHEN LOWER(COALESCE(city,'')) LIKE ? THEN 3
            WHEN LOWER(COALESCE(name,'')) LIKE ? THEN 4
            ELSE 5
          END,
          LENGTH(COALESCE(iata,'')) ASC,
          LENGTH(COALESCE(icao,'')) ASC,
          LENGTH(COALESCE(city,'')) ASC
        LIMIT ?
        `,
        [
          like,
          like,
          like,
          like,
          like,
          qLower,
          starts,
          starts,
          starts,
          starts,
          limit,
        ]
      );

      res.json(rows);
    } catch (e) {
      console.error("[AIRPORTS] search error", e);
      res.status(500).json({ error: "search_failed" });
    }
  });

  // 2) Health
  app.get("/api/airports/health", async (_req, res) => {
    try {
      const r = await all(`SELECT COUNT(*) AS count FROM airports`);
      res.json({ ok: true, count: r?.[0]?.count ?? 0 });
    } catch (_e) {
      res.status(500).json({ ok: false });
    }
  });

  // 3) Seed
  // GET /api/airports/seed         -> seeds only if table empty
  // GET /api/airports/seed?force=1 -> deletes + reseeds
  // Optional: ?file=ABS_OR_REL_PATH  -> overrides json path
  app.get("/api/airports/seed", async (req, res) => {
    try {
      if (process.env.NODE_ENV === "production") {
        return res
          .status(403)
          .json({ ok: false, error: "disabled_in_production" });
      }

      await ensureAirportsSchema();

      const force = String(req.query.force || "") === "1";

      const before = await all(`SELECT COUNT(*) AS count FROM airports`);
      const beforeCount = Number(before?.[0]?.count || 0);

      if (beforeCount > 0 && !force) {
        return res.json({
          ok: true,
          skipped: true,
          count: beforeCount,
          note: "Already seeded. Use ?force=1 to reseed.",
        });
      }

      // ✅ Source priority:
      // 1) query ?file=
      // 2) env AIRPORTS_JSON
      // 3) backend/src/data/airports.json
      // 4) projectRoot/src/data/airports.min.json (your old path)
      const override = String(req.query.file || "").trim();
      const envPath = String(process.env.AIRPORTS_JSON || "").trim();

      const backendDataPath = path.resolve(
        __dirname,
        "..",
        "data",
        "airports.json"
      );
      const projectRootDataPath = path.resolve(
        __dirname,
        "..",
        "..",
        "..",
        "src",
        "data",
        "airports.min.json"
      );

      const candidates = [];
      if (override) {
        candidates.push(
          path.isAbsolute(override)
            ? override
            : path.resolve(process.cwd(), override)
        );
      }
      if (envPath) {
        candidates.push(
          path.isAbsolute(envPath)
            ? envPath
            : path.resolve(process.cwd(), envPath)
        );
      }
      candidates.push(backendDataPath);
      candidates.push(projectRootDataPath);

      let picked = null;
      let pickResult = null;

      for (const p of candidates) {
        const r = safeReadJsonArray(p);
        if (r.ok) {
          picked = p;
          pickResult = r;
          break;
        }
        // keep last error for debug
        pickResult = r;
      }

      if (!picked || !pickResult?.ok) {
        return res.status(500).json({
          ok: false,
          error:
            pickResult?.error === "empty_array"
              ? "airports_json_empty"
              : "airports_json_invalid_or_missing",
          tried: candidates,
          last: pickResult || null,
          note: "حط ملف داتا حقيقي. أسهل حل: حط airports.json داخل backend/src/data/airports.json",
        });
      }

      const arr = pickResult.data;

      if (force) {
        await run(`DELETE FROM airports`);
      }

      await run("BEGIN TRANSACTION");

      const stmt = db.prepare(`
        INSERT INTO airports (iata, icao, name, city, country, country_code, lat, lon)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      `);

      let inserted = 0;

      try {
        for (const x of arr) {
          const a = normalizeAirport(x);

          // skip totally empty records
          if (!a.iata && !a.icao && !a.name && !a.city && !a.country) continue;

          await new Promise((resolve, reject) => {
            stmt.run(
              [
                a.iata,
                a.icao,
                a.name,
                a.city,
                a.country,
                a.country_code,
                a.lat,
                a.lon,
              ],
              (err) => (err ? reject(err) : resolve())
            );
          });

          inserted++;
        }

        await new Promise((resolve, reject) =>
          stmt.finalize((err) => (err ? reject(err) : resolve()))
        );

        await run("COMMIT");
      } catch (e) {
        try {
          await run("ROLLBACK");
        } catch (_e2) {}
        throw e;
      }

      const after = await all(`SELECT COUNT(*) AS count FROM airports`);
      const afterCount = Number(after?.[0]?.count || 0);

      return res.json({
        ok: true,
        inserted,
        count: afterCount,
        jsonPath: picked,
      });
    } catch (e) {
      console.error("[AIRPORTS] seed error", e);
      res.status(500).json({ ok: false, error: "seed_failed" });
    }
  });
};
