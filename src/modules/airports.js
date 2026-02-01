// backend/src/modules/airports.js
/* =====================
   AIRPORTS MODULE
   - SQLite table + indexes
   - Search endpoint for autocomplete
===================== */

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

  // ✅ 1) Ensure schema
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

  // ✅ run once on boot
  ensureAirportsSchema().catch((e) =>
    console.error("[AIRPORTS] schema init error", e)
  );

  // ✅ 2) Search endpoint
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
        SELECT
          iata, icao, name, city, country, country_code, lat, lon
        FROM airports
        WHERE
          LOWER(COALESCE(iata,'')) LIKE ? OR
          LOWER(COALESCE(icao,'')) LIKE ? OR
          LOWER(COALESCE(city,'')) LIKE ? OR
          LOWER(COALESCE(name,'')) LIKE ? OR
          LOWER(COALESCE(country,'')) LIKE ?
        ORDER BY
          CASE
            WHEN LOWER(COALESCE(iata,'')) = ? THEN 0
            WHEN LOWER(COALESCE(iata,'')) LIKE ? THEN 1
            WHEN LOWER(COALESCE(city,'')) LIKE ? THEN 2
            WHEN LOWER(COALESCE(name,'')) LIKE ? THEN 3
            ELSE 4
          END,
          LENGTH(COALESCE(iata,'')) ASC,
          LENGTH(COALESCE(city,'')) ASC
        LIMIT ?
        `,
        [like, like, like, like, like, qLower, starts, starts, starts, limit]
      );

      res.json(rows);
    } catch (e) {
      console.error("[AIRPORTS] search error", e);
      res.status(500).json({ error: "search_failed" });
    }
  });

  // ✅ 3) optional health
  app.get("/api/airports/health", async (_req, res) => {
    try {
      const r = await all(`SELECT COUNT(*) AS count FROM airports`);
      res.json({ ok: true, count: r?.[0]?.count ?? 0 });
    } catch (e) {
      res.status(500).json({ ok: false });
    }
  });
};
