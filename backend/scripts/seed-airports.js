// backend/scripts/seed-airports.js
const path = require("path");
const fs = require("fs");
const sqlite3 = require("sqlite3").verbose();

const dbPath = path.resolve(__dirname, "..", "database.sqlite");

// ✅ عدّل المسار ده حسب مكان ملف airports.min.json عندك
// لو الملف عندك في frontend: src/data/airports.min.json
// يبقى المسار ده صحيح لأن backend/scripts يطلع لفوق ثم يروح للفولدر ده
const airportsJsonPath = path.resolve(
  __dirname,
  "..",
  "..",
  "src",
  "data",
  "airports.min.json"
);

function openDb() {
  return new sqlite3.Database(dbPath);
}

function run(db, sql, params = []) {
  return new Promise((resolve, reject) => {
    db.run(sql, params, function (err) {
      if (err) reject(err);
      else resolve(this);
    });
  });
}

function get(db, sql, params = []) {
  return new Promise((resolve, reject) => {
    db.get(sql, params, (err, row) => (err ? reject(err) : resolve(row)));
  });
}

async function main() {
  const db = openDb();

  try {
    // ✅ تأكد الجدول موجود (نفس schema بتاع module)
    await run(
      db,
      `
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
    `
    );
    await run(
      db,
      `CREATE INDEX IF NOT EXISTS idx_airports_iata ON airports(iata);`
    );
    await run(
      db,
      `CREATE INDEX IF NOT EXISTS idx_airports_icao ON airports(icao);`
    );
    await run(
      db,
      `CREATE INDEX IF NOT EXISTS idx_airports_city ON airports(city);`
    );
    await run(
      db,
      `CREATE INDEX IF NOT EXISTS idx_airports_name ON airports(name);`
    );
    await run(
      db,
      `CREATE INDEX IF NOT EXISTS idx_airports_country ON airports(country);`
    );

    const before = await get(db, `SELECT COUNT(*) AS c FROM airports`);
    if ((before?.c || 0) > 0) {
      console.log(`✅ airports already seeded. count=${before.c}`);
      db.close();
      return;
    }

    if (!fs.existsSync(airportsJsonPath)) {
      throw new Error(
        `airports.min.json not found at:\n${airportsJsonPath}\n\nFix airportsJsonPath in seed-airports.js`
      );
    }

    const raw = fs.readFileSync(airportsJsonPath, "utf8");
    const arr = JSON.parse(raw);
    if (!Array.isArray(arr))
      throw new Error("airports.min.json must be an array");

    console.log(`📦 Loading airports: ${arr.length}`);

    await run(db, "BEGIN TRANSACTION");

    const stmt = db.prepare(`
      INSERT INTO airports (iata, icao, name, city, country, country_code, lat, lon)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `);

    let inserted = 0;
    for (const x of arr) {
      const iata = (x.iata || x.code || "").trim() || null;
      const icao = (x.icao || "").trim() || null;
      const name = (x.name || "").trim() || null;
      const city = (x.city || "").trim() || null;
      const country = (x.country || "").trim() || null;
      const country_code =
        (x.country_code || x.countryCode || "").trim() || null;

      const lat = x.lat == null ? null : Number(x.lat);
      const lon = x.lon == null ? null : Number(x.lon);

      await new Promise((resolve, reject) => {
        stmt.run(
          [
            iata,
            icao,
            name,
            city,
            country,
            country_code,
            Number.isFinite(lat) ? lat : null,
            Number.isFinite(lon) ? lon : null,
          ],
          (err) => (err ? reject(err) : resolve())
        );
      });

      inserted++;
      if (inserted % 5000 === 0) console.log(`… inserted ${inserted}`);
    }

    stmt.finalize();
    await run(db, "COMMIT");

    const after = await get(db, `SELECT COUNT(*) AS c FROM airports`);
    console.log(`✅ Done. airports count=${after?.c || 0}`);
  } catch (e) {
    try {
      await run(db, "ROLLBACK");
    } catch {}
    console.error("❌ Seed failed:", e.message || e);
    process.exitCode = 1;
  } finally {
    db.close();
  }
}

main();
