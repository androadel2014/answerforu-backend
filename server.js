// server.js (FULL FILE - UPDATED after refactor)

// =====================
// Imports
// =====================
const express = require("express");
const sqlite3 = require("sqlite3").verbose();
const path = require("path");
const bcrypt = require("bcryptjs");
const jwt = require("jsonwebtoken");
require("dotenv").config();
const registerAirports = require("./src/modules/airports");

// =====================
// Modules
// =====================
const registerCore = require("./src/modules/core");
const registerCarry = require("./src/modules/carry");

const registerSocialProfileRoutes = require("./src/modules/social-profile.routes");
const registerAuthRoutes = require("./src/modules/auth.routes");
const registerUsersMeRoutes = require("./src/modules/profile.routes");
const registerCvRoutes = require("./src/modules/cv.routes");
const registerCommunityRoutes = require("./src/modules/community.routes");

// ✅ actual files in your tree are in /src (not /src/modules)
const registerFeed = require("./src/feed");
const registerMarketplace = require("./src/marketplace");

// ✅ needed because you pass it to social-profile
const { parseAnyPostId } = require("./src/feed");

// =====================
// App
// =====================
const app = express();
const PORT = process.env.PORT || 5000;
const JWT_SECRET = process.env.JWT_SECRET || "CHANGE_ME_SECRET";
const ADMIN_EMAILS = process.env.ADMIN_EMAILS || "";

// ✅ خليه true مؤقتًا لو عايز تشوف تحذيرات ALTER / أعمدة ناقصة + SQL errors
const ALTER_LOG = false;
const SQL_LOG = true;

/* =====================
   CORS
===================== */
function isAllowedOrigin(origin) {
  if (!origin) return true;

  const allow = ["https://answerforu.com", "https://www.answerforu.com"];

  // لو عندك دومين فرونت تاني مؤقت/ستيجينج ضيفه هنا
  // allow.push("https://staging.answerforu.com");

  return (
    allow.includes(origin) ||
    /^http:\/\/localhost:\d+$/.test(origin) ||
    /^http:\/\/127\.0\.0\.1:\d+$/.test(origin)
  );
}

app.use((req, res, next) => {
  const origin = req.headers.origin;

  if (isAllowedOrigin(origin)) {
    if (origin) res.setHeader("Access-Control-Allow-Origin", origin);
    res.setHeader("Vary", "Origin");

    res.setHeader(
      "Access-Control-Allow-Headers",
      "Content-Type, Authorization, Accept"
    );
    res.setHeader(
      "Access-Control-Allow-Methods",
      "GET,POST,PUT,PATCH,DELETE,OPTIONS"
    );
  }

  if (req.method === "OPTIONS") return res.sendStatus(204);
  next();
});

/* =====================
   Body Parsers
===================== */
app.use(express.json({ limit: "50mb" }));
app.use(express.urlencoded({ extended: true }));

/* =====================
   DB
===================== */
const dbPath = path.resolve(__dirname, "database.sqlite");
const db = new sqlite3.Database(dbPath);
registerAirports({ app, db });

db.serialize(() => {
  db.run("PRAGMA foreign_keys = ON");
  db.run("PRAGMA journal_mode = WAL");
  db.run("PRAGMA synchronous = NORMAL");
  db.run("PRAGMA busy_timeout = 5000");
});

function safeAlterTable(sql) {
  db.run(sql, (err) => {
    if (!err) return;

    const msg = String(err.message || "");
    const ignorable =
      msg.includes("duplicate column") ||
      msg.includes("already exists") ||
      msg.includes("no such table") ||
      msg.includes("cannot add a NOT NULL column") ||
      false;

    if (!ignorable && ALTER_LOG) {
      console.warn("SAFE_ALTER_WARNING:", msg, "SQL:", sql);
    }
  });
}

// ✅ DB wrappers (logs the real SQL errors that cause 500)
function dbAll(sql, params, cb) {
  db.all(sql, params, (err, rows) => {
    if (err && SQL_LOG) {
      console.error("SQL_ERROR:", err.message);
      console.error("SQL:", sql);
      console.error("PARAMS:", params);
    }
    cb(err, rows);
  });
}
function dbGet(sql, params, cb) {
  db.get(sql, params, (err, row) => {
    if (err && SQL_LOG) {
      console.error("SQL_ERROR:", err.message);
      console.error("SQL:", sql);
      console.error("PARAMS:", params);
    }
    cb(err, row);
  });
}
function dbRun(sql, params, cb) {
  db.run(sql, params, function (err) {
    if (err && SQL_LOG) {
      console.error("SQL_ERROR:", err.message);
      console.error("SQL:", sql);
      console.error("PARAMS:", params);
    }
    cb && cb.call(this, err);
  });
}

/* =====================
   Helpers
===================== */
function toInt(v) {
  const n = parseInt(String(v ?? "").trim(), 10);
  return Number.isFinite(n) ? n : 0;
}

/* =====================
   Core (auth + helpers)
===================== */
const core = registerCore({ JWT_SECRET, jwt, ADMIN_EMAILS });

const {
  safeTrim,
  safeUrl,
  safeJsonParse,
  signToken,
  authRequired,
  authOptional,
  isAdminReq,
  adminRequired,
} = core;

// ✅ object expected by src/marketplace.js
const auth = { authRequired, authOptional, isAdminReq, adminRequired };

// ✅ support both names (old/new)
const signJwt = core.signJwt || signToken;
// ✅ Carry / Shipments routes
registerCarry({
  app,
  db,
  auth: { authRequired, authOptional, isAdminReq },
  safeTrim,
  safeJsonParse,
  toInt,
});

/* =====================
   Profile bootstrap
===================== */
function ensureProfileRow(userId, cb) {
  dbGet(
    `SELECT up.user_id FROM user_profile up WHERE up.user_id = ?`,
    [userId],
    (e1, row) => {
      if (e1) return cb(e1);
      if (row) return cb(null);

      dbGet(
        `SELECT id, username FROM users WHERE id = ?`,
        [userId],
        (e2, u) => {
          if (e2) return cb(e2);

          const baseUsername = safeTrim(u?.username) || `user${userId}`;
          const displayName = safeTrim(u?.username) || `User ${userId}`;

          const candidates = [
            baseUsername,
            `${baseUsername}${userId}`,
            `user${userId}`,
            `member${userId}`,
          ];

          (function tryInsert(i) {
            if (i >= candidates.length) {
              const last = `user${userId}_${Date.now()}`;
              return dbRun(
                `INSERT OR IGNORE INTO user_profile (user_id, username, display_name) VALUES (?,?,?)`,
                [userId, last, displayName],
                () => cb(null)
              );
            }

            dbRun(
              `INSERT OR IGNORE INTO user_profile (user_id, username, display_name) VALUES (?,?,?)`,
              [userId, candidates[i], displayName],
              function () {
                dbGet(
                  `SELECT user_id FROM user_profile WHERE user_id = ?`,
                  [userId],
                  (e3, okRow) => {
                    if (e3) return cb(e3);
                    if (okRow) return cb(null);
                    tryInsert(i + 1);
                  }
                );
              }
            );
          })(0);
        }
      );
    }
  );
}

/* ✅ delete feed-post (posts table) safely */
function deleteFeedPostOwnedBy(userId, postId, res) {
  const pid = toInt(postId);
  if (!pid) return res.status(400).json({ message: "Bad postId" });

  dbGet(`SELECT id, user_id FROM posts WHERE id = ?`, [pid], (err, row) => {
    if (err) return res.status(500).json({ message: "Delete failed" });
    if (!row) return res.status(404).json({ message: "Post not found" });
    if (row.user_id !== userId) return res.sendStatus(403);

    dbRun(`DELETE FROM posts WHERE id = ?`, [pid], function (err2) {
      if (err2) return res.status(500).json({ message: "Delete failed" });
      return res.json({ ok: true });
    });
  });
}

/* =====================
   Routes registration
===================== */

// ✅ community.routes (depends on auth/core)
registerCommunityRoutes({
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
});

// ✅ social profile routes
registerSocialProfileRoutes({
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
});

// ✅ Feed (posts + comments + likes + legacy aliases)
registerFeed({
  app,
  db,
  dbAll,
  dbGet,
  dbRun,
  authRequired,
  authOptional,
  safeTrim,
  safeJsonParse,
  toInt,
  // ✅ important for delete to work
  deleteFeedPostOwnedBy,
});

// console.log("✅ registerMarketplace loaded:", typeof registerMarketplace);

// ✅ Marketplace (listings unified layer)
// ✅ Ensure marketplace_listings has needed columns (prevents SQLITE_ERROR)
safeAlterTable(`ALTER TABLE marketplace_listings ADD COLUMN category TEXT`);
safeAlterTable(`ALTER TABLE marketplace_listings ADD COLUMN city TEXT`);
safeAlterTable(`ALTER TABLE marketplace_listings ADD COLUMN state TEXT`);
safeAlterTable(`ALTER TABLE marketplace_listings ADD COLUMN address TEXT`);
safeAlterTable(`ALTER TABLE marketplace_listings ADD COLUMN price TEXT`);
safeAlterTable(`ALTER TABLE marketplace_listings ADD COLUMN contact TEXT`);
safeAlterTable(`ALTER TABLE marketplace_listings ADD COLUMN website TEXT`);
safeAlterTable(`ALTER TABLE marketplace_listings ADD COLUMN notes TEXT`);
safeAlterTable(`ALTER TABLE marketplace_listings ADD COLUMN created_at TEXT`);
safeAlterTable(`ALTER TABLE marketplace_listings ADD COLUMN updated_at TEXT`);

registerMarketplace({
  app,
  db,
  auth,
  safeTrim,
  safeJsonParse,
  toInt,
  dbAll,
  dbGet,
  dbRun,
  safeAlterTable,
});

// ✅ Auth routes
registerAuthRoutes({
  app,
  bcrypt,
  dbRun,
  dbGet,
  safeTrim,
  signToken: signJwt, // ✅ important: auth.routes.js expects "signToken"
  ensureProfileRow,
});

// ✅ /api/users/me etc
registerUsersMeRoutes({
  app,
  authRequired,
  dbGet,
  dbRun,
  safeTrim,
});

// لو عايز تفعل الـ CV routes سيبه زي ما هو/فعّله
// const normalizeCvResponse = (x) => x; // لو عندك
registerCvRoutes({
  app,
  authRequired,
  dbAll,
  dbGet,
  dbRun,
  safeTrim,
  safeJsonParse,
  toInt,
});

/* =====================
   HEALTH
===================== */
app.get("/api/health", (req, res) => res.json({ ok: true }));

/* =================================================
   START SERVER (ONLY ONCE)
==================================================== */
app.listen(PORT, () => {
  console.log(`
✅ Server is running on: http://localhost:${PORT}
📂 Database: ${dbPath}
🚀 Mode: Ready for Frontend integration
`);
});
