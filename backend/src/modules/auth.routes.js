// src/modules/auth.routes.js
const crypto = require("crypto");
const jwt = require("jsonwebtoken");

function verifyToken(req) {
  const h = req.headers.authorization || "";
  const token = h.startsWith("Bearer ") ? h.slice(7) : "";
  if (!token) return null;

  try {
    // نفس SECRET اللي signToken بيستخدمه (غالبًا JWT_SECRET)
    const secret = process.env.JWT_SECRET || process.env.SECRET || "secret";
    return jwt.verify(token, secret);
  } catch {
    return null;
  }
}

module.exports = function registerAuthRoutes({
  app,
  bcrypt,
  dbRun,
  dbGet,
  safeTrim,
  signToken,
  ensureProfileRow,
}) {
  /* =========================
     ✅ users.public_id (non-guessable) + unique index
  ========================= */
  const genPublicId = () => crypto.randomBytes(9).toString("base64url"); // ~12 chars

  // ✅ best-effort schema (no noisy errors on restart)
  dbGet(`PRAGMA table_info(users)`, [], (e, rows) => {
    const cols = Array.isArray(rows) ? rows : [];
    const hasPublicId = cols.some((c) => String(c?.name || "") === "public_id");

    if (!hasPublicId) {
      dbRun(`ALTER TABLE users ADD COLUMN public_id TEXT`, [], () => {});
    }

    dbRun(
      `CREATE UNIQUE INDEX IF NOT EXISTS idx_users_public_id ON users(public_id)`,
      [],
      () => {},
    );
  });

  try {
    dbRun(
      `CREATE UNIQUE INDEX IF NOT EXISTS idx_users_public_id ON users(public_id)`,
      [],
      () => {},
    );
  } catch {}

  function insertUserWithPublicId({ username, email, password_hash }, cb) {
    let tries = 0;

    const attempt = () => {
      tries += 1;
      const public_id = genPublicId();

      dbRun(
        `INSERT INTO users (public_id, username, email, password_hash) VALUES (?, ?, ?, ?)`,
        [public_id, username, email, password_hash],
        function (err) {
          if (!err) return cb(null, { id: this.lastID, public_id });

          // retry on rare public_id collision (unique index)
          const msg = String(err?.message || "").toLowerCase();
          const isPublicIdUnique =
            msg.includes("idx_users_public_id") ||
            (msg.includes("unique") && msg.includes("public_id"));

          if (isPublicIdUnique && tries < 6) return attempt();

          return cb(err);
        },
      );
    };

    attempt();
  }

  // =========================
  // REGISTER
  // =========================
  app.post("/api/auth/register", (req, res) => {
    const { username, email, password } = req.body || {};
    if (!username || !email || !password) {
      return res.status(400).json({ message: "Missing fields" });
    }

    const cleanEmail = safeTrim(email).toLowerCase();
    const cleanUsername = safeTrim(username);
    const hash = bcrypt.hashSync(password, 10);

    // ✅ CHECK EMAIL FIRST (prevents UNIQUE crash)
    dbGet(`SELECT id FROM users WHERE email = ?`, [cleanEmail], (err, row) => {
      if (err) return res.status(500).json({ message: "Register failed" });
      if (row) return res.status(409).json({ message: "Email already exists" });

      // ✅ CHECK USERNAME (avoid duplicates)
      dbGet(
        `SELECT id FROM users WHERE LOWER(username) = LOWER(?)`,
        [cleanUsername],
        (errU, rowU) => {
          if (errU) return res.status(500).json({ message: "Register failed" });
          if (rowU)
            return res.status(409).json({ message: "Username already exists" });

          insertUserWithPublicId(
            { username: cleanUsername, email: cleanEmail, password_hash: hash },
            (err2, created) => {
              if (err2)
                return res.status(500).json({ message: "Register failed" });

              const user = {
                id: created.id,
                public_id: created.public_id,
                username: cleanUsername,
                email: cleanEmail,
                phone: "",
                address: "",
                bio: "",
              };

              ensureProfileRow(user.id, () => {});

              return res.json({
                token: signToken({
                  id: user.id,
                  public_id: user.public_id,
                  username: user.username,
                  email: user.email,
                }),
                user,
              });
            },
          );
        },
      );
    });
  });

  // =========================
  // LOGIN
  // =========================

  app.post("/api/auth/login", (req, res) => {
    const { email, password } = req.body || {};
    if (!email || !password) {
      return res.status(400).json({ message: "Missing fields" });
    }

    dbGet(
      `SELECT * FROM users WHERE email = ?`,
      [safeTrim(email).toLowerCase()],
      (err, user) => {
        if (err) return res.status(500).json({ message: "Login failed" });
        if (!user) return res.sendStatus(401);

        const ok = bcrypt.compareSync(password, user.password_hash);
        if (!ok) return res.sendStatus(401);

        const me = {
          id: user.id,
          public_id: user.public_id || "",
          username: user.username,
          email: user.email,
          phone: user.phone || "",
          address: user.address || "",
          bio: user.bio || "",
        };

        ensureProfileRow(user.id, () => {});

        // ✅ backfill public_id for old users (retry on rare collision)
        if (!me.public_id) {
          let tries = 0;

          const attempt = () => {
            tries += 1;
            const pid = genPublicId();

            dbRun(
              `UPDATE users
       SET public_id = ?
       WHERE id = ?
         AND (public_id IS NULL OR TRIM(public_id) = '')`,
              [pid, user.id],
              (e) => {
                if (!e) {
                  me.public_id = pid;
                  return;
                }

                const msg = String(e?.message || "").toLowerCase();
                const isUnique =
                  msg.includes("idx_users_public_id") ||
                  (msg.includes("unique") && msg.includes("public_id"));

                if (isUnique && tries < 6) return attempt();
              },
            );
          };

          attempt();
        }

        return res.json({
          token: signToken({
            id: user.id,
            public_id: me.public_id,
            username: user.username,
            email: user.email,
          }),
          user: me,
        });
      },
    );
  });
  /* =========================
   ✅ ME endpoints (for frontend PostComposer)
========================= */
  function sendMe(req, res) {
    const payload = verifyToken(req);
    const uid = payload?.id ?? payload?.user_id ?? payload?.uid ?? null;
    if (!uid) return res.status(401).json({ message: "Unauthorized" });

    dbGet(
      `SELECT id, public_id, username, email, avatar_url, phone, address, bio
     FROM users
     WHERE id = ?`,
      [uid],
      (err, row) => {
        if (err) return res.status(500).json({ message: "DB error" });
        if (!row) return res.status(404).json({ message: "User not found" });
        return res.json({ user: row });
      },
    );
  }

  app.get("/api/me", sendMe);
  app.get("/api/users/me", sendMe);
  app.get("/api/profile/me", sendMe);
  app.get("/api/auth/me", sendMe);
};
