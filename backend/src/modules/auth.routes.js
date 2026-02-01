// src/modules/auth.routes.js
module.exports = function registerAuthRoutes({
  app,
  bcrypt,
  dbRun,
  dbGet,
  safeTrim,
  signToken,
  ensureProfileRow,
}) {
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
      if (err) {
        return res.status(500).json({ message: "Register failed" });
      }

      if (row) {
        return res.status(409).json({ message: "Email already exists" });
      }

      // ✅ INSERT ONLY IF NOT EXISTS
      dbRun(
        `INSERT INTO users (username, email, password_hash) VALUES (?, ?, ?)`,
        [cleanUsername, cleanEmail, hash],
        function (err2) {
          if (err2) {
            return res.status(500).json({ message: "Register failed" });
          }

          const user = {
            id: this.lastID,
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
              username: user.username,
              email: user.email,
            }),
            user,
          });
        }
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
          username: user.username,
          email: user.email,
          phone: user.phone || "",
          address: user.address || "",
          bio: user.bio || "",
        };

        ensureProfileRow(user.id, () => {});

        return res.json({
          token: signToken({
            id: user.id,
            username: user.username,
            email: user.email,
          }),
          user: me,
        });
      }
    );
  });
};
