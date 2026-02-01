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
  app.post("/api/auth/register", (req, res) => {
    const { username, email, password } = req.body || {};
    if (!username || !email || !password) {
      return res.status(400).json({ message: "Missing fields" });
    }

    const hash = bcrypt.hashSync(password, 10);

    dbRun(
      `INSERT INTO users (username, email, password_hash) VALUES (?, ?, ?)`,
      [safeTrim(username), safeTrim(email).toLowerCase(), hash],
      function (err) {
        if (err) {
          const msg = String(err.message || "").toLowerCase();

          if (msg.includes("unique") || msg.includes("constraint")) {
            return res.status(400).json({ message: "Email already exists" });
          }

          return res.status(500).json({ message: "Register failed" });
        }

        const user = {
          id: this.lastID,
          username: safeTrim(username),
          email: safeTrim(email).toLowerCase(),
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

  app.post("/api/auth/login", (req, res) => {
    const { email, password } = req.body || {};
    if (!email || !password)
      return res.status(400).json({ message: "Missing fields" });

    dbGet(
      `SELECT * FROM users WHERE email = ?`,
      [safeTrim(email).toLowerCase()],
      (err, user) => {
        if (err) return res.status(500).json({ message: "Login failed" });
        if (!user) return res.sendStatus(401);

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
