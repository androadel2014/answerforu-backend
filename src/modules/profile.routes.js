// src/modules/profile.routes.js
module.exports = function registerUsersMeRoutes({
  app,
  authRequired,
  dbGet,
  dbRun,
  safeTrim,
}) {
  app.get("/api/users/me", authRequired, (req, res) => {
    dbGet(
      `SELECT id, username, email, phone, address, bio FROM users WHERE id = ?`,
      [req.user.id],
      (err, me) => {
        if (err)
          return res.status(500).json({ message: "Failed to load user" });
        if (!me) return res.sendStatus(404);
        res.json(me);
      }
    );
  });

  app.put("/api/users/me", authRequired, (req, res) => {
    const { username, phone, address, bio } = req.body || {};

    dbRun(
      `
      UPDATE users
      SET username = ?, phone = ?, address = ?, bio = ?
      WHERE id = ?
      `,
      [
        safeTrim(username),
        safeTrim(phone),
        safeTrim(address),
        safeTrim(bio),
        req.user.id,
      ],
      function (err) {
        if (err) return res.status(500).json({ message: "Update failed" });

        dbGet(
          `SELECT id, username, email, phone, address, bio FROM users WHERE id = ?`,
          [req.user.id],
          (e2, me) => {
            if (e2) return res.status(500).json({ message: "Update failed" });
            if (!me) return res.status(500).json({ message: "Update failed" });
            return res.json(me);
          }
        );
      }
    );
  });
};
