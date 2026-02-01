// src/modules/cv.routes.js
module.exports = function registerCvRoutes({
  app,
  authRequired,
  dbAll,
  dbGet,
  dbRun,
  safeTrim,
  safeJsonParse,
  toInt,
}) {
  function normalizeCvResponse(row) {
    if (!row) return null;
    const cv_data =
      typeof row.cv_data === "string"
        ? safeJsonParse(row.cv_data) ?? row.cv_data
        : row.cv_data;

    return {
      id: row.id,
      user_id: row.user_id,
      cv_name: row.cv_name || "",
      cv_data,
      updated_at: row.updated_at || null,
    };
  }

  /* =====================
     CVS (New)
  ===================== */
  app.get("/api/cv", authRequired, (req, res) => {
    dbAll(
      `SELECT id, user_id, cv_name, updated_at FROM cvs WHERE user_id = ? ORDER BY id DESC`,
      [req.user.id],
      (err, rows) => {
        if (err) return res.status(500).json({ message: "Failed to load CVs" });
        res.json(rows || []);
      }
    );
  });

  app.get("/api/cv/:id", authRequired, (req, res) => {
    const id = toInt(req.params.id);
    if (!id) return res.status(400).json({ message: "Bad id" });

    dbGet(
      `SELECT id, user_id, cv_name, cv_data, updated_at FROM cvs WHERE id = ? AND user_id = ?`,
      [id, req.user.id],
      (err, row) => {
        if (err) return res.status(500).json({ message: "Failed to load CV" });
        if (!row) return res.status(404).json({ message: "CV not found" });
        return res.json(normalizeCvResponse(row));
      }
    );
  });

  app.post("/api/cv", authRequired, (req, res) => {
    const { cv_name, cv_data } = req.body || {};
    const name = safeTrim(cv_name || "RESUME");
    const dataStr =
      typeof cv_data === "string" ? cv_data : JSON.stringify(cv_data || {});

    dbRun(
      `INSERT INTO cvs (user_id, cv_name, cv_data, updated_at) VALUES (?, ?, ?, datetime('now'))`,
      [req.user.id, name, dataStr],
      function (err) {
        if (err)
          return res.status(500).json({ message: "Failed to create CV" });
        res.json({ ok: true, id: this.lastID });
      }
    );
  });

  app.put("/api/cv/:id", authRequired, (req, res) => {
    const id = toInt(req.params.id);
    if (!id) return res.status(400).json({ message: "Bad id" });

    const { cv_name, cv_data } = req.body || {};
    const name = safeTrim(cv_name || "RESUME");
    const dataStr =
      typeof cv_data === "string" ? cv_data : JSON.stringify(cv_data || {});

    dbRun(
      `
      UPDATE cvs
      SET cv_name = ?, cv_data = ?, updated_at = datetime('now')
      WHERE id = ? AND user_id = ?
      `,
      [name, dataStr, id, req.user.id],
      function (err) {
        if (err)
          return res.status(500).json({ message: "Failed to update CV" });
        if (this.changes === 0)
          return res.status(404).json({ message: "CV not found" });
        res.json({ ok: true });
      }
    );
  });

  app.delete("/api/cv/:id", authRequired, (req, res) => {
    const id = toInt(req.params.id);
    if (!id) return res.status(400).json({ message: "Bad id" });

    dbRun(
      `DELETE FROM cvs WHERE id = ? AND user_id = ?`,
      [id, req.user.id],
      function (err) {
        if (err) return res.status(500).json({ message: "Delete failed" });
        if (this.changes === 0)
          return res.status(404).json({ message: "CV not found" });
        res.json({ ok: true });
      }
    );
  });

  /* =====================
     ✅ LEGACY CVS ENDPOINTS
  ===================== */
  app.get("/api/get-cv/:id", authRequired, (req, res) => {
    const id = toInt(req.params.id);
    if (!id) return res.status(400).json({ message: "Bad id" });

    dbGet(
      `SELECT id, user_id, cv_name, cv_data, updated_at FROM cvs WHERE id = ? AND user_id = ?`,
      [id, req.user.id],
      (err, row) => {
        if (err) return res.status(500).json({ message: "Failed to load CV" });
        if (!row) return res.status(404).json({ message: "CV not found" });
        return res.json(normalizeCvResponse(row));
      }
    );
  });

  app.get("/api/get-all-cvs/:userId", authRequired, (req, res) => {
    const userId = toInt(req.params.userId);
    if (!userId) return res.status(400).json({ message: "Bad userId" });
    if (userId !== req.user.id) return res.sendStatus(403);

    dbAll(
      `SELECT id, user_id, cv_name, updated_at FROM cvs WHERE user_id = ? ORDER BY id DESC`,
      [userId],
      (err, rows) => {
        if (err) return res.status(500).json({ message: "Failed to load CVs" });
        res.json(rows || []);
      }
    );
  });

  app.get("/api/cv/latest/:userId", authRequired, (req, res) => {
    dbGet(
      `SELECT id, user_id, cv_name, cv_data, updated_at
       FROM cvs
       WHERE user_id = ?
       ORDER BY id DESC
       LIMIT 1`,
      [req.user.id],
      (err, row) => {
        if (err) return res.status(500).json({ message: "Failed to load CV" });
        if (!row) return res.status(404).json({ message: "No CV yet" });
        return res.json(normalizeCvResponse(row));
      }
    );
  });

  app.put("/api/update-cv/:id", authRequired, (req, res) => {
    const id = toInt(req.params.id);
    if (!id) return res.status(400).json({ message: "Bad id" });

    const { cv_name, cv_data } = req.body || {};
    const name = safeTrim(cv_name || "RESUME");
    const dataStr =
      typeof cv_data === "string" ? cv_data : JSON.stringify(cv_data || {});

    dbRun(
      `
      UPDATE cvs
      SET cv_name = ?, cv_data = ?, updated_at = datetime('now')
      WHERE id = ? AND user_id = ?
      `,
      [name, dataStr, id, req.user.id],
      function (err) {
        if (err)
          return res.status(500).json({ message: "Failed to update CV" });
        if (this.changes === 0)
          return res.status(404).json({ message: "CV not found" });
        res.json({ ok: true });
      }
    );
  });

  app.post("/api/create-cv", authRequired, (req, res) => {
    const { cv_name, cv_data } = req.body || {};
    const name = safeTrim(cv_name || "RESUME");
    const dataStr =
      typeof cv_data === "string" ? cv_data : JSON.stringify(cv_data || {});

    dbRun(
      `INSERT INTO cvs (user_id, cv_name, cv_data, updated_at) VALUES (?, ?, ?, datetime('now'))`,
      [req.user.id, name, dataStr],
      function (err) {
        if (err)
          return res.status(500).json({ message: "Failed to create CV" });
        res.json({ ok: true, id: this.lastID });
      }
    );
  });

  app.delete("/api/delete-cv/:id", authRequired, (req, res) => {
    const id = toInt(req.params.id);
    if (!id) return res.status(400).json({ message: "Bad id" });

    dbRun(
      `DELETE FROM cvs WHERE id = ? AND user_id = ?`,
      [id, req.user.id],
      function (err) {
        if (err) return res.status(500).json({ message: "Delete failed" });
        if (this.changes === 0)
          return res.status(404).json({ message: "CV not found" });
        res.json({ ok: true });
      }
    );
  });
};
