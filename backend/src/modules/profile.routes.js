// src/modules/profile.routes.js
const path = require("path");
const fs = require("fs");
const multer = require("multer");

module.exports = function registerUsersMeRoutes({
  app,
  authRequired,
  dbGet,
  dbRun,
  safeTrim,
}) {
  /* =========================
     Upload setup (avatars + covers)
     ✅ save inside backend/uploads (beside server.js)
  ========================= */
  const UPLOADS_DIR = path.resolve(__dirname, "..", "..", "uploads");
  const AVATAR_DIR = path.join(UPLOADS_DIR, "avatars");
  const COVER_DIR = path.join(UPLOADS_DIR, "covers");

  try {
    if (!fs.existsSync(UPLOADS_DIR))
      fs.mkdirSync(UPLOADS_DIR, { recursive: true });
    if (!fs.existsSync(AVATAR_DIR))
      fs.mkdirSync(AVATAR_DIR, { recursive: true });
    if (!fs.existsSync(COVER_DIR)) fs.mkdirSync(COVER_DIR, { recursive: true });
  } catch {}

  const avatarStorage = multer.diskStorage({
    destination: (_req, _file, cb) => cb(null, AVATAR_DIR),
    filename: (req, file, cb) => {
      const ext = path.extname(file.originalname || "").toLowerCase() || ".jpg";
      cb(null, `u_${req.user.id}_${Date.now()}${ext}`);
    },
  });

  const coverStorage = multer.diskStorage({
    destination: (_req, _file, cb) => cb(null, COVER_DIR),
    filename: (req, file, cb) => {
      const ext = path.extname(file.originalname || "").toLowerCase() || ".jpg";
      cb(null, `c_${req.user.id}_${Date.now()}${ext}`);
    },
  });

  const uploadAvatar = multer({
    storage: avatarStorage,
    limits: { fileSize: 2 * 1024 * 1024 }, // 2MB
  });

  const uploadCover = multer({
    storage: coverStorage,
    limits: { fileSize: 6 * 1024 * 1024 }, // 6MB
  });

  // =========================
  // Helpers
  // =========================
  function ensureProfileRow(userId, cb) {
    dbRun(
      `INSERT OR IGNORE INTO user_profile (user_id, username, display_name, updated_at)
       VALUES (?, ?, ?, datetime('now'))`,
      [userId, null, null],
      () => cb && cb()
    );
  }

  function deleteFileIfUnder(dir, urlPrefix, oldUrl) {
    try {
      const s = String(oldUrl || "");
      if (!s || !s.startsWith(urlPrefix)) return;
      const name = s.split(urlPrefix)[1];
      if (!name) return;
      const p = path.join(dir, name);
      if (fs.existsSync(p)) fs.unlinkSync(p);
    } catch {}
  }

  // =========================
  // GET me (users)
  // =========================
  app.get("/api/users/me", authRequired, (req, res) => {
    dbGet(
      `SELECT id, username, email, phone, address, bio, avatar_url FROM users WHERE id = ?`,
      [req.user.id],
      (err, me) => {
        if (err)
          return res.status(500).json({ message: "Failed to load user" });
        if (!me) return res.sendStatus(404);
        res.json(me);
      }
    );
  });

  // =========================
  // PUT me (users)  (kept)
  // =========================
  app.put("/api/users/me", authRequired, (req, res) => {
    const { username, phone, address, bio, avatar_url } = req.body || {};
    const av = safeTrim(avatar_url);

    ensureProfileRow(req.user.id, () => {
      dbRun(
        `
        UPDATE users
        SET username = ?, phone = ?, address = ?, bio = ?, avatar_url = ?
        WHERE id = ?
        `,
        [
          safeTrim(username),
          safeTrim(phone),
          safeTrim(address),
          safeTrim(bio),
          av,
          req.user.id,
        ],
        function (err) {
          if (err) return res.status(500).json({ message: "Update failed" });

          // sync user_profile avatar (page reads it)
          dbRun(
            `UPDATE user_profile SET avatar_url = ?, updated_at = datetime('now') WHERE user_id = ?`,
            [av || null, req.user.id],
            () => {}
          );

          dbGet(
            `SELECT id, username, email, phone, address, bio, avatar_url FROM users WHERE id = ?`,
            [req.user.id],
            (e2, me) => {
              if (e2) return res.status(500).json({ message: "Update failed" });
              if (!me)
                return res.status(500).json({ message: "Update failed" });
              return res.json(me);
            }
          );
        }
      );
    });
  });

  /* =========================
     ✅ PROFILE (user_profile) - GET/PUT
     - this is what your /u/:id page needs
  ========================= */

  app.get("/api/profile/me", authRequired, (req, res) => {
    ensureProfileRow(req.user.id, () => {
      dbGet(
        `
        SELECT
          up.user_id,
          up.username,
          up.display_name,
          up.bio,
          up.phone,
          up.location,
          up.website,
          up.avatar_url,
          up.cover_url,
          up.updated_at
        FROM user_profile up
        WHERE up.user_id = ?
        `,
        [req.user.id],
        (err, row) => {
          if (err) return res.status(500).json({ message: "Failed" });
          if (!row) return res.status(404).json({ message: "Not found" });
          return res.json(row);
        }
      );
    });
  });

  app.put("/api/profile/me", authRequired, (req, res) => {
    const display_name = safeTrim(
      req.body?.display_name ?? req.body?.displayName
    );
    const bio = safeTrim(req.body?.bio);
    const phone = safeTrim(req.body?.phone);
    const location = safeTrim(
      req.body?.location ?? req.body?.address ?? req.body?.addr
    );
    const website = safeTrim(req.body?.website);

    ensureProfileRow(req.user.id, () => {
      dbRun(
        `
        UPDATE user_profile
        SET
          display_name = COALESCE(?, display_name),
          bio = COALESCE(?, bio),
          phone = COALESCE(?, phone),
          location = COALESCE(?, location),
          website = COALESCE(?, website),
          updated_at = datetime('now')
        WHERE user_id = ?
        `,
        [
          display_name || null,
          bio || null,
          phone || null,
          location || null,
          website || null,
          req.user.id,
        ],
        function (err) {
          if (err) return res.status(500).json({ message: "Update failed" });

          // (optional) keep users table somewhat in sync
          dbRun(
            `UPDATE users SET phone = COALESCE(?, phone), address = COALESCE(?, address), bio = COALESCE(?, bio) WHERE id = ?`,
            [phone || null, location || null, bio || null, req.user.id],
            () => {}
          );

          dbGet(
            `SELECT * FROM user_profile WHERE user_id = ?`,
            [req.user.id],
            (e2, row) => {
              if (e2) return res.status(500).json({ message: "Update failed" });
              return res.json(row || { ok: true });
            }
          );
        }
      );
    });
  });

  // =========================
  // Avatar handlers (shared)
  // =========================
  function handleUploadAvatar(req, res) {
    if (!req.file) return res.status(400).json({ message: "No file" });
    const nextUrl = `/uploads/avatars/${req.file.filename}`;

    ensureProfileRow(req.user.id, () => {
      // read old from user_profile then users
      dbGet(
        `SELECT avatar_url FROM user_profile WHERE user_id = ?`,
        [req.user.id],
        (_e0, upRow) => {
          const oldUp = upRow?.avatar_url ? String(upRow.avatar_url) : "";

          dbGet(
            `SELECT avatar_url FROM users WHERE id = ?`,
            [req.user.id],
            (_e00, uRow) => {
              const oldU = uRow?.avatar_url ? String(uRow.avatar_url) : "";
              const oldUrl = oldUp || oldU || "";

              // update users
              dbRun(
                `UPDATE users SET avatar_url = ? WHERE id = ?`,
                [nextUrl, req.user.id],
                (e1) => {
                  if (e1) {
                    try {
                      fs.unlinkSync(path.join(AVATAR_DIR, req.file.filename));
                    } catch {}
                    return res.status(500).json({ message: "Update failed" });
                  }

                  // update user_profile
                  dbRun(
                    `UPDATE user_profile SET avatar_url = ?, updated_at = datetime('now') WHERE user_id = ?`,
                    [nextUrl, req.user.id],
                    () => {
                      deleteFileIfUnder(
                        AVATAR_DIR,
                        "/uploads/avatars/",
                        oldUrl
                      );
                      return res.json({ avatar_url: nextUrl });
                    }
                  );
                }
              );
            }
          );
        }
      );
    });
  }

  function handleDeleteAvatar(req, res) {
    ensureProfileRow(req.user.id, () => {
      dbGet(
        `SELECT avatar_url FROM user_profile WHERE user_id = ?`,
        [req.user.id],
        (e0, upRow) => {
          if (e0) return res.status(500).json({ message: "Failed" });

          dbGet(
            `SELECT avatar_url FROM users WHERE id = ?`,
            [req.user.id],
            (_e00, uRow) => {
              const oldUp = upRow?.avatar_url ? String(upRow.avatar_url) : "";
              const oldU = uRow?.avatar_url ? String(uRow.avatar_url) : "";
              const oldUrl = oldUp || oldU || "";

              dbRun(
                `UPDATE users SET avatar_url = NULL WHERE id = ?`,
                [req.user.id],
                (e1) => {
                  if (e1)
                    return res.status(500).json({ message: "Update failed" });

                  dbRun(
                    `UPDATE user_profile SET avatar_url = NULL, updated_at = datetime('now') WHERE user_id = ?`,
                    [req.user.id],
                    () => {
                      deleteFileIfUnder(
                        AVATAR_DIR,
                        "/uploads/avatars/",
                        oldUrl
                      );
                      return res.json({ ok: true });
                    }
                  );
                }
              );
            }
          );
        }
      );
    });
  }

  // =========================
  // Cover handlers (shared)
  // =========================
  function handleUploadCover(req, res) {
    if (!req.file) return res.status(400).json({ message: "No file" });
    const nextUrl = `/uploads/covers/${req.file.filename}`;

    ensureProfileRow(req.user.id, () => {
      dbGet(
        `SELECT cover_url FROM user_profile WHERE user_id = ?`,
        [req.user.id],
        (_e0, upRow) => {
          const oldUrl = upRow?.cover_url ? String(upRow.cover_url) : "";

          dbRun(
            `UPDATE user_profile SET cover_url = ?, updated_at = datetime('now') WHERE user_id = ?`,
            [nextUrl, req.user.id],
            (e1) => {
              if (e1) {
                try {
                  fs.unlinkSync(path.join(COVER_DIR, req.file.filename));
                } catch {}
                return res.status(500).json({ message: "Update failed" });
              }
              deleteFileIfUnder(COVER_DIR, "/uploads/covers/", oldUrl);
              return res.json({ cover_url: nextUrl });
            }
          );
        }
      );
    });
  }

  function handleDeleteCover(req, res) {
    ensureProfileRow(req.user.id, () => {
      dbGet(
        `SELECT cover_url FROM user_profile WHERE user_id = ?`,
        [req.user.id],
        (e0, upRow) => {
          if (e0) return res.status(500).json({ message: "Failed" });
          const oldUrl = upRow?.cover_url ? String(upRow.cover_url) : "";

          dbRun(
            `UPDATE user_profile SET cover_url = NULL, updated_at = datetime('now') WHERE user_id = ?`,
            [req.user.id],
            (e1) => {
              if (e1) return res.status(500).json({ message: "Update failed" });
              deleteFileIfUnder(COVER_DIR, "/uploads/covers/", oldUrl);
              return res.json({ ok: true });
            }
          );
        }
      );
    });
  }

  // =========================
  // Routes (avatar + cover) + aliases
  // =========================
  const AVATAR_POST_PATHS = [
    "/api/profile/avatar",
    "/api/profile/me/avatar",
    "/api/me/profile/avatar",
    "/api/user/profile/me/avatar",
  ];
  const AVATAR_DELETE_PATHS = [
    "/api/profile/avatar",
    "/api/profile/me/avatar",
    "/api/me/profile/avatar",
    "/api/user/profile/me/avatar",
  ];

  AVATAR_POST_PATHS.forEach((p) =>
    app.post(p, authRequired, uploadAvatar.single("avatar"), handleUploadAvatar)
  );
  AVATAR_DELETE_PATHS.forEach((p) =>
    app.delete(p, authRequired, handleDeleteAvatar)
  );

  const COVER_POST_PATHS = [
    "/api/profile/cover",
    "/api/profile/me/cover",
    "/api/me/profile/cover",
    "/api/user/profile/me/cover",
  ];
  const COVER_DELETE_PATHS = [
    "/api/profile/cover",
    "/api/profile/me/cover",
    "/api/me/profile/cover",
    "/api/user/profile/me/cover",
  ];

  COVER_POST_PATHS.forEach((p) =>
    app.post(p, authRequired, uploadCover.single("cover"), handleUploadCover)
  );
  COVER_DELETE_PATHS.forEach((p) =>
    app.delete(p, authRequired, handleDeleteCover)
  );
};
