// src/modules/chat.routes.js
module.exports = function registerChatRoutes({
  app,
  db,
  authRequired,
  dbAll,
  dbGet,
  dbRun,
  safeTrim,
  toInt,
  safeAlterTable,
}) {
  // =========================
  // Schema (safe)
  // =========================
  db.serialize(() => {
    db.run(`
      CREATE TABLE IF NOT EXISTS chat_threads (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user1_id INTEGER NOT NULL,
        user2_id INTEGER NOT NULL,
        context_type TEXT,
        context_id TEXT,
        context_label TEXT,
        last_message TEXT,
        last_message_at TEXT,
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        updated_at TEXT NOT NULL DEFAULT (datetime('now'))
      )
    `);

    db.run(`
      CREATE TABLE IF NOT EXISTS chat_messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        thread_id INTEGER NOT NULL,
        sender_id INTEGER NOT NULL,
        body TEXT NOT NULL,
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        read_at TEXT,
        notif_id INTEGER
      )
    `);

    // upgrades for old DBs
    safeAlterTable(`ALTER TABLE chat_threads ADD COLUMN context_type TEXT`);
    safeAlterTable(`ALTER TABLE chat_threads ADD COLUMN context_id TEXT`);
    safeAlterTable(`ALTER TABLE chat_threads ADD COLUMN context_label TEXT`);
    safeAlterTable(`ALTER TABLE chat_threads ADD COLUMN last_message TEXT`);
    safeAlterTable(`ALTER TABLE chat_threads ADD COLUMN last_message_at TEXT`);
    safeAlterTable(`ALTER TABLE chat_threads ADD COLUMN created_at TEXT`);
    safeAlterTable(`ALTER TABLE chat_threads ADD COLUMN updated_at TEXT`);

    safeAlterTable(`ALTER TABLE chat_messages ADD COLUMN read_at TEXT`);
    safeAlterTable(`ALTER TABLE chat_messages ADD COLUMN notif_id INTEGER`);

    db.run(
      `CREATE INDEX IF NOT EXISTS idx_chat_threads_u1 ON chat_threads(user1_id)`
    );
    db.run(
      `CREATE INDEX IF NOT EXISTS idx_chat_threads_u2 ON chat_threads(user2_id)`
    );
    db.run(
      `CREATE INDEX IF NOT EXISTS idx_chat_messages_thread ON chat_messages(thread_id)`
    );
    db.run(
      `CREATE INDEX IF NOT EXISTS idx_chat_messages_sender ON chat_messages(sender_id)`
    );
  });

  // =========================
  // Helpers
  // =========================
  const now = () => new Date().toISOString();

  function meFromReq(req) {
    // core/auth puts req.user
    const id = toInt(req.user?.id);
    return id;
  }

  function pickOtherUserId(thread, meId) {
    if (!thread) return 0;
    const u1 = toInt(thread.user1_id);
    const u2 = toInt(thread.user2_id);
    return u1 === meId ? u2 : u1;
  }

  function labelTime(iso) {
    // lightweight label for UI (no intl deps)
    try {
      const d = new Date(iso);
      if (!Number.isFinite(d.getTime())) return "";
      const hh = String(d.getHours()).padStart(2, "0");
      const mm = String(d.getMinutes()).padStart(2, "0");
      const yyyy = d.getFullYear();
      const m = String(d.getMonth() + 1).padStart(2, "0");
      const day = String(d.getDate()).padStart(2, "0");
      return `${yyyy}-${m}-${day} ${hh}:${mm}`;
    } catch {
      return "";
    }
  }

  function resolveOtherProfile(otherId, cb) {
    // prefer user_profile.display_name, fallback users.username
    dbGet(
      `
      SELECT
        up.display_name AS display_name,
        up.avatar_url AS avatar_url,
        u.username AS username,
        u.avatar_url AS u_avatar
      FROM users u
      LEFT JOIN user_profile up ON up.user_id = u.id
      WHERE u.id = ?
      `,
      [otherId],
      (e, row) => {
        if (e) return cb(e);
        const name =
          safeTrim(row?.display_name) ||
          safeTrim(row?.username) ||
          `User ${otherId}`;
        const avatar_url =
          safeTrim(row?.avatar_url) || safeTrim(row?.u_avatar) || "";
        cb(null, { id: otherId, name, avatar_url });
      }
    );
  }

  function ensureThread(
    meId,
    otherId,
    context_type,
    context_id,
    context_label,
    cb
  ) {
    const a = Math.min(meId, otherId);
    const b = Math.max(meId, otherId);
    const cType = safeTrim(context_type) || null;
    const cId = safeTrim(context_id) || null;
    const cLabel = safeTrim(context_label) || null;

    // find existing
    dbGet(
      `
      SELECT * FROM chat_threads
      WHERE user1_id = ? AND user2_id = ?
        AND (context_type IS ? OR context_type = ?)
        AND (context_id   IS ? OR context_id   = ?)
      LIMIT 1
      `,
      [a, b, cType, cType, cId, cId],
      (e, row) => {
        if (e) return cb(e);
        if (row) return cb(null, row);

        dbRun(
          `
          INSERT INTO chat_threads
            (user1_id, user2_id, context_type, context_id, context_label, created_at, updated_at)
          VALUES (?,?,?,?,?, datetime('now'), datetime('now'))
          `,
          [a, b, cType, cId, cLabel],
          function (e2) {
            if (e2) return cb(e2);
            dbGet(
              `SELECT * FROM chat_threads WHERE id = ?`,
              [this.lastID],
              (e3, row2) => cb(e3, row2)
            );
          }
        );
      }
    );
  }

  // =========================
  // Routes
  // =========================

  // Summary unread
  app.get("/api/chat/summary", authRequired, (req, res) => {
    const meId = meFromReq(req);
    if (!meId) return res.status(401).json({ message: "Unauthorized" });

    dbGet(
      `
      SELECT COUNT(*) AS c
      FROM chat_messages m
      JOIN chat_threads t ON t.id = m.thread_id
      WHERE m.read_at IS NULL
        AND m.sender_id <> ?
        AND (t.user1_id = ? OR t.user2_id = ?)
      `,
      [meId, meId, meId],
      (e, row) => {
        if (e) return res.status(500).json({ message: "Failed" });
        return res.json({ unread_total: toInt(row?.c) });
      }
    );
  });

  // Inbox threads
  app.get("/api/chat/threads", authRequired, (req, res) => {
    const meId = meFromReq(req);
    if (!meId) return res.status(401).json({ message: "Unauthorized" });

    dbAll(
      `
      SELECT
        t.*,
        (
          SELECT COUNT(*)
          FROM chat_messages m
          WHERE m.thread_id = t.id
            AND m.read_at IS NULL
            AND m.sender_id <> ?
        ) AS unread_count
      FROM chat_threads t
      WHERE t.user1_id = ? OR t.user2_id = ?
      ORDER BY COALESCE(t.last_message_at, t.updated_at, t.created_at) DESC
      LIMIT 200
      `,
      [meId, meId, meId],
      (e, rows) => {
        if (e) return res.status(500).json({ message: "Failed" });

        const items = rows || [];
        // attach other profile
        let pending = items.length;
        if (!pending) return res.json({ items: [] });

        const out = [];
        items.forEach((t) => {
          const otherId = pickOtherUserId(t, meId);
          resolveOtherProfile(otherId, (e2, other) => {
            out.push({
              id: t.id,
              other,
              context_label: t.context_label || "",
              last_message: t.last_message || "",
              last_message_at: t.last_message_at || "",
              unread_count: toInt(t.unread_count),
            });
            pending -= 1;
            if (pending === 0) {
              // keep order like SQL
              out.sort((a, b) => {
                const A = a.last_message_at || "";
                const B = b.last_message_at || "";
                return B.localeCompare(A);
              });
              res.json({ items: out });
            }
          });
        });
      }
    );
  });

  // Get messages
  app.get("/api/chat/threads/:id/messages", authRequired, (req, res) => {
    const meId = meFromReq(req);
    const threadId = toInt(req.params.id);
    if (!meId || !threadId) return res.status(400).json({ message: "Bad" });

    dbGet(`SELECT * FROM chat_threads WHERE id = ?`, [threadId], (e0, t) => {
      if (e0) return res.status(500).json({ message: "Failed" });
      if (!t) return res.status(404).json({ message: "Not found" });

      if (toInt(t.user1_id) !== meId && toInt(t.user2_id) !== meId) {
        return res.sendStatus(403);
      }

      dbAll(
        `
          SELECT id, thread_id, sender_id, body, created_at, read_at
          FROM chat_messages
          WHERE thread_id = ?
          ORDER BY id ASC
          LIMIT 500
          `,
        [threadId],
        (e1, rows) => {
          if (e1) return res.status(500).json({ message: "Failed" });
          const items =
            (rows || []).map((m) => ({
              ...m,
              created_at_label: labelTime(m.created_at),
            })) || [];
          res.json({ items });
        }
      );
    });
  });

  // Mark thread read (best-effort)
  app.post("/api/chat/threads/:id/read", authRequired, (req, res) => {
    const meId = meFromReq(req);
    const threadId = toInt(req.params.id);
    if (!meId || !threadId) return res.status(400).json({ message: "Bad" });

    // only mark messages from other as read
    dbRun(
      `
      UPDATE chat_messages
      SET read_at = datetime('now')
      WHERE thread_id = ?
        AND sender_id <> ?
        AND read_at IS NULL
      `,
      [threadId, meId],
      function (e) {
        if (e) return res.status(500).json({ message: "Failed" });
        res.json({ ok: true, updated: this.changes || 0 });
      }
    );
  });

  // Send message
  app.post("/api/chat/threads/:id/messages", authRequired, (req, res) => {
    const meId = meFromReq(req);
    const threadId = toInt(req.params.id);
    const body = safeTrim(req.body?.body);
    if (!meId || !threadId || !body) {
      return res.status(400).json({ message: "Bad" });
    }

    dbGet(`SELECT * FROM chat_threads WHERE id = ?`, [threadId], (e0, t) => {
      if (e0) return res.status(500).json({ message: "Failed" });
      if (!t) return res.status(404).json({ message: "Not found" });
      if (toInt(t.user1_id) !== meId && toInt(t.user2_id) !== meId) {
        return res.sendStatus(403);
      }

      dbRun(
        `
          INSERT INTO chat_messages (thread_id, sender_id, body, created_at)
          VALUES (?,?,?, datetime('now'))
          `,
        [threadId, meId, body],
        function (e1) {
          if (e1) return res.status(500).json({ message: "Failed" });

          const msgId = this.lastID;
          const ts = now();

          dbRun(
            `
              UPDATE chat_threads
              SET last_message = ?,
                  last_message_at = datetime('now'),
                  updated_at = datetime('now')
              WHERE id = ?
              `,
            [body.slice(0, 300), threadId],
            () => {
              dbGet(
                `
                  SELECT id, thread_id, sender_id, body, created_at, read_at
                  FROM chat_messages
                  WHERE id = ?
                  `,
                [msgId],
                (e2, row) => {
                  if (e2 || !row)
                    return res.json({
                      ok: true,
                      item: {
                        id: msgId,
                        thread_id: threadId,
                        sender_id: meId,
                        body,
                        created_at: ts,
                        created_at_label: labelTime(ts),
                      },
                    });

                  res.json({
                    ok: true,
                    item: {
                      ...row,
                      created_at_label: labelTime(row.created_at),
                    },
                  });
                }
              );
            }
          );
        }
      );
    });
  });

  // Create (or get) thread with user + optional context
  app.post("/api/chat/threads", authRequired, (req, res) => {
    const meId = meFromReq(req);
    const otherId = toInt(req.body?.other_user_id);
    const context_type = safeTrim(req.body?.context_type);
    const context_id = safeTrim(req.body?.context_id);
    const context_label = safeTrim(req.body?.context_label);

    if (!meId || !otherId || meId === otherId) {
      return res.status(400).json({ message: "Bad" });
    }

    ensureThread(
      meId,
      otherId,
      context_type,
      context_id,
      context_label,
      (e, t) => {
        if (e) return res.status(500).json({ message: "Failed" });

        resolveOtherProfile(otherId, (e2, other) => {
          if (e2) return res.status(500).json({ message: "Failed" });
          res.json({
            ok: true,
            thread: {
              id: t.id,
              other,
              context_label: t.context_label || "",
              last_message: t.last_message || "",
              last_message_at: t.last_message_at || "",
              unread_count: 0,
            },
          });
        });
      }
    );
  });
};
