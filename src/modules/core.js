// src/modules/core.js
module.exports = function registerCore({ JWT_SECRET, jwt, ADMIN_EMAILS }) {
  const admins = Array.isArray(ADMIN_EMAILS)
    ? ADMIN_EMAILS.map((s) => String(s).trim().toLowerCase()).filter(Boolean)
    : String(ADMIN_EMAILS || "")
        .split(",")
        .map((s) => s.trim().toLowerCase())
        .filter(Boolean);

  function safeTrim(v) {
    return String(v ?? "").trim();
  }

  function safeUrl(v) {
    const s = safeTrim(v);
    return s || "";
  }

  function safeJsonParse(s) {
    try {
      return JSON.parse(s);
    } catch {
      return null;
    }
  }

  function signJwt(payload) {
    return jwt.sign(payload, JWT_SECRET, { expiresIn: "30d" });
  }

  function authRequired(req, res, next) {
    const h = req.headers.authorization || "";
    const token = h.startsWith("Bearer ") ? h.slice(7) : null;
    if (!token) return res.sendStatus(401);
    try {
      req.user = jwt.verify(token, JWT_SECRET);
      next();
    } catch {
      return res.sendStatus(401);
    }
  }

  function authOptional(req, res, next) {
    const h = req.headers.authorization || "";
    const token = h.startsWith("Bearer ") ? h.slice(7) : null;
    if (!token) return next();
    try {
      req.user = jwt.verify(token, JWT_SECRET);
    } catch {}
    next();
  }

  function isAdminReq(req) {
    const email = String(req.user?.email || "").toLowerCase();
    return admins.includes(email);
  }

  function adminRequired(req, res, next) {
    if (!req.user) return res.sendStatus(401);
    if (!isAdminReq(req)) return res.sendStatus(403);
    next();
  }

  return {
    safeTrim,
    safeUrl,
    safeJsonParse,
    signJwt,
    authRequired,
    authOptional,
    isAdminReq,
    adminRequired,
  };
};
