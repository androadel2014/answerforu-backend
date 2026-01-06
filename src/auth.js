module.exports = function registerAuth({ JWT_SECRET, dbGet, dbRun, safeTrim }) {
  const jwt = require("jsonwebtoken");

  function signToken(payload) {
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

  // Admin helpers
  const ADMIN_EMAILS = (process.env.ADMIN_EMAILS || "")
    .split(",")
    .map((s) => s.trim().toLowerCase())
    .filter(Boolean);

  function isAdminReq(req) {
    const email = String(req.user?.email || "").toLowerCase();
    return ADMIN_EMAILS.includes(email);
  }

  function adminRequired(req, res, next) {
    if (!req.user) return res.sendStatus(401);
    if (!isAdminReq(req)) return res.sendStatus(403);
    next();
  }

  return { signToken, authRequired, authOptional, isAdminReq, adminRequired };
};
