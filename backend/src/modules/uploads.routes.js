const path = require("path");
const fs = require("fs");
const multer = require("multer");
const crypto = require("crypto");

module.exports = function registerUploadRoutes({ app, authRequired }) {
  console.log("[uploads] routes registered ✅");

  const UPLOAD_DIR = path.resolve(process.cwd(), "uploads");

  if (!fs.existsSync(UPLOAD_DIR)) fs.mkdirSync(UPLOAD_DIR, { recursive: true });

  const storage = multer.diskStorage({
    destination: (_, __, cb) => cb(null, UPLOAD_DIR),
    filename: (_, file, cb) => {
      const ext = path.extname(file.originalname || "");
      const id = crypto.randomBytes(12).toString("hex");
      cb(null, `${Date.now()}_${id}${ext}`);
    },
  });

  function fileFilter(req, file, cb) {
    if (String(file.mimetype || "").startsWith("image/")) return cb(null, true);
    cb(new Error("Only images allowed"));
  }

  const upload = multer({
    storage,
    fileFilter,
    limits: { fileSize: 6 * 1024 * 1024, files: 10 },
  });

  function abs(req, p) {
    const proto = req.headers["x-forwarded-proto"] || req.protocol;
    const host = req.headers["x-forwarded-host"] || req.get("host");
    return `${proto}://${host}${p}`;
  }

  // ============================================
  // POST /api/uploads/images
  // field name = images (multiple)
  // ============================================
  app.post(
    "/api/uploads/images",
    authRequired,
    upload.array("images", 10),
    (req, res) => {
      const files = req.files || [];

      const urls = files.map((f) => abs(req, `/uploads/${f.filename}`));

      res.json({
        ok: true,
        files: urls,
      });
    },
  );
};
