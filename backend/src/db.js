// backend/src/db.js  (FULL FILE - copy/paste)
// ✅ Adds posts.media_url + posts.media_urls (multi images for FEED)
// ✅ Adds profile_posts.media_urls (optional multi images for PROFILE posts)
// ✅ Uses safeAlterTable so existing DB won't crash
// ✅ Keeps your existing tables/indexes

const path = require("path");
const sqlite3 = require("sqlite3").verbose();

const DB_PATH =
  process.env.DB_PATH || path.join(__dirname, "..", "database.sqlite");

const db = new sqlite3.Database(DB_PATH);

// =====================
// Safe ALTER helper
// =====================
function safeAlterTable(sql) {
  db.run(sql, (err) => {
    if (!err) return;
    const msg = String(err.message || "");
    // ignore "already exists" / duplicate column errors
    if (
      msg.includes("duplicate column name") ||
      msg.includes("already exists")
    ) {
      return;
    }
    // log only real issues
    console.warn("[safeAlterTable]", sql, "->", msg);
  });
}

/* =====================
   Schema
===================== */
db.serialize(() => {
  db.run("PRAGMA foreign_keys = ON");

  /* =====================
     USERS
  ===================== */
  db.run(`
    CREATE TABLE IF NOT EXISTS users (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      username TEXT,
      email TEXT UNIQUE,
      password_hash TEXT,
      phone TEXT,
      address TEXT,
      bio TEXT,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
  `);

  /* =====================
     CVS
  ===================== */
  db.run(`
    CREATE TABLE IF NOT EXISTS cvs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id INTEGER NOT NULL,
      cv_name TEXT,
      cv_data TEXT,
      updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
    )
  `);
  db.run(`CREATE INDEX IF NOT EXISTS idx_cvs_user ON cvs(user_id)`);

  /* =====================
     FEED POSTS  ✅ multi images
  ===================== */
  db.run(`
    CREATE TABLE IF NOT EXISTS posts (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id INTEGER NOT NULL,
      content TEXT NOT NULL,
      category TEXT,
      media_url TEXT,
      media_urls TEXT,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
    )
  `);

  // ✅ SAFE: لو جدول قديم ناقص الأعمدة
  safeAlterTable(`ALTER TABLE posts ADD COLUMN media_url TEXT`);
  safeAlterTable(`ALTER TABLE posts ADD COLUMN media_urls TEXT`);

  db.run(`CREATE INDEX IF NOT EXISTS idx_posts_user ON posts(user_id)`);
  db.run(`CREATE INDEX IF NOT EXISTS idx_posts_created ON posts(created_at)`);

  db.run(`
    CREATE TABLE IF NOT EXISTS post_likes (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      post_id INTEGER NOT NULL,
      user_id INTEGER NOT NULL,
      UNIQUE(post_id, user_id),
      FOREIGN KEY (post_id) REFERENCES posts(id) ON DELETE CASCADE,
      FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
    )
  `);
  db.run(
    `CREATE INDEX IF NOT EXISTS idx_post_likes_post ON post_likes(post_id)`
  );
  db.run(
    `CREATE INDEX IF NOT EXISTS idx_post_likes_user ON post_likes(user_id)`
  );

  db.run(`
    CREATE TABLE IF NOT EXISTS post_comments (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      post_id INTEGER NOT NULL,
      user_id INTEGER NOT NULL,
      comment TEXT NOT NULL,
      created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
      parent_comment_id INTEGER,
      FOREIGN KEY (post_id) REFERENCES posts(id) ON DELETE CASCADE,
      FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
    )
  `);

  // ✅ SAFE: لو جدول قديم ناقص العمود
  safeAlterTable(
    `ALTER TABLE post_comments ADD COLUMN parent_comment_id INTEGER`
  );

  db.run(
    `CREATE INDEX IF NOT EXISTS idx_comments_post ON post_comments(post_id)`
  );
  db.run(
    `CREATE INDEX IF NOT EXISTS idx_comments_user ON post_comments(user_id)`
  );
  db.run(
    `CREATE INDEX IF NOT EXISTS idx_comments_parent ON post_comments(parent_comment_id)`
  );

  db.run(`
    CREATE TABLE IF NOT EXISTS post_comment_likes (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      comment_id INTEGER NOT NULL,
      user_id INTEGER NOT NULL,
      UNIQUE(comment_id, user_id),
      FOREIGN KEY (comment_id) REFERENCES post_comments(id) ON DELETE CASCADE,
      FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
    )
  `);
  db.run(
    `CREATE INDEX IF NOT EXISTS idx_comment_likes_comment ON post_comment_likes(comment_id)`
  );
  db.run(
    `CREATE INDEX IF NOT EXISTS idx_comment_likes_user ON post_comment_likes(user_id)`
  );

  /* =====================
     SOCIAL PROFILE TABLES
  ===================== */
  db.run(`
    CREATE TABLE IF NOT EXISTS user_profile (
      user_id INTEGER PRIMARY KEY,
      username TEXT UNIQUE,
      display_name TEXT,
      avatar_url TEXT,
      cover_url TEXT,
      bio TEXT,
      location TEXT,
      phone TEXT,
      whatsapp TEXT,
      website TEXT,
      is_verified INTEGER DEFAULT 0,
      created_at TEXT DEFAULT (datetime('now')),
      updated_at TEXT DEFAULT (datetime('now')),
      FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
    )
  `);

  // ✅ SAFE ALTERS: لو user_profile قديم ناقص أعمدة
  safeAlterTable(`ALTER TABLE user_profile ADD COLUMN username TEXT`);
  safeAlterTable(`ALTER TABLE user_profile ADD COLUMN display_name TEXT`);
  safeAlterTable(`ALTER TABLE user_profile ADD COLUMN avatar_url TEXT`);
  safeAlterTable(`ALTER TABLE user_profile ADD COLUMN cover_url TEXT`);
  safeAlterTable(`ALTER TABLE user_profile ADD COLUMN bio TEXT`);
  safeAlterTable(`ALTER TABLE user_profile ADD COLUMN location TEXT`);
  safeAlterTable(`ALTER TABLE user_profile ADD COLUMN phone TEXT`);
  safeAlterTable(`ALTER TABLE user_profile ADD COLUMN whatsapp TEXT`);
  safeAlterTable(`ALTER TABLE user_profile ADD COLUMN website TEXT`);
  safeAlterTable(
    `ALTER TABLE user_profile ADD COLUMN is_verified INTEGER DEFAULT 0`
  );
  safeAlterTable(`ALTER TABLE user_profile ADD COLUMN created_at TEXT`);
  safeAlterTable(`ALTER TABLE user_profile ADD COLUMN updated_at TEXT`);

  db.run(`
    CREATE TABLE IF NOT EXISTS follows (
      follower_id INTEGER NOT NULL,
      following_id INTEGER NOT NULL,
      created_at TEXT DEFAULT (datetime('now')),
      PRIMARY KEY (follower_id, following_id),
      FOREIGN KEY (follower_id) REFERENCES users(id) ON DELETE CASCADE,
      FOREIGN KEY (following_id) REFERENCES users(id) ON DELETE CASCADE
    )
  `);

  /* =====================
     PROFILE POSTS ✅ multi images (optional)
  ===================== */
  db.run(`
    CREATE TABLE IF NOT EXISTS profile_posts (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id INTEGER NOT NULL,
      content TEXT NOT NULL,
      media_url TEXT,
      media_urls TEXT,
      created_at TEXT DEFAULT (datetime('now')),
      FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
    )
  `);

  // ✅ SAFE: لو profile_posts قديم ناقص أعمدة
  safeAlterTable(`ALTER TABLE profile_posts ADD COLUMN media_url TEXT`);
  safeAlterTable(`ALTER TABLE profile_posts ADD COLUMN media_urls TEXT`);
  safeAlterTable(`ALTER TABLE profile_posts ADD COLUMN created_at TEXT`);

  /* =====================
     SERVICES
  ===================== */
  db.run(`
    CREATE TABLE IF NOT EXISTS services (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id INTEGER NOT NULL,
      title TEXT NOT NULL,
      description TEXT,
      category TEXT,
      price_type TEXT DEFAULT 'negotiable',
      price_value REAL,
      location TEXT,
      is_active INTEGER DEFAULT 1,
      created_at TEXT DEFAULT (datetime('now')),
      FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
    )
  `);

  /* =====================
     PRODUCTS
  ===================== */
  db.run(`
    CREATE TABLE IF NOT EXISTS products (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id INTEGER NOT NULL,
      title TEXT NOT NULL,
      description TEXT,
      price REAL,
      currency TEXT DEFAULT 'USD',
      images_json TEXT,
      location TEXT,
      is_available INTEGER DEFAULT 1,
      created_at TEXT DEFAULT (datetime('now')),
      FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
    )
  `);

  /* =====================
     REVIEWS
  ===================== */
  db.run(`
    CREATE TABLE IF NOT EXISTS reviews (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      user_id INTEGER NOT NULL,
      author_id INTEGER NOT NULL,
      rating INTEGER NOT NULL,
      comment TEXT,
      created_at TEXT DEFAULT (datetime('now')),
      UNIQUE(user_id, author_id),
      FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
      FOREIGN KEY (author_id) REFERENCES users(id) ON DELETE CASCADE
    )
  `);

  // indexes
  db.run(
    `CREATE INDEX IF NOT EXISTS idx_profile_posts_user ON profile_posts(user_id)`
  );
  db.run(`CREATE INDEX IF NOT EXISTS idx_services_user ON services(user_id)`);
  db.run(`CREATE INDEX IF NOT EXISTS idx_products_user ON products(user_id)`);
  db.run(`CREATE INDEX IF NOT EXISTS idx_reviews_user ON reviews(user_id)`);
  db.run(
    `CREATE INDEX IF NOT EXISTS idx_follows_following ON follows(following_id)`
  );
});

module.exports = { db, DB_PATH, safeAlterTable };
