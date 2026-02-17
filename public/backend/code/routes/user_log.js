const express = require("express");
const bcrypt = require("bcrypt");
const { pool } = require("../db");

const router = express.Router();

const BCRYPT_ROUNDS = 12;

// helper: normalize identifier/email
function normalizeIdentifier(s) {
  return String(s || "").trim();
}
function normalizeEmail(s) {
  const e = String(s || "").trim().toLowerCase();
  return e.length ? e : null;
}

// POST /auth/signup -> creates user, stores password_hash, sets session.userId

router.post("/signup", async (req, res) => {
  try {
    const identifier = String(req.body.identifier || "").trim();
    const password = String(req.body.password || "");

    if (!identifier) {
      return res.status(400).json({ error: "Username or email is required." });
    }
    if (!password || password.length < 3) {
      return res.status(400).json({ error: "Password must be at least 3 characters." });
    }

    const isEmail = identifier.includes("@");
    const email = isEmail ? identifier.toLowerCase() : null;

    // username: either provided directly, or derived from email
    let username = !isEmail ? identifier : identifier.split("@")[0];

    if (!username || username.length < 3) {
      return res.status(400).json({ error: "Username must be at least 3 characters." });
    }

    // ff email provided, ensure it's not already taken
    if (email) {
      const dupEmail = await pool.query(`SELECT 1 FROM users WHERE email = $1 LIMIT 1;`, [email]);
      if (dupEmail.rowCount > 0) {
        return res.status(409).json({ error: "Email already exists." });
      }
    }

    // ensure username is unique 
    if (!isEmail) {
      const dupUser = await pool.query(`SELECT 1 FROM users WHERE username = $1 LIMIT 1;`, [username]);
    if (dupUser.rowCount > 0) {
      return res.status(409).json({ error: "Username already exists." });
    }}

  const password_hash = await bcrypt.hash(password, BCRYPT_ROUNDS);

  const inserted = await pool.query(
      `INSERT INTO users (username, email, password_hash, created_at, updated_at, last_login_at)
       VALUES ($1, $2, $3, NOW(), NOW(), NOW())
       RETURNING id, username, email;`,
      [username, email, password_hash]
    );

    const user = inserted.rows[0];

    req.session.userId = user.id;
    req.session.username = user.username;

    return res.status(201).json({
      ok: true,
      user: { id: user.id, username: user.username, email: user.email },
    });
  } catch (err) {
    console.error("SIGNUP error:", err);
    return res.status(500).json({ error: "Internal server error." });
  }
});


// POST /auth/login -> verifies password, sets session.userId

router.post("/login", async (req, res) => {
  try {
    const identifier = normalizeIdentifier(req.body.identifier);
    const password = String(req.body.password || "");

    if (!identifier) {
      return res.status(400).json({ error: "Identifier (username or email) is required." });
    }
    if (!password) {
      return res.status(400).json({ error: "Password is required." });
    }

    // find user by username or email
    const found = await pool.query(
      `SELECT id, username, email, password_hash
       FROM users
       WHERE username = $1 OR email = $1
       LIMIT 1;`,
      [identifier]
    );

    if (found.rowCount === 0) {
      return res.status(401).json({ error: "Invalid credentials." });
    }

    const user = found.rows[0];

    if (!user.password_hash) {
      return res.status(403).json({ error: "This user has no password set." });
    }

    const ok = await bcrypt.compare(password, user.password_hash);
    if (!ok) {
      return res.status(401).json({ error: "Invalid credentials." });
    }

    // update last login timestamp
    await pool.query(
      `UPDATE users SET last_login_at = NOW(), updated_at = NOW() WHERE id = $1;`,
      [user.id]
    );

    // start session
    req.session.userId = user.id;
    req.session.username = user.username;

    // recompute user embedding ON LOGIN 
    try {
      const resp = await fetch(`http://user_profile_service:8010/users/${user.id}/recompute`, {
        method: "POST",
      });

      if (!resp.ok) {
        const txt = await resp.text();
        console.warn("user_profile_service recompute failed:", resp.status, txt);
        // not breaking login — just log the issue
      }
    } catch (e) {
      console.warn("user_profile_service recompute unreachable:", e?.message || e);
      // not breaking login — just log the issue
    }

    return res.json({
      ok: true,
      user: { id: user.id, username: user.username, email: user.email },
    });
  } catch (err) {
    console.error("LOGIN error:", err);
    return res.status(500).json({ error: "Internal server error." });
  }
});


// POST /auth/logout -> destroys session

router.post("/logout", (req, res) => {
  req.session.destroy((err) => {
    if (err) {
      console.error("LOGOUT error:", err);
      return res.status(500).json({ error: "Failed to logout." });
    }
    // clear cookie on client
    res.clearCookie("sid");
    return res.json({ ok: true });
  });
});


// GET /auth/me -> returns session user if logged in

router.get("/me", async (req, res) => {
  try {
    if (!req.session.userId) {
      return res.status(401).json({ loggedIn: false });
    }

    const result = await pool.query(
      `SELECT id, username, email, last_login_at
       FROM users
       WHERE id = $1
       LIMIT 1;`,
      [req.session.userId]
    );

    if (result.rowCount === 0) {
      return res.status(401).json({ loggedIn: false });
    }

    return res.json({
      loggedIn: true,
      user: result.rows[0],
    });
  } catch (err) {
    console.error("ME error:", err);
    return res.status(500).json({ error: "Internal server error." });
  }
});

module.exports = router;
