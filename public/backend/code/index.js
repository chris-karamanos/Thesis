const express = require("express");
const cors = require("cors");
const path = require("path");
const session = require("express-session");
const pgSession = require("connect-pg-simple")(session);

require("dotenv").config();

const { pool } = require("./db");

const app = express();
const PORT = process.env.PORT || 3000;


// middlewares
app.use(cors());
app.use(express.json());

app.use(session({
  store: new pgSession({
    pool: pool,
    tableName: "session",
  }),
  name: "sid",
  secret: process.env.SESSION_SECRET,
  resave: false,
  saveUninitialized: false,
  cookie: {
    maxAge: 60 * 60 * 1000,
    httpOnly: true,
    sameSite: "lax",
    secure: false, // true only on HTTPS
  },
}));


// feed API
const feedRouter = require("./routes/feed");
app.use("/api/feed", feedRouter);

// login API
const userLog = require('./routes/user_log');
app.use('/auth', userLog);


// serve frontend static files
app.use(express.static(path.join(__dirname, "..", "..", "frontend"))); 


app.get("/", (req, res) => {
  res.redirect("/feed/");
});

app.get("/login", (req, res) => {
  res.sendFile(path.join(__dirname, "..", "..", "frontend", "login", "login.html"));
});


// healthcheck endpoint
app.get("/health", async (req, res) => {
  try {
    const result = await pool.query("SELECT 1;");
    res.json({ status: "ok", db: "connected" });
  } catch (err) {
    console.error(err);
    res.status(500).json({ status: "error", db: "disconnected" });
  }
});
 

// start server
app.listen(PORT, () => {
  console.log(`Server listening on http://localhost:${PORT}`);
});
