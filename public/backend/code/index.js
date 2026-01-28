const express = require("express");
const cors = require("cors");
const path = require("path");

require("dotenv").config();

const { pool } = require("./db");

const app = express();
const PORT = process.env.PORT || 3000;


// Middlewares
app.use(cors());
app.use(express.json());

app.get("/", (req, res) => {
  res.sendFile(path.join(__dirname, "..", "..", "frontend", "index.html"));
});

// Healthcheck endpoint
app.get("/health", async (req, res) => {
  try {
    const result = await pool.query("SELECT 1;");
    res.json({ status: "ok", db: "connected" });
  } catch (err) {
    console.error(err);
    res.status(500).json({ status: "error", db: "disconnected" });
  }
});

// Articles API
const articlesRouter = require("./routes/articles");
app.use("/articles", articlesRouter);

// Feed API
const feedRouter = require("./routes/feed");
app.use("/feed", feedRouter);

// Serve frontend static files
app.use(express.static(path.join(__dirname, "../..", "frontend")));  

// Start server
app.listen(PORT, () => {
  console.log(`Server listening on http://localhost:${PORT}`);
});
