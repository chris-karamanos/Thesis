const express = require("express");
const cors = require("cors");
require("dotenv").config();

const { pool } = require("./db");
const articlesRouter = require("./routes/articles");

const app = express();
const PORT = process.env.PORT || 3000;

// Middlewares
app.use(cors());
app.use(express.json());

app.get("/", (req, res) => {
  res.send("Welcome to the News Aggregator API");
});

// Healthcheck (για να δεις ότι ζει ο server)
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
app.use("/articles", articlesRouter);

// Start server
app.listen(PORT, () => {
  console.log(`Server listening on http://localhost:${PORT}`);
});
