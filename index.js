require("dotenv").config();
const express = require("express");
const cors = require("cors");
const path = require("path");
const { sendToQueue } = require("./utils");
const app = express();

// Middleware
app.use(
  cors({
    origin: "*",
    methods: ["GET", "HEAD", "PUT", "PATCH", "POST", "DELETE"],
    allowedHeaders: ["Content-Type", "Authorization"],
  })
);

app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// Log API requests
app.use((req, res, next) => {
  console.log(req.method + " " + req.path);
  next();
});

app.post("/api/webhooks/cloudflare", async (req, res) => {
  const data = req.body;
  const queueName = `cloudflare.livestream`;

  try {
    await sendToQueue(queueName, data);

    res.status(200).json({ message: "OK" });
  } catch (error) {
    res.status(500).json({ error: "Failed to process webhook" });
  }
})

getMessage(`cloudflare.livestream`);
getMessage(`bunny_livestream`);

// Start server
const port = process.env.DEVELOPMENT_PORT || 3101;

app.listen(port, (err) => {
  if (err) {
    console.log("Failed to start server:", err);
    process.exit(1);
  } else {
    console.log(`Server is running at: http://localhost:${port}`);
  }
});
