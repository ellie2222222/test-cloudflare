require("dotenv").config();
const express = require("express");
const cors = require("cors");
const app = express();
const server = require("http").createServer(app);

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

app.use("/api/webhooks/cloudflare", () => {
    console.log("test")
})

// Start server
const port = process.env.DEVELOPMENT_PORT || 3000;

server.listen(port, (err) => {
  if (err) {
    console.log("Failed to start server:", err);
    process.exit(1);
  } else {
    console.log(`Server is running at: http://localhost:${port}`);
  }
});
