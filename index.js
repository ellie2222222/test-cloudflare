require("dotenv").config();
const express = require("express");
const cors = require("cors");
const os = require("os");
const { sendToQueue, getMessage, uploadToBunnyCDN } = require("./utils");
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
  const queueName = `bunny_livestream_${process.env.RABBITMQ_PREFIX}`;

  console.log(data.data.event_type);
  const ip = getServerIpAddress();
  console.log(ip);

  try {
    await sendToQueue(queueName, data);

    res.status(200).json({ message: "OK" });
  } catch (error) {
    res.status(500).json({ error: "Failed to process webhook" });
  }
})

const getServerIpAddress = () => {
  const networkInterfaces = os.networkInterfaces();
  for (const interface in networkInterfaces) {
      for (const addr of networkInterfaces[interface]) {
          // Check for IPv4 address that is not internal (loopback)
          if (addr.family === 'IPv4' && !addr.internal) {
              return addr.address;
          }
      }
  }
  return '127.0.0.1'; // Fallback to localhost
};

// getMessage(`bunny_livestream_${process.env.RABBITMQ_PREFIX}`);

// Start server
const port = process.env.DEVELOPMENT_PORT || 3000;

app.listen(port, (err) => {
  if (err) {
    console.log("Failed to start server:", err);
    process.exit(1);
  } else {
    console.log(`Server is running at: http://localhost:${port}`);
  }
});
