const amqp = require("amqplib");
const fs = require("fs");
const path = require("path");
const https = require("https");
const { spawn } = require("child_process");
const moment = require("moment");
const { default: axios } = require("axios");
const chokidar = require("chokidar");
require("dotenv").config();

// RabbitMQ connection URL
const rabbitMQUrl =
  `amqp://${process.env.RABBITMQ_USER}:${process.env.RABBITMQ_PASS}@${process.env.RABBITMQ_URL}` ||
  `amqp://livestream_1:DMCF5qyDg6wx2g3m8n@62.77.156.171`;
const developmentPort = process.env.DEVELOPMENT_PORT || "3101";

const liveStreamDir = path.resolve("./live-stream");
if (!fs.existsSync(liveStreamDir)) {
  fs.mkdirSync(liveStreamDir, { recursive: true });
}
const pidDir = path.resolve(path.join(liveStreamDir, "pid"));
if (!fs.existsSync(pidDir)) {
  fs.mkdirSync(pidDir, { recursive: true });
}

// Function to send message to RabbitMQ queue
const sendToQueue = async (queueName, message) => {
  console.log("Sending queue: ", queueName);
  try {
    const connection = await amqp.connect(rabbitMQUrl);
    const channel = await connection.createChannel();

    await channel.assertQueue(queueName, { durable: true });

    channel.sendToQueue(queueName, Buffer.from(JSON.stringify(message)), {
      persistent: true,
    });

    await channel.close();
    await connection.close();
  } catch (error) {
    console.error("Error sending to RabbitMQ:", error);
    throw error;
  }
};

// Function to process messages from RabbitMQ
const getMessage = async (queueName) => {
  try {
    const connection = await amqp.connect(rabbitMQUrl);
    const channel = await connection.createChannel();

    await channel.assertQueue(queueName, { durable: true });

    channel.consume(queueName, async (msg) => {
      if (msg !== null) {
        try {
          const messageContent = msg.content.toString();
          const parsedMessage = JSON.parse(messageContent);

          console.log(`Cloudflare Event: `, parsedMessage.data?.event_type);

          const id = parsedMessage.data?.input_id;
          switch (queueName) {
            case "cloudflare.livestream":
              // Extract event type
              const event = parsedMessage.data?.event_type;

              // Retrieve stream key using input id
              const stream = await retrieveCloudFlareStreamLiveInput(id);
              const streamId = stream?.srtPlayback?.streamId;
              const passphrase = stream?.srtPlayback?.passphrase;

              const streamServer = `srt://live.cloudflare.com:778?passphrase=${passphrase}&streamid=${streamId}`;
              switch (event) {
                case "live_input.connected":
                  console.log("Connected");
                  startFFmpeg(streamServer, id);
                  break;
                case "live_input.disconnected":
                  console.log("Disconnected");
                  stopFFmpeg(id, false);
                  break;
                case "live_input.errored":
                  console.log("Cloudflare Error");
                  console.log("Error");
                  break;
                default:
                  console.log("Default: ", event);
              }
              break;
            case `${process.env.RABBITMQ_PREFIX_BUNNYUPLOAD}`:
              console.log("Uploading to Bunny...");
              const identifier = id;
              const outputDir = path.join(liveStreamDir, identifier);

              // Define m3u8 file name
              const m3u8FileName = `${identifier}.m3u8`;

              const filePath = path.join(outputDir, m3u8FileName);
              await replaceTsFilePath(filePath, identifier);
              await uploadToBunnyCDN(
                filePath,
                identifier,
                path.basename(filePath)
              );
              await uploadTsFiles(identifier);

              await sendToQueue("live_stream.disconnected", {
                live_input_id: identifier,
              });
              break;
            default:
              console.log("Default event");
          }

          channel.ack(msg);
        } catch (error) {
          console.error(`Error while processing message: ${error}`);
          channel.nack(msg);
        }
      }
    });
  } catch (error) {
    console.error("Error consuming from RabbitMQ:", error);
  }
};

// Start FFmpeg process
const startFFmpeg = async (streamUrl, output) => {
  try {
    const outputDir = path.join(liveStreamDir, output);
    if (!fs.existsSync(outputDir)) {
      fs.mkdirSync(outputDir, { recursive: true });
    }

    // Define m3u8 file name
    const outputFileName = `${output}.m3u8`;
    const outputPath = path.join(outputDir, outputFileName);

    // Define ts file name
    const segmentPath = path.join(outputDir, `${output}-segment-%03d.ts`);

    // Send to queue live event
    await sendToQueue("live_stream.connected", { live_input_id: output });

    const ffmpeg = spawn(
      "ffmpeg",
      [
        "-i",
        streamUrl,
        "-c:v",
        "copy",
        "-c:a",
        "copy",
        "-f",
        "hls",
        "-hls_time",
        "1",
        "-hls_list_size",
        "3",
        "-hls_flags",
        "split_by_time",
        "-hls_segment_filename",
        segmentPath,
        "-tune",
        "zerolatency",
        outputPath,
      ],
      { detached: true, stdio: "ignore" }
    );

    ffmpeg.unref();

    // Store the PID of the process
    if (!fs.existsSync(pidDir)) {
      fs.mkdirSync(pidDir, { recursive: true });
    }
    const pidFilePath = path.join(pidDir, `ffmpeg-${output}-pid.pid`);
    fs.writeFileSync(pidFilePath, ffmpeg.pid.toString());

    console.log(`FFmpeg started with PID: ${ffmpeg.pid}`);

    ffmpeg.on("exit", async (code) => {
      try {
        console.log("Exiting FFmpeg...");
        await stopFFmpeg(output, true);

        console.log(
          `FFmpeg process with PID ${ffmpeg.pid} exited with code ${code} and PID file deleted`
        );
      } catch (err) {
        console.error(`Error:`, err);
      }
    });

    ffmpeg.on("error", (error) => {
      console.log(error);
    });
  } catch (error) {
    console.log(error);
  }
};

// Stop FFmpeg process
const stopFFmpeg = async (identifier, hasEndTag) => {
  try {
    const pidFilePath = path.join(pidDir, `ffmpeg-${identifier}-pid.pid`);

    // Read pid file
    const pid = fs.readFileSync(pidFilePath, "utf8");

    // Stop the FFmpeg process
    process.kill(pid);
    fs.unlinkSync(pidFilePath);
    console.log(`FFmpeg process with PID ${pid} stopped`);

    // Find the most recent .m3u8 file in the output directory
    const outputDir = path.join(liveStreamDir, identifier);
    // Define m3u8 file name
    const m3u8FileName = `${identifier}.m3u8`;
    if (!hasEndTag) {
      const m3u8FilePath = path.join(outputDir, m3u8FileName);
      fs.appendFileSync(m3u8FilePath, "#EXT-X-ENDLIST", "utf8");
    }
  } catch (err) {
    console.error(`Failed to stop FFmpeg for ${identifier}:`, err.message);
  }
};

const uploadToBunnyCDN = async (filePath, identifier, fileName) => {
  const readStream = fs.createReadStream(filePath);
  const storageZone = process.env.BUNNY_STORAGE_ZONE_NAME;
  const path = `/${storageZone}/video/${identifier}/${fileName}`;

  const options = {
    method: "PUT",
    host: "storage.bunnycdn.com",
    path: path,
    headers: {
      AccessKey: process.env.BUNNY_STORAGE_PASSWORD,
      "Content-Type": "application/octet-stream",
      "Cache-Control": "no-store, no-cache, must-revalidate, proxy-revalidate", // Disable caching
      Expires: "0",
      Pragma: "no-cache",
    },
  };

  const req = https.request(options, (res) => {
    res.on("data", (chunk) => {
      console.log(chunk.toString("utf8"));
    });
  });

  req.on("error", (error) => {
    console.error(error);
  });

  readStream.pipe(req);
};

// Function to delete folder or file from BunnyCDN
const deleteFromBunnyCDN = async (folder, fileName) => {
  const storageZone = process.env.BUNNY_STORAGE_ZONE_NAME;

  let path = "";
  if (fileName) {
    path = `/${storageZone}/video/${folder}/${fileName}`;
  } else {
    path = `/${storageZone}/video/${folder}/`;
  }

  const options = {
    method: "DELETE",
    host: "storage.bunnycdn.com",
    path: path,
    headers: {
      AccessKey: process.env.BUNNY_STORAGE_PASSWORD,
    },
  };

  return new Promise((resolve, reject) => {
    const req = https.request(options, (res) => {
      let responseBody = "";
      res.on("data", (chunk) => {
        responseBody += chunk.toString();
      });
      res.on("end", () => {
        if (res.statusCode === 404) {
          resolve("Folder or file not found");
        } else if (res.statusCode === 200) {
          console.log(`Deleted ${folder}/${fileName}: ${responseBody}`);
          resolve(responseBody);
        } else {
          reject(
            new Error(
              `Failed to delete ${folder}/${fileName}: ${res.statusCode}`
            )
          );
        }
      });
    });

    req.on("error", (error) => {
      reject(error);
    });

    req.end();
  });
};

const purgeBunnyCDNCache = async () => {
  const options = {
    method: "POST",
    host: "api.bunny.net",
    path: `/pullzone/${process.env.BUNNY_PULLZONE_ID}/purgeCache`,
    headers: {
      AccessKey: process.env.BUNNY_ACCOUNT_API_KEY,
    },
  };

  const req = https.request(options, (res) => {
    res.on("data", (chunk) => {
      console.log(chunk.toString("utf8"));
    });
  });

  req.on("error", (error) => {
    console.error(error);
  });

  req.end();
};

// Function to replace .ts file paths in .m3u8 with BunnyCDN URLs
const replaceTsFilePath = async (m3u8FilePath, identifier) => {
  const cdnUrl = `https://${process.env.BUNNY_DOMAIN}/video/${identifier}/`;
  let m3u8Content = fs.readFileSync(m3u8FilePath, "utf8");

  const regex = new RegExp(`${identifier}-segment-\\d+\\.ts`, "g");

  m3u8Content = m3u8Content.replace(regex, (match) => {
    return `${cdnUrl}/${match}`;
  });

  fs.writeFileSync(m3u8FilePath, m3u8Content);
};

// Function to upload .ts segment files
const uploadTsFiles = async (identifier) => {
  const outputDir = path.join(liveStreamDir, identifier);
  const files = fs.readdirSync(outputDir);
  for (const file of files) {
    if (file.endsWith(".ts") && file.includes(identifier)) {
      const fileName = path.basename(file);
      const filePath = path.join(outputDir, fileName);
      await uploadToBunnyCDN(filePath, identifier, fileName);
    }
  }
};

const watcher = chokidar.watch("./live-stream", {
  persistent: true,
  ignoreInitial: true,
  awaitWriteFinish: {
    stabilityThreshold: 1000,
    pollInterval: 100,
  },
});

let isUpdating = false;

watcher
  .on("add", async (filePath) => {
    if (filePath.endsWith(".m3u8") && isUpdating === false) {
      console.log(`File added: ${filePath}`);
      isUpdating = true;
      const folderName = path.basename(path.dirname(filePath));
      debounceReset();
    }
  })
  .on("change", async (filePath) => {
    if (filePath.endsWith(".m3u8") && isUpdating === false) {
      // console.log(`File modified: ${filePath}`);
      isUpdating = true;
      const folderName = path.basename(path.dirname(filePath));
      debounceReset();
    }
  })
  .on("error", (error) => console.error(`Watcher error: ${error.message}`));

const debounceTimeout = 2000;
const debounceReset = () => {
  setTimeout(() => {
    isUpdating = false;
  }, debounceTimeout);
};

function stopWatcher(watcher) {
  watcher.close().then(() => console.log("Watcher stopped."));
}

const retrieveCloudFlareStreamLiveInput = async (uid) => {
  try {
    let stream = null;
    let live = null;
    var options = {
      method: "GET",
      url: `${process.env.CLOUDFLARE_STREAM_API_URL}/accounts/${process.env.CLOUDFLARE_ACCOUNT_ID}/stream/live_inputs/${uid}`,
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${process.env.CLOUDFLARE_API_KEY}`,
      },
    };

    await axios
      .request(options)
      .then(async function (response) {
        stream = response.data.result;
      })
      .catch(function (error) {
        console.error("Error retrieving live input");
      });
    return stream;
  } catch (error) {
    throw error;
  }
};

module.exports = { sendToQueue, getMessage };
