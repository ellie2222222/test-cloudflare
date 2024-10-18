const amqp = require("amqplib");
const fs = require('fs');
const path = require('path');
const https = require("https");
const { spawn } = require('child_process');
const moment = require("moment");
const { default: axios } = require("axios");

// RabbitMQ connection URL
const rabbitMQUrl = `amqp://${process.env.RABBITMQ_USER}:${process.env.RABBITMQ_PASS}@${process.env.RABBITMQ_URL}` || `amqp://livestream_1:DMCF5qyDg6wx2g3m8n@62.77.156.171`;

// Function to start processing videos from RabbitMQ queue
const startProcessingQueueBunny = async (socketNamespace, userSocketMap) => {
    try {
        const connection = await amqp.connect(rabbitMQUrl);
        const channel = await connection.createChannel();
        const queue = `video_processing_bunny${process.env.RABBITMQ_PREFIX}`;

        await channel.assertQueue(queue, { durable: true });
        const concurrentTasks = 10;
        channel.prefetch(concurrentTasks);

        channel.consume(queue, async (msg) => {
            if (msg !== null) {
                try {
                    const message = JSON.parse(msg.content.toString());
                    const { videoUrl, m3u8Path, userId } = message;
                    const fileNameWithExtension = path.basename(videoUrl);
                    const videoId = path.parse(fileNameWithExtension).name;

                    console.log('Processing video for userId:', userId);

                    const socketId = userSocketMap.get(userId);
                    const videoFilePath = path.resolve(`./uploads/${userId}/raw/`, path.basename(videoId));
                    const outputDir = path.resolve(`./uploads/${m3u8Path}`);
                    const outputFileName = `${path.basename(videoFilePath, path.extname(videoFilePath))}.m3u8`;

                    fs.mkdirSync(outputDir, { recursive: true });

                    await convertVideoToM3U8(videoUrl, outputDir, outputFileName);
                    await uploadDirectoryToBunnyCDN(outputDir, userId, videoId, socketNamespace, socketId, m3u8Path);

                    // Clean up processed files
                    fs.rmSync(outputDir, { recursive: true, force: true });
                    deleteFolderContents(`./uploads/${userId}/raw/`);

                    // Notify client
                    // socketNamespace.to(socketId).emit('videoUploadComplete', { videoId: outputFileName, userId });

                    // Acknowledge message from RabbitMQ
                    channel.ack(msg);
                } catch (err) {
                    console.error('Failed to process video:', err);
                    // Handle error
                }
            }
        });
    } catch (error) {
        console.error('Failed to start processing queue:', error);
    }
};

// Function to send message to RabbitMQ queue
const sendToQueue = async (queueName, message) => {
    try {
        const connection = await amqp.connect(rabbitMQUrl);
        const channel = await connection.createChannel();

        await channel.assertQueue(queueName, { durable: true });
        const concurrentTasks = 10;
        channel.prefetch(concurrentTasks);

        channel.sendToQueue(queueName, Buffer.from(JSON.stringify(message)), {
            persistent: true,
        });

        setTimeout(() => {
            channel.close();
            connection.close();
        }, 500);
    } catch (error) {
        console.error("Error sending to RabbitMQ:", error);
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
                const messageContent = msg.content.toString();
                const parsedMessage = JSON.parse(messageContent);
                console.log(`Parsed Message: `, parsedMessage);

                // Extract event type and input id
                const id = parsedMessage.data?.input_id;
                const event = parsedMessage.data?.event_type;

                // Retrieve stream key using input id
                const stream = await retrieveCloudFlareStreamLiveInput(id);
                const streamId = stream?.srtPlayback?.streamId;
                const passphrase = stream?.srtPlayback?.passphrase;

                const streamServer = `srt://live.cloudflare.com:778?passphrase=${passphrase}&streamid=${streamId}`
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
                        console.log("Cloudflare Error")
                        console.log("Error");
                        break;
                    default:
                        console.log("Default: ", event)
                }

                channel.ack(msg);
            }
        });
    } catch (error) {
        console.error("Error consuming from RabbitMQ:", error);
    }
};

const liveStreamDir = path.resolve('./live-stream');
if (!fs.existsSync(liveStreamDir)) {
    fs.mkdirSync(liveStreamDir, { recursive: true });
}
const pidDir = path.resolve(path.join(liveStreamDir, 'pid'));
if (!fs.existsSync(pidDir)) {
    fs.mkdirSync(pidDir, { recursive: true });
}

// Start FFmpeg process
const startFFmpeg = (streamUrl, output) => {
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

        const ffmpeg = spawn('ffmpeg', [
            '-i', streamUrl,
            '-c:v', 'copy',
            '-c:a', 'copy',
            '-f', 'hls',
            '-hls_time', '2',
            '-hls_list_size', '3',
            '-hls_flags', 'split_by_time',
            '-hls_segment_filename', segmentPath,
            '-tune', 'zerolatency',
            outputPath
        ], { detached: true, stdio: 'ignore' });

        ffmpeg.unref();

        // Store the PID of the process
        if (!fs.existsSync(pidDir)) {
            fs.mkdirSync(pidDir, { recursive: true });
        }
        const pidFilePath = path.join(pidDir, `ffmpeg-${output}-pid.pid`);
        fs.writeFileSync(pidFilePath, ffmpeg.pid.toString());

        console.log(`FFmpeg started with PID: ${ffmpeg.pid}`);

        ffmpeg.on('exit', async (code) => {
            try {
                // Upload to Bunny Storage
                console.log("Exiting FFmpeg...")
                await stopFFmpeg(output, true);

                console.log(`FFmpeg process with PID ${ffmpeg.pid} exited with code ${code} and PID file deleted`);
            } catch (err) {
                console.error(`Error:`, err);
            }
        });

        ffmpeg.on('error', (error) => {
            console.log(error)
        })
    } catch (error) {
        console.log(error)
    }
};

// Stop FFmpeg process
const stopFFmpeg = async (identifier, hasEndTag) => {
    try {
        const pidFilePath = path.join(pidDir, `ffmpeg-${identifier}-pid.pid`);

        // Read pid file
        const pid = fs.readFileSync(pidFilePath, 'utf8');

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
            fs.appendFileSync(m3u8FilePath, '\n#EXT-X-ENDLIST\n', 'utf8');
        }

        replaceTsFileLocalPath(filePath, identifier);
        // await replaceTsFilePath(filePath, identifier);
        // await uploadToBunnyCDN(filePath, identifier, path.basename(filePath));
        // await uploadTsFiles(identifier);
        // await purgeBunnyCDNCache();
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
    console.log(options.path);

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

    const regex = new RegExp(
        `${identifier}-segment-\\d+\\.ts`,
        "g"
    );

    m3u8Content = m3u8Content.replace(regex, (match) => {
        return `${cdnUrl}/${match}`;
    });

    fs.writeFileSync(m3u8FilePath, m3u8Content);
};

const replaceTsFileLocalPath = async (m3u8FilePath, identifier) => {
    const url = `http://localhost:3000/live-stream/894367337b8247671f78fa82e424134f/${identifier}-segment-%03d.ts`;
    let m3u8Content = fs.readFileSync(m3u8FilePath, "utf8");

    const regex = new RegExp(
        `${identifier}-segment-\\d+\\.ts`,
        "g"
    );

    m3u8Content = m3u8Content.replace(regex, (match) => {
        return `${url}/${match}`;
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
}

// watcher
//   .on('add', async (filePath) => {
//     console.log(`File added: ${filePath}`);

//     if (filePath.endsWith('.m3u8')) {
//       // Handle M3U8 file addition
    //   await replaceTsFilePath(filePath, identifier);
    //   await uploadToBunnyCDN(filePath, identifier, path.basename(filePath));
    //   await uploadTsFiles(identifier); // Upload TS files if necessary
    //   await purgeBunnyCDNCache(); // Clear BunnyCDN cache
    // } else if (filePath.endsWith('.ts')) {
//       // Handle TS file addition
//       await uploadToBunnyCDN(filePath, identifier, path.basename(filePath));
//     }
//   })
//   .on('change', async (filePath) => {
//     console.log(`File modified: ${filePath}`);

//     if (filePath.endsWith('.m3u8') || filePath.endsWith('.ts')) {
//       // Upload the modified M3U8 or TS file
//       await uploadToBunnyCDN(filePath, identifier, path.basename(filePath));
//     }
//   })
//   .on('error', (error) => console.error(`Watcher error: ${error.message}`));

// function stopWatcher(watcher) {
//     watcher.close().then(() => console.log('Watcher stopped.'));
// }

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

        await axios.request(options)
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
