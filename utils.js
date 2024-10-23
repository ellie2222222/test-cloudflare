const amqp = require("amqplib");
const fs = require('fs');
const path = require('path');
const https = require("https");
const { spawn } = require('child_process');
const moment = require("moment");
const { default: axios } = require("axios");
const chokidar = require("chokidar");
require("dotenv").config();

// RabbitMQ connection URL
const rabbitMQUrl = `amqp://${process.env.RABBITMQ_USER}:${process.env.RABBITMQ_PASS}@${process.env.RABBITMQ_URL}` || `amqp://livestream_1:DMCF5qyDg6wx2g3m8n@62.77.156.171`;
const developmentPort = process.env.DEVELOPMENT_PORT || "3101";

const liveStreamDir = path.resolve('./live-stream');
if (!fs.existsSync(liveStreamDir)) {
    fs.mkdirSync(liveStreamDir, { recursive: true });
}
const pidDir = path.resolve(path.join(liveStreamDir, 'pid'));
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
    console.log("Consuming queue: ", queueName);
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

                            const streamServer = `srt://live.cloudflare.com:778?passphrase=${passphrase}&streamid=${streamId}`
                            switch (event) {
                                case "live_input.connected":
                                    console.log("Connected");
                                    startFFmpeg(streamServer, id);
                                    startFFmpegFull(streamServer, id);
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
                            console.log("Uploading to Bunny...")
                            const identifier = id;
                            const outputDir = path.join(liveStreamDir, identifier + '_full');
                            if (!fs.existsSync(outputDir)) {
                                fs.mkdirSync(outputDir, { recursive: true });
                            }

                            // Define m3u8 file name
                            const m3u8FileName = `${identifier}_full.m3u8`;
                            const m3u8FilePath = path.join(outputDir, m3u8FileName);

                            handleStreamFinish(outputDir, m3u8FilePath, identifier);

                            await sendToQueue("live_stream.disconnected", {
                                live_input_id: identifier,
                                streamOnlineUrl: `https://${process.env.BUNNY_DOMAIN}/video/${identifier}/${m3u8FileName}`
                            });
                            break;
                        default:
                            console.log("Default event");
                    }

                    channel.ack(msg);
                } catch (error) {
                    console.error(`Error while processing message: ${error}`);
                    channel.nack(msg, true, false);
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

        const ffmpeg = spawn('ffmpeg', [
            '-i', streamUrl,
            '-c:v', 'copy',
            '-c:a', 'copy',
            '-f', 'hls',
            '-hls_time', '1',
            '-hls_list_size', '3',
            '-hls_flags', 'delete_segments',
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

         // Send to queue live event
        await sendToQueue("live_stream.connected", { 
            live_input_id: output,
            streamServerUrl: outputPath,
        });

        ffmpeg.on('exit', async (code) => {
            try {
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

const startFFmpegFull = async (streamUrl, output) => {
    try {
        const outputDir = path.join(liveStreamDir, output + '_full');
        if (!fs.existsSync(outputDir)) {
            fs.mkdirSync(outputDir, { recursive: true });
        }

        // Define m3u8 file name for the full segment list
        const outputFileName = `${output}_full.m3u8`;
        const outputPath = path.join(outputDir, outputFileName);

        // Define ts file name (same as in the original process)
        const segmentPath = path.join(outputDir, `${output}-segment-%03d.ts`);

        const ffmpegAll = spawn('ffmpeg', [
            '-i', streamUrl,
            '-c:v', 'copy',
            '-c:a', 'copy',
            '-f', 'hls',
            '-hls_time', '10',
            '-hls_list_size', '0',
            '-hls_flags', 'append_list',
            '-hls_segment_filename', segmentPath,
            '-tune', 'zerolatency',
            outputPath
        ], { detached: true, stdio: 'ignore' });

        ffmpegAll.unref();

        ffmpegAll.on('exit', async (code) => {
            try {
                console.log(`FFmpeg process (all segments) with PID ${ffmpegAll.pid} exited with code ${code} and PID file deleted`);
            } catch (err) {
                console.error(`Error:`, err);
            }
        });

        ffmpegAll.on('error', (error) => {
            console.log(error);
        });

    } catch (error) {
        console.log(error);
    }
};

// Function to delete extra ts files
const deleteExtraTSFiles = async (tsFiles, outputDir) => {
    try {
        const allFiles = fs.readdirSync(outputDir);
        allFiles.forEach(file => {
            if (file.endsWith('.ts') && !tsFiles.includes(file)) {
                fs.unlinkSync(path.join(outputDir, file));
                console.log(`Deleted extra file: ${file}`);
            }
        });
    } catch (error) {
        throw error;
    }
};

const removeTsEntries = async (m3u8File, tsFiles) => {
    try {
        const filePath = path.resolve(m3u8File);
        let lines = fs.readFileSync(filePath, 'utf-8').split('\n');

        let newLines = [];

        for (let i = 0; i < lines.length; i++) {
            let line = lines[i].trim();

            if (line.endsWith('.ts') && !tsFiles.includes(line)) {
                continue;
            }

            if (i < lines.length - 1 && lines[i + 1].trim().endsWith('.ts') && !tsFiles.includes(lines[i + 1].trim())) {
                continue;  // Skip the #EXTINF line
            }

            // Otherwise, add the line to newLines
            newLines.push(lines[i]);
        }

        // Write the updated content back to the M3U8 file
        fs.writeFileSync(filePath, newLines.join('\n'), 'utf-8');
    } catch (error) {
        throw error;
    }
}

// Main function to handle the stream finishing
const handleStreamFinish = async (outputDir, m3u8FilePath, identifier) => {
    try {
        const tsFiles = fs.readdirSync(outputDir).filter(file => file.endsWith('.ts'));
        const totalTSFiles = tsFiles.length;

        console.log(`Total .ts files: ${totalTSFiles}`);

        const m3u8FileName = `${identifier}_full.m3u8`;
        const filePath = path.join(outputDir, m3u8FileName);

        if (totalTSFiles > 30) {
            // Scenario 2: Select the middle 30 .ts files
            const middleIndex = Math.floor(totalTSFiles / 2);
            const start = Math.max(middleIndex - 15, 0);
            const selectedTSFiles = tsFiles.slice(start, start + 30);

            // Delete extra .ts files
            await deleteExtraTSFiles(selectedTSFiles, outputDir);

            // Delete ts file references in m3u8 file
            await removeTsEntries(m3u8FilePath, selectedTSFiles);
        }
        
        // Upload files
        await replaceTsFilePath(filePath, identifier);
        await uploadTsFiles(outputDir, identifier);
        await uploadToBunnyCDN(filePath, identifier, path.basename(filePath));
    } catch (error) {
        console.error("Error: ", error);
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
            fs.appendFileSync(m3u8FilePath, '#EXT-X-ENDLIST', 'utf8');
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

    const regex = new RegExp(
        `${identifier}-segment-\\d+\\.ts`,
        "g"
    );

    m3u8Content = m3u8Content.replace(regex, (match) => {
        return `${cdnUrl}/${match}`;
    });

    fs.writeFileSync(m3u8FilePath, m3u8Content);
};

// Function to upload .ts segment files
const uploadTsFiles = async (outputDir, identifier) => {
    const files = fs.readdirSync(outputDir);
    for (const file of files) {
        if (file.endsWith(".ts") && file.includes(identifier)) {
            const fileName = path.basename(file);
            const filePath = path.join(outputDir, fileName);
            await uploadToBunnyCDN(filePath, identifier, fileName);
        }
    }
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