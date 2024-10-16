const amqp = require("amqplib");

// Hàng đợi để xử lý chuyển đổi và upload
const startProcessingQueueBunny = async (socketNamespace, userSocketMap) => {
    try {
        const connection = await amqp.connect({
            protocol: 'amqp',
            hostname: process.env.RABBITMQ_URL,
            username: process.env.RABBITMQ_USER,
            password: process.env.RABBITMQ_PASS,
        });

        const channel = await connection.createChannel();
        const queue = `video_processing_bunny${process.env.RABBITMQ_PREFIX}`;

        await channel.assertQueue(queue, { durable: true });
        const concurrentTasks = 10;
        channel.prefetch(concurrentTasks);

        channel.consume(queue, async (msg) => {
            if (msg !== null) {
                const message = JSON.parse(msg.content.toString());
                const { videoUrl, m3u8Path, userId } = message;

                const fileNameWithExtension = path.basename(videoUrl); 

                const videoId = path.parse(fileNameWithExtension).name;

                console.log('Processing video for userId:', videoUrl);

                const socketId = userSocketMap.get(userId); 

                const videoFilePath = path.resolve(`./uploads/${userId}/raw/`, path.basename(videoId));
                const outputDir = path.resolve(`./uploads/${m3u8Path}`);
                const outputFileName = `${path.basename(videoFilePath, path.extname(videoFilePath))}.m3u8`;

                fs.mkdirSync(outputDir, { recursive: true });

                try {
                    await convertVideoToM3U8(videoUrl, outputDir, outputFileName);
                    await uploadDirectoryToBunnyCDN(outputDir, userId, videoId, socketNamespace, socketId, m3u8Path);

                    try {

                        console.log("delete file m3u8", outputDir);
                        // Xóa tất cả tệp trong thư mục
                        fs.rmSync(outputDir, { recursive: true, force: true });
                        console.log("delete file gốc", outputDir);
                        deleteFolderContents(`./uploads/${userId}/raw/`);
                        console.log("delete file done");
                        // Gửi sự kiện cho client có userId
                        // socketNamespace.to(socketId).emit('videoUploadComplete', { videoId: outputFileName, userId });
                    } finally {
                        channel.ack(msg);
                        console.log("queue channel.ack(msg);");

                    }
                } catch (err) {
                    console.error('Failed to process video:', err);
                }

            }
        });
    } catch (error) {
        console.error('Failed to start processing queue:', error);
    }
};

const rabbitMQUrl = `amqp://${process.env.RABBITMQ_USER}:${process.env.RABBITMQ_PASS}@${process.env.RABBITMQ_URL}:${process.env.RABBITMQ_PORT}`;

// Helper function to send message to RabbitMQ
async function sendToQueue(queueName, message) {
  try {
    const connection = await amqp.connect(rabbitMQUrl);
    const channel = await connection.createChannel();

    // Ensure the queue exists
    await channel.assertQueue(queueName, { durable: true });

    // Send the message
    channel.sendToQueue(queueName, Buffer.from(JSON.stringify(message)), {
      persistent: true,
    });

    console.log(`[x] Sent to queue ${queueName}:`, message);

    // Close the connection after a short delay
    setTimeout(() => {
      channel.close();
      connection.close();
    }, 500);
  } catch (error) {
    console.error("Error sending to RabbitMQ:", error);
  }
}

module.exports = { sendToQueue };