const amqp = require("amqplib");
const fs = require('fs');
const path = require('path');
const https = require("https");
const { spawn } = require('child_process');
const moment = require("moment");
const { default: axios } = require("axios");
const chokidar = require("chokidar");

// RabbitMQ connection URL
const rabbitMQUrl = `amqp://${process.env.RABBITMQ_USER}:${process.env.RABBITMQ_PASS}@${process.env.RABBITMQ_URL}` || `amqp://livestream_1:DMCF5qyDg6wx2g3m8n@62.77.156.171`;

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

module.exports = { sendToQueue };
