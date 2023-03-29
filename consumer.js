"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const kafkajs_1 = require("kafkajs");
const cloudevents_1 = require("cloudevents");
const input_prompter_1 = require("./utils/input-prompter");
async function runConsumer() {
    try {
        // Prompt for Kafka connection details
        const inputPrompter = new input_prompter_1.InputPrompter();
        const kafkaTopic = await inputPrompter.prompt("Enter the Kafka topic: ");
        const kafkaUsername = await inputPrompter.prompt("Enter the Kafka consumer username: ");
        const kafkaPassword = await inputPrompter.prompt("Enter the Kafka consumer password: ");
        const kafkaBrokers = await inputPrompter.prompt("Enter the Kafka brokers (comma separated): ");
        inputPrompter.close();
        // Define the Kafka broker and topic
        const kafka = new kafkajs_1.Kafka({
            logLevel: kafkajs_1.logLevel.INFO,
            brokers: kafkaBrokers.split(","),
            clientId: "consumer-sample-javascript",
            ssl: true,
            sasl: {
                mechanism: "scram-sha-512",
                username: kafkaUsername,
                password: kafkaPassword,
            },
        });
        // Create a Kafka consumer instance
        const consumer = kafka.consumer({ groupId: "test-group" });
        // Subscribe to topic
        await consumer.connect();
        await consumer.subscribe({ topic: kafkaTopic, fromBeginning: true });
        console.log(`\nListening to topic: "${kafkaTopic}"...\n`);
        // Listen incoming Kafka messages
        await consumer.run({
            eachMessage: async ({ topic, partition, message, }) => {
                const event = new cloudevents_1.CloudEvent(JSON.parse(message?.value?.toString() || ""));
                console.log("\n------------------ Message -------------------\n");
                console.log(`Topic Name: ${topic}\n`);
                console.log("------------------- key ----------------------\n");
                console.log(`Key: ${message.key}\n`);
                console.log("------------------ headers -------------------\n");
                const headers = message.headers || {};
                Object.keys(headers).forEach((key) => {
                    console.log(`${key}: ${headers[key]}`);
                });
                console.log("\n------------------- value --------------------\n");
                console.log(JSON.stringify(event, null, 2));
                console.log("----------------------------------------------\n");
            },
        });
    }
    catch (error) {
        throw new Error(error.message);
    }
}
runConsumer().catch((error) => {
    console.log("\n------------------ ERROR -------------------\n");
    console.error(`${error}\n`);
});
