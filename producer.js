"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const kafkajs_1 = require("kafkajs");
const cloudevents_1 = require("cloudevents");
const input_prompter_1 = require("./utils/input-prompter");
// Define the CloudEvent
const event = new cloudevents_1.CloudEvent({
    specversion: "1.0",
    type: "com.example.someevent",
    source: "/mycontext/subcontext",
    id: "1234-1234-1234",
    time: new Date().toISOString(),
    dataschema: "http://my-schema",
    datacontenttype: "application/cloudevents+json; charset=UTF-8",
    data: {
        message: "Hello, world!",
    },
});
// Convert the CloudEvent to a Kafka message
const message = {
    key: event.id,
    value: JSON.stringify(event),
    headers: {
        "content-type": event.datacontenttype,
    },
};
// Send the message to Kafka
async function sendMessage() {
    try {
        // Prompt for Kafka connection details
        const inputPrompter = new input_prompter_1.InputPrompter();
        const kafkaTopic = await inputPrompter.prompt("Enter the Kafka topic: ");
        const kafkaUsername = await inputPrompter.prompt("Enter the Kafka producer username: ");
        const kafkaPassword = await inputPrompter.prompt("Enter the Kafka producer password: ");
        const kafkaBrokers = await inputPrompter.prompt("Enter the Kafka brokers (comma separated): ");
        inputPrompter.close();
        // Initialize Kafka client
        const kafka = new kafkajs_1.Kafka({
            logLevel: kafkajs_1.logLevel.INFO,
            brokers: kafkaBrokers.split(","),
            clientId: "producer-sample-javascript",
            ssl: true,
            sasl: {
                mechanism: "scram-sha-512",
                username: kafkaUsername,
                password: kafkaPassword,
            },
        });
        // Create a Kafka producer instance
        const producer = kafka.producer();
        await producer.connect();
        const response = await producer.send({
            topic: kafkaTopic,
            messages: [message],
        });
        await producer.disconnect();
        console.log(`Sent message: ${JSON.stringify(response[0])}`);
    }
    catch (error) {
        throw new Error(error.message);
    }
}
sendMessage().catch((error) => {
    console.log("\n------------------ ERROR -------------------\n");
    console.error(`${error}\n`);
});
