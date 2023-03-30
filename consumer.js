"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const kafkajs_1 = require("kafkajs");
const cloudevents_1 = require("cloudevents");
const commander_1 = require("commander");
// Define the expected command line arguments
const program = new commander_1.Command();
program
    .requiredOption("-t, --topic <topic>", "The name of the topic to subscribe to.")
    .requiredOption("-u, --username <username>", "The username to use for authentication with Kafka.")
    .requiredOption("-p, --password <password>", "The password to use for authentication with Kafka.")
    .requiredOption("-b, --brokers <brokers>", "The Kafka brokers to connect to should be specified as a comma-separated list of <host>:<port> pairs. For example: kafka1:9092,kafka2:9092.");
program.parse(process.argv);
const options = program.opts();
const { username, password, topic, brokers } = options;
program.parse();
async function runConsumer() {
    try {
        // Define the Kafka broker and topic
        const kafka = new kafkajs_1.Kafka({
            logLevel: kafkajs_1.logLevel.INFO,
            brokers: brokers.split(","),
            clientId: "consumer-sample-javascript",
            ssl: true,
            sasl: {
                mechanism: "scram-sha-512",
                username,
                password,
            },
        });
        // Create a Kafka consumer instance
        const consumer = kafka.consumer({ groupId: "test-group" });
        // Subscribe to topic
        await consumer.connect();
        await consumer.subscribe({ topic: topic, fromBeginning: true });
        console.log(`\nListening to topic: "${topic}"...\n`);
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
                console.log("------------------- End -----------------------\n");
                console.log(`Listening to topic: "${topic}"...\n`);
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
