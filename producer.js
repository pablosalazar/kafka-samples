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
    .requiredOption("-b, --brokers <brokers>", "The Kafka brokers to connect to should be specified as a comma-separated list of <host>:<port> pairs. For example: kafka1:9092,kafka2:9092.")
    .requiredOption("-m, --message <message>", "The CloudEvents message payload to send to Kafka.")
    .option("-v, --version <version>", "The CloudEvents specification version to use.", "1.0")
    .option("--datacontenttype <datacontenttype>", "The content type of the CloudEvents message data to send to Kafka.", "application/cloudevents+json; charset=UTF-8")
    .option("--source <source>", "The CloudEvents source attribute, representing the context of the event.", "/mycontext/subcontext")
    .option("--dataschema <dataschema>", "The CloudEvents data schema URL for validating the message data.", "http://my-schema")
    .option("--type <type>", "The CloudEvents type attribute, describing the type of event.", "com.example.myevent");
program.parse(process.argv);
const options = program.opts();
const { username, password, topic, brokers, message, version, datacontenttype, source, dataschema, type, } = options;
// Define the CloudEvent
const event = new cloudevents_1.CloudEvent({
    specversion: version,
    type,
    source,
    id: "1234-1234-1234",
    time: new Date().toISOString(),
    dataschema,
    datacontenttype,
    data: {
        message,
    },
});
// Convert the CloudEvent to a Kafka message
const formattedMessage = {
    key: event.id,
    value: JSON.stringify(event),
    headers: {
        "content-type": event.datacontenttype,
    },
};
// Send the message to Kafka
async function sendMessage() {
    try {
        // Initialize Kafka client
        const kafka = new kafkajs_1.Kafka({
            logLevel: kafkajs_1.logLevel.INFO,
            brokers: brokers.split(","),
            clientId: "producer-sample-javascript",
            ssl: true,
            sasl: {
                mechanism: "scram-sha-512",
                username,
                password,
            },
        });
        // Create a Kafka producer instance
        const producer = kafka.producer();
        await producer.connect();
        const response = await producer.send({
            topic,
            messages: [formattedMessage],
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
