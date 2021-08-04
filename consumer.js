const { Kafka } = require('kafkajs');

const kafka = new Kafka({
    clientId: 'my-app',
    brokers: ['localhost:9092']
});

const topicName = 'orderbook';
const consumerNumber = process.argv[2] || '1';

const processConsumer  = async () => {
    const ordersConsumer = kafka.consumer({groupId: 'orders'});
    const ordersTwoConsumer = kafka.consumer({groupId: 'orders'});
    const ordersThreeConsumer = kafka.consumer({groupId: 'orders'});
    await Promise.all([
        ordersConsumer.connect(),
        ordersTwoConsumer.connect(),
        ordersThreeConsumer.connect(),
    ]);

    await Promise.all([
        await ordersConsumer.subscribe({ topic: topicName }),
        await ordersTwoConsumer.subscribe({ topic: topicName }),
        await ordersThreeConsumer.subscribe({ topic: topicName }),
    ]);

    let orderCounter = 1;
    let paymentCounter = 1;
    let notificationCounter = 1;
    await ordersConsumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            logMessage(orderCounter, `ordersConsumer#${consumerNumber}`, topic, partition, message);
            orderCounter++;
        },
    });
    await ordersTwoConsumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            logMessage(paymentCounter, `ordersTwoConsumer#${consumerNumber}`, topic, partition, message);
            paymentCounter++;
        },
    });
    await ordersThreeConsumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            logMessage(notificationCounter, `ordersThreeConsumer#${consumerNumber}`, topic, partition, message);
            notificationCounter++;
        },
    });

};

const logMessage = (counter, consumerName, topic, partition, message) => {
    console.log(`received a new message number: ${counter} on ${consumerName}: `, {
        topic,
        partition,
        message: {
            offset: message.offset,
            headers: message.headers,
            value: message.value.toString()
        },
    });
};

processConsumer();