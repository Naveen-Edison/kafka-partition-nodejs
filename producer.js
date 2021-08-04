const { Kafka } = require('kafkajs');

const kafka = new Kafka({
    clientId: 'my-app',
    brokers: ['localhost:9092']
});

const topicName = 'orderbook';

const processProducer  = async () => {
    const producer = kafka.producer();
    await producer.connect();
    // const msg = JSON.stringify({customerId: 1, orderId: 1});
    let i=0;
    for (let i = 0; i < 10; i++) {
        await producer.send({
            topic: topicName,
            messages: [
                { value: JSON.stringify({customerId: 1, orderId: i}) },
            ],
        });
    }
};

processProducer().then(() => {
    console.log('done');
    process.exit();
});