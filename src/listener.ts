import nats, { Message } from 'node-nats-streaming';
import { randomBytes } from 'crypto';

console.clear();

//client
const stan = nats.connect('ticketing', randomBytes(4).toString('hex'), {
  url: 'http://localhost:4222'
});

stan.on('connect', () => {
  console.log('Listener connected to nats');

  stan.on('close', () => {
    console.log('NATS connection closed!');
    process.exit();
  });

  const options = stan
    .subscriptionOptions()
    .setManualAckMode(true)
    .setDeliverAllAvailable()
    // .setStartWithLastReceived()
    .setDurableName('orders-service');
  const subscription = stan.subscribe(
    'ticket:created',
    // queue group used for
    // 1 - if we have 2 listeners with same queue group message will be consumed in one of them using round robin
    // 2 - persist durable name subscription as if we restart listener durable name will be removed & will consume all messages from beginning
    'orders-service-queue-group',
    options
  );

  subscription.on('message', (msg: Message) => {
    const data = msg.getData();

    if (typeof data === 'string') {
      console.log(`Received event #${msg.getSequence()}, with data ${data}}`);
    }

    msg.ack();
  });
});

process.on('SIGINT', () => stan.close());
process.on('SIGTERM', () => stan.close());
