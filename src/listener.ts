import nats from 'node-nats-streaming';

console.clear();

//client
const stan = nats.connect('ticketing', '123', {
  url: 'http://localhost:4222'
});

stan.on('connect', () => {
  console.log('Listener connected to nats');

  const subscription = stan.subscribe('ticket:created');

  subscription.on('message', (msg) => {
    console.log('message received successfully');
  });
});
