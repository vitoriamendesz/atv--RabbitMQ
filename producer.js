const amqp = require('amqplib');

// Simulação de banco de dados em memória
let items = [];

async function connectRabbitMQ() {
  const connection = await amqp.connect('amqp://localhost');
  const channel = await connection.createChannel();

  const queue = 'crud_operations';
  await channel.assertQueue(queue, { durable: true });

  console.log("Conectado ao RabbitMQ. Pronto para enviar mensagens.");
  return { connection, channel, queue };
}

async function sendMessage(action, item) {
  const { channel, queue } = await connectRabbitMQ();
  const message = JSON.stringify({ action, item });
  channel.sendToQueue(queue, Buffer.from(message));
  console.log(`Mensagem enviada: ${message}`);
}

// Funções CRUD
async function createItem(name) {
  const item = { id: items.length + 1, name };
  items.push(item);
  await sendMessage('create', item);
  console.log("Item criado:", item);
}

async function readItems() {
  console.log("Itens no banco:", items);
}

async function updateItem(id, newName) {
  const item = items.find(item => item.id === id);
  if (item) {
    item.name = newName;
    await sendMessage('update', item);
    console.log("Item atualizado:", item);
  } else {
    console.log("Item não encontrado.");
  }
}

async function deleteItem(id) {
  items = items.filter(item => item.id !== id);
  await sendMessage('delete', { id });
  console.log("Item deletado com ID:", id);
}

// Simulação das operações CRUD
async function performOperations() {
  await createItem('Item A');
  await createItem('Item B');
  await readItems();
  await updateItem(1, 'Item A atualizado');
  await deleteItem(2);
  await readItems();
}

performOperations().catch(console.error);
