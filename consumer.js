const amqp = require('amqplib');
const mongoose = require('mongoose');

// Configuração da conexão com o MongoDB
async function connectMongoDB() {
  await mongoose.connect('mongodb://localhost:27017/crudDB', {
    useNewUrlParser: true,
    useUnifiedTopology: true,
  });
  console.log("Conectado ao MongoDB.");
}

// Definindo o modelo para os dados no MongoDB
const itemSchema = new mongoose.Schema({
  name: String,
});
const Item = mongoose.model('Item', itemSchema);

async function startConsumer() {
  await connectMongoDB();

  const connection = await amqp.connect('amqp://localhost');
  const channel = await connection.createChannel();

  const queue = 'crud_operations';
  await channel.assertQueue(queue, { durable: true });

  console.log("Esperando por mensagens na fila:", queue);

  channel.consume(queue, async (msg) => {
    if (msg !== null) {
      const message = JSON.parse(msg.content.toString());
      console.log(`Mensagem recebida: Ação - ${message.action}, Item -`, message.item);
      
      // Operações CRUD com base nas mensagens recebidas
      const { action, item } = message;

      switch (action) {
        case 'create':
          const newItem = new Item({ name: item.name });
          await newItem.save();
          console.log("Item criado no MongoDB:", newItem);
          break;
        case 'read':
          const foundItem = await Item.findById(item.id);
          console.log("Item encontrado:", foundItem);
          break;
        case 'update':
          await Item.findByIdAndUpdate(item.id, { name: item.newName });
          console.log("Item atualizado no MongoDB com ID:", item.id);
          break;
        case 'delete':
          await Item.findByIdAndDelete(item.id);
          console.log("Item deletado do MongoDB com ID:", item.id);
          break;
        default:
          console.log("Ação desconhecida:", action);
      }

      channel.ack(msg); // Confirma o processamento da mensagem
    }
  });
}

startConsumer().catch(console.error);
