import { Kafka, Consumer } from "kafkajs";
import { v4 as uuidv4 } from 'uuid';
import { Db, MongoClient } from 'mongodb';

export class ConsumingService {
    
    private kafka: Kafka;
    private consumer: Consumer;
    private client: MongoClient;
    private database: Db;

    constructor() {
        this.kafka = new Kafka({
            clientId: 'streets-publisher',
            brokers: ['localhost:9092']
        });
        this.consumer = this.kafka.consumer({ groupId: uuidv4() }); 
        console.log('Starting Consumer service')
        this.connectToMongo()
    }
    
    async connectToMongo() {
        this.client = new MongoClient('mongodb://localhost:27017');
        await this.client.connect();
        this.database = this.client.db('streetsDB');
    }
    

    async disconnectFromMongo() {
        await this.client.close();
    }

    persistStreetsFromQueue = async () => {
        await this.consumer.connect();
        await this.consumer.subscribe({ topic: 'streets', fromBeginning: false });
        await this.consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
                const formattedValue = JSON.parse((message.value as Buffer).toString()); // everything comes as a buffer
                console.log('Received message', {
                    topic,
                    partition,
                    key: message.key?.toString() || 'N/A',
                    value: formattedValue,
                });
                
                
                const collection = this.database.collection('streets');
                await collection.insertOne(formattedValue);

                this.consumer.commitOffsets([{ topic, partition, offset: (parseInt(message.offset) + 1).toString() }]);
            },
        });
    }

}