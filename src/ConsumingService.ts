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
                formattedValue['createdAt'] = new Date();
                formattedValue['UpdatedAt'] = new Date();
                console.log('Received message', {
                    topic,
                    partition,
                    key: message.key?.toString() || 'N/A',
                    value: formattedValue,
                });
                
                
                const collection = this.database.collection('streets');
                await collection.updateOne(
                    { streetId: formattedValue.streetId },
                    { $set: {
                        "streetId" : formattedValue.streetId,
                        "city_code" : formattedValue.city_code,
                        "city_name" : formattedValue.city_name,
                        "official_code" : formattedValue.official_code,
                        "region_code" : formattedValue.region_code,
                        "region_name" : formattedValue.region_name,
                        "street_code" : formattedValue.street_code,
                        "street_name" : formattedValue.street_name,
                        "street_name_status" : formattedValue.street_name_status,
                        "updatedAt" : formattedValue.UpdatedAt,
                    }, $setOnInsert: {"createdAt": formattedValue.createdAt}},
                    { upsert: true }
                );

                this.consumer.commitOffsets([{ topic, partition, offset: (parseInt(message.offset) + 1).toString() }]);
            },
        });
    }

}