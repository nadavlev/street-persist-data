import { StreetsService } from './israeliStreets/StreetsService';
import { city, enlishNameByCity, Street } from './israeliStreets';
import { Kafka } from 'kafkajs';


export class PublishingService {

    private kafka: Kafka;

    constructor() {
        this.kafka = new Kafka({
            clientId: 'streets-publisher',
            brokers: ['localhost:9092']
        });
    }

    public async publishStreets(cityName: city) {
        const streetList = await StreetsService.getStreetsInCity(enlishNameByCity[cityName]);
        const streetInfo = await StreetsService.getStreetInfoById(streetList.streets.map(street => street.streetId));
        const connection = this.kafka.producer();
        return connection.connect().then(() => {
            this.sendStreets(connection, streetInfo);
        })
    }

    sendStreets(connection: any, streets: Street[]) {
        streets.forEach(async street => {
            console.log(`Publishing street: ${street.street_name}`);
            connection.send({
                topic: 'streets',
                messages: [
                    { value: JSON.stringify(street) }
                ],
            });
        });
    }
}