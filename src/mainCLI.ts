import * as readline from 'readline';
import { cities, city, enlishNameByCity } from './israeliStreets/cities';
import { PublishingService } from './PublishingServce';
import { ConsumingService } from './ConsumingService';


const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
});


function start(){
    const publishingService = new PublishingService();
    
        rl.question('Please enter a city name in israel', (cityName)=> {

            const selectedCity = cities[cityName];

            if (!selectedCity) {
                console.log(`City ${cityName} not found`);
            }
            else {
                publishingService.publishStreets(selectedCity);
                console.log(`Request for adding ${cityName} was executed`);
            }
        })
    
}

(new ConsumingService()).persistStreetsFromQueue();
start()

process.on('SIGINT', () => process.exit(0))

process.on('exit', () => {
    rl.close();
  });