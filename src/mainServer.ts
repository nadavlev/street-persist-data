import * as express from 'express';
import { NextFunction, Request, Response } from 'express';
import { cities } from './israeliStreets/cities';
import { PublishingService } from './PublishingServce';
import { ConsumingService } from './ConsumingService';


const app = express();
const port = 3000;

app.use(express.json());

interface StreetsRequestQuery {
    cityName: string;
}

app.get(
    '/get-streets',
    async (req: Request<{}, {}, {}, StreetsRequestQuery>, res: Response) => {
        const cityName = req.query.cityName;

        const selectedCity = cities[cityName];

        
        if (!selectedCity) {
            res.status(400).send(`City: ${cityName} not found.`);
        }
        else {
            const publishingService = new PublishingService();
            publishingService.publishStreets(selectedCity);
        }
        
        res.send('Request received.');

    }
);

app.listen(port, () => {
    console.log(`Server is running on port ${port}`);
    (new ConsumingService()).persistStreetsFromQueue();
    console.log('Consumer is listening for messages...');
});

// Middleware to ensure only 'cityName' is allowed as query parameter
app.use((req: Request, res: Response, next: NextFunction) => {
    const queryKeys = Object.keys(req.query);
    if (queryKeys.some(key => key !== 'cityName')) {
        res.status(400).send('Invalid query parameters.');
        return;
    }
    next();
});