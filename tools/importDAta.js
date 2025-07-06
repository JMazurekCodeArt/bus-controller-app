const fs = require('fs');
const xml2js = require('xml2js');
const { MongoClient } = require('mongodb');

const XML_FILE_PATH = './TransXChange.xml';

const MONGODB_URI = process.env.MONGODB_URI || 'mongodb://localhost:27017';
const DB_NAME = "mzk";

const parser = new xml2js.Parser({
    explicitArray: false,
    mergeAttrs: true
});

async function importData() { 
    let client;
    try {
        console.log('Łączenie z bazą danych MongoDB...');
        client = new MongoClient(MONGODB_URI);

        await client.connect();
        const db = client.db(DB_NAME);

        const stopsCollection = db.collection('stops');
        const linesCollection = db.collection('lines');

        await stopsCollection.deleteMany({});
        await linesCollection.deleteMany({});
        console.log('Wyczyszczono istnijące dane w "stops" i "lines".');

        console.log(`Wczytywanie pliku XML z: ${XML_FILE_PATH}`);
        const xmlData = fs.readFileSync(XML_FILE_PATH, `utf8`);
        console.log(`Parsowanie danych XML...`);
        const result = await parser.parseStringPromise(xmlData);
        console.log(`Parsowanie XML zakończone. Przetwarzanie danych...`);

        let stopPoints = result.TransXChange?.StopPoints?.StopPoint;
        if (!Array.isArray(stopPoints)) {
            stopPoints = [stopPoints];
        }
        stopPoints = stopPoints.filter(sp => sp != null);

        const stopsToInsert = [];
        const stopsMap = new Map();

        console.log(`Znaleziono ${stopPoints.length} przystankow. Przetwarzanie...`);
        for (const sp of stopPoints) {
            const stop = {
                _id: sp.id,
                name: sp.Descriptor?.CommonName || null,
                publicCode: sp.Extensions?.PublicCode || null,
                location:{
                    type: "Point",
                    coordinates: [
                        parseFloat(sp.Place?.Location?.Longitude || '0'),
                        parseFloat(sp.Place?.Location?.Latitude || '0')
                    ]
                },
                bearing: sp.StopClassification?.OnStreet?.Bus?.MarkedPoint?.Bearing?.CompassPoint || null,
                lines: []
            };
            stopsToInsert.push(stop);
            stopsMap.set(stop._id, stop);
        }
        await stopsCollection.insertMany(stopsToInsert);
        console.log(`Wstawiono ${stopsToInsert.length} przystanków do kolekcji "stops".`); 
        
        let vehicleJourneys = result.TransXChange?.VehicleJourneys?.VehicleJourney;
        if (!Array.isArray(vehicleJourneys)) {
            vehicleJourneys = [vehicleJourneys];
        }
        vehicleJourneys = vehicleJourneys.filter(vj => vj != null);

        const linesToInsert = [];
        const processedLinePatterns = new Set();

        console.log(`Znaleziono ${vehicleJourneys.length} wzorców podróży. Przetwarzanie...`);
        for (const vj of vehicleJourneys){
            const lineRef = vj.LineRef;
            const journeyPatternRef = vj.JourneyPatternRef;
            const uniqueLinePatternId = `${lineRef}_${journeyPatternRef}`;

            if (processedLinePatterns.has(uniqueLinePatternId)) {
                continue;
            }
            processedLinePatterns.add(uniqueLinePatternId);

            const stopSequence = [];

            if (vj.Extensions?.VehicleJourneyStopPoints?.VehicleJourneyStopPoint) {
                const vjStopPoints = vj.Extensions.VehicleJourneyStopPoints.VehicleJourneyStopPoint;
                if (Array.isArray(vjStopPoints)) {
                    for (const vjsp of vjStopPoints) {
                        stopSequence.push(vjsp.StopPointRef);
                    }
                } else {
                    stopSequence.push(vjsp.StopPointRef);
                }
            }
            
            if (stopSequence.length === 0) {
                console.warn(`Pominięto linię ${lineRef}, wzorzec podróży ${journeyPatternRef} - brak sekwencji przystanków.`);
                continue; 
            }

            for (const stopId of stopSequence) {
                const stopDoc = stopsMap.get(stopId);
                if (stopDoc && !stopDoc.lines.includes(lineRef)) {
                    stopDoc.lines.push(lineRef);
                }
            }

            let directionName = '';
            if(vj.VehicleJourneyTimingLink) {
                let timingLinks = vj.VehicleJourneyTimingLink;
                if (!Array.isArray(timingLinks)) {
                    timingLinks = [timingLinks];
                }
                const lastLink = timingLinks[timingLinks.length - 1];
                if (lastLink && lastLink.To && lastLink.To.DynamicDestinationDisplay) {
                    directionName = lastLink.To.DynamicDestinationDisplay;
                }
            }

            linesToInsert.push({
                _id: uniqueLinePatternId,
                lineNumber: lineRef,
                journeyPatternRef: journeyPatternRef,
                directionName: directionName,
                stopSequence: stopSequence
            });
        }
        
        const bulkOps = stopsToInsert.map(stop => ({
            updateOne: {
                filter: { _id: stop._id },
                update: { $set: { lines: stop.lines } }
            }
        }));

        if (bulkOps.length > 0) {
            await stopsCollection.bulkWrite(bulkOps);
            console.log(`Zaktualizowano pole 'lines' dla ${bulkOps.length} przystanków.`);
        }

        if (linesToInsert.length > 0) {
            await linesCollection.insertMany(linesToInsert);
            console.log(`Wstawiono ${linesToInsert.length} wzorców tras linii do kolekcji "lines".`);
        } else {
            console.log('Brak wzorców tras linii do wstawienia do kolekcji "lines".');
        }

        console.log('Import danych zakończony pomyślnie!');

    } catch (error) {
        console.error('Błąd podczas importu danych: ', error);
    } finally {
        if (client) {
            await client.close();
            console.log('Połączenie z MongoDB zamknięte.');
        }
    }
}

importData();