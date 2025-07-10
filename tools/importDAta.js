const { parseString } = require('xml2js');
const fs = require('fs');
const { MongoClient } = require('mongodb');
const { promisify } = require('util');
const parseStringAsync = promisify(parseString);

const config = {
  mongoUri: 'mongodb://localhost:27017',
  dbName: 'mzk',
  xmlPath: './TransXChange.xml',
  batchSize: 500
};

function getArray(item) {
  return Array.isArray(item) ? item : item ? [item] : [];
}

async function main() {
  const client = new MongoClient(config.mongoUri);
  
  try {
    await client.connect();
    const db = client.db(config.dbName);
    
    const xmlData = fs.readFileSync(config.xmlPath, 'utf-8');
    const { TransXChange: txc } = await parseStringAsync(xmlData);

    const stops = await processStops(txc, db);
    const lines = await processLines(txc, db);
    await processStopAreas(txc, db);
    await processAdminAreas(txc, db);
    const journeyPatterns = await processJourneyPatterns(txc, db);
    await processRoutes(txc, db, journeyPatterns);
    await updateStopsWithLines(db, lines, journeyPatterns, txc);
    await processVehicleJourneys(txc, db);
    await processServiceCalendars(txc, db);
    await createIndexes(db);
  } catch (error) {
    console.error('Błąd podczas przetwarzania:', error);
  } finally {
    await client.close();
  }
}

async function processStops(txc, db) {
  const stops = getArray(txc.StopPoints?.[0]?.StopPoint).map(stop => ({
    _id: stop.$.id,
    atcoCode: stop.AtcoCode?.[0],
    commonName: stop.Descriptor?.[0]?.CommonName?.[0] || 'Brak nazwy',
    street: stop.Descriptor?.[0]?.StreetRef?.[0] || null,
    locality: stop.Place?.[0]?.NptgLocalityRef?.[0] || null,
    location: {
      type: "Point",
      coordinates: [
        parseFloat(stop.Place?.[0]?.Location?.[0]?.Longitude?.[0] || '0'),
        parseFloat(stop.Place?.[0]?.Location?.[0]?.Latitude?.[0] || '0')
      ]
    },
    isControlStop: stop.Extensions?.[0]?.IsControlStop?.[0] === 'true',
    isPrincipal: stop.Extensions?.[0]?.IsPrincipal?.[0] === 'true',
    stopAreaRef: stop.StopAreas?.[0]?.StopAreaRef?.[0] || null,
    administrativeAreaRef: stop.AdministrativeAreaRef?.[0] || null,
    lines: [],
    directions: []
  }));

  await db.collection('stops').deleteMany({});
  await batchInsert(db.collection('stops'), stops);
  return stops;
}

async function processLines(txc, db) {
  const lines = getArray(txc.Lines?.[0]?.Line).map(line => ({
    _id: line.$.id,
    lineNumber: line.LineName?.[0] || 'Brak',
    marketingName: line.MarketingName?.[0] || line.LineName?.[0] || 'Brak',
    lineType: line.Extensions?.[0]?.LineType?.[0] || 'unknown',
    fleetKind: line.Extensions?.[0]?.FleetKind?.[0] || 'unknown',
    color: line.LineColour?.[0] || null
  }));

  await db.collection('lines').deleteMany({});
  await batchInsert(db.collection('lines'), lines);
  return lines;
}

async function processStopAreas(txc, db) {
  const stopAreas = getArray(txc.StopAreas?.[0]?.StopArea).map(area => ({
    _id: area.$.id,
    areaCode: area.StopAreaCode?.[0],
    name: area.Name?.[0] || 'Brak nazwy',
    administrativeAreaRef: area.AdministrativeAreaRef?.[0] || null,
    areaType: area.StopAreaType?.[0] || null,
    location: {
      easting: area.Location?.[0]?.Easting?.[0] || '0',
      northing: area.Location?.[0]?.Northing?.[0] || '0'
    }
  }));

  await db.collection('stop_areas').deleteMany({});
  await batchInsert(db.collection('stop_areas'), stopAreas);
}

async function processAdminAreas(txc, db) {
  const adminAreas = getArray(txc.AdministrativeAreas?.[0]?.AdministrativeArea).map(area => ({
    _id: area.$.id,
    areaCode: area.AdministrativeAreaCode?.[0],
    name: area.Name?.[0] || 'Brak nazwy',
    extensions: {
      areaKind: area.Extensions?.[0]?.AdministrativeAreaKindID?.[0] || null
    }
  }));

  await db.collection('administrative_areas').deleteMany({});
  await batchInsert(db.collection('administrative_areas'), adminAreas);
}

async function processJourneyPatterns(txc, db) {
  const journeyPatterns = getArray(txc.JourneyPatternSections?.[0]?.JourneyPatternSection).map(jps => {
    const links = getArray(jps.JourneyPatternTimingLink);
    const stopSequence = [];
    const timingLinks = [];
    
    links.forEach(link => {
      const fromStop = link.From?.[0]?.StopPointRef?.[0];
      const toStop = link.To?.[0]?.StopPointRef?.[0];
      
      if (fromStop && !stopSequence.includes(fromStop)) stopSequence.push(fromStop);
      if (toStop && !stopSequence.includes(toStop)) stopSequence.push(toStop);
      
      timingLinks.push({
        id: link.$.id,
        from: fromStop,
        to: toStop,
        direction: link.Direction?.[0] || 'outbound',
        runTime: link.RunTime?.[0] || 'PT0H0M',
        routeLinkRef: link.RouteLinkRef?.[0]
      });
    });
    
    return {
      _id: jps.$.id,
      stopSequence,
      timingLinks: timingLinks.filter(link => link.from && link.to)
    };
  });

  await db.collection('journey_patterns').deleteMany({});
  await batchInsert(db.collection('journey_patterns'), journeyPatterns);
  return journeyPatterns;
}

async function processRoutes(txc, db, journeyPatterns) {
  const routes = getArray(txc.Routes?.[0]?.Route).map(route => {
    const sectionRefs = getArray(route.RouteSectionRef).map(ref => ref);
    const stopsSequence = [];
    
    sectionRefs.forEach(ref => {
      const pattern = journeyPatterns.find(jp => jp._id === ref);
      if (pattern) {
        pattern.stopSequence.forEach(stop => {
          if (!stopsSequence.includes(stop)) stopsSequence.push(stop);
        });
      }
    });
    
    return {
      _id: route.$.id,
      description: route.Description?.[0] || '',
      lineRef: route.Extensions?.[0]?.LineRef?.[0] || null,
      direction: route.Extensions?.[0]?.Direction?.[0] || 'unknown',
      journeyPatternSections: sectionRefs,
      stopsSequence,
      isTechnical: route.Extensions?.[0]?.IsTechnical?.[0] === 'true',
      isForPassengers: route.Extensions?.[0]?.IsForPassengers?.[0] !== 'false'
    };
  });

  await db.collection('routes').deleteMany({});
  await batchInsert(db.collection('routes'), routes);
}

async function updateStopsWithLines(db, lines, journeyPatterns, txc) {
  const routes = await db.collection('routes').find().toArray();
  const services = getArray(txc.Services?.[0]?.Service);

  const routeToLineMap = new Map(routes.map(r => [r._id, r.lineRef]));
  const jpToRouteMap = new Map();

  routes.forEach(route => {
    getArray(route.journeyPatternSections).forEach(jpRef => {
      if (jpRef && route._id) jpToRouteMap.set(jpRef, route._id);
    });
  });

  services.forEach(service => {
    getArray(service.StandardService?.[0]?.JourneyPattern).forEach(jp => {
      if (jp?.$?.id && jp.RouteRef?.[0]) jpToRouteMap.set(jp.$.id, jp.RouteRef[0]);
    });
  });

  const bulkOps = [];
  const lineIds = new Set(lines.map(l => l._id));
  const processedStopLinePairs = new Set();

  journeyPatterns.forEach(jp => {
    const routeRef = jpToRouteMap.get(jp._id);
    if (!routeRef) return;

    const lineRef = routeToLineMap.get(routeRef);
    if (!lineRef || !lineIds.has(lineRef)) return;

    const directionsMap = new Map();
    jp.timingLinks.forEach(link => {
      if (link.direction && link.from && link.to) {
        if (!directionsMap.has(link.direction)) {
          directionsMap.set(link.direction, new Set());
        }
        directionsMap.get(link.direction).add(link.to);
      }
    });

    jp.stopSequence.forEach((stopRef, sequenceIndex) => {
      const pairKey = `${stopRef}_${lineRef}`;
      if (processedStopLinePairs.has(pairKey)) return;
      processedStopLinePairs.add(pairKey);

      const directionsForStop = [];
      directionsMap.forEach((toStops, direction) => {
        toStops.forEach(toStop => {
          directionsForStop.push({
            lineRef,
            direction,
            nextStop: toStop,
            sequence: sequenceIndex
          });
        });
      });

      if (directionsForStop.length > 0) {
        bulkOps.push({
          updateOne: {
            filter: { _id: stopRef },
            update: {
              $addToSet: {
                lines: lineRef,
                directions: { $each: directionsForStop }
              }
            }
          }
        });
      }
    });
  });

  if (bulkOps.length > 0) {
    await db.collection('stops').bulkWrite(bulkOps, { ordered: false });
  }
}

async function processVehicleJourneys(txc, db) {
  const vehicleJourneys = getArray(txc.VehicleJourneys?.[0]?.VehicleJourney).map(vj => {
    const timingLinks = getArray(vj.VehicleJourneyTimingLink);
    const stopPoints = timingLinks.map((link, i) => ({
      sequence: i + 1,
      stopPointRef: link.From?.[0]?.StopPointRef?.[0],
      timingStatus: link.From?.[0]?.TimingStatus?.[0] || 'PTP',
      activity: link.From?.[0]?.Activity?.[0] || null,
      dynamicDisplay: link.From?.[0]?.DynamicDestinationDisplay?.[0] || null
    }));
    
    if (timingLinks.length > 0 && timingLinks[timingLinks.length-1].To?.[0]?.StopPointRef?.[0]) {
      stopPoints.push({
        sequence: timingLinks.length + 1,
        stopPointRef: timingLinks[timingLinks.length-1].To[0].StopPointRef[0],
        timingStatus: timingLinks[timingLinks.length-1].To[0].TimingStatus?.[0] || 'PTP',
        activity: timingLinks[timingLinks.length-1].To[0].Activity?.[0] || null,
        dynamicDisplay: timingLinks[timingLinks.length-1].To[0].DynamicDestinationDisplay?.[0] || null
      });
    }
    
    return {
      _id: vj.VehicleJourneyCode?.[0] || `journey_${Math.random().toString(36).substr(2, 9)}`,
      lineRef: vj.LineRef?.[0],
      serviceRef: vj.ServiceRef?.[0],
      departureTime: vj.DepartureTime?.[0],
      journeyPatternRef: vj.JourneyPatternRef?.[0],
      stopPoints: stopPoints.filter(sp => sp.stopPointRef),
      vehicleJourneyKind: vj.Extensions?.[0]?.VehicleJourneyKind?.[0] || '0',
      frequency: vj.Frequency?.[0] ? {
        endTime: vj.Frequency[0].EndTime?.[0],
        interval: vj.Frequency[0].Interval?.[0]?.ScheduledFrequency?.[0]
      } : null
    };
  });

  await db.collection('vehicle_journeys').deleteMany({});
  await batchInsert(db.collection('vehicle_journeys'), vehicleJourneys);
}

async function processServiceCalendars(txc, db) {
  await db.collection('service_calendars').deleteMany({});
  const calendars = getArray(txc.ServiceCalendars?.[0]?.ServiceCalendar || txc.ServiceCalendar);
  
  for (const item of calendars) {
    const calendar = item.$ ? item : { $: item };
    const operatingDays = getArray(calendar.OperatingDays?.[0]?.OperatingDay).map(day => ({
      date: day.Date?.[0],
      dayTypeRef: day.ServiceDayAssignment?.[0]?.DayTypeRef?.[0]
    }));
    
    const dayTypes = getArray(calendar.DayTypes?.[0]?.DayType).map(type => ({
      id: type.$.id,
      name: type.Name?.[0] || 'Brak',
      symbol: type.Extensions?.[0]?.Symbol?.[0]
    }));
    
    const calendarId = `${calendar.CalendarPeriod?.[0]?.StartDate?.[0]}_${calendar.CalendarPeriod?.[0]?.EndDate?.[0]}`;
    
    try {
      await db.collection('service_calendars').insertOne({
        _id: calendarId,
        startDate: calendar.CalendarPeriod?.[0]?.StartDate?.[0],
        endDate: calendar.CalendarPeriod?.[0]?.EndDate?.[0],
        operatingDays,
        dayTypes
      });
    } catch (error) {
      if (error.code === 11000) {
        await db.collection('service_calendars').updateOne(
          { _id: calendarId },
          { $set: { operatingDays, dayTypes } }
        );
      } else {
        throw error;
      }
    }
  }
}

async function createIndexes(db) {
  await db.collection('stops').createIndex({ location: '2dsphere' });
  await db.collection('stops').createIndex({ lines: 1 });
  await db.collection('routes').createIndex({ lineRef: 1 });
  await db.collection('vehicle_journeys').createIndex({ lineRef: 1, departureTime: 1 });
  await db.collection('vehicle_journeys').createIndex({ 'stopPoints.stopPointRef': 1 });
}

async function batchInsert(collection, documents) {
  for (let i = 0; i < documents.length; i += config.batchSize) {
    const batch = documents.slice(i, i + config.batchSize);
    await collection.insertMany(batch);
  }
}

main().catch(console.error);