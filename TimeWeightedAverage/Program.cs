using Nest;
using System;

namespace TimeWeightedAverage
{
    class Program
    {
        static void Main(string[] args)
        {
            //Connect to elastic search cluster
            var connSettings = new ConnectionSettings(new Uri("http://192.168.0.150:9200"));
            connSettings.DefaultIndex("readings_test");

            var elastic = new ElasticClient(connSettings);

            //Form the query
            var searchResponse = elastic.Search<dynamic>(s => s
            .Size(0)
            .Type(string.Empty)
            .Aggregations(a => a
                .Terms("sensors", t => t.Field("sensor_id.keyword")
                .Aggregations(h => h
                    .TopHits("readings", th=>th
                        .Sort(ss=>ss.Ascending("timestamp")))))));

            var temp = searchResponse.Aggregations.Terms("sensors");

            //Iterate thru the sensors
            foreach(var item in temp.Buckets)
            {
                var sensor = item.Key;
                var readings = item.TopHits("readings").Hits<dynamic>();
                
                //Setup to do a first pass to seed the previous time and measurement
                var firstPass = true;
                DateTime previousTimestamp = DateTime.MinValue;
                double previousMeasurement = 0.0;

                //Declare the accumulators to hold both the accumulated time weighted measurement and the total accumulated hours
                double accumulatedHours = 0.0;
                double accumulatedMeasurement = 0.0;

                //Process each measurement from the sensor
                foreach (var reading in readings)
                {
                    DateTime timestamp = reading.Source.timestamp;
                    double measurement = reading.Source.measurement;

                    //No processing is done on the first reading
                    if (firstPass)
                    {
                        previousTimestamp = timestamp;
                        previousMeasurement = measurement;
                        firstPass = false;
                        continue;
                    }

                    //Compute the time weighted measurement and accumulate both the measurements and the total time
                    var timewieghtedAverage = previousMeasurement * (timestamp - previousTimestamp).TotalHours;
                    accumulatedHours += (timestamp - previousTimestamp).TotalHours;
                    accumulatedMeasurement += timewieghtedAverage;

                    //Update the previous time and measurements
                    previousTimestamp = timestamp;
                    previousMeasurement = measurement;
                }

                //Compute the timeweighted exposure for the total hours of the measurements
                var timeWeightedExposure = accumulatedMeasurement / accumulatedHours;
                Console.WriteLine("SensorId: {0}  Exposure Amount: {1} Exposure Period: {2} hours", sensor, timeWeightedExposure, accumulatedHours);
            }
            Console.WriteLine("Press any key to end.");
            Console.ReadKey();
        }
    }
}
