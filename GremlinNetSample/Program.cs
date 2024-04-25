using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Gremlin.Net.Driver;
using Gremlin.Net.Driver.Exceptions;
using Gremlin.Net.Structure.IO.GraphSON;
using Newtonsoft.Json;
using DotNetEnv;

namespace GremlinNetSample
{
    /// <summary>
    /// Sample program that shows how to get started with the Graph (Gremlin) APIs for Azure Cosmos DB using the open-source connector Gremlin.Net
    /// </summary>
    internal static class Program
    {
        // Starts a console application that executes every Gremlin query in the gremlinQueries dictionary. 
        private static async Task Main(string[] args)
        {
            Env.Load();

            var hostname = Env.GetString("HOSTNAME");
            var port = Env.GetInt("PORT");
            var authKey = Env.GetString("AUTHKEY");
            var database = Env.GetString("DATABASE");
            var collection = Env.GetString("COLLECTION");
            var continueOnErrorString = Env.GetString("CONTINUE_ON_ERROR");
            // Check if any of the environment variables are null
            if (hostname == null || port == 0 || authKey == null || database == null || collection == null ||
                continueOnErrorString == null)
            {
                Console.WriteLine(
                    "One or more environment variables are not set. Please copy the .env.sample into .env and update the values.");
                return;
            }

            var continueOnError = bool.Parse(continueOnErrorString);

            var filename = args[0];

            var fileContent = (await File.ReadAllLinesAsync(filename)).ToList();

            var gremlinServer = new GremlinServer(hostname, port, enableSsl: true,
                username: "/dbs/" + database + "/colls/" + collection,
                password: authKey);

            using var gremlinClient = new GremlinClient(gremlinServer, new GraphSON2Reader(), new GraphSON2Writer(),
                GremlinClient.GraphSON2MimeType);
            foreach (var query in fileContent)
            {
                Console.WriteLine($"Running this query: {query}");

                // Create async task to execute the Gremlin query.
                try
                {
                    var resultSet = await SubmitRequest(gremlinClient, query);
                    if (resultSet is { Count: > 0 })
                    {
                        Console.WriteLine("\tResult:");
                        foreach (var result in resultSet)
                        {
                            // The vertex results are formed as Dictionaries with a nested dictionary for their properties
                            string output = JsonConvert.SerializeObject(result);
                            Console.WriteLine($"\t{output}");
                        }

                        Console.WriteLine();
                    }

                    // Print the status attributes for the result set.
                    // This includes the following:
                    //  x-ms-status-code            : This is the sub-status code which is specific to Cosmos DB.
                    //  x-ms-total-request-charge   : The total request units charged for processing a request.

                    Console.WriteLine();
                }
                catch (Exception e)
                {
                    Console.WriteLine($"\tError: {e.Message}");
                    if (!continueOnError)
                    {
                        break;
                    }
                }
            }
        }
        
        private static async Task<ResultSet<dynamic>> SubmitRequest(IGremlinClient gremlinClient, string query)
        {
            try
            {
                return await gremlinClient.SubmitAsync<dynamic>(query);
            }
            catch (ResponseException e)
            {
                Console.WriteLine("\tRequest Error!");

                // Print the Gremlin status code.
                Console.WriteLine($"\tStatusCode: {e.StatusCode}");

                // On error, ResponseException.StatusAttributes will include the common StatusAttributes for successful requests, as well as
                // additional attributes for retry handling and diagnostics.
                // These include:
                //  x-ms-retry-after-ms         : The number of milliseconds to wait to retry the operation after an initial operation was throttled. This will be populated when
                //                              : attribute 'x-ms-status-code' returns 429.
                //  x-ms-activity-id            : Represents a unique identifier for the operation. Commonly used for troubleshooting purposes.
                PrintStatusAttributes(e.StatusAttributes);
                Console.WriteLine(
                    $"\t[\"x-ms-retry-after-ms\"] : {GetValueAsString(e.StatusAttributes, "x-ms-retry-after-ms")}");
                Console.WriteLine(
                    $"\t[\"x-ms-activity-id\"] : {GetValueAsString(e.StatusAttributes, "x-ms-activity-id")}");
                throw;
            }
        }

        private static void PrintStatusAttributes(IReadOnlyDictionary<string, object> attributes)
        {
            Console.WriteLine($"\tStatusAttributes:");
            Console.WriteLine($"\t[\"x-ms-status-code\"] : {GetValueAsString(attributes, "x-ms-status-code")}");
            Console.WriteLine(
                $"\t[\"x-ms-total-request-charge\"] : {GetValueAsString(attributes, "x-ms-total-request-charge")}");
        }

        private static string GetValueAsString(IReadOnlyDictionary<string, object> dictionary, string key)
        {
            return JsonConvert.SerializeObject(dictionary.GetValueOrDefault(key));
        }
    }
}