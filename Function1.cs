using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Azure.Messaging.EventHubs;
using Azure.Storage.Blobs;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace FunctionApp1
{
    public static class Function1
    {
        [FunctionName("app-gastos-sa")]


        public static async Task Run([EventHubTrigger("%EventHubName%", Connection = "EventHubConnectionString")] EventData[] events, ILogger log)
        {
            var exceptions = new List<Exception>();

            // Get blob storage connection string from environment variable
            string blobConnectionString = Environment.GetEnvironmentVariable("BlobConnectionString");

            // Get blob container name from environment variable
            string blobContainerName = Environment.GetEnvironmentVariable("BlobContainerName");

            // Connect to the blob storage account
            var blobServiceClient = new BlobServiceClient(blobConnectionString);
            var blobContainerClient = blobServiceClient.GetBlobContainerClient(blobContainerName);

            foreach (EventData eventData in events)
            {
                try
                {
                    // Serialize the event data to JSON
                    var jsonString = Encoding.UTF8.GetString(eventData.EventBody.ToArray());

                    // Generate a shorter unique identifier
                    var shortGuid = Guid.NewGuid().ToString("N").Substring(0, 8); // Utiliza los primeros 8 caracteres del GUID
                                                                                  // Obtener la fecha y hora actual
                    DateTime now = DateTime.Now;

                    // Generate a unique name for the blob using current date and time and the short GUID
                    var blobName = $"{now:yyyyMMdd-HHmmss}-{shortGuid}.json";

                    // Get a reference to a blob
                    var blobClient = blobContainerClient.GetBlobClient(blobName);

                    // Upload the JSON data to the blob
                    using (var stream = new MemoryStream(Encoding.UTF8.GetBytes(jsonString)))
                    {
                        await blobClient.UploadAsync(stream);
                    }

                    // Replace these two lines with your processing logic.
                    log.LogInformation($"C# Event Hub trigger function processed a message: {eventData.EventBody}");
                    log.LogInformation($"C# Event Hub trigger function processed a message and saved it to blob: {blobName}");
                    await Task.Yield();
                }
                catch (Exception e)
                {
                    // We need to keep processing the rest of the batch - capture this exception and continue.
                    // Also, consider capturing details of the message that failed processing so it can be processed again later.
                    exceptions.Add(e);
                }
            }

            // If any messages failed processing, throw an exception
            if (exceptions.Count > 1)
                throw new AggregateException(exceptions);
            // Once processing of the batch is complete, if any messages in the batch failed processing throw an exception so that there is a record of the failure.

            if (exceptions.Count > 1)
                throw new AggregateException(exceptions);

            if (exceptions.Count == 1)
                throw exceptions.Single();
        }
    }
}
