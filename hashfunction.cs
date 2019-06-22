// Default URL for triggering event grid function in the local environment.
// http://localhost:7071/runtime/webhooks/EventGrid?functionName={functionname}

using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Azure.EventGrid.Models;
using Microsoft.Azure.WebJobs.Extensions.EventGrid;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.Azure.EventGrid;
using System;
using System.IO;
using System.Text.RegularExpressions;
using System.Threading.Tasks; 
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Auth;
using Newtonsoft.Json.Linq;
using System.Security.Cryptography;
using System.Text;


namespace hashfunctionproject
{
    public static class hashfunction
    {

        private static readonly string BLOB_STORAGE_CONNECTION_STRING = Environment.GetEnvironmentVariable("AzureWebJobsStorage");
        [FunctionName("hashfunction")]
        public async static void Run([EventGridTrigger]EventGridEvent eventGridEvent, ILogger log)
        {
            
            var startTime = DateTime.Now;
            var createdEvent = ((JObject)eventGridEvent.Data).ToObject<StorageBlobCreatedEventData>();
            log.LogInformation(eventGridEvent.Data.ToString()); 
            
            var storageAccount = CloudStorageAccount.Parse(BLOB_STORAGE_CONNECTION_STRING);
            var blobClient = storageAccount.CreateCloudBlobClient();
            log.LogInformation(BLOB_STORAGE_CONNECTION_STRING.ToString());
            var container = blobClient.GetContainerReference("REPLACE-WITH-YOUR-CONTAINER-NAME");
            var blockBlob = container.GetBlockBlobReference(GetFileNameFromBlobURI(new System.Uri(createdEvent.Url),"REPLACE-WITH-YOUR-CONTAINER-NAME"));
            await blockBlob.FetchAttributesAsync();

            /* Download the entire file and calculate hash - THis works too - TODO move to a function/method 
            string contentMD5 = blockBlob.Properties.ContentMD5;
            byte[] retrievedBuffer = new byte[blockBlob.Properties.Length];
            await blockBlob.DownloadToByteArrayAsync(retrievedBuffer,0);
            var md5Check = System.Security.Cryptography.MD5.Create();
            md5Check.TransformBlock(retrievedBuffer, 0, retrievedBuffer.Length, null, 0);     
            md5Check.TransformFinalBlock(new byte[0], 0, 0);
            byte[] hashBytes = md5Check.Hash;
            string hashVal = Convert.ToBase64String(hashBytes);
            log.LogInformation(hashVal); */

            string hashVal1 = DownloadPartsandCalculateHash(createdEvent.Url,log).Result;
            blockBlob.Properties.ContentMD5=hashVal1;
            await blockBlob.SetPropertiesAsync();
            log.LogInformation("Time Elapsed:" + DateTime.Now.Subtract(startTime).TotalMinutes.ToString());
        }
        private static string GetFileNameFromBlobURI(Uri theUri, string containerName)
        {
            string theFile = theUri.ToString();
            int dirIndex = theFile.IndexOf(containerName);
            string oneFile = theFile.Substring(dirIndex + containerName.Length + 1,
                theFile.Length - (dirIndex + containerName.Length + 1));
            return oneFile;
        }

        static async Task<string> DownloadPartsandCalculateHash(string url, ILogger log)
        {
            var storageAccount = CloudStorageAccount.Parse(BLOB_STORAGE_CONNECTION_STRING);
            var blobClient = storageAccount.CreateCloudBlobClient();
            var container = blobClient.GetContainerReference("REPLACE-WITH-YOUR-CONTAINER-NAME");
            var blob = container.GetBlockBlobReference(GetFileNameFromBlobURI(new System.Uri(url),"REPLACE-WITH-YOUR-CONTAINER-NAME"));
            await blob.FetchAttributesAsync();

            int segmentSize = 1 * 1024 * 1024;//1 MB chunk
           
            var blobLengthRemaining = blob.Properties.Length;
            long startPosition = 0;
            var md5Check = System.Security.Cryptography.MD5.Create();
            byte[] blobContents = new byte[segmentSize];
            int count=0;
            try {

                while (blobLengthRemaining > 0)
                {
                    long blockSize = Math.Min(segmentSize, blobLengthRemaining);
                    blobContents = new byte[blockSize];
                    using (MemoryStream ms = new MemoryStream())
                    {
                        await blob.DownloadRangeToStreamAsync(ms, startPosition, blockSize);
                        ms.Position = 0;
                        ms.Read(blobContents, 0, blobContents.Length);
                        count++;
                        if(blobLengthRemaining<=segmentSize) {
                            md5Check.TransformFinalBlock(blobContents, 0, blobContents.Length);
                            log.LogInformation("Transformed final block-" + blobContents.Length);
                        }
                        else {
                            
                            md5Check.TransformBlock(blobContents, 0, blobContents.Length, blobContents, 0); 
                        }
                    }
                    startPosition += blockSize;
                    blobLengthRemaining -= blockSize;
                }
                
                
                byte[] hashBytes = md5Check.Hash;
                string hashVal = Convert.ToBase64String(hashBytes);
                log.LogInformation("Generated Hash Value" + hashVal);
                return hashVal;
            }
            catch(Exception e) {
                var x= e.Message;
                log.LogError(new EventId(),e,e.Message);
                return "Error generateting hash" + e.Message;
            }
           
        }

        
    }
}
