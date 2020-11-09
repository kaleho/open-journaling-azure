using System;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos.Table;

namespace Open.Journaling.Azure.Tests
{
    /// <summary>
    ///     Emulator constants are provided at the following:
    ///     https://docs.microsoft.com/en-us/azure/storage/common/storage-use-emulator
    /// </summary>
    public class StorageFixture
        : IDisposable
    {
        private readonly CloudTableClient _client;

        public const string EmulatorAccountName = "devstoreaccount1";

        public const string EmulatorIPAddress = "127.0.0.1";

        private const string EmulatorAccountKey =
            "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

        public StorageFixture()
        {
            ConnectionString =
                "DefaultEndpointsProtocol=http;" +
                $"AccountName={EmulatorAccountName};" +
                $"AccountKey={EmulatorAccountKey};" +
                $"BlobEndpoint=http://{EmulatorIPAddress}:10000/{EmulatorAccountName};" +
                $"TableEndpoint=http://{EmulatorIPAddress}:10002/{EmulatorAccountName};" +
                $"QueueEndpoint=http://{EmulatorIPAddress}:10001/{EmulatorAccountName};";

            try
            {
                var account = CloudStorageAccount.Parse(ConnectionString);

                _client = account.CreateCloudTableClient();

                ServiceProperties properties = null;

                Task.Run(() => { properties = _client.GetServiceProperties(); }).Wait(1000);

                IsServerAvailable = properties != null;
            }
            catch
            {
                IsServerAvailable = false;
            }
        }

        public string ConnectionString { get; }

        public bool IsServerAvailable { get; }

        public void Dispose()
        {
        }

        public Task<bool> CleanupCloudTable(
            string tableName)
        {
            var table = _client.GetTableReference(tableName);

            return table.DeleteIfExistsAsync();
        }
    }
}