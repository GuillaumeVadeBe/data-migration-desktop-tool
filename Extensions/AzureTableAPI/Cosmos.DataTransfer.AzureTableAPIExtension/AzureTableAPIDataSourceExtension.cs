using System.ComponentModel.Composition;
using System.Runtime.CompilerServices;
using Azure;
using Azure.Data.Tables;
using Cosmos.DataTransfer.AzureTableAPIExtension.Data;
using Cosmos.DataTransfer.AzureTableAPIExtension.Settings;
using Cosmos.DataTransfer.Interfaces;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace Cosmos.DataTransfer.AzureTableAPIExtension
{
    [Export(typeof(IDataSourceExtension))]
    public class AzureTableAPIDataSourceExtension : IDataSourceExtensionWithSettings
    {
        public string DisplayName => "AzureTableAPI";

        public async IAsyncEnumerable<IDataItem> ReadAsync(IConfiguration config, ILogger logger, [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            var settings = config.Get<AzureTableAPIDataSourceSettings>();
            settings.Validate();

            var serviceClient = new TableServiceClient(settings.ConnectionString);
            var tableClient = serviceClient.GetTableClient(settings.Table);

            var queryResults = !string.IsNullOrWhiteSpace(settings.QueryFilter)
                ? tableClient.QueryAsync<TableEntity>(filter: settings.QueryFilter, cancellationToken: cancellationToken)
                : tableClient.QueryAsync<TableEntity>(cancellationToken: cancellationToken);

            await foreach (var entity in queryResults.WithCancellation(cancellationToken))
            {
                yield return new AzureTableAPIDataItem(entity, settings.PartitionKeyFieldName, settings.RowKeyFieldName);
            }
        }

        public IEnumerable<IDataExtensionSettings> GetSettings()
        {
            yield return new AzureTableAPIDataSourceSettings();
        }
    }
}