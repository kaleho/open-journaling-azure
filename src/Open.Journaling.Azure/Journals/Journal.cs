using Microsoft.Azure.Cosmos.Table;
using Microsoft.Extensions.Logging;
using Open.Journaling.Azure.Entities;
using Open.Journaling.Azure.Extensions;
using Open.Journaling.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Open.Journaling.Azure.Journals
{
    public sealed class Journal
        : IJournal,
          IJournalReader,
          IJournalWriter
    {
        private readonly CloudTable _cloudTable;
        private readonly DateTime _initializationTimeLimit;
        private readonly ILogger<Journal> _log;
        private readonly JournalSettings _settings;
        private readonly CloudStorageAccount _storageAccount;
        private JournalProps _props;

        public Journal(
            ILogger<Journal> log,
            JournalSettings settings)
        {
            _initializationTimeLimit = DateTime.UtcNow.AddMilliseconds(settings.InitializationTimeout);

            _log = log;

            _settings = settings;

            _storageAccount = CloudStorageAccount.Parse(settings.ConnectionString);

            _cloudTable = SyncRunner.Run(() => Initialize(250));

            _props =
                new JournalProps(
                    _cloudTable.GetHighestSequenceNumber(_settings.JournalId),
                    _cloudTable.GetInitialUtcTicks(_settings.JournalId));
        }

        public JournalId JournalId => _settings.JournalId;

        public IJournalProps Props => _props;

        public Task<IJournalEntry[]> Read(
            LocationKind kind,
            long from,
            CancellationToken cancellationToken,
            long? to = null)
        {
            return
                kind == LocationKind.Sequence
                    ? Read(from, cancellationToken, to)
                    : ReadByUtcTicks(from, cancellationToken, to);
        }

        public async Task<IJournalEntry> ReadByEntryId(
            string entryId,
            CancellationToken cancellationToken)
        {
            var filter =
                GetExactFilter(
                    _settings.JournalId.ToString(),
                    entryId,
                    EntityType.Consistent);

            var returnValue =
                new List<IJournalEntry>(
                    await GetResults<ConsistentEntity>(filter, cancellationToken).ConfigureAwait(false));

            return returnValue.FirstOrDefault();
        }

        public Task<IJournalEntry[]> ReadWithTags(
            LocationKind kind,
            long from,
            CancellationToken cancellationToken,
            long? to = null,
            params string[] tags)
        {
            var safeTags = tags.ToSafeStrings();

            return
                kind == LocationKind.Sequence
                    ? ReadWithTags(from, cancellationToken, to, safeTags)
                    : ReadByUtcTicksWithTags(from, cancellationToken, to, safeTags);
        }

        public void ResetProps()
        {
            var slimLock = new SlimLock();

            using (slimLock.WriteLock())
            {
                _props =
                    new JournalProps(
                        _cloudTable.GetHighestSequenceNumber(_settings.JournalId),
                        _cloudTable.GetInitialUtcTicks(_settings.JournalId));
            }
        }

        public async Task<IJournalEntry[]> Write(
            CancellationToken cancellationToken,
            params ISerializedEntry[] entries)
        {
            return await WriteWithRetry(cancellationToken, entries, 0).ConfigureAwait(false);
        }

        private ITableEntity GetCheckpointEntity<T>(
            T entry)
            where T : ISerializedEntry
        {
            var returnValue =
                new Entity(
                    _settings.JournalId.ToString(),
                    entry.EntryId,
                    _props.IncrementAndReturnHighestSequenceNumber(),
                    (byte[])entry.Payload,
                    (byte[])entry.Meta,
                    entry.Tags);

            return returnValue;
        }

        private ITableEntity GetConsistentEntity<T>(
            T entry)
            where T : ISerializedEntry
        {
            var returnValue =
                new ConsistentEntity(
                    _settings.JournalId.ToString(),
                    entry.EntryId,
                    -1,
                    (byte[])entry.Payload,
                    (byte[])entry.Meta,
                    entry.Tags);

            return returnValue;
        }

        private string GetExactFilter(
            string journalId,
            string entryId,
            string prefix = null)
        {
            var partitionKeyFilter =
                TableQuery.GenerateFilterCondition(
                    "PartitionKey",
                    QueryComparisons.Equal,
                    journalId);

            var rowKeyFilter =
                TableQuery.GenerateFilterCondition(
                    "RowKey",
                    QueryComparisons.Equal,
                    $"{(string.IsNullOrWhiteSpace(prefix) ? "" : $"{prefix}{Delimiters.Delimiter}")}{entryId}");

            var returnValue =
                TableQuery.CombineFilters(
                    partitionKeyFilter,
                    TableOperators.And,
                    rowKeyFilter);

            return returnValue;
        }

        private string GetIndexRangedFilter(
            string journalId,
            IEnumerable<string> indices,
            string from,
            string prefix = null,
            string to = null)
        {
            var partitionKeyFilter =
                TableQuery.GenerateFilterCondition(
                    "PartitionKey",
                    QueryComparisons.Equal,
                    journalId);

            var indexFilters = "";

            foreach (var index in indices)
            {
                var indexFilter =
                    TableQuery.CombineFilters(
                        TableQuery.GenerateFilterCondition(
                            "RowKey",
                            QueryComparisons.GreaterThan,
                            $"{(string.IsNullOrWhiteSpace(prefix) ? "" : $"{prefix}{Delimiters.Delimiter}")}{index}{Delimiters.Delimiter}{from}{Delimiters.Delimiter}"),
                        TableOperators.And,
                        TableQuery.GenerateFilterCondition(
                            "RowKey",
                            QueryComparisons.LessThanOrEqual,
                            $"{(string.IsNullOrWhiteSpace(prefix) ? "" : $"{prefix}{Delimiters.Delimiter}")}{index}{Delimiters.Delimiter}{to}{Delimiters.AsciiIncrementedDelimiter}"));

                if (!string.IsNullOrWhiteSpace(indexFilters))
                {
                    indexFilters =
                        TableQuery.CombineFilters(
                            indexFilters,
                            TableOperators.Or,
                            indexFilter);
                }
                else
                {
                    indexFilters = indexFilter;
                }
            }

            var returnValue =
                TableQuery.CombineFilters(
                    partitionKeyFilter,
                    TableOperators.And,
                    indexFilters);

            return returnValue;
        }

        private string GetRangedFilter(
            string journalId,
            long from,
            string prefix = null,
            long? to = null)
        {
            var partitionKeyFilter =
                TableQuery.GenerateFilterCondition(
                    "PartitionKey",
                    QueryComparisons.Equal,
                    journalId);

            var rowKeyFromFilter =
                TableQuery.GenerateFilterCondition(
                    "RowKey",
                    QueryComparisons.GreaterThan,
                    $"{(string.IsNullOrWhiteSpace(prefix) ? "" : $"{prefix}{Delimiters.Delimiter}")}{from:D19}");

            var rowKeyToFilter =
                TableQuery.GenerateFilterCondition(
                    "RowKey",
                    QueryComparisons.LessThanOrEqual,
                    $"{(string.IsNullOrWhiteSpace(prefix) ? "" : $"{prefix}{Delimiters.Delimiter}")}{to ?? _props.HighestSequenceNumber:D19}");

            var rowKeyFilter =
                TableQuery.CombineFilters(
                    rowKeyFromFilter,
                    TableOperators.And,
                    rowKeyToFilter);

            var returnValue =
                TableQuery.CombineFilters(
                    partitionKeyFilter,
                    TableOperators.And,
                    rowKeyFilter);

            return returnValue;
        }

        private string GetRangedFilter(
            string journalId,
            string from,
            string prefix = null,
            string to = null)
        {
            var partitionKeyFilter =
                TableQuery.GenerateFilterCondition(
                    "PartitionKey",
                    QueryComparisons.Equal,
                    journalId);

            var rowKeyFilter =
                TableQuery.CombineFilters(
                    TableQuery.GenerateFilterCondition(
                        "RowKey",
                        QueryComparisons.GreaterThan,
                        $"{(string.IsNullOrWhiteSpace(prefix) ? "" : $"{prefix}{Delimiters.Delimiter}")}{from}{Delimiters.Delimiter}"),
                    TableOperators.And,
                    TableQuery.GenerateFilterCondition(
                        "RowKey",
                        QueryComparisons.LessThanOrEqual,
                        $"{(string.IsNullOrWhiteSpace(prefix) ? "" : $"{prefix}{Delimiters.Delimiter}")}{to}{Delimiters.AsciiIncrementedDelimiter}"));

            var returnValue =
                TableQuery.CombineFilters(
                    partitionKeyFilter,
                    TableOperators.And,
                    rowKeyFilter);

            return returnValue;
        }

        private async Task<T[]> GetResults<T>(
            string filter,
            CancellationToken cancellationToken)
            where T : ITableEntity, IJournalEntry, new()
        {
            var returnValue = new List<T>();

            var query = new TableQuery<T>().Where(filter);

            var nextTask = _cloudTable.ExecuteQuerySegmentedAsync(query, null, cancellationToken);

            while (nextTask != null)
            {
                var tableQueryResult = await nextTask.ConfigureAwait(false);

                if (tableQueryResult.ContinuationToken != null)
                {
                    nextTask =
                        _cloudTable.ExecuteQuerySegmentedAsync(
                            query,
                            tableQueryResult.ContinuationToken,
                            cancellationToken);
                }
                else
                {
                    nextTask = null;
                }

                if (tableQueryResult.Results.Any())
                {
                    returnValue.AddRange(tableQueryResult.Results);
                }
            }

            return returnValue.ToArray();
        }

        private List<ITableEntity> GetSequencedEntities<T>(
            T entry)
            where T : ISerializedEntry
        {
            var returnValue = new List<ITableEntity>();

            var entity =
                new Entity(
                    _settings.JournalId.ToString(),
                    entry.EntryId,
                    _props.IncrementAndReturnHighestSequenceNumber(),
                    (byte[])entry.Payload,
                    (byte[])entry.Meta,
                    entry.Tags);

            returnValue.Add(entity);

            var tagIndexed =
                IndexedEntity.AsIndexedEntity(
                    entity,
                    EntityType.UtcTicks,
                    $"{entity.UtcTicks:D19}");

            returnValue.Add(tagIndexed);

            if (entry.Tags.Any())
            {
                var tagged = TaggedEntity.AsTaggedEntities(entity);

                returnValue.AddRange(tagged);

                var taggedIndexed =
                    entry.Tags
                        .Select(
                            x =>
                                IndexedEntity.AsIndexedEntity(
                                    entity,
                                    EntityType.Tag,
                                    x,
                                    $"{entity.UtcTicks:D19}"));

                returnValue.AddRange(taggedIndexed);
            }

            return returnValue;
        }

        private async Task<CloudTable> Initialize(
            int delayMilliseconds)
        {
            try
            {
                var client = _storageAccount.CreateCloudTableClient();

                var cloudTable = client.GetTableReference(_settings.TableName);

                var context = new OperationContext();

                using (var token = new CancellationTokenSource(_settings.ConnectionTimeout))
                {
                    if (await cloudTable.CreateIfNotExistsAsync(new TableRequestOptions(), context, token.Token)
                        .ConfigureAwait(false))
                    {
                        cloudTable.Initialize(_settings.JournalId.ToString());
                    }
                }

                return cloudTable;
            }
            // TODO: Revisit an error occurring here
            //catch (Exception ex)
            catch
            {
                if (DateTime.UtcNow >= _initializationTimeLimit)
                {
                    throw;
                }

                await Task.Delay(delayMilliseconds).ConfigureAwait(false);

                return await Initialize(delayMilliseconds * 2).ConfigureAwait(false);
            }
        }

        private long ProjectEntityCount<T>(
            params T[] entities)
            where T : ISerializedEntry
        {
            var sequenceEntityCount = entities.Length;

            var utcTicksEntityCount = entities.Length;

            var taggedEntityCount = entities.Length * 2;

            long returnValue =
                sequenceEntityCount +
                utcTicksEntityCount +
                taggedEntityCount;

            return returnValue;
        }

        private async Task<IJournalEntry[]> Read(
            long fromSequenceNumber,
            CancellationToken cancellationToken,
            long? toSequenceNumber = null)
        {
            var filter =
                GetRangedFilter(
                    _settings.JournalId.ToString(),
                    fromSequenceNumber,
                    EntityType.Entry,
                    toSequenceNumber);

            var returnValue =
                new List<IJournalEntry>(
                    await GetResults<Entity>(filter, cancellationToken).ConfigureAwait(false));

            return returnValue.ToArray();
        }

        private async Task<IJournalEntry[]> ReadByUtcTicks(
            long fromUtcTicks,
            CancellationToken cancellationToken,
            long? toUtcTicks = null)
        {
            var filter =
                GetRangedFilter(
                    _settings.JournalId.ToString(),
                    $"{fromUtcTicks:D19}",
                    EntityType.UtcTicks,
                    $"{toUtcTicks ?? DateTime.UtcNow.Ticks:D19}");

            var returnValue =
                new List<IJournalEntry>(
                    await GetResults<Entity>(filter, cancellationToken).ConfigureAwait(false));

            return returnValue.ToArray();
        }

        private async Task<IJournalEntry[]> ReadByUtcTicksWithTags(
            long fromUtcTicks,
            CancellationToken cancellationToken,
            long? toUtcTicks = null,
            params string[] tags)
        {
            var filter =
                GetIndexRangedFilter(
                    _settings.JournalId.ToString(),
                    tags,
                    $"{fromUtcTicks:D19}",
                    EntityType.Tag,
                    $"{toUtcTicks ?? DateTime.UtcNow.Ticks:D19}");

            var returnValue =
                new List<IJournalEntry>(
                    await GetResults<Entity>(filter, cancellationToken).ConfigureAwait(false));

            return returnValue.ToArray();
        }

        private async Task<IJournalEntry[]> ReadWithTags(
            long fromSequenceNumber,
            CancellationToken cancellationToken,
            long? toSequenceNumber = null,
            params string[] tags)
        {
            var filter =
                GetIndexRangedFilter(
                    _settings.JournalId.ToString(),
                    tags,
                    $"{fromSequenceNumber:D19}",
                    EntityType.Tag,
                    $"{toSequenceNumber ?? _props.HighestSequenceNumber:D19}");

            var returnValue =
                new List<IJournalEntry>(
                    await GetResults<Entity>(filter, cancellationToken).ConfigureAwait(false));

            return returnValue.ToArray();
        }

        private async Task<IJournalEntry[]> WriteWithRetry(
            CancellationToken cancellationToken,
            ISerializedEntry[] entries,
            int retryCount)
        {
            var projectedEntityCount = ProjectEntityCount(entries);

            if (projectedEntityCount > 100)
            {
                throw new EntityCountExceededException(
                    $"{EntityCountExceededException.DefaultMessage} " +
                    $"Entity count is projected to be {projectedEntityCount}.");
            }

            var operation = new TableBatchOperation();

            foreach (var entry in entries)
            {
                switch (entry)
                {
                    case IKnownImmutableEntry uniqueEntry:
                        operation.Insert(GetConsistentEntity(uniqueEntry));

                        operation.Insert(GetCheckpointEntity(uniqueEntry));

                        break;

                    case IKnownMutableEntry consistentEntry:
                        operation.InsertOrReplace(GetConsistentEntity(consistentEntry));

                        operation.Insert(GetCheckpointEntity(consistentEntry));

                        break;

                    default:
                        foreach (var sequenced in GetSequencedEntities(entry))
                        {
                            operation.Insert(sequenced);
                        }

                        break;
                }
            }

            operation.InsertOrReplace(
                new HighestSequenceNumberEntity(
                    _settings.JournalId.ToString(),
                    _props.HighestSequenceNumber));

            TableBatchResult results = null;

            try
            {
                results = await _cloudTable.ExecuteBatchAsync(operation, cancellationToken).ConfigureAwait(false);
            }
            catch (StorageException ex)
            {
                // TODO: log this at least as a WARN

                // Another writer might be accessing this store, reset the _props and try again.
                if (ex.RequestInformation?.ExtendedErrorInformation?.ErrorCode == "EntityAlreadyExists")
                {
                    _log.LogTrace(
                        $"There was a RowKey collision in the batch operation on the {retryCount} attempt with" +
                        $" RowKeys: {string.Join(' ', operation.Select(x => x.Entity.RowKey))}");

                    if (retryCount < _settings.WriterRetryLimit)
                    {
                        ResetProps();

                        await Task.Delay((retryCount + 1) * 100, cancellationToken);

                        return await WriteWithRetry(cancellationToken, entries, retryCount + 1).ConfigureAwait(false);
                    }
                }

                throw;
            }

            // TODO: check for errors

            var persistedEntries = new List<IJournalEntry>();

            foreach (var result in results)
            {
                switch (result.Result)
                {
                    case IndexedEntity indexed:
                        // TODO: log
                        break;

                    case TaggedEntity tagged:
                        // TODO: log
                        break;

                    case ConsistentEntity consistent:
                        // TODO: log
                        break;

                    case Entity entry:
                        persistedEntries.Add(entry);

                        break;

                    case HighestSequenceNumberEntity highestNumber:
                        // TODO: log
                        break;
                }
            }

            return persistedEntries.ToArray();
        }
    }
}