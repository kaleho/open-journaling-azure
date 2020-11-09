using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Azure.Cosmos.Table;
using Open.Journaling.Azure.Entities;
using Open.Monikers;

namespace Open.Journaling.Azure.Extensions
{
    public static class CloudTableExtensions
    {
        public static long GetHighestSequenceNumber(
            this CloudTable cloudTable,
            RefId journalId)
        {
            var partitionKeyFilter =
                TableQuery.GenerateFilterCondition(
                    "PartitionKey",
                    QueryComparisons.Equal,
                    journalId.ToString());

            var rowKeyFilter =
                TableQuery.GenerateFilterCondition(
                    "RowKey",
                    QueryComparisons.Equal,
                    $"{EntityType.Internal}{Delimiters.Delimiter}{HighestSequenceNumberEntity.HighestSequenceNumberKey}");

            var filter =
                TableQuery.CombineFilters(
                    partitionKeyFilter,
                    TableOperators.And,
                    rowKeyFilter);

            var query = new TableQuery<HighestSequenceNumberEntity>().Where(filter);

            var result = cloudTable.ExecuteQuery(query).FirstOrDefault();

            var returnValue = result?.Value ?? 0L;

            return returnValue;
        }

        public static long GetInitialUtcTicks(
            this CloudTable cloudTable,
            RefId journalId)
        {
            var partitionKeyFilter =
                TableQuery.GenerateFilterCondition(
                    "PartitionKey",
                    QueryComparisons.Equal,
                    journalId.ToString());

            var rowKeyFilter =
                TableQuery.GenerateFilterCondition(
                    "RowKey",
                    QueryComparisons.Equal,
                    $"{EntityType.Internal}{Delimiters.Delimiter}{InitialUtcTicksEntity.InitialUtcTicksKey}");

            var filter =
                TableQuery.CombineFilters(
                    partitionKeyFilter,
                    TableOperators.And,
                    rowKeyFilter);

            var query = new TableQuery<InitialUtcTicksEntity>().Where(filter);

            var result = cloudTable.ExecuteQuery(query).FirstOrDefault();

            var returnValue = result?.Value ?? 0L;

            return returnValue;
        }

        public static void Initialize(
            this CloudTable cloudTable,
            string journalId)
        {
            var operation = new TableBatchOperation();

            var entities =
                new List<ITableEntity>
                {
                    new HighestSequenceNumberEntity(journalId, 0L),
                    new InitialUtcTicksEntity(journalId, DateTime.UtcNow.Ticks)
                };

            entities.ForEach(operation.InsertOrReplace);

            // TODO: log this result
            var result = cloudTable.ExecuteBatch(operation);
        }
    }
}