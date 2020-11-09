using System;
using System.Collections.Generic;
using Microsoft.Azure.Cosmos.Table;

namespace Open.Journaling.Azure.Entities
{
    internal sealed class InitialUtcTicksEntity
        : ITableEntity
    {
        public const string InitialUtcTicksKey = "initialUtcTicks";

        public InitialUtcTicksEntity()
        {
        }

        public InitialUtcTicksEntity(
            string JournalId,
            long value)
        {
            PartitionKey = JournalId;

            RowKey = $"{EntityType.Internal}{Delimiters.Delimiter}{InitialUtcTicksKey}";

            Value = value;
        }

        public long Value { get; set; }

        public string ETag { get; set; }

        public string PartitionKey { get; set; }

        public string RowKey { get; set; }

        public DateTimeOffset Timestamp { get; set; }

        public void ReadEntity(
            IDictionary<string, EntityProperty> properties,
            OperationContext operationContext)
        {
            Value = properties[InitialUtcTicksKey].Int64Value.Value;
        }

        public IDictionary<string, EntityProperty> WriteEntity(
            OperationContext operationContext)
        {
            var dict =
                new Dictionary<string, EntityProperty>
                {
                    [InitialUtcTicksKey] = EntityProperty.GeneratePropertyForLong(Value)
                };

            return dict;
        }
    }
}