using System;
using System.Collections.Generic;
using Microsoft.Azure.Cosmos.Table;

namespace Open.Journaling.Azure.Entities
{
    internal sealed class HighestSequenceNumberEntity
        : ITableEntity
    {
        public const string HighestSequenceNumberKey = "highestSequenceNumber";
        private const string MetaKeyName = "meta";

        public HighestSequenceNumberEntity()
        {
        }

        public HighestSequenceNumberEntity(
            string JournalId,
            long value,
            string meta = "")
        {
            PartitionKey = JournalId;

            RowKey = $"{EntityType.Internal}{Delimiters.Delimiter}{HighestSequenceNumberKey}";

            Value = value;

            Meta = meta;
        }

        public string Meta { get; set; }

        public long Value { get; set; }

        public string ETag { get; set; }

        public string PartitionKey { get; set; }

        public void ReadEntity(
            IDictionary<string, EntityProperty> properties,
            OperationContext operationContext)
        {
            Meta =
                properties.ContainsKey(MetaKeyName)
                    ? properties[MetaKeyName].StringValue
                    : string.Empty;

            Value = properties[HighestSequenceNumberKey].Int64Value.Value;
        }

        public string RowKey { get; set; }

        public DateTimeOffset Timestamp { get; set; }

        public IDictionary<string, EntityProperty> WriteEntity(
            OperationContext operationContext)
        {
            var dict =
                new Dictionary<string, EntityProperty>
                {
                    [HighestSequenceNumberKey] = EntityProperty.GeneratePropertyForLong(Value),
                    [MetaKeyName] = EntityProperty.GeneratePropertyForString(Meta)
                };

            return dict;
        }
    }
}