using Microsoft.Azure.Cosmos.Table;
using Open.Journaling.Model;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Open.Journaling.Azure.Entities
{
    internal class Entity
        : ITableEntity,
          IJournalEntry
    {
        public const string TagsKeyName = "tags";
        public const string UtcTicksKeyName = "utcTicks";
        private const string EntryIdKeyName = "entryId";
        private const string MetaKeyName = "meta";
        private const string PayloadKeyName = "payload";
        private const string SequenceKeyName = "sequence";

        public Entity()
        {
        }

        public Entity(
            string journalId,
            string entryId,
            long sequence,
            byte[] payload,
            byte[] meta = null,
            params string[] tags)
        {
            if (entryId.Any(x => x == Delimiters.Delimiter))
            {
                throw new ArgumentException($"Must not contain {Delimiters.Delimiter}.", nameof(entryId));
            }

            PartitionKey = journalId;
            JournalId = journalId;
            Meta = meta;
            EntryId = entryId;
            Payload = payload;
            RowKey = $"{EntityType.Entry}{Delimiters.Delimiter}{sequence:D19}";
            Sequence = sequence;
            Tags = tags ?? new string[0];
            UtcTicks = DateTime.UtcNow.Ticks;
        }

        public string EntryId { get; set; }

        public string ETag { get; set; }

        public string JournalId { get; set; }

        public object Meta { get; set; }

        public string PartitionKey { get; set; }

        public object Payload { get; set; }

        public string RowKey { get; set; }

        public long Sequence { get; set; }

        public string[] Tags { get; set; }

        public DateTimeOffset Timestamp { get; set; }

        public long UtcTicks { get; set; }

        public void ReadEntity(
                                                    IDictionary<string, EntityProperty> properties,
            OperationContext operationContext)
        {
            EntryId = properties[EntryIdKeyName].StringValue;

            Meta =
                properties.ContainsKey(MetaKeyName)
                    ? properties[MetaKeyName].BinaryValue
                    : null;

            Sequence = properties[SequenceKeyName].Int64Value.Value;

            Payload = properties[PayloadKeyName].BinaryValue;

            JournalId = PartitionKey;

            Tags = properties[TagsKeyName]
                .StringValue
                .Split(new[] { ';' }, StringSplitOptions.RemoveEmptyEntries);

            UtcTicks = properties[UtcTicksKeyName].Int64Value.Value;
        }

        public IDictionary<string, EntityProperty> WriteEntity(
            OperationContext operationContext)
        {
            var returnValue =
                new Dictionary<string, EntityProperty>
                {
                    [EntryIdKeyName] = EntityProperty.GeneratePropertyForString(EntryId),
                    [PayloadKeyName] = EntityProperty.GeneratePropertyForByteArray((byte[])Payload),
                    [MetaKeyName] = EntityProperty.GeneratePropertyForByteArray((byte[])Meta),
                    [SequenceKeyName] = EntityProperty.GeneratePropertyForLong(Sequence),
                    [TagsKeyName] = EntityProperty.GeneratePropertyForString(string.Join(";", Tags)),
                    [UtcTicksKeyName] = EntityProperty.GeneratePropertyForLong(UtcTicks)
                };

            return returnValue;
        }
    }
}