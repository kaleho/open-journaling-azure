using System;
using System.Linq;

namespace Open.Journaling.Azure.Entities
{
    internal class IndexedEntity
        : Entity
    {
        public IndexedEntity()
        {
        }

        public IndexedEntity(
            string journalId,
            string entryId,
            string type,
            string[] indices,
            long sequence,
            byte[] payload,
            byte[] meta = null,
            long? utcTicks = null,
            params string[] tags)
        {
            if (indices.Any(i => i.Any(x => x == Delimiters.Delimiter)))
            {
                throw new ArgumentException($"No index value may contain {Delimiters.Delimiter}.", nameof(indices));
            }

            EntryId = entryId;

            Sequence = sequence;

            PartitionKey = journalId;

            Tags = tags;

            Meta = meta;

            UtcTicks = utcTicks ?? DateTime.UtcNow.Ticks;

            RowKey =
                GetRowKey(
                    Delimiters.Delimiter,
                    type,
                    indices.ToSafeStrings());

            Payload = payload;
        }

        public static IndexedEntity AsIndexedEntity(
            Entity source,
            string type,
            params string[] indices)
        {
            var returnValue =
                new IndexedEntity(
                    source.PartitionKey,
                    source.EntryId,
                    type,
                    indices,
                    source.Sequence,
                    (byte[]) source.Payload,
                    (byte[]) source.Meta,
                    source.UtcTicks,
                    source.Tags);

            return returnValue;
        }

        public static string GetRowKey(
            char delimiter,
            string prefix,
            string[] indices)
        {
            var returnValue =
                string.Join(
                    delimiter,
                    prefix,
                    string.Join(Delimiters.Delimiter, indices));

            return returnValue;
        }
    }
}