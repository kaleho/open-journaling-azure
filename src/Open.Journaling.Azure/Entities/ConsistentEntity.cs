using System;
using System.Linq;

namespace Open.Journaling.Azure.Entities
{
    internal class ConsistentEntity
        : Entity
    {
        public ConsistentEntity()
        {
        }

        public ConsistentEntity(
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
            RowKey = $"{EntityType.Consistent}{Delimiters.Delimiter}{entryId}";
            Sequence = sequence;
            Tags = tags ?? new string[] { };
            UtcTicks = DateTime.UtcNow.Ticks;
        }
    }
}