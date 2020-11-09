using System;
using System.Collections.Generic;
using System.Linq;

namespace Open.Journaling.Azure.Entities
{
    internal class TaggedEntity
        : Entity
    {
        public TaggedEntity()
        {
        }

        public TaggedEntity(
            string journalId,
            string entryId,
            string tag,
            long sequence,
            byte[] payload,
            byte[] meta = null,
            long? utcTicks = null,
            params string[] tags)
        {
            if (tag.Any(x => x == Delimiters.Delimiter))
            {
                throw new ArgumentException($"Must not contain {Delimiters.Delimiter}.", nameof(tag));
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
                    EntityType.Tag,
                    tag.ToSafeString(),
                    sequence.ToString("D19"));

            Payload = payload;
        }

        public static IEnumerable<TaggedEntity> AsTaggedEntities(
            Entity source)
        {
            var returnValue = new List<TaggedEntity>();

            foreach (var tag in source.Tags)
            {
                returnValue.Add(
                    new TaggedEntity(
                        source.PartitionKey,
                        source.EntryId,
                        tag,
                        source.Sequence,
                        (byte[])source.Payload,
                        (byte[])source.Meta,
                        source.UtcTicks,
                        source.Tags));
            }

            return returnValue;
        }

        public static string GetRowKey(
            char delimiter,
            string prefix,
            string index,
            string sequence)
        {
            var returnValue =
                string.Join(
                    delimiter,
                    prefix,
                    index,
                    sequence);

            return returnValue;
        }
    }
}