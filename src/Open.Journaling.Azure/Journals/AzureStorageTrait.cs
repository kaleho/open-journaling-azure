using System;
using Open.Journaling.Traits;

namespace Open.Journaling.Azure.Journals
{
    public class AzureStorageTrait
        : IJournalTrait
    {
        public AzureStorageTrait(
            TriState value)
        {
            Value = value;
        }

        public TriState Value { get; }
    }
}