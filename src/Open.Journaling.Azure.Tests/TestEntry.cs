namespace Open.Journaling.Azure.Tests
{
    public sealed class TestEntry
    {
        public TestEntry(
            string entryId)
        {
            EntryId = entryId;
        }

        public string EntryId { get; }
    }
}