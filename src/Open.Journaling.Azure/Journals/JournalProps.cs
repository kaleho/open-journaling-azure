using System.Threading;

namespace Open.Journaling.Azure.Journals
{
    public sealed class JournalProps
        : IJournalProps
    {
        private long _highestSequenceNumber;

        public JournalProps(
            long highestSequenceNumber,
            long initialUtcTicks)
        {
            _highestSequenceNumber = highestSequenceNumber;
            InitialUtcTicks = initialUtcTicks;
        }

        public long HighestSequenceNumber => _highestSequenceNumber;

        public long InitialUtcTicks { get; }

        public long IncrementAndReturnHighestSequenceNumber()
        {
            return Interlocked.Increment(ref _highestSequenceNumber);
        }
    }
}