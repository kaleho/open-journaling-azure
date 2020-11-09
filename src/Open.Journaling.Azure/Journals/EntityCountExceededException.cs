using System;

namespace Open.Journaling.Azure.Journals
{
    public class EntityCountExceededException
        : Exception
    {
        public const string DefaultMessage =
            "The maximum number of entries that may be sent in a TableBatchOperation is 100.";

        public EntityCountExceededException()
            : this(DefaultMessage)
        {
        }

        public EntityCountExceededException(
            string message)
            : base(message)
        {
        }

        public EntityCountExceededException(
            string message,
            Exception innerException)
            : base(message, innerException)
        {
        }
    }
}